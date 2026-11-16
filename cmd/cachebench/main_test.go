package main

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/httpapi"
	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

const testToken = "bench-test-token-0123456789"

type fixture struct {
	cat    *catalog
	origin *origin
	redis  *httptest.Server
	client *client
}

func newFixture(t *testing.T, keys int) *fixture {
	t.Helper()
	cat, err := newCatalog(keys, 1024)
	if err != nil {
		t.Fatal(err)
	}
	org, err := startOrigin(cat)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(org.close)
	s := store.New(nil, nil)
	t.Cleanup(func() { _ = s.Close() })
	redis := httptest.NewServer(httpapi.NewHandlerWithOptions(s, nil, httpapi.Options{AuthToken: testToken}))
	t.Cleanup(redis.Close)
	hc, dials := newHTTPClient(8)
	c := &client{http: hc, cat: cat, originURL: org.url, redisURL: redis.URL, token: testToken, timeout: 5 * time.Second, dials: dials}
	return &fixture{cat, org, redis, c}
}

func TestCatalogSizeAndValidation(t *testing.T) {
	cat, err := newCatalog(3, 1024)
	if err != nil {
		t.Fatal(err)
	}
	for i, b := range cat.bodies {
		if len(b) < 1000 || len(b) > 1100 {
			t.Fatalf("body %d is %d bytes, want about 1 KiB", i, len(b))
		}
		if !cat.validate(i, b) {
			t.Fatalf("body %d does not validate", i)
		}
	}
	if cat.validate(0, cat.bodies[1]) {
		t.Fatal("product 1 must not validate as product 0")
	}
	tampered := bytes.Replace(cat.bodies[0], []byte(`"version":1`), []byte(`"version":2`), 1)
	if cat.validate(0, tampered) {
		t.Fatal("changed version must fail validation")
	}
}

// The benchmark origin must let concurrent requests overlap; a lock held
// across the delay (as in the demo fixture) would serialise them.
func TestOriginDoesNotSerialiseDelay(t *testing.T) {
	f := newFixture(t, 10)
	f.origin.setDelay(100 * time.Millisecond)
	start := time.Now()
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			if l := f.client.get(false, "", i, 0); l.fail != failNone {
				t.Errorf("lookup %d failed: %s", i, l.errText)
			}
		}(i)
	}
	wg.Wait()
	if elapsed := time.Since(start); elapsed > 600*time.Millisecond {
		t.Fatalf("10 concurrent 100ms requests took %v; origin is serialising", elapsed)
	}
	if got := f.origin.calls.Load(); got != 10 {
		t.Fatalf("origin calls = %d, want 10", got)
	}
}

func TestCacheAsideFlowAndValidation(t *testing.T) {
	f := newFixture(t, 2)
	if l := f.client.get(true, "t:", 0, 60000); l.kind != kindMiss || l.fail != failNone || l.originCalls != 1 || l.setFailed {
		t.Fatalf("first lookup = %+v, want clean miss", l)
	}
	if l := f.client.get(true, "t:", 0, 60000); l.kind != kindHit || l.fail != failNone || l.originCalls != 0 {
		t.Fatalf("second lookup = %+v, want hit", l)
	}
	// A wrong cached value must count as a validation failure, not a success.
	body, _ := json.Marshal(map[string]any{"value": string(f.cat.bodies[1]), "ttl_ms": 60000})
	req, _ := http.NewRequest(http.MethodPut, f.client.keyURL("t:", 0), bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testToken)
	res, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	res.Body.Close()
	if l := f.client.get(true, "t:", 0, 60000); l.fail != failValidation {
		t.Fatalf("tampered lookup = %+v, want validation failure", l)
	}
	// A wrong token is an error, never a fast success.
	bad := *f.client
	bad.token = "wrong-token-0123456789"
	if l := bad.get(true, "t:", 1, 60000); l.fail != failError || l.kind != kindUnknown {
		t.Fatalf("unauthorised lookup = %+v, want error", l)
	}
}

func TestTimeoutIsCountedSeparately(t *testing.T) {
	f := newFixture(t, 1)
	f.origin.setDelay(300 * time.Millisecond)
	c := *f.client
	c.timeout = 50 * time.Millisecond
	if l := c.get(false, "", 0, 0); l.fail != failTimeout {
		t.Fatalf("lookup = %+v, want timeout", l)
	}
}

func TestWarmRunIsAllHitsWithoutOriginCalls(t *testing.T) {
	f := newFixture(t, 20)
	spec := runSpec{scenario: "warm", cacheOn: true, concurrency: 4, keys: 20, perKey: 1, warmup: 50 * time.Millisecond,
		duration: 200 * time.Millisecond, ttlMs: 60000, prefix: "w:", seed: 1}
	raw := runWarm(context.Background(), f.client, spec)
	row := buildRow(spec, raw)
	if !row.Valid || raw.prefillFails != 0 {
		t.Fatalf("run invalid: %q, prefill failures %d", row.Invalid, raw.prefillFails)
	}
	if row.Successes == 0 || row.HitRatio != 1 || row.OriginCalls != 0 || row.Misses != 0 {
		t.Fatalf("warm row = successes %d hit %.3f origin %d misses %d", row.Successes, row.HitRatio, row.OriginCalls, row.Misses)
	}
	if row.Errors+row.Timeouts+row.Validation != 0 || raw.cleanupFails != 0 {
		t.Fatalf("unexpected failures: %+v cleanup %d", row.ErrSamples, raw.cleanupFails)
	}
	if !row.hasResources || row.NewConns != 0 {
		t.Fatalf("connections opened inside the measured interval: %d", row.NewConns)
	}
	// Cleanup removed this run's keys.
	if l := f.client.get(true, "w:", 0, 60000); l.kind != kindMiss {
		t.Fatalf("key survived cleanup: %+v", l)
	}
}

// A warm run whose prefill does not store every key must be marked invalid
// instead of quietly measuring a partly cold cache.
func TestWarmRunWithFailedPrefillIsInvalid(t *testing.T) {
	f := newFixture(t, 5)
	bad := *f.client
	bad.token = "wrong-token-0123456789"
	spec := runSpec{scenario: "warm", cacheOn: true, concurrency: 2, keys: 5, perKey: 1,
		duration: 100 * time.Millisecond, ttlMs: 60000, prefix: "bad:", seed: 1}
	row := buildRow(spec, runWarm(context.Background(), &bad, spec))
	if row.Valid || row.PrefillFails != 5 || row.Successes != 0 {
		t.Fatalf("row = valid %v prefill failures %d successes %d", row.Valid, row.PrefillFails, row.Successes)
	}
	s := summarize([]runRow{row})
	if s.Repeats != 0 || s.InvalidRuns != 1 {
		t.Fatalf("summary = repeats %d invalid %d", s.Repeats, s.InvalidRuns)
	}
}

func TestColdAndStampedeCountOriginCallsPerKey(t *testing.T) {
	f := newFixture(t, 30)

	cold := runSpec{scenario: "cold", cacheOn: true, concurrency: 4, keys: 30, perKey: 1, ttlMs: 60000, prefix: "c0:"}
	row := buildRow(cold, runList(context.Background(), f.client, cold))
	if row.Requests != 30 || row.Misses != 30 || row.Hits != 0 || row.OriginKeyMax != 1 {
		t.Fatalf("cold row must be first lookups only: %+v", row)
	}

	seq := runSpec{scenario: "stampede", cacheOn: true, concurrency: 1, keys: 30, perKey: 3, ttlMs: 60000, prefix: "c1:"}
	before := f.origin.calls.Load()
	row = buildRow(seq, runList(context.Background(), f.client, seq))
	if row.Requests != 90 || row.Misses != 30 || row.Hits != 60 || row.OriginCalls != 30 || row.OriginKeyMax != 1 || row.KeysMultiCall != 0 {
		t.Fatalf("sequential stampede row = %+v", row)
	}
	if got := f.origin.calls.Load() - before; got != row.OriginCalls {
		t.Fatalf("origin server saw %d calls, client counted %d", got, row.OriginCalls)
	}

	off := runSpec{scenario: "stampede", concurrency: 4, keys: 30, perKey: 3, ttlMs: 60000, prefix: "c2:"}
	row = buildRow(off, runList(context.Background(), f.client, off))
	if row.OriginCalls != 90 || row.OriginKeyMax != 3 || !math.IsNaN(row.HitRatio) {
		t.Fatalf("cache off row = %+v", row)
	}
}

func TestWriteRunStoresAndReadsBack(t *testing.T) {
	f := newFixture(t, 10)
	spec := runSpec{scenario: "write", cacheOn: true, concurrency: 4, keys: 10, perKey: 1, warmup: 20 * time.Millisecond,
		duration: 200 * time.Millisecond, ttlMs: 60000, prefix: "wr:", seed: 1}
	before := f.origin.calls.Load()
	row := buildRow(spec, runWrite(context.Background(), f.client, spec))
	if row.Successes == 0 || row.Errors+row.Timeouts+row.Validation != 0 {
		t.Fatalf("write row = %+v", row)
	}
	if !math.IsNaN(row.HitRatio) || !math.IsNaN(row.OriginPerReq) {
		t.Fatalf("write rows have no hit ratio or origin calls: %+v", row)
	}
	if !row.Valid {
		t.Fatalf("a correct server must pass read-back: %s %v", row.Invalid, row.ErrSamples)
	}
	if got := f.origin.calls.Load() - before; got != 0 {
		t.Fatalf("write scenario must never call the origin, saw %d calls", got)
	}
}

func TestHistogramAccuracyAndMerge(t *testing.T) {
	var a, b, whole hist
	for i := 1; i <= 100000; i++ {
		ns := int64(i) * 1000 // 1µs .. 100ms
		whole.add(ns)
		if i%2 == 0 {
			a.add(ns)
		} else {
			b.add(ns)
		}
	}
	a.merge(&b)
	for _, p := range []float64{0.5, 0.95, 0.99, 0.999} {
		want := math.Ceil(p*100000) * 1000 / 1e6
		got := a.percentileMs(p)
		if math.Abs(got-want)/want > 0.005 {
			t.Fatalf("p%v = %v ms, want %v within 0.5%%", p*100, got, want)
		}
		if got != whole.percentileMs(p) {
			t.Fatalf("merged histogram differs from the whole at p%v", p*100)
		}
	}
	if a.percentileMs(1) != 100 || a.n != 100000 || math.Abs(a.meanMs()-50.0005) > 1e-6 {
		t.Fatalf("max %v n %d mean %v must be exact", a.percentileMs(1), a.n, a.meanMs())
	}
	for _, v := range []int64{0, 1, 255, 256, 257, 1e3, 1e6, 1e9, 1e12, 1 << 50} {
		if i := histIndex(v); i < 0 || i >= histBuckets {
			t.Fatalf("index %d out of range for %d", i, v)
		}
	}
	var empty hist
	if !math.IsNaN(empty.percentileMs(0.5)) {
		t.Fatal("empty histogram must report NaN")
	}
}

// Ratios are paired within a repeat, not taken between medians measured far apart.
func TestPairedComparison(t *testing.T) {
	mk := func(cache, aof string, rep int, rps, p50 float64, calls int64) runRow {
		h := &hist{}
		h.add(int64(p50 * 1e6))
		r := runRow{Scenario: "warm", Cache: cache, AOF: aof, DelayMs: 20, Concurrency: 1, Repeat: rep, Valid: true,
			SuccessRPS: rps, P50: p50, P95: p50, P99: p50, MissP50: math.NaN(), HitP50: math.NaN(), Requests: 100, OriginCalls: calls,
			OriginPerReq: float64(calls) / 100, HitRatio: math.NaN(), OriginPerKey: math.NaN(), lat: h}
		return r
	}
	runs := []runRow{
		mk("off", "-", 1, 50, 20, 100), mk("on", "off", 1, 5000, 0.2, 0), mk("on", "on", 1, 4000, 0.25, 0),
		mk("off", "-", 2, 40, 25, 100), mk("on", "off", 2, 4000, 0.25, 0), mk("on", "on", 2, 3600, 0.26, 0),
	}
	groups := map[groupKey][]runRow{}
	for _, r := range runs {
		groups[r.key()] = append(groups[r.key()], r)
	}
	var rows []summaryRow
	for _, g := range groups {
		rows = append(rows, summarize(g))
	}
	compare(rows, runs)
	for _, s := range rows {
		switch {
		case s.Cache == "on" && s.AOF == "off":
			// Both repeats give exactly 100x; a ratio of medians (4500/45) would too,
			// but p50: 20/0.2 = 100 and 25/0.25 = 100.
			if s.PairedOff != 2 || math.Abs(s.RPSRatio-100) > 1e-9 || math.Abs(s.SpeedupP50-100) > 1e-9 || s.OriginReductionPct != 100 {
				t.Fatalf("on/off summary = %+v", s)
			}
			if s.PairedAOF != 0 || !math.IsNaN(s.AOFRPSRatio) {
				t.Fatalf("AOF off row must have no AOF pairing: %+v", s)
			}
		case s.Cache == "on" && s.AOF == "on":
			if s.PairedAOF != 2 || math.Abs(s.AOFRPSRatio-0.85) > 1e-9 || math.Abs(s.AOFRPSRatioMin-0.8) > 1e-9 || math.Abs(s.AOFRPSRatioMax-0.9) > 1e-9 {
				t.Fatalf("AOF pairing = %+v", s)
			}
		case s.Cache == "off":
			if s.RPSMedian != 45 || s.P50Med != 22.5 || !math.IsNaN(s.RPSRatio) {
				t.Fatalf("off summary = %+v", s)
			}
		}
	}
}

func TestVariantsRotateAndWriteHasNoCacheOff(t *testing.T) {
	o, err := parseOptions(nil, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	vs := variantsFor(o, "warm")
	if len(vs) != 3 || vs[0] != (variant{"off", "-"}) {
		t.Fatalf("warm variants = %v", vs)
	}
	first := map[variant]int{}
	for rep := 0; rep < 3; rep++ {
		first[rotate(vs, rep)[0]]++
	}
	if len(first) != 3 {
		t.Fatalf("each variant must run first once in three repeats: %v", first)
	}
	if w := variantsFor(o, "write"); len(w) != 2 || w[0].cache != "on" || w[1].cache != "on" {
		t.Fatalf("write variants = %v", w)
	}
	if d := delaysFor(o, "write"); len(d) != 1 || d[0] != 0 {
		t.Fatalf("write delays = %v", d)
	}
}

func TestParseOptionsQuickAndExternal(t *testing.T) {
	o, err := parseOptions([]string{"-quick", "-repeats", "4"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.duration != 5*time.Second || o.repeats != 4 || o.coldKeys != 200 || o.writeDuration != 3*time.Second {
		t.Fatalf("quick options = %+v", o)
	}
	if _, err := parseOptions([]string{"-redis-url", "http://127.0.0.1:8080"}, &bytes.Buffer{}); err == nil {
		t.Fatal("external mode with two AOF values must be rejected")
	}
	if _, err := parseOptions([]string{"-scenarios", "ttl"}, &bytes.Buffer{}); err == nil {
		t.Fatal("unknown scenario must be rejected")
	}
	if _, err := parseOptions([]string{"-stampede-requests-per-key", "1"}, &bytes.Buffer{}); err == nil {
		t.Fatal("a stampede needs at least two requests per key")
	}
}

// lossyCache answers like mini-Redis but never stores anything: PUT returns
// 200, GET always returns 404. A benchmark that only trusts status codes
// would report it as a very fast cache.
func lossyCache(t *testing.T) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.Method {
		case http.MethodPut:
			_, _ = w.Write([]byte(`{"ok":true}`))
		case http.MethodDelete:
			_, _ = w.Write([]byte(`{"deleted":0}`))
		default:
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"KEY_NOT_FOUND","message":"x"}}`))
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestWriteRunDetectsPutsThatWereNotStored(t *testing.T) {
	f := newFixture(t, 10)
	c := *f.client
	c.redisURL = lossyCache(t).URL
	spec := runSpec{scenario: "write", cacheOn: true, concurrency: 4, keys: 10, perKey: 1, warmup: 20 * time.Millisecond,
		duration: 200 * time.Millisecond, ttlMs: 60000, prefix: "lost:", seed: 1}
	before := f.origin.calls.Load()
	row := buildRow(spec, runWrite(context.Background(), &c, spec))
	if row.Successes == 0 {
		t.Fatal("the lossy server answers 200, so PUTs must have been counted")
	}
	if row.Valid || row.Validation == 0 {
		t.Fatalf("unstored PUTs passed verification: valid=%v validation_failures=%d", row.Valid, row.Validation)
	}
	if got := f.origin.calls.Load() - before; got != 0 {
		t.Fatalf("verification must not fall back to the origin, saw %d origin calls", got)
	}
	if s := summarize([]runRow{row}); s.Repeats != 0 || s.InvalidRuns != 1 {
		t.Fatalf("invalid write run reached the summary: %+v", s)
	}
}

func TestWriteRunWithNoSuccessfulPutIsInvalid(t *testing.T) {
	f := newFixture(t, 5)
	bad := *f.client
	bad.token = "wrong-token-0123456789"
	spec := runSpec{scenario: "write", cacheOn: true, concurrency: 2, keys: 5, perKey: 1,
		duration: 100 * time.Millisecond, ttlMs: 60000, prefix: "none:", seed: 1}
	row := buildRow(spec, runWrite(context.Background(), &bad, spec))
	if row.Valid || row.Successes != 0 || row.Errors == 0 {
		t.Fatalf("row = valid %v successes %d errors %d", row.Valid, row.Successes, row.Errors)
	}
}

func TestWarmRunDetectsPrefillThatWasNotStored(t *testing.T) {
	f := newFixture(t, 5)
	c := *f.client
	c.redisURL = lossyCache(t).URL
	spec := runSpec{scenario: "warm", cacheOn: true, concurrency: 2, keys: 5, perKey: 1,
		duration: 100 * time.Millisecond, ttlMs: 60000, prefix: "lostwarm:", seed: 1}
	row := buildRow(spec, runWarm(context.Background(), &c, spec))
	if row.Valid || row.PrefillFails != 5 || row.Successes != 0 {
		t.Fatalf("row = valid %v prefill failures %d successes %d", row.Valid, row.PrefillFails, row.Successes)
	}
}
