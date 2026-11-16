package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type kind uint8

const (
	kindHit     kind = iota // cache ON, served from mini-Redis
	kindMiss                // cache ON, origin fetched and stored
	kindOrigin              // cache OFF, origin only
	kindUnknown             // cache ON, the cache read itself failed
	kindWrite               // write scenario, one PUT
)

type failure uint8

const (
	failNone failure = iota
	failError
	failTimeout
	failValidation
)

// client performs one logical product lookup the way an application would.
type client struct {
	http      *http.Client
	cat       *catalog
	originURL string
	redisURL  string
	token     string
	timeout   time.Duration
	dials     *atomic.Int64
}

// newHTTPClient returns a keep-alive client and a counter of DialContext
// calls, i.e. attempts to open a TCP connection. The counter lets a run prove that no connection was created
// inside its measurement window.
func newHTTPClient(concurrency int) (*http.Client, *atomic.Int64) {
	// Twice the worker count: a finished response returns its connection to
	// the pool asynchronously, so a worker that immediately sends its next
	// request needs a spare idle connection or the transport dials a new one.
	idle := 2*concurrency + 4
	dials := &atomic.Int64{}
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}
	tr := &http.Transport{
		Proxy: nil, // never route loopback measurements through a proxy
		DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			return dialer.DialContext(ctx, network, addr)
		},
		MaxIdleConns:        idle * 2,
		MaxIdleConnsPerHost: idle,
		IdleConnTimeout:     90 * time.Second,
		DisableCompression:  true,
	}
	return &http.Client{Transport: tr}, dials
}

type lookup struct {
	kind        kind
	fail        failure
	setFailed   bool // origin value was served but storing it in the cache failed
	originCalls int
	errText     string
}

func (c *client) do(ctx context.Context, method, rawURL string, body []byte, auth bool) (int, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequestWithContext(ctx, method, rawURL, rd)
	if err != nil {
		return 0, nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if auth && c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	res, err := c.http.Do(req)
	if err != nil {
		return 0, nil, err
	}
	// Read to EOF and close so the keep-alive connection returns to the pool.
	b, err := io.ReadAll(res.Body)
	res.Body.Close()
	return res.StatusCode, b, err
}

func (c *client) keyURL(prefix string, i int) string {
	return c.redisURL + "/keys/" + url.PathEscape(prefix+productID(i))
}

func classify(err error) failure {
	var ne net.Error
	if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return failTimeout
	}
	return failError
}

func (c *client) fromOrigin(ctx context.Context, i int) ([]byte, lookup) {
	status, body, err := c.do(ctx, http.MethodGet, c.originURL+"/products/"+productID(i), nil, false)
	l := lookup{originCalls: 1}
	switch {
	case err != nil:
		l.fail, l.errText = classify(err), "origin: "+err.Error()
	case status != http.StatusOK:
		l.fail, l.errText = failError, fmt.Sprintf("origin: HTTP %d", status)
	case !c.cat.validate(i, body):
		l.fail, l.errText = failValidation, "origin: body mismatch"
	}
	return body, l
}

// get runs one lookup. Timing covers the whole cache-aside flow, including the
// cache read, JSON decoding, validation and the cache write on a miss.
func (c *client) get(cacheOn bool, prefix string, i int, ttlMs int64) lookup {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	if !cacheOn {
		_, l := c.fromOrigin(ctx, i)
		l.kind = kindOrigin
		return l
	}
	status, body, err := c.do(ctx, http.MethodGet, c.keyURL(prefix, i), nil, true)
	if err != nil {
		return lookup{kind: kindUnknown, fail: classify(err), errText: "cache get: " + err.Error()}
	}
	switch status {
	case http.StatusOK:
		var wrapped struct {
			Value string `json:"value"`
		}
		if json.Unmarshal(body, &wrapped) != nil || !c.cat.validate(i, []byte(wrapped.Value)) {
			return lookup{kind: kindHit, fail: failValidation, errText: "cache: value mismatch"}
		}
		return lookup{kind: kindHit}
	case http.StatusNotFound:
		originBody, l := c.fromOrigin(ctx, i)
		l.kind = kindMiss
		if l.fail != failNone {
			return l
		}
		put, _ := json.Marshal(struct {
			Value string `json:"value"`
			TTL   int64  `json:"ttl_ms"`
		}{string(originBody), ttlMs})
		ps, _, perr := c.do(ctx, http.MethodPut, c.keyURL(prefix, i), put, true)
		if perr != nil && classify(perr) == failTimeout {
			// The caller waited past the deadline; the user-visible request timed out.
			l.fail, l.errText = failTimeout, "cache put: "+perr.Error()
		} else if perr != nil || ps != http.StatusOK {
			l.setFailed = true
		}
		return l
	default:
		return lookup{kind: kindUnknown, fail: failError, errText: fmt.Sprintf("cache get: HTTP %d", status)}
	}
}

// readCached reads one key straight from mini-Redis and validates it. Unlike
// get it never falls back to the origin and never writes, so it can prove
// that a value really is stored. It returns "" on success.
func (c *client) readCached(prefix string, i int) string {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	status, body, err := c.do(ctx, http.MethodGet, c.keyURL(prefix, i), nil, true)
	if err != nil {
		return "cache get: " + err.Error()
	}
	if status != http.StatusOK {
		return fmt.Sprintf("cache get: HTTP %d for a key that was stored", status)
	}
	var wrapped struct {
		Value string `json:"value"`
	}
	if json.Unmarshal(body, &wrapped) != nil || !c.cat.validate(i, []byte(wrapped.Value)) {
		return "cache: stored value mismatch"
	}
	return ""
}

// verifyStored reads back every listed key without touching the origin and
// returns how many are missing or wrong, with a few sample messages.
func (c *client) verifyStored(prefix string, keys []int) (bad int64, samples []string) {
	for _, i := range keys {
		if msg := c.readCached(prefix, i); msg != "" {
			bad++
			if len(samples) < 3 {
				samples = append(samples, fmt.Sprintf("read-back %s: %s", productID(i), msg))
			}
		}
	}
	return bad, samples
}

// put stores one product (write scenario). It returns once mini-Redis has
// answered, which is after the AOF append but not after the periodic fsync.
func (c *client) put(prefix string, i int, ttlMs int64) lookup {
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	body, _ := json.Marshal(struct {
		Value string `json:"value"`
		TTL   int64  `json:"ttl_ms"`
	}{string(c.cat.bodies[i]), ttlMs})
	status, _, err := c.do(ctx, http.MethodPut, c.keyURL(prefix, i), body, true)
	switch {
	case err != nil:
		return lookup{kind: kindWrite, fail: classify(err), errText: "cache put: " + err.Error()}
	case status != http.StatusOK:
		return lookup{kind: kindWrite, fail: failError, errText: fmt.Sprintf("cache put: HTTP %d", status)}
	}
	return lookup{kind: kindWrite}
}

// recorder holds one worker's measured samples in fixed-size histograms, so
// recording never allocates during a run. Recorders are merged afterwards.
type recorder struct {
	all, hit, miss hist // successful latencies
	hits, misses   int64
	errors         int64
	timeouts       int64
	validation     int64
	setFailures    int64
	originCalls    int64
	errSamples     []string
	lastEnd        time.Time
}

func (r *recorder) add(l lookup, lat time.Duration, end time.Time, perKey []atomic.Int64, i int) {
	if l.originCalls > 0 {
		r.originCalls += int64(l.originCalls)
		if perKey != nil {
			perKey[i].Add(int64(l.originCalls))
		}
	}
	if end.After(r.lastEnd) {
		r.lastEnd = end
	}
	switch l.kind {
	case kindHit:
		r.hits++
	case kindMiss:
		r.misses++
	}
	if l.setFailed {
		r.setFailures++
	}
	switch l.fail {
	case failNone:
		ns := int64(lat)
		r.all.add(ns)
		switch l.kind {
		case kindHit:
			r.hit.add(ns)
		case kindMiss:
			r.miss.add(ns)
		}
		return
	case failTimeout:
		r.timeouts++
	case failValidation:
		r.validation++
	default:
		r.errors++
	}
	if len(r.errSamples) < 3 {
		r.errSamples = append(r.errSamples, l.errText)
	}
}

func merge(rs []*recorder) *recorder {
	m := &recorder{}
	for _, r := range rs {
		m.all.merge(&r.all)
		m.hit.merge(&r.hit)
		m.miss.merge(&r.miss)
		m.hits += r.hits
		m.misses += r.misses
		m.errors += r.errors
		m.timeouts += r.timeouts
		m.validation += r.validation
		m.setFailures += r.setFailures
		m.originCalls += r.originCalls
		if r.lastEnd.After(m.lastEnd) {
			m.lastEnd = r.lastEnd
		}
		for _, e := range r.errSamples {
			if len(m.errSamples) < 5 {
				m.errSamples = append(m.errSamples, e)
			}
		}
	}
	return m
}

type runSpec struct {
	scenario    string // warm, cold, stampede or write
	cacheOn     bool
	concurrency int
	keys        int // key space (warm, write) or distinct keys (cold, stampede)
	perKey      int // cold = 1, stampede >= 2
	warmup      time.Duration
	duration    time.Duration
	ttlMs       int64
	prefix      string
	seed        int64
	aofPath     string // spawned server's AOF file, when AOF is on
}

// window captures what the measuring process itself consumed, and how many
// connections it opened, between the start and end of the measured interval.
type window struct {
	c       *client
	cpu     time.Duration
	gcCount uint32
	gcPause time.Duration
	dials   int64
}

func (c *client) openWindow() *window {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	w := &window{c: c, cpu: selfCPU(), gcCount: m.NumGC, gcPause: time.Duration(m.PauseTotalNs)}
	if c.dials != nil {
		w.dials = c.dials.Load()
	}
	return w
}

func (w *window) close() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	w.cpu = selfCPU() - w.cpu
	w.gcCount = m.NumGC - w.gcCount
	w.gcPause = time.Duration(m.PauseTotalNs) - w.gcPause
	if w.c.dials != nil {
		w.dials = w.c.dials.Load() - w.dials
	}
}

type rawRun struct {
	rec          *recorder
	elapsed      time.Duration
	requests     int64
	perKey       []int64 // origin calls per key during measurement
	keysTouched  int
	prefillFails int64
	cleanupFails int
	invalid      string // non-empty: the run must not be used
	win          *window
	aof          *aofWatch
}

// warmConnections opens 2n keep-alive connections to each server before the
// measured interval (n workers plus n spares, see newHTTPClient). It sends
// the requests together and holds every response body unread until all have
// answered: a connection cannot return to the pool while its body is open, so
// the requests are forced onto distinct connections. Reading the bodies then
// parks all of them as idle. The run still records how many connections were
// opened inside the measured interval, which is the actual proof.
func (c *client) warmConnections(n int) {
	hosts := []string{c.originURL}
	if c.redisURL != "" {
		hosts = append(hosts, c.redisURL)
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	for _, host := range hosts {
		held := make(chan *http.Response, 2*n)
		var wg sync.WaitGroup
		for w := 0; w < 2*n; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, host+"/health", nil)
				if err != nil {
					return
				}
				if res, err := c.http.Do(req); err == nil {
					held <- res
				}
			}()
		}
		wg.Wait()
		close(held)
		for res := range held {
			_, _ = io.Copy(io.Discard, res.Body)
			res.Body.Close()
		}
	}
}

func execute(ctx context.Context, c *client, spec runSpec) rawRun {
	switch spec.scenario {
	case "warm":
		return runWarm(ctx, c, spec)
	case "write":
		return runWrite(ctx, c, spec)
	default:
		return runList(ctx, c, spec)
	}
}

// timedLoop is a time-boxed closed loop. Requests that start during warm-up
// are executed but not recorded; requests that start inside the measured
// interval are.
func timedLoop(ctx context.Context, c *client, spec runSpec, out *rawRun, perKey []atomic.Int64, op func(i int) lookup) {
	c.warmConnections(spec.concurrency)
	start := time.Now()
	measureStart := start.Add(spec.warmup)
	end := measureStart.Add(spec.duration)
	if spec.aofPath != "" {
		out.aof = startAOFWatch(spec.aofPath, measureStart)
	}
	opened := make(chan *window, 1)
	go func() {
		time.Sleep(time.Until(measureStart))
		opened <- c.openWindow()
	}()
	recs := make([]*recorder, spec.concurrency)
	var wg sync.WaitGroup
	for w := 0; w < spec.concurrency; w++ {
		recs[w] = &recorder{}
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(spec.seed + int64(w)*7919))
			rec := recs[w]
			for ctx.Err() == nil {
				t0 := time.Now()
				if !t0.Before(end) {
					return
				}
				i := rng.Intn(spec.keys)
				l := op(i)
				t1 := time.Now()
				if !t0.Before(measureStart) {
					rec.add(l, t1.Sub(t0), t1, perKey, i)
				}
			}
		}(w)
	}
	wg.Wait()
	out.win = <-opened
	out.win.close()
	if out.aof != nil {
		out.aof.finish()
	}
	out.rec = merge(recs)
	out.elapsed = out.rec.lastEnd.Sub(measureStart)
	if out.elapsed <= 0 {
		out.elapsed = spec.duration
	}
}

func (o *rawRun) finish(perKey []atomic.Int64) {
	r := o.rec
	o.requests = int64(r.all.n) + r.errors + r.timeouts + r.validation
	if perKey != nil {
		o.perKey = snapshot(perKey)
	}
}

// runWarm: prefill every key, then a time-boxed closed loop of lookups.
func runWarm(ctx context.Context, c *client, spec runSpec) rawRun {
	out := rawRun{keysTouched: spec.keys, rec: &recorder{}}
	if spec.cacheOn {
		// Every prefill lookup must be a clean miss that stored its value;
		// otherwise the "warm" run would silently measure a partly cold cache.
		for i := 0; i < spec.keys; i++ {
			if l := c.get(true, spec.prefix, i, spec.ttlMs); l.kind != kindMiss || l.fail != failNone || l.setFailed {
				out.prefillFails++
				if len(out.rec.errSamples) < 3 {
					out.rec.errSamples = append(out.rec.errSamples, "prefill: "+l.errText)
				}
			}
		}
		if out.prefillFails == 0 {
			// A 200 on the prefill PUT is not proof of storage: read every key back.
			all := make([]int, spec.keys)
			for i := range all {
				all[i] = i
			}
			bad, samples := c.verifyStored(spec.prefix, all)
			out.prefillFails = bad
			out.rec.errSamples = append(out.rec.errSamples, samples...)
		}
		if out.prefillFails > 0 {
			out.invalid = fmt.Sprintf("prefill failed for %d of %d keys", out.prefillFails, spec.keys)
			out.cleanupFails = cleanup(c, spec.prefix, spec.keys)
			return out
		}
	}
	perKey := make([]atomic.Int64, spec.keys)
	timedLoop(ctx, c, spec, &out, perKey, func(i int) lookup { return c.get(spec.cacheOn, spec.prefix, i, spec.ttlMs) })
	out.finish(perKey)
	if spec.cacheOn {
		out.cleanupFails = cleanup(c, spec.prefix, spec.keys)
	}
	return out
}

// runWrite: a time-boxed closed loop of PUTs over a fixed key ring. It keeps
// mini-Redis writing for the whole interval, so the interval spans several
// seconds of AOF activity and, with enough volume, AOF rewrites.
//
// Afterwards every key that received at least one successful PUT (warm-up
// included) is read back with a plain GET: no origin fallback, no re-store.
// A missing or wrong value, or no successful PUT at all, invalidates the run,
// because a server that answers 200 without storing would otherwise look fast.
func runWrite(ctx context.Context, c *client, spec runSpec) rawRun {
	out := rawRun{keysTouched: spec.keys}
	stored := make([]atomic.Bool, spec.keys)
	timedLoop(ctx, c, spec, &out, nil, func(i int) lookup {
		l := c.put(spec.prefix, i, spec.ttlMs)
		if l.fail == failNone {
			stored[i].Store(true)
		}
		return l
	})
	if ctx.Err() == nil {
		var keys []int
		for i := range stored {
			if stored[i].Load() {
				keys = append(keys, i)
			}
		}
		bad, samples := c.verifyStored(spec.prefix, keys)
		out.rec.validation += bad
		out.rec.errSamples = append(out.rec.errSamples, samples...)
		switch {
		case len(keys) == 0:
			out.invalid = "no PUT succeeded, nothing could be verified"
		case bad > 0:
			out.invalid = fmt.Sprintf("read-back failed for %d of %d written keys", bad, len(keys))
		}
	}
	out.finish(nil)
	out.cleanupFails = cleanup(c, spec.prefix, spec.keys)
	return out
}

// runList: a fixed request list on an empty key prefix (cold and stampede).
// Each key appears perKey times back to back. With perKey = 1 every request
// is a first lookup. With perKey >= 2 and concurrency > 1 the copies of one
// key run at the same time and all miss (cache stampede). The run ends when
// the list is exhausted, never by time, so the cache cannot quietly turn warm.
func runList(ctx context.Context, c *client, spec runSpec) rawRun {
	out := rawRun{keysTouched: spec.keys}
	c.warmConnections(spec.concurrency)
	list := make([]int, 0, spec.keys*spec.perKey)
	for i := 0; i < spec.keys; i++ {
		for r := 0; r < spec.perKey; r++ {
			list = append(list, i)
		}
	}
	perKey := make([]atomic.Int64, spec.keys)
	var next atomic.Int64
	recs := make([]*recorder, spec.concurrency)
	for w := range recs {
		recs[w] = &recorder{}
	}
	start := time.Now()
	if spec.aofPath != "" {
		out.aof = startAOFWatch(spec.aofPath, start)
	}
	out.win = c.openWindow()
	var wg sync.WaitGroup
	for w := 0; w < spec.concurrency; w++ {
		wg.Add(1)
		go func(rec *recorder) {
			defer wg.Done()
			for ctx.Err() == nil {
				n := int(next.Add(1)) - 1
				if n >= len(list) {
					return
				}
				i := list[n]
				t0 := time.Now()
				l := c.get(spec.cacheOn, spec.prefix, i, spec.ttlMs)
				t1 := time.Now()
				rec.add(l, t1.Sub(t0), t1, perKey, i)
			}
		}(recs[w])
	}
	wg.Wait()
	out.win.close()
	if out.aof != nil {
		out.aof.finish()
	}
	out.rec = merge(recs)
	out.elapsed = out.rec.lastEnd.Sub(start)
	out.finish(perKey)
	if spec.cacheOn {
		out.cleanupFails = cleanup(c, spec.prefix, spec.keys)
	}
	return out
}

func snapshot(a []atomic.Int64) []int64 {
	s := make([]int64, len(a))
	for i := range a {
		s[i] = a[i].Load()
	}
	return s
}

// cleanup deletes this run's keys only. Not measured.
func cleanup(c *client, prefix string, n int) int {
	var fails atomic.Int64
	var next atomic.Int64
	var wg sync.WaitGroup
	for w := 0; w < 8; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= n {
					return
				}
				ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
				status, _, err := c.do(ctx, http.MethodDelete, c.keyURL(prefix, i), nil, true)
				cancel()
				if err != nil || status != http.StatusOK {
					fails.Add(1)
				}
			}
		}()
	}
	wg.Wait()
	return int(fails.Load())
}
