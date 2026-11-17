package main

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/httpapi"
	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

// fakeClock lets TTL expiry be tested without sleeping.
type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// stack is the three services of PRD section 2, each on its own HTTP server.
type stack struct {
	t                           *testing.T
	client                      *http.Client
	clock                       *fakeClock
	originURL, redisURL, appURL string
	redisCalls                  *counter
}

type counter struct {
	mu sync.Mutex
	n  int
}

func (c *counter) add() { c.mu.Lock(); c.n++; c.mu.Unlock() }
func (c *counter) get() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.n
}

const testTTL = 1000

func newStack(t *testing.T, token string) *stack {
	t.Helper()
	clock := &fakeClock{now: time.Unix(1_800_000_000, 0)}
	s := store.New(clock, nil)
	t.Cleanup(func() { _ = s.Close() })
	redisCalls := &counter{}
	redisHandler := httpapi.NewHandlerWithOptions(s, nil, httpapi.Options{AuthToken: token})
	redis := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		redisCalls.add()
		redisHandler.ServeHTTP(w, r)
	}))
	t.Cleanup(redis.Close)
	originServer := httptest.NewServer(newOrigin(3, 0).handler())
	t.Cleanup(originServer.Close)
	client := &http.Client{Timeout: 3 * time.Second}
	consumer := &app{client: client, originURL: originServer.URL, redisURL: redis.URL, token: token, ttlMillis: testTTL}
	appServer := httptest.NewServer(consumer.handler())
	t.Cleanup(appServer.Close)
	return &stack{t: t, client: client, clock: clock, originURL: originServer.URL, redisURL: redis.URL, appURL: appServer.URL, redisCalls: redisCalls}
}

type reply struct {
	status int
	cache  string
	body   []byte
}

func (s *stack) do(method, address, body string) reply {
	s.t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	request, err := http.NewRequest(method, address, reader)
	if err != nil {
		s.t.Fatal(err)
	}
	response, err := s.client.Do(request)
	if err != nil {
		s.t.Fatal(err)
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		s.t.Fatal(err)
	}
	return reply{response.StatusCode, response.Header.Get("X-Cache"), raw}
}

func (s *stack) getCalls() int64 {
	s.t.Helper()
	var counters struct {
		GetCalls *int64 `json:"get_calls"`
	}
	r := s.do("GET", s.originURL+"/test/counters", "")
	if r.status != 200 || json.Unmarshal(r.body, &counters) != nil || counters.GetCalls == nil {
		s.t.Fatalf("counters: %d %s", r.status, r.body)
	}
	return *counters.GetCalls
}

// expect checks status, X-Cache, the product and the origin call increment.
func (s *stack) expect(r reply, status int, cache string, want *product, callsBefore, callDelta int64) {
	s.t.Helper()
	if r.status != status || r.cache != cache {
		s.t.Fatalf("status=%d X-Cache=%q body=%s; want %d %q", r.status, r.cache, r.body, status, cache)
	}
	if want != nil {
		var got product
		if err := json.Unmarshal(r.body, &got); err != nil || got != *want {
			s.t.Fatalf("product=%s err=%v; want %+v", r.body, err, *want)
		}
	}
	if got := s.getCalls() - callsBefore; got != callDelta {
		s.t.Fatalf("origin calls increased by %d; want %d", got, callDelta)
	}
}

func (s *stack) product(id, query string) reply {
	return s.do("GET", s.appURL+"/demo/products/"+id+query, "")
}

func TestHealthOfBothServices(t *testing.T) {
	s := newStack(t, "")
	for _, base := range []string{s.originURL, s.appURL} {
		r := s.do("GET", base+"/health", "")
		if r.status != 200 || string(r.body) != `{"status":"ok"}` {
			t.Fatalf("%s/health: %d %s", base, r.status, r.body)
		}
	}
}

// F2 and F3: the first request per product is a miss, later ones are hits.
func TestF2F3MissThenHitPerProduct(t *testing.T) {
	s := newStack(t, "")
	one := product{ID: "1", Name: "product-1", Version: 1}
	two := product{ID: "2", Name: "product-2", Version: 1}
	before := s.getCalls()
	s.expect(s.product("1", ""), 200, "MISS", &one, before, 1)
	s.expect(s.product("1", "?cache=on"), 200, "HIT", &one, before, 1)
	s.expect(s.product("2", ""), 200, "MISS", &two, before, 2)
	s.expect(s.product("2", ""), 200, "HIT", &two, before, 2)
	s.expect(s.product("1", ""), 200, "HIT", &one, before, 2)
}

// F4: an origin-only change is invisible until the TTL passes.
func TestF4OriginChangeAppearsAfterTTL(t *testing.T) {
	s := newStack(t, "")
	v1 := product{ID: "1", Name: "product-1", Version: 1}
	v2 := product{ID: "1", Name: "changed", Version: 2}
	before := s.getCalls()
	s.expect(s.product("1", ""), 200, "MISS", &v1, before, 1)
	if r := s.do("PUT", s.originURL+"/products/1", `{"name":"changed","version":2}`); r.status != 200 {
		t.Fatalf("origin PUT: %d %s", r.status, r.body)
	}
	s.clock.advance((testTTL - 1) * time.Millisecond)
	s.expect(s.product("1", ""), 200, "HIT", &v1, before, 1)
	s.clock.advance(time.Millisecond)
	s.expect(s.product("1", ""), 200, "MISS", &v2, before, 2)
	s.expect(s.product("1", ""), 200, "HIT", &v2, before, 2)
}

// F5: a change through the app invalidates the key before its TTL.
func TestF5AppChangeInvalidatesBeforeTTL(t *testing.T) {
	s := newStack(t, "")
	v1 := product{ID: "1", Name: "product-1", Version: 1}
	v2 := product{ID: "1", Name: "changed", Version: 2}
	before := s.getCalls()
	s.expect(s.product("1", ""), 200, "MISS", &v1, before, 1)
	s.expect(s.do("PUT", s.appURL+"/demo/products/1", `{"name":"changed","version":2}`), 200, "", &v2, before, 1)
	s.expect(s.product("1", ""), 200, "MISS", &v2, before, 2)
	s.expect(s.product("1", ""), 200, "HIT", &v2, before, 2)

	// A new product can be created the same way; the path decides the id.
	created := product{ID: "new", Name: "fresh", Version: 7}
	s.expect(s.do("PUT", s.appURL+"/demo/products/new", `{"id":"ignored","name":"fresh","version":7}`), 200, "", &created, before, 2)
	s.expect(s.product("new", ""), 200, "MISS", &created, before, 3)
}

// F6: cache=off always asks the origin and never touches the cache.
func TestF6BypassLeavesCacheUntouched(t *testing.T) {
	v1 := product{ID: "1", Name: "product-1", Version: 1}
	t.Run("without_existing_cache", func(t *testing.T) {
		s := newStack(t, "")
		before, redisBefore := s.getCalls(), s.redisCalls.get()
		s.expect(s.product("1", "?cache=off"), 200, "BYPASS", &v1, before, 1)
		s.expect(s.product("1", "?cache=off"), 200, "BYPASS", &v1, before, 2)
		if got := s.redisCalls.get() - redisBefore; got != 0 {
			t.Fatalf("bypass made %d cache requests", got)
		}
		// Nothing was stored, so the next cached request is a miss.
		s.expect(s.product("1", ""), 200, "MISS", &v1, before, 3)
	})
	t.Run("with_existing_cache", func(t *testing.T) {
		s := newStack(t, "")
		before := s.getCalls()
		s.expect(s.product("1", ""), 200, "MISS", &v1, before, 1)
		s.clock.advance(400 * time.Millisecond)
		stored := s.do("GET", s.redisURL+"/keys/product%3A1", "")
		redisBefore := s.redisCalls.get()
		s.expect(s.product("1", "?cache=off"), 200, "BYPASS", &v1, before, 2)
		s.expect(s.product("1", "?cache=off"), 200, "BYPASS", &v1, before, 3)
		if got := s.redisCalls.get() - redisBefore; got != 0 {
			t.Fatalf("bypass made %d cache requests", got)
		}
		// Same value and the same remaining TTL: the expiry was not reset.
		if after := s.do("GET", s.redisURL+"/keys/product%3A1", ""); after.status != 200 || !bytes.Equal(after.body, stored.body) {
			t.Fatalf("cache entry changed: before=%s after=%s", stored.body, after.body)
		}
		s.expect(s.product("1", ""), 200, "HIT", &v1, before, 3)
	})
	t.Run("invalid_option", func(t *testing.T) {
		s := newStack(t, "")
		before, redisBefore := s.getCalls(), s.redisCalls.get()
		for _, query := range []string{"?cache=xyz", "?cache=", "?cache=ON", "?cache=on&cache=off"} {
			s.expect(s.product("1", query), 400, "", nil, before, 0)
		}
		if got := s.redisCalls.get() - redisBefore; got != 0 {
			t.Fatalf("invalid option made %d cache requests", got)
		}
	})
}

// F7: a missing product is 404 every time and absence is not cached.
func TestF7MissingProductIsNotCached(t *testing.T) {
	s := newStack(t, "")
	before := s.getCalls()
	s.expect(s.product("missing", ""), 404, "", nil, before, 1)
	s.expect(s.product("missing", ""), 404, "", nil, before, 2)
	if r := s.do("GET", s.redisURL+"/keys/product%3Amissing", ""); r.status != 404 {
		t.Fatalf("absence was cached: %d %s", r.status, r.body)
	}
}

// F8: origin failures are 502, are not cached, and recovery works.
func TestF8OriginFailureIsNotCached(t *testing.T) {
	s := newStack(t, "")
	v1 := product{ID: "1", Name: "product-1", Version: 1}
	fail := func(enabled string) {
		if r := s.do("PUT", s.originURL+"/test/fail", `{"enabled":`+enabled+`}`); r.status != 200 || string(r.body) != `{"ok":true}` {
			t.Fatalf("fail mode: %d %s", r.status, r.body)
		}
	}
	fail("true")
	t.Cleanup(func() { fail("false") })
	before := s.getCalls()
	s.expect(s.product("1", ""), 502, "", nil, before, 1)
	if r := s.do("GET", s.redisURL+"/keys/product%3A1", ""); r.status != 404 {
		t.Fatalf("failure was cached: %d %s", r.status, r.body)
	}
	// A failed origin change must not go on to delete the cache entry.
	if r := s.do("PUT", s.originURL+"/products/1", `{"name":1}`); r.status != 400 {
		t.Fatalf("invalid origin PUT: %d %s", r.status, r.body)
	}
	fail("false")
	s.expect(s.product("1", ""), 200, "MISS", &v1, before, 2)
	s.expect(s.product("1", ""), 200, "HIT", &v1, before, 2)
}

func TestCountersResetAndCountFailures(t *testing.T) {
	s := newStack(t, "")
	s.product("1", "?cache=off")
	s.product("missing", "?cache=off")
	if got := s.getCalls(); got != 2 {
		t.Fatalf("get_calls=%d; want 2 (successes and failures)", got)
	}
	if r := s.do("DELETE", s.originURL+"/test/counters", ""); r.status != 200 || string(r.body) != `{"ok":true}` {
		t.Fatalf("reset: %d %s", r.status, r.body)
	}
	if got := s.getCalls(); got != 0 {
		t.Fatalf("get_calls=%d after reset", got)
	}
}

// Cache failures are 502, and a rejected origin change leaves the cache alone.
func TestCacheFailuresAndRejectedChange(t *testing.T) {
	t.Run("cache_unreachable", func(t *testing.T) {
		s := newStack(t, "")
		dead := httptest.NewServer(http.NotFoundHandler())
		dead.Close()
		consumer := &app{client: s.client, originURL: s.originURL, redisURL: dead.URL, ttlMillis: testTTL}
		server := httptest.NewServer(consumer.handler())
		defer server.Close()
		if r := s.do("GET", server.URL+"/demo/products/1", ""); r.status != 502 {
			t.Fatalf("GET with dead cache: %d %s", r.status, r.body)
		}
		if r := s.do("GET", server.URL+"/demo/products/1?cache=off", ""); r.status != 200 || r.cache != "BYPASS" {
			t.Fatalf("bypass must not need the cache: %d %s", r.status, r.body)
		}
		if r := s.do("PUT", server.URL+"/demo/products/1", `{"name":"x","version":2}`); r.status != 502 {
			t.Fatalf("PUT with dead cache: %d %s", r.status, r.body)
		}
	})
	t.Run("rejected_change_keeps_cache", func(t *testing.T) {
		s := newStack(t, "")
		v1 := product{ID: "1", Name: "product-1", Version: 1}
		before := s.getCalls()
		s.expect(s.product("1", ""), 200, "MISS", &v1, before, 1)
		s.expect(s.do("PUT", s.appURL+"/demo/products/1", `{"name":"no version"}`), 400, "", nil, before, 1)
		s.expect(s.product("1", ""), 200, "HIT", &v1, before, 1)
	})
	t.Run("bearer_token", func(t *testing.T) {
		s := newStack(t, "test-only-token-0123456789")
		v1 := product{ID: "1", Name: "product-1", Version: 1}
		before := s.getCalls()
		s.expect(s.product("1", ""), 200, "MISS", &v1, before, 1)
		s.expect(s.product("1", ""), 200, "HIT", &v1, before, 1)
	})
}

func TestParseOptions(t *testing.T) {
	env := func(key string) (string, bool) {
		if key == "MINI_REDIS_AUTH_TOKEN" {
			return "secret-token-0123456789", true
		}
		return "", false
	}
	o, err := parseOptions([]string{"-redis-url", "http://127.0.0.1:9000/", "-origin-delay", "20ms"}, env)
	if err != nil || o.redisURL != "http://127.0.0.1:9000" || o.delay != 20*time.Millisecond ||
		o.cacheFallback || o.cacheTimeout != 200*time.Millisecond || o.ttlMillis != 60_000 || o.products != 100 || o.token != "secret-token-0123456789" {
		t.Fatalf("options=%+v err=%v", o, err)
	}
	enabled, err := parseOptions([]string{"-cache-fallback", "-cache-timeout", "50ms"}, env)
	if err != nil || !enabled.cacheFallback || enabled.cacheTimeout != 50*time.Millisecond {
		t.Fatalf("options=%+v err=%v", enabled, err)
	}
	for _, args := range [][]string{{"-cache-timeout", "0"}, {"-cache-timeout", "-1ms"}, {"-cache-fallback=invalid"}, {"-cache-ttl-ms", "0"}, {"-origin-delay", "-1s"}, {"extra"}, {"-unknown"}} {
		if _, err := parseOptions(args, env); err == nil {
			t.Fatalf("%v: expected an error", args)
		}
	}
}
