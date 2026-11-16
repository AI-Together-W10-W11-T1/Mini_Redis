package demo_test

// These fixtures verify the current mini-Redis HTTP API as an external cache.
// They are test-only programs, not the production origin/demo implementations.

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/httpapi"
	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

type product struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Price   int    `json:"price"`
	Version int    `json:"version"`
}

type cacheClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *cacheClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *cacheClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func errorReply(w http.ResponseWriter, status int, code string) {
	jsonReply(w, status, map[string]any{"error": map[string]string{"code": code, "message": code}})
}

// originFixture is independent of mini-Redis. Its only shared state is guarded.
type originFixture struct {
	mu       sync.Mutex
	products map[string]product
	calls    int
	fail     bool
}

func (o *originFixture) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if r.Method == "GET" && r.URL.Path == "/stats" {
		jsonReply(w, 200, map[string]int{"calls": o.calls})
		return
	}
	id, ok := strings.CutPrefix(r.URL.Path, "/products/")
	if !ok || id == "" {
		errorReply(w, 404, "ROUTE_NOT_FOUND")
		return
	}
	switch r.Method {
	case "GET":
		o.calls++
		if o.fail {
			errorReply(w, 500, "ORIGIN_ERROR")
			return
		}
		p, found := o.products[id]
		if !found {
			errorReply(w, 404, "PRODUCT_NOT_FOUND")
			return
		}
		jsonReply(w, 200, p)
	case "PUT":
		var p product
		if err := json.NewDecoder(r.Body).Decode(&p); err != nil || p.Version < 1 || p.Price < 0 {
			errorReply(w, 400, "INVALID_ARGUMENT")
			return
		}
		p.ID = id
		o.products[id] = p
		jsonReply(w, 200, p)
	default:
		errorReply(w, 405, "METHOD_NOT_ALLOWED")
	}
}

type response struct {
	status int
	body   []byte
	cache  string
}

func exchange(client *http.Client, method, address string, body []byte) (response, error) {
	r, err := http.NewRequest(method, address, bytes.NewReader(body))
	if err != nil {
		return response{}, err
	}
	if body != nil {
		r.Header.Set("Content-Type", "application/json")
	}
	res, err := client.Do(r)
	if err != nil {
		return response{}, err
	}
	defer res.Body.Close()
	b, err := io.ReadAll(res.Body)
	return response{res.StatusCode, b, res.Header.Get("X-Cache")}, err
}

// consumerFixture knows only URLs and HTTP JSON; it never calls Store directly.
func consumerFixture(client *http.Client, originURL, redisURL string, ttl int64) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id, ok := strings.CutPrefix(r.URL.Path, "/products/")
		mode := r.URL.Query().Get("cache")
		if mode == "" {
			mode = "on"
		}
		if !ok || id == "" || (mode != "on" && mode != "off") {
			errorReply(w, 400, "INVALID_ARGUMENT")
			return
		}
		cacheURL := redisURL + "/keys/" + url.PathEscape("product:"+id)
		if mode == "on" {
			cached, err := exchange(client, "GET", cacheURL, nil)
			if err != nil || (cached.status != 200 && cached.status != 404) {
				errorReply(w, 502, "UPSTREAM_ERROR")
				return
			}
			if cached.status == 200 {
				var payload struct {
					Value string `json:"value"`
				}
				if err := json.Unmarshal(cached.body, &payload); err != nil || !json.Valid([]byte(payload.Value)) {
					errorReply(w, 502, "UPSTREAM_ERROR")
					return
				}
				w.Header().Set("Content-Type", "application/json")
				w.Header().Set("X-Cache", "HIT")
				_, _ = w.Write([]byte(payload.Value))
				return
			}
		}
		original, err := exchange(client, "GET", originURL+"/products/"+url.PathEscape(id), nil)
		if err != nil || (original.status != 200 && original.status != 404) {
			errorReply(w, 502, "UPSTREAM_ERROR")
			return
		}
		if original.status == 404 {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(404)
			_, _ = w.Write(original.body)
			return
		}
		if mode == "on" {
			body, _ := json.Marshal(map[string]any{"value": string(original.body), "ttl_ms": ttl})
			saved, err := exchange(client, "PUT", cacheURL, body)
			if err != nil || saved.status != 200 {
				errorReply(w, 502, "UPSTREAM_ERROR")
				return
			}
		}
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "MISS")
		_, _ = w.Write(original.body)
	})
}

type cacheScenario struct {
	client                      *http.Client
	origin                      *originFixture
	clock                       *cacheClock
	originURL, redisURL, appURL string
}

func newScenario(t *testing.T) *cacheScenario {
	t.Helper()
	transport := &http.Transport{MaxIdleConns: 60, MaxIdleConnsPerHost: 20}
	client := &http.Client{Transport: transport, Timeout: 3 * time.Second}
	t.Cleanup(transport.CloseIdleConnections)
	clock := &cacheClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	origin := &originFixture{products: map[string]product{
		"1001": {"1001", "Keyboard", 59000, 1},
		"1002": {"1002", "Mouse", 29000, 1},
	}}
	o := httptest.NewServer(origin)
	t.Cleanup(o.Close)
	cache := store.New(clock, nil)
	// Store는 만료 정리 고루틴을 갖고 있으므로 HTTP 서버와 함께 닫는다.
	t.Cleanup(func() { _ = cache.Close() })
	redis := httptest.NewServer(httpapi.NewHandler(cache, nil))
	t.Cleanup(redis.Close)
	app := httptest.NewServer(consumerFixture(client, o.URL, redis.URL, 100))
	t.Cleanup(app.Close)
	return &cacheScenario{client, origin, clock, o.URL, redis.URL, app.URL}
}

func (s *cacheScenario) call(t *testing.T, method, address string, body any, status int) response {
	t.Helper()
	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	r, err := exchange(s.client, method, address, b)
	if err != nil || r.status != status {
		t.Fatalf("%s %s: status=%d body=%s error=%v, want %d", method, address, r.status, r.body, err, status)
	}
	return r
}

func (s *cacheScenario) expectProduct(t *testing.T, id, mode, cache string, want product) {
	t.Helper()
	r := s.call(t, "GET", s.appURL+"/products/"+id+"?cache="+mode, nil, 200)
	var got product
	if err := json.Unmarshal(r.body, &got); err != nil {
		t.Fatal(err)
	}
	if got != want || r.cache != cache {
		t.Fatalf("product=%+v cache=%s, want %+v/%s", got, r.cache, want, cache)
	}
}

func (s *cacheScenario) expectCalls(t *testing.T, want int) {
	t.Helper()
	r := s.call(t, "GET", s.originURL+"/stats", nil, 200)
	var stats struct {
		Calls int `json:"calls"`
	}
	if err := json.Unmarshal(r.body, &stats); err != nil {
		t.Fatal(err)
	}
	if stats.Calls != want {
		t.Fatalf("origin calls=%d, want %d", stats.Calls, want)
	}
}

func TestCacheMissHitAndKeyIsolation(t *testing.T) {
	s := newScenario(t)
	keyboard := product{"1001", "Keyboard", 59000, 1}
	mouse := product{"1002", "Mouse", 29000, 1}
	s.expectCalls(t, 0)
	s.expectProduct(t, "1001", "on", "MISS", keyboard)
	s.expectCalls(t, 1)
	// 캐시 값도 공개 HTTP API로 확인한다. Store 직접 접근은 하지 않는다.
	r := s.call(t, "GET", s.redisURL+"/keys/product%3A1001", nil, 200)
	var cached struct {
		Value string `json:"value"`
		TTL   int64  `json:"ttl_ms"`
	}
	if err := json.Unmarshal(r.body, &cached); err != nil {
		t.Fatal(err)
	}
	var stored product
	if err := json.Unmarshal([]byte(cached.Value), &stored); err != nil {
		t.Fatal(err)
	}
	if stored != keyboard || cached.TTL != 100 {
		t.Fatalf("stored=%+v ttl=%d", stored, cached.TTL)
	}
	s.expectProduct(t, "1001", "on", "HIT", keyboard)
	s.expectCalls(t, 1)
	s.expectProduct(t, "1002", "on", "MISS", mouse)
	s.expectProduct(t, "1001", "on", "HIT", keyboard)
	s.expectProduct(t, "1002", "on", "HIT", mouse)
	s.expectCalls(t, 2)
	t.Log("CACHE: 첫 조회 MISS·원본 1회 → 재조회 HIT·원본 추가 0회; 상품별 캐시 독립")
}

func TestCacheExpiryAndInvalidation(t *testing.T) {
	s := newScenario(t)
	v1 := product{"1001", "Keyboard", 59000, 1}
	v2 := product{"1001", "Keyboard", 49000, 2}
	v3 := product{"1001", "Keyboard", 39000, 3}
	s.expectProduct(t, "1001", "on", "MISS", v1)
	s.call(t, "PUT", s.originURL+"/products/1001", v2, 200)
	s.expectProduct(t, "1001", "on", "HIT", v1)
	s.expectCalls(t, 1)
	s.clock.advance(100 * time.Millisecond)
	s.expectProduct(t, "1001", "on", "MISS", v2)
	s.expectProduct(t, "1001", "on", "HIT", v2)
	s.expectCalls(t, 2)
	s.call(t, "PUT", s.originURL+"/products/1001", v3, 200)
	deleted := s.call(t, "DELETE", s.redisURL+"/keys/product%3A1001", nil, 200)
	var result struct {
		Deleted int `json:"deleted"`
	}
	if err := json.Unmarshal(deleted.body, &result); err != nil || result.Deleted != 1 {
		t.Fatalf("delete=%s error=%v", deleted.body, err)
	}
	s.expectProduct(t, "1001", "on", "MISS", v3)
	s.expectProduct(t, "1001", "on", "HIT", v3)
	s.expectCalls(t, 3)
	t.Log("TTL: 만료 정각 재조회는 MISS·새 값; INVALIDATION: 원본 PUT → 캐시 DELETE → 새 값 재저장")
}

func TestCacheBypassPreservesExistingCache(t *testing.T) {
	s := newScenario(t)
	v1 := product{"1001", "Keyboard", 59000, 1}
	v2 := product{"1001", "Keyboard", 49000, 2}
	s.expectProduct(t, "1001", "on", "MISS", v1)
	s.clock.advance(10 * time.Millisecond)
	before := s.call(t, "GET", s.redisURL+"/keys/product%3A1001", nil, 200)
	s.call(t, "PUT", s.originURL+"/products/1001", v2, 200)
	s.expectProduct(t, "1001", "off", "MISS", v2)
	s.expectProduct(t, "1001", "off", "MISS", v2)
	s.expectCalls(t, 3)
	after := s.call(t, "GET", s.redisURL+"/keys/product%3A1001", nil, 200)
	if !bytes.Equal(before.body, after.body) {
		t.Fatalf("bypass changed cache or TTL: %s → %s", before.body, after.body)
	}
	s.expectProduct(t, "1001", "on", "HIT", v1)
	s.expectCalls(t, 3)
	t.Log("BYPASS: off는 요청마다 원본 호출; 기존 캐시 값·남은 TTL은 보존")
}

func TestCacheDoesNotStoreFailedOriginResponses(t *testing.T) {
	s := newScenario(t)
	for i := 0; i < 2; i++ {
		s.call(t, "GET", s.appURL+"/products/missing?cache=on", nil, 404)
	}
	s.call(t, "GET", s.redisURL+"/keys/product%3Amissing", nil, 404)
	s.expectCalls(t, 2)
	s.origin.mu.Lock()
	s.origin.fail = true
	s.origin.mu.Unlock()
	for i := 0; i < 2; i++ {
		r := s.call(t, "GET", s.appURL+"/products/1001?cache=on", nil, 502)
		if !bytes.Contains(r.body, []byte("UPSTREAM_ERROR")) {
			t.Fatal(string(r.body))
		}
	}
	s.call(t, "GET", s.redisURL+"/keys/product%3A1001", nil, 404)
	s.expectCalls(t, 4)
	s.origin.mu.Lock()
	s.origin.fail = false
	s.origin.mu.Unlock()
	s.expectProduct(t, "1001", "on", "MISS", product{"1001", "Keyboard", 59000, 1})
	s.expectCalls(t, 5)
	t.Log("ERRORS: 원본 404·500은 캐싱하지 않음; 원본 복구 후 정상 저장")
}
