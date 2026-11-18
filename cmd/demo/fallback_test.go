package main

import (
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/httpapi"
	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

func TestCacheFallbackReadFailures(t *testing.T) {
	for _, status := range []int{0, 401, 503, 200} {
		for _, enabled := range []bool{false, true} {
			name := http.StatusText(status)
			if status == 0 {
				name = "unreachable"
			}
			if enabled {
				name += "_fallback"
			}
			t.Run(name, func(t *testing.T) {
				var gets, puts, origins atomic.Int32
				cache := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "PUT" {
						puts.Add(1)
					} else {
						gets.Add(1)
					}
					w.WriteHeader(status)
					_, _ = io.WriteString(w, `{"unexpected":true}`)
				}))
				defer cache.Close()
				if status == 0 {
					cache.Close()
				}
				origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					origins.Add(1)
					writeRaw(w, 200, []byte(`{"id":"1","name":"one","version":1}`))
				}))
				defer origin.Close()
				var logs bytes.Buffer
				a := &app{client: cache.Client(), redisURL: cache.URL, originURL: origin.URL, cacheFallback: enabled,
					token: "secret-do-not-log", logger: slog.New(slog.NewTextHandler(&logs, nil))}
				w := httptest.NewRecorder()
				a.handler().ServeHTTP(w, httptest.NewRequest("GET", "/demo/products/1", nil))
				want := 502
				if enabled {
					want = 200
				}
				wantGets := int32(1)
				if status == 0 {
					wantGets = 0
				}
				if w.Code != want || gets.Load() != wantGets || puts.Load() != 0 {
					t.Fatalf("response=%d gets=%d puts=%d", w.Code, gets.Load(), puts.Load())
				}
				if enabled {
					if w.Header().Get("X-Cache") != "BYPASS-ERROR" || origins.Load() != 1 || !strings.Contains(w.Body.String(), `"name":"one"`) {
						t.Fatalf("fallback response=%s headers=%v origin=%d", w.Body, w.Header(), origins.Load())
					}
				} else if origins.Load() != 0 {
					t.Fatal("strict mode contacted origin")
				}
				if !strings.Contains(logs.String(), "CACHE_FAILURE") || strings.Contains(logs.String(), a.token) || strings.Contains(logs.String(), "product:1") {
					t.Fatalf("unsafe/missing log: %s", &logs)
				}
				if status == 401 && !strings.Contains(logs.String(), "status=401") {
					t.Fatalf("auth error missing: %s", &logs)
				}
			})
		}
	}
}

func TestCacheFallbackCapacityExceeded(t *testing.T) {
	s := store.NewWithLimit(nil, nil, 1) // Every product exceeds capacity.
	defer s.Close()
	cache := httptest.NewServer(httpapi.NewHandler(s, nil))
	defer cache.Close()
	origin := httptest.NewServer(newOrigin(1, 0).handler())
	defer origin.Close()
	for _, enabled := range []bool{false, true} {
		var logs bytes.Buffer
		a := &app{client: cache.Client(), redisURL: cache.URL, originURL: origin.URL, ttlMillis: 1000,
			cacheFallback: enabled, logger: slog.New(slog.NewTextHandler(&logs, nil))}
		w := httptest.NewRecorder()
		a.handler().ServeHTTP(w, httptest.NewRequest("GET", "/demo/products/1", nil))
		want := 502
		if enabled {
			want = 200
		}
		if w.Code != want {
			t.Fatalf("fallback=%v status=%d body=%s", enabled, w.Code, w.Body)
		}
		if enabled && (w.Header().Get("X-Cache") != "MISS" || !strings.Contains(w.Body.String(), `"name":"product-1"`)) {
			t.Fatalf("response=%s headers=%v", w.Body, w.Header())
		}
		if !strings.Contains(logs.String(), "method=PUT status=503") {
			t.Fatalf("write failure not logged: %s", &logs)
		}
	}
}

func TestCacheFallbackStillRejectsInvalidationFailure(t *testing.T) {
	var deletes atomic.Int32
	cache := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { deletes.Add(1); w.WriteHeader(503) }))
	defer cache.Close()
	o := newOrigin(1, 0)
	origin := httptest.NewServer(o.handler())
	defer origin.Close()
	a := &app{client: cache.Client(), redisURL: cache.URL, originURL: origin.URL, cacheFallback: true}
	w := httptest.NewRecorder()
	a.handler().ServeHTTP(w, httptest.NewRequest("PUT", "/demo/products/1", strings.NewReader(`{"name":"changed","version":2}`)))
	if w.Code != 502 || deletes.Load() != 1 {
		t.Fatalf("status=%d deletes=%d", w.Code, deletes.Load())
	}
	// The origin really changed; fallback must not pretend invalidation succeeded.
	response, err := origin.Client().Get(origin.URL + "/products/1")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, _ := io.ReadAll(response.Body)
	if !strings.Contains(string(body), `"name":"changed"`) {
		t.Fatalf("origin=%s", body)
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestCacheFallbackTimeoutAndOriginBudget(t *testing.T) {
	var cacheCalls, originCalls atomic.Int32
	client := &http.Client{Timeout: time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.URL.Host == "cache.test" {
			cacheCalls.Add(1)
			if _, ok := r.Context().Deadline(); !ok {
				t.Error("missing cache deadline")
			}
			<-r.Context().Done()
			return nil, r.Context().Err()
		}
		originCalls.Add(1)
		// Origin outlives the cache budget, using the original request context.
		select {
		case <-time.After(40 * time.Millisecond):
		case <-r.Context().Done():
			return nil, r.Context().Err()
		}
		return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"id":"1"}`))}, nil
	})}
	a := &app{client: client, redisURL: "http://cache.test", originURL: "http://origin.test", cacheFallback: true, cacheTimeout: 10 * time.Millisecond}
	w := httptest.NewRecorder()
	a.handler().ServeHTTP(w, httptest.NewRequest("GET", "/demo/products/1", nil))
	if w.Code != 200 || w.Header().Get("X-Cache") != "BYPASS-ERROR" || cacheCalls.Load() != 1 || originCalls.Load() != 1 {
		t.Fatalf("status=%d header=%v cache=%d origin=%d", w.Code, w.Header(), cacheCalls.Load(), originCalls.Load())
	}
}

func TestCacheFallbackPreservesNormalPaths(t *testing.T) {
	for _, mode := range []string{"hit", "miss", "off", "origin404", "origin500"} {
		t.Run(mode, func(t *testing.T) {
			var cacheCalls atomic.Int32
			cache := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				cacheCalls.Add(1)
				if mode == "hit" {
					writeRaw(w, 200, []byte(`{"value":"{\"id\":\"1\"}"}`))
					return
				}
				if r.Method == "GET" {
					w.WriteHeader(404)
				} else {
					writeRaw(w, 200, []byte(`{"ok":true}`))
				}
			}))
			defer cache.Close()
			origin := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if mode == "origin404" {
					w.WriteHeader(404)
					return
				}
				if mode == "origin500" {
					w.WriteHeader(500)
					return
				}
				writeRaw(w, 200, []byte(`{"id":"1"}`))
			}))
			defer origin.Close()
			a := &app{client: cache.Client(), redisURL: cache.URL, originURL: origin.URL, ttlMillis: 1000, cacheFallback: true}
			path := "/demo/products/1"
			if mode == "off" {
				path += "?cache=off"
			}
			w := httptest.NewRecorder()
			a.handler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
			wantStatus, wantHeader, wantCalls := 200, "MISS", int32(2)
			switch mode {
			case "hit":
				wantHeader, wantCalls = "HIT", 1
			case "off":
				wantHeader, wantCalls = "BYPASS", 0
			case "origin404":
				wantStatus, wantHeader, wantCalls = 404, "", 1
			case "origin500":
				wantStatus, wantHeader, wantCalls = 502, "", 1
			}
			if w.Code != wantStatus || w.Header().Get("X-Cache") != wantHeader || cacheCalls.Load() != wantCalls {
				t.Fatalf("status=%d header=%v calls=%d", w.Code, w.Header(), cacheCalls.Load())
			}
		})
	}
}

// Strict mode keeps the existing client timeout even if a short cache timeout
// is configured. Check the context directly rather than sleeping for 3 seconds.
func TestCacheTimeoutOnlyInFallbackMode(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete} {
			name := "strict_" + method
			if enabled {
				name = "fallback_" + method
			}
			t.Run(name, func(t *testing.T) {
				client := &http.Client{Timeout: 3 * time.Second, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					deadline, ok := r.Context().Deadline()
					if !ok {
						t.Fatal("missing request deadline")
					}
					remaining := time.Until(deadline)
					if enabled && remaining > 200*time.Millisecond {
						t.Fatalf("fallback deadline too long: %s", remaining)
					}
					if !enabled && remaining < 2*time.Second {
						t.Fatalf("strict deadline shortened: %s", remaining)
					}
					return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{}`))}, nil
				})}
				a := &app{client: client, cacheFallback: enabled, cacheTimeout: 200 * time.Millisecond}
				status, _, err := a.call(httptest.NewRequest("GET", "/", nil), method, "http://cache.test/keys/x", nil, true)
				if err != nil || status != 200 {
					t.Fatalf("status=%d err=%v", status, err)
				}
			})
		}
	}
}
