package httpapi

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"mini-redis/internal/store"
)

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

func testHandler(t *testing.T, limit int64) (http.Handler, *store.Store, *fakeClock) {
	t.Helper()
	clock := &fakeClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	var logger *slog.Logger
	if raw := os.Getenv("MINI_REDIS_DEBUG"); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatal(err)
		}
		if enabled {
			logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
				Level: slog.LevelDebug,
				ReplaceAttr: func(_ []string, attr slog.Attr) slog.Attr {
					if attr.Key == slog.TimeKey {
						return slog.Attr{}
					}
					return attr
				},
			}))
		}
	}
	s := store.New(clock, logger)
	return NewHandlerWithMaxBodyBytes(s, logger, limit), s, clock
}

func request(h http.Handler, method, path, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

func expectJSON(t *testing.T, w *httptest.ResponseRecorder, status int, want string) {
	t.Helper()
	if w.Code != status || w.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("status=%d type=%q body=%s, want %d JSON", w.Code, w.Header().Get("Content-Type"), w.Body, status)
	}
	var gotJSON, wantJSON any
	if err := json.Unmarshal(w.Body.Bytes(), &gotJSON); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(want), &wantJSON); err != nil {
		t.Fatal(err)
	}
	got, _ := json.Marshal(gotJSON)
	expected, _ := json.Marshal(wantJSON)
	if !bytes.Equal(got, expected) {
		t.Fatalf("JSON=%s, want %s", got, expected)
	}
}

func expectError(t *testing.T, w *httptest.ResponseRecorder, status int, code string) {
	t.Helper()
	var body errorResponse
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != status || body.Error.Code != code || body.Error.Message == "" ||
		w.Header().Get("Content-Type") != "application/json; charset=utf-8" {
		t.Fatalf("status=%d body=%s, want %d/%s JSON", w.Code, w.Body, status, code)
	}
	if w.Header().Get("Location") != "" {
		t.Fatal("unexpected redirect")
	}
}

func TestHTTPKeyRoundTrip(t *testing.T) {
	h, _, _ := testHandler(t, DefaultMaxBodyBytes)
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	client := server.Client()
	keys := []string{"user:101", "user name", "민지", "a/b", "/", "a?b#c", "a+b", "%2F", ".%2F", "a.b", "a..b", " ", "A", "a"}
	do := func(method, key, body string) (int, string) {
		t.Helper()
		r, err := http.NewRequest(method, server.URL+"/keys/"+url.PathEscape(key), strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		response, err := client.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		defer response.Body.Close()
		b, err := io.ReadAll(response.Body)
		if err != nil {
			t.Fatal(err)
		}
		if response.Header.Get("Content-Type") != "application/json; charset=utf-8" {
			t.Fatal("not JSON")
		}
		return response.StatusCode, string(b)
	}
	for _, key := range keys {
		body, _ := json.Marshal(map[string]string{"value": "값:" + key})
		if status, b := do("PUT", key, string(body)); status != 200 {
			t.Fatalf("PUT %q: %d %s", key, status, b)
		}
	}
	for _, key := range keys {
		status, body := do("GET", key, "")
		var got struct {
			Key   string `json:"key"`
			Value string `json:"value"`
			TTL   int64  `json:"ttl_ms"`
		}
		if err := json.Unmarshal([]byte(body), &got); err != nil {
			t.Fatal(err)
		}
		if status != 200 || got.Key != key || got.Value != "값:"+key || got.TTL != -1 {
			t.Fatalf("round trip %q: %d %s", key, status, body)
		}
	}
	expectJSON(t, request(h, "PUT", "/keys/empty", `{"value":""}`), 200, `{"ok":true}`)
	expectJSON(t, request(h, "GET", "/keys/empty", ""), 200, `{"key":"empty","value":"","ttl_ms":-1}`)
	expectJSON(t, request(h, "DELETE", "/keys/empty", ""), 200, `{"deleted":1}`)
	expectJSON(t, request(h, "DELETE", "/keys/empty", ""), 200, `{"deleted":0}`)
	expectError(t, request(h, "GET", "/keys/empty", ""), 404, "KEY_NOT_FOUND")
	for _, path := range []string{"/keys/.", "/keys/..", "/keys/%2E", "/keys/%2e%2E"} {
		expectError(t, request(h, "PUT", path, `{"value":"bad"}`), 400, "INVALID_ARGUMENT")
	}
	// Go 1.22~1.24의 vet는 t.Log 안의 %2F를 서식 지시자로 오인해 빌드를 막으므로 Logf로 %%를 쓴다.
	t.Logf("ROUNDTRIP: 한글·공백·슬래시·%%2F·빈 값 왕복, 키별 독립 저장, 점 키는 리다이렉트 없이 400")
}

func TestHTTPValidationAndErrors(t *testing.T) {
	h, s, clock := testHandler(t, DefaultMaxBodyBytes)
	ttl := int64(100)
	if err := s.Set("keep", "original", &ttl); err != nil {
		t.Fatal(err)
	}
	clock.advance(10 * time.Millisecond)
	t.Run("invalid_json_and_ttl", func(t *testing.T) {
		for _, body := range []string{
			"", "{", "null", "[]", `"value"`, `{}`, `{"value":null}`, `{"value":3}`, `{"value":false}`,
			`{"value":"bad","extra":1}`, `{"VALUE":"bad"}`, `{"value":"bad"} {}`,
			`{"value":"bad","ttl_ms":0}`, `{"value":"bad","ttl_ms":-1}`,
			`{"value":"bad","ttl_ms":1.5}`, `{"value":"bad","ttl_ms":"1"}`,
			`{"value":"bad","ttl_ms":null}`, `{"value":"bad","ttl_ms":true}`,
			`{"value":"bad","ttl_ms":9223372036855}`, `{"value":"bad","ttl_ms":9223372036854001e-3}`,
			`{"value":"bad","ttl_ms":1e999999999999999999999}`, `{"value":"bad","ttl_ms":1e-999999}`,
		} {
			expectError(t, request(h, "PUT", "/keys/keep", body), 400, "INVALID_ARGUMENT")
			got, err := s.Get("keep")
			if err != nil || got != (store.LookupResult{State: store.Live, Value: "original", TTLMillis: 90}) {
				t.Fatalf("invalid request changed data: %+v %v", got, err)
			}
		}
	})
	t.Run("routes_methods_keys", func(t *testing.T) {
		for _, path := range []string{"/", "/unknown", "/keys/a/b", "/keys//", "/keys/x/../y"} {
			expectError(t, request(h, "GET", path, ""), 404, "ROUTE_NOT_FOUND")
		}
		for _, path := range []string{"/keys", "/keys/"} {
			for _, method := range []string{"GET", "PUT", "DELETE", "POST"} {
				expectError(t, request(h, method, path, "{}"), 400, "INVALID_ARGUMENT")
			}
		}
		for _, method := range []string{"POST", "PATCH", "HEAD", "OPTIONS", "CONNECT"} {
			w := request(h, method, "/keys/keep", "")
			expectError(t, w, 405, "METHOD_NOT_ALLOWED")
			if w.Header().Get("Allow") != "GET, PUT, DELETE" {
				t.Fatal("wrong Allow")
			}
		}
		for _, key := range []string{"a\x00b", "a\nb", "a\u0085b", string([]byte{0xff})} {
			for _, method := range []string{"PUT", "GET", "DELETE"} {
				expectError(t, request(h, method, "/keys/"+url.PathEscape(key), `{"value":"x"}`), 400, "INVALID_ARGUMENT")
			}
		}
	})
	t.Run("content_type", func(t *testing.T) {
		for _, mediaType := range []string{"text/plain", "application/xml", "invalid;", "", "application/json; charset=latin1"} {
			r := httptest.NewRequest("PUT", "/keys/keep", strings.NewReader(`{"value":"bad"}`))
			r.Header.Set("Content-Type", mediaType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			expectError(t, w, 415, "UNSUPPORTED_MEDIA_TYPE")
		}
		r := httptest.NewRequest("PUT", "/keys/keep", strings.NewReader(`{"value":"bad"}`))
		r.Header["Content-Type"] = []string{"application/json", "text/plain"}
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		expectError(t, w, 415, "UNSUPPORTED_MEDIA_TYPE")
		for _, mediaType := range []string{"application/json", "application/json; charset=utf-8"} {
			r := httptest.NewRequest("PUT", "/keys/type", strings.NewReader(`{"value":"ok"}`))
			r.Header.Set("Content-Type", mediaType)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			expectJSON(t, w, 200, `{"ok":true}`)
		}
	})
	t.Run("size_boundaries", func(t *testing.T) {
		for _, key := range []string{strings.Repeat("k", 256), strings.Repeat("가", 85) + "a"} {
			expectJSON(t, request(h, "PUT", "/keys/"+url.PathEscape(key), `{"value":"ok"}`), 200, `{"ok":true}`)
		}
		for _, key := range []string{strings.Repeat("k", 257), strings.Repeat("가", 86)} {
			for _, method := range []string{"PUT", "GET", "DELETE"} {
				expectError(t, request(h, method, "/keys/"+url.PathEscape(key), `{"value":"bad"}`), 413, "LIMIT_EXCEEDED")
			}
		}
		for _, value := range []string{strings.Repeat("x", 65536), strings.Repeat("가", 21845) + "a"} {
			body, _ := json.Marshal(map[string]string{"value": value})
			expectJSON(t, request(h, "PUT", "/keys/size", string(body)), 200, `{"ok":true}`)
		}
		for _, value := range []string{strings.Repeat("x", 65537), strings.Repeat("가", 21846)} {
			body, _ := json.Marshal(map[string]string{"value": value})
			expectError(t, request(h, "PUT", "/keys/keep", string(body)), 413, "LIMIT_EXCEEDED")
		}
		limited := NewHandlerWithMaxBodyBytes(s, nil, 64)
		base := `{"value":"ok"}`
		exact := base + strings.Repeat(" ", 64-len(base))
		expectJSON(t, request(limited, "PUT", "/keys/body", exact), 200, `{"ok":true}`)
		for _, declaredLength := range []int64{-1, 0, 65} {
			r := httptest.NewRequest("PUT", "/keys/keep", strings.NewReader(exact+" "))
			r.ContentLength = declaredLength
			w := httptest.NewRecorder()
			limited.ServeHTTP(w, r)
			expectError(t, w, 413, "LIMIT_EXCEEDED")
		}
		// 본문 크기 설정을 기본값보다 크게 해도 적용되는지 확인한다.
		larger := NewHandlerWithMaxBodyBytes(s, nil, DefaultMaxBodyBytes+64)
		largeBody := base + strings.Repeat(" ", int(DefaultMaxBodyBytes)-len(base)+1)
		expectJSON(t, request(larger, "PUT", "/keys/body", largeBody), 200, `{"ok":true}`)
		expectError(t, request(h, "PUT", "/keys/keep", largeBody), 413, "LIMIT_EXCEEDED")
	})
	got, err := s.Get("keep")
	if err != nil || got != (store.LookupResult{State: store.Live, Value: "original", TTLMillis: 90}) {
		t.Fatalf("state changed: %+v %v", got, err)
	}
	t.Log("VALIDATION: 400/413/415, JSON 404/405, Allow, 실제 본문 상한, 오류 시 값·TTL 보존")
}

func TestHTTPGetRemainingTTL(t *testing.T) {
	h, _, clock := testHandler(t, DefaultMaxBodyBytes)
	expectJSON(t, request(h, "PUT", "/keys/ttl", `{"value":"Minji","ttl_ms":2}`), 200, `{"ok":true}`)
	expectJSON(t, request(h, "GET", "/keys/ttl", ""), 200, `{"key":"ttl","value":"Minji","ttl_ms":2}`)
	clock.advance(1600 * time.Microsecond)
	expectJSON(t, request(h, "GET", "/keys/ttl", ""), 200, `{"key":"ttl","value":"Minji","ttl_ms":1}`)
	clock.advance(400 * time.Microsecond)
	expired := request(h, "GET", "/keys/ttl", "")
	missing := request(h, "GET", "/keys/ttl", "")
	expectError(t, expired, 404, "KEY_NOT_FOUND")
	if expired.Body.String() != missing.Body.String() || missing.Code != 404 {
		t.Fatal("expiry must match missing")
	}
	expectJSON(t, request(h, "PUT", "/keys/ttl", `{"value":"forever"}`), 200, `{"ok":true}`)
	expectJSON(t, request(h, "GET", "/keys/ttl", ""), 200, `{"key":"ttl","value":"forever","ttl_ms":-1}`)
	for _, tc := range []struct {
		number string
		want   int64
	}{{"1.0", 1}, {"1e1", 10}, {"0.001e3", 1}, {"100e-2", 1}, {"9223372036854", 9223372036854}, {"9223372036854.000", 9223372036854}} {
		expectJSON(t, request(h, "PUT", "/keys/number", fmt.Sprintf(`{"value":"n","ttl_ms":%s}`, tc.number)), 200, `{"ok":true}`)
		got := request(h, "GET", "/keys/number", "")
		expectJSON(t, got, 200, fmt.Sprintf(`{"key":"number","value":"n","ttl_ms":%d}`, tc.want))
	}
	expectJSON(t, request(h, "PUT", "/keys/delete", `{"value":"old","ttl_ms":1}`), 200, `{"ok":true}`)
	clock.advance(time.Millisecond)
	expectJSON(t, request(h, "DELETE", "/keys/delete", ""), 200, `{"deleted":0}`)
	t.Log("TTL: 남은 0.4ms는 1, 만료 정각은 404, 없음과 같은 JSON, 무기한은 -1")
}

type panicClock struct{}

func (panicClock) Now() time.Time { panic("test internal failure") }

func TestHTTPInternalError(t *testing.T) {
	h := NewHandler(store.New(panicClock{}, nil), nil)
	expectError(t, request(h, "GET", "/keys/error", ""), 500, "INTERNAL_ERROR")
}

func TestExperimentWithDoubleDecoding(t *testing.T) {
	if os.Getenv("MINI_REDIS_EXPERIMENT") != "double-decode" {
		t.Skip("실험 실행: MINI_REDIS_EXPERIMENT=double-decode 지정")
	}
	h, _, _ := testHandler(t, DefaultMaxBodyBytes)
	expectJSON(t, request(h, "PUT", "/keys/%252F", `{"value":"literal-percent"}`), 200, `{"ok":true}`)
	expectJSON(t, request(h, "PUT", "/keys/%2F", `{"value":"slash"}`), 200, `{"ok":true}`)
	correct := request(h, "GET", "/keys/%252F", "")
	expectJSON(t, correct, 200, `{"key":"%2F","value":"literal-percent","ttl_ms":-1}`)
	bad := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// 제품 코드는 바꾸지 않고, 라우터가 해석한 키를 다시 디코딩하는 오류를 재현한다.
		decoded, _ := url.PathUnescape(r.PathValue("key"))
		r.SetPathValue("key", decoded)
		h.(*Handler).Get(w, r)
	})
	r := httptest.NewRequest("GET", "/keys/%252F", nil)
	r.SetPathValue("key", "%2F")
	w := httptest.NewRecorder()
	bad.ServeHTTP(w, r)
	if !strings.Contains(w.Body.String(), `"value":"slash"`) {
		t.Fatal("experiment setup failed")
	}
	t.Fatalf("의도한 실패: 키를 두 번 디코딩 → %%2F 조회가 다른 키 /의 값 반환: %s", w.Body)
}
