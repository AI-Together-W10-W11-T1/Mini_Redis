package httpapi

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

func TestAuthHealthAndCapacity(t *testing.T) {
	s := store.NewWithLimit(nil, nil, 70)
	defer s.Close()
	const token = "0123456789abcdef"
	h := NewHandlerWithOptions(s, nil, Options{AuthToken: token})
	expectError(t, request(h, "GET", "/keys/a", ""), 401, "UNAUTHORIZED")
	for _, auth := range []string{"Bearer incorrect", "Basic " + token, "Bearer " + token, "bearer " + token, "bEaReR " + token} {
		r := httptest.NewRequest("PUT", "/keys/a", nil)
		r.Header.Set("Authorization", auth)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if strings.EqualFold(auth[:7], "Bearer ") && strings.HasSuffix(auth, token) {
			expectError(t, w, 400, "INVALID_ARGUMENT")
		} else {
			expectError(t, w, 401, "UNAUTHORIZED")
			if w.Header().Get("WWW-Authenticate") != "Bearer" {
				t.Fatal("missing auth challenge")
			}
		}
	}
	expectJSON(t, request(h, "GET", "/health", ""), 200, `{"status":"ok","persistence":"disabled"}`)
	expectError(t, request(h, "POST", "/health", ""), 405, "METHOD_NOT_ALLOWED")
	for _, key := range []string{"a", "b"} {
		r := httptest.NewRequest("PUT", "/keys/"+key, strings.NewReader(`{"value":"12345"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		if key == "a" {
			expectJSON(t, w, 200, `{"ok":true}`)
		} else {
			expectError(t, w, 503, "CAPACITY_EXCEEDED")
		}
	}
	s.Close()
	expectJSON(t, request(h, "GET", "/health", ""), 503, `{"status":"degraded","persistence":"error"}`)
}
