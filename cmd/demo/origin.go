package main

import (
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// product is the origin's record. The PRD example is
// {"id":"42","name":"sample","version":1}.
type product struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Version int64  `json:"version"`
}

// origin is the slow "source of truth" that the consuming app caches.
// The artificial delay is slept outside every lock so concurrent requests
// wait in parallel, like independent database queries would.
type origin struct {
	mu       sync.RWMutex
	products map[string]product
	delay    atomic.Int64 // nanoseconds
	getCalls atomic.Int64
	failing  atomic.Bool
}

func newOrigin(seed int, delay time.Duration) *origin {
	o := &origin{products: make(map[string]product, seed)}
	for i := 1; i <= seed; i++ {
		id := strconv.Itoa(i)
		o.products[id] = product{ID: id, Name: "product-" + id, Version: 1}
	}
	o.delay.Store(int64(delay))
	return o
}

func (o *origin) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /products/{id}", o.getProduct)
	mux.HandleFunc("PUT /products/{id}", o.putProduct)
	mux.HandleFunc("GET /test/counters", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]int64{"get_calls": o.getCalls.Load()})
	})
	mux.HandleFunc("DELETE /test/counters", func(w http.ResponseWriter, _ *http.Request) {
		o.getCalls.Store(0)
		writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
	})
	mux.HandleFunc("PUT /test/fail", o.setFailure)
	return mux
}

// getProduct counts every product GET, successful or not.
func (o *origin) getProduct(w http.ResponseWriter, r *http.Request) {
	o.getCalls.Add(1)
	if d := time.Duration(o.delay.Load()); d > 0 {
		select {
		case <-time.After(d):
		case <-r.Context().Done():
			return
		}
	}
	if o.failing.Load() {
		writeError(w, http.StatusInternalServerError, "ORIGIN_ERROR", "실패 모드가 켜져 있습니다.")
		return
	}
	o.mu.RLock()
	p, found := o.products[r.PathValue("id")]
	o.mu.RUnlock()
	if !found {
		writeError(w, http.StatusNotFound, "PRODUCT_NOT_FOUND", "없는 상품입니다.")
		return
	}
	writeJSON(w, http.StatusOK, p)
}

func (o *origin) putProduct(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name    *string `json:"name"`
		Version *int64  `json:"version"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if body.Name == nil || body.Version == nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name과 version이 필요합니다.")
		return
	}
	// The path decides the identifier; an id in the body is ignored.
	p := product{ID: r.PathValue("id"), Name: *body.Name, Version: *body.Version}
	o.mu.Lock()
	o.products[p.ID] = p
	o.mu.Unlock()
	writeJSON(w, http.StatusOK, p)
}

func (o *origin) setFailure(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Enabled *bool `json:"enabled"`
	}
	if !decodeBody(w, r, &body) {
		return
	}
	if body.Enabled == nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "enabled가 필요합니다.")
		return
	}
	o.failing.Store(*body.Enabled)
	writeJSON(w, http.StatusOK, map[string]bool{"ok": true})
}

const maxBodyBytes = 64 << 10

func decodeBody(w http.ResponseWriter, r *http.Request, target any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil || json.Unmarshal(raw, target) != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "JSON 본문이 올바르지 않습니다.")
		return false
	}
	return true
}

func health(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		status, body = http.StatusInternalServerError, []byte(`{"error":{"code":"INTERNAL_ERROR","message":"encoding failed"}}`)
	}
	writeRaw(w, status, body)
}

func writeRaw(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	var response struct {
		Error struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	response.Error.Code, response.Error.Message = code, message
	writeJSON(w, status, response)
}
