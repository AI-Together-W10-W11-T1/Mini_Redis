package main

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

// product is the payload the origin serves and the client validates.
type product struct {
	ID          string `json:"id"`
	Name        string `json:"name"`
	Price       int    `json:"price"`
	Version     int    `json:"version"`
	Description string `json:"description"`
}

// catalog holds immutable, pre-encoded origin responses. Nothing in it is
// written after construction, so the origin needs no lock on the hot path.
type catalog struct {
	bodies   [][]byte
	expected []product
}

func newCatalog(n, targetBytes int) (*catalog, error) {
	c := &catalog{bodies: make([][]byte, n), expected: make([]product, n)}
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789 \"" // a quote forces JSON escaping in the cached value
	for i := 0; i < n; i++ {
		p := product{ID: productID(i), Name: fmt.Sprintf("상품 %d", i), Price: (i*7919 + 1000) % 100000, Version: 1}
		base, err := json.Marshal(p)
		if err != nil {
			return nil, err
		}
		var body []byte
		// Shrink the description until the encoded body fits the target size.
		for size := targetBytes - len(base); ; size-- {
			var b strings.Builder
			for j := 0; j < size; j++ {
				b.WriteByte(alphabet[(i*31+j*7)%len(alphabet)])
			}
			p.Description = b.String()
			if body, err = json.Marshal(p); err != nil {
				return nil, err
			}
			if len(body) <= targetBytes || size <= 0 {
				break
			}
		}
		c.bodies[i], c.expected[i] = body, p
	}
	return c, nil
}

func productID(i int) string { return fmt.Sprintf("p-%06d", i) }

// validate decodes a product body and checks every field against the catalog.
// Both cache modes pay this same cost.
func (c *catalog) validate(i int, body []byte) bool {
	var got product
	if err := json.Unmarshal(body, &got); err != nil {
		return false
	}
	return got == c.expected[i]
}

// origin is the benchmark's own origin API. Unlike the demo fixture it never
// holds a lock while sleeping: each request reads its immutable body, counts
// the call atomically and then waits, so N concurrent requests overlap.
type origin struct {
	cat     *catalog
	delayNs atomic.Int64
	calls   atomic.Int64
	server  *http.Server
	url     string
}

func startOrigin(cat *catalog) (*origin, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	o := &origin{cat: cat, url: "http://" + ln.Addr().String()}
	o.server = &http.Server{Handler: o, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 2 * time.Minute}
	go func() { _ = o.server.Serve(ln) }()
	return o, nil
}

func (o *origin) setDelay(d time.Duration) { o.delayNs.Store(int64(d)) }

func (o *origin) close() { _ = o.server.Close() }

func (o *origin) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/health" {
		w.WriteHeader(http.StatusOK)
		return
	}
	raw, ok := strings.CutPrefix(r.URL.Path, "/products/p-")
	if !ok || r.Method != http.MethodGet {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	i, err := strconv.Atoi(raw)
	if err != nil || i < 0 || i >= len(o.cat.bodies) {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	o.calls.Add(1)
	if d := time.Duration(o.delayNs.Load()); d > 0 {
		time.Sleep(d)
	}
	body := o.cat.bodies[i]
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(body)))
	_, _ = w.Write(body)
}
