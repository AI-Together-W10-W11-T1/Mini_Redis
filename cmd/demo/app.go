package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strings"
)

// app is the consuming application: it answers product requests and uses
// mini-Redis purely over HTTP as a cache in front of the origin (Cache-Aside).
type app struct {
	client    *http.Client
	originURL string
	redisURL  string
	token     string
	ttlMillis int64
}

// A product id becomes the cache key "product:{id}", so it must fit the
// 256-byte key limit together with that prefix.
const maxProductIDBytes = 200

var errUpstream = errors.New("upstream failure")

func (a *app) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /demo/products/{id}", a.getProduct)
	mux.HandleFunc("PUT /demo/products/{id}", a.putProduct)
	return mux
}

func cacheKeyURL(redisURL, id string) string {
	return redisURL + "/keys/" + url.PathEscape("product:"+id)
}

func originProductURL(originURL, id string) string {
	return originURL + "/products/" + url.PathEscape(id)
}

func validID(w http.ResponseWriter, id string) bool {
	if id == "" || len(id) > maxProductIDBytes {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "상품 id가 올바르지 않습니다.")
		return false
	}
	return true
}

func (a *app) getProduct(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	mode := "on"
	if values, present := r.URL.Query()["cache"]; present {
		mode = strings.Join(values, ",")
	}
	// An invalid option is rejected before the origin or the cache is touched.
	if mode != "on" && mode != "off" {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "cache는 on 또는 off여야 합니다.")
		return
	}
	if !validID(w, id) {
		return
	}

	if mode == "on" {
		value, hit, err := a.cacheGet(r, id)
		if err != nil {
			writeError(w, http.StatusBadGateway, "CACHE_ERROR", "캐시 조회에 실패했습니다.")
			return
		}
		if hit {
			w.Header().Set("X-Cache", "HIT")
			writeRaw(w, http.StatusOK, []byte(value))
			return
		}
	}

	status, body, err := a.call(r, http.MethodGet, originProductURL(a.originURL, id), nil, false)
	switch {
	case err != nil || status >= 500:
		writeError(w, http.StatusBadGateway, "ORIGIN_ERROR", "원본 조회에 실패했습니다.")
		return
	case status == http.StatusNotFound:
		// Absence is never cached (no negative caching).
		writeError(w, http.StatusNotFound, "PRODUCT_NOT_FOUND", "없는 상품입니다.")
		return
	case status != http.StatusOK:
		writeError(w, http.StatusBadGateway, "ORIGIN_ERROR", "원본이 예상하지 못한 응답을 반환했습니다.")
		return
	}

	if mode == "off" {
		w.Header().Set("X-Cache", "BYPASS")
		writeRaw(w, http.StatusOK, body)
		return
	}
	if err := a.cachePut(r, id, string(body)); err != nil {
		writeError(w, http.StatusBadGateway, "CACHE_ERROR", "캐시 저장에 실패했습니다.")
		return
	}
	w.Header().Set("X-Cache", "MISS")
	writeRaw(w, http.StatusOK, body)
}

// putProduct changes the origin first and invalidates the cache only after
// that succeeded. There is no transaction across the two systems: if the
// delete fails the origin has already changed and the caller gets 502.
func (a *app) putProduct(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	if !validID(w, id) {
		return
	}
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "본문을 읽지 못했습니다.")
		return
	}
	status, body, err := a.call(r, http.MethodPut, originProductURL(a.originURL, id), payload, false)
	switch {
	case err != nil || status >= 500:
		writeError(w, http.StatusBadGateway, "ORIGIN_ERROR", "원본 변경에 실패했습니다.")
		return
	case status == http.StatusBadRequest:
		writeError(w, http.StatusBadRequest, "INVALID_ARGUMENT", "name과 version이 필요합니다.")
		return
	case status != http.StatusOK:
		writeError(w, http.StatusBadGateway, "ORIGIN_ERROR", "원본이 예상하지 못한 응답을 반환했습니다.")
		return
	}
	status, _, err = a.call(r, http.MethodDelete, cacheKeyURL(a.redisURL, id), nil, true)
	if err != nil || status != http.StatusOK {
		writeError(w, http.StatusBadGateway, "CACHE_ERROR", "원본은 변경했지만 캐시 삭제에 실패했습니다.")
		return
	}
	writeRaw(w, http.StatusOK, body)
}

// cacheGet treats 404 as a normal miss and anything else unexpected as failure.
func (a *app) cacheGet(r *http.Request, id string) (value string, hit bool, err error) {
	status, body, err := a.call(r, http.MethodGet, cacheKeyURL(a.redisURL, id), nil, true)
	if err != nil {
		return "", false, err
	}
	switch status {
	case http.StatusOK:
		var stored struct {
			Value *string `json:"value"`
		}
		if json.Unmarshal(body, &stored) != nil || stored.Value == nil {
			return "", false, errUpstream
		}
		return *stored.Value, true, nil
	case http.StatusNotFound:
		return "", false, nil
	default:
		return "", false, errUpstream
	}
}

func (a *app) cachePut(r *http.Request, id, value string) error {
	payload, err := json.Marshal(struct {
		Value string `json:"value"`
		TTL   int64  `json:"ttl_ms"`
	}{value, a.ttlMillis})
	if err != nil {
		return err
	}
	status, _, err := a.call(r, http.MethodPut, cacheKeyURL(a.redisURL, id), payload, true)
	if err != nil {
		return err
	}
	if status != http.StatusOK {
		return errUpstream
	}
	return nil
}

// call reads the whole response so the connection can be reused.
func (a *app) call(r *http.Request, method, address string, payload []byte, toCache bool) (int, []byte, error) {
	request, err := http.NewRequestWithContext(r.Context(), method, address, bytes.NewReader(payload))
	if err != nil {
		return 0, nil, err
	}
	if payload != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if toCache && a.token != "" {
		request.Header.Set("Authorization", "Bearer "+a.token)
	}
	response, err := a.client.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return 0, nil, err
	}
	return response.StatusCode, body, nil
}
