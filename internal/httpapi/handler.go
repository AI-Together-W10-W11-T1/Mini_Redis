package httpapi

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

const DefaultMaxBodyBytes int64 = 512 * 1024

type Handler struct {
	store        *store.Store
	logger       *slog.Logger
	mux          *http.ServeMux
	maxBodyBytes int64
	debugEnabled bool
	authToken    string
}

type errorResponse struct {
	Error struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"error"`
}

func NewHandler(s *store.Store, logger *slog.Logger) http.Handler {
	return NewHandlerWithMaxBodyBytes(s, logger, DefaultMaxBodyBytes)
}

// NewHandlerWithMaxBodyBytes keeps the default constructor's API unchanged
// while allowing the server to apply the validated body-limit setting.
type Options struct {
	MaxBodyBytes int64
	AuthToken    string
}

func NewHandlerWithMaxBodyBytes(s *store.Store, logger *slog.Logger, limit int64) http.Handler {
	return NewHandlerWithOptions(s, logger, Options{MaxBodyBytes: limit})
}
func NewHandlerWithOptions(s *store.Store, logger *slog.Logger, opts Options) http.Handler {
	limit := opts.MaxBodyBytes
	if limit == 0 {
		limit = DefaultMaxBodyBytes
	}
	if opts.AuthToken != "" && len(opts.AuthToken) < 16 {
		panic("auth token must contain at least 16 bytes")
	}
	if limit <= 0 {
		panic("HTTP body limit must be positive")
	}
	h := &Handler{
		authToken: opts.AuthToken, store: s, logger: logger, mux: http.NewServeMux(), maxBodyBytes: limit,
		debugEnabled: logger != nil && logger.Enabled(context.Background(), slog.LevelDebug),
	}
	h.mux.HandleFunc("/keys/{key}", h.ServeKey)
	emptyKey := func(w http.ResponseWriter, r *http.Request) {
		// 일부 Go 라우터 버전은 단일 %2F 키도 끝 슬래시 패턴으로 보낸다.
		// 실제 빈 경로만 거부하고, 인코딩된 키는 원래 구간에서 한 번 해석한다.
		path := r.URL.EscapedPath()
		if path != "/keys" && path != "/keys/" {
			key, err := url.PathUnescape(strings.TrimPrefix(path, "/keys/"))
			if err == nil && key != "" {
				r.SetPathValue("key", key)
				h.ServeKey(w, r)
				return
			}
		}
		h.writeError(w, 400, "INVALID_ARGUMENT", "키는 비어 있을 수 없습니다.")
	}
	h.mux.HandleFunc("/keys", emptyKey)
	h.mux.HandleFunc("/keys/{$}", emptyKey)
	h.mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		h.writeError(w, 404, "ROUTE_NOT_FOUND", "정의되지 않은 경로입니다.")
	})
	return h
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			return
		}
		// 연결을 의도적으로 끊는 신호는 net/http가 처리하도록 그대로 다시 던진다.
		if recovered == http.ErrAbortHandler {
			panic(recovered)
		}
		if h.logger != nil {
			// 원인을 추적할 수 있도록 panic 내용을 남긴다. 요청 본문·값은 기록하지 않는다.
			h.logger.Error("HTTP_INTERNAL_ERROR", "method", r.Method, "panic", fmt.Sprint(recovered))
		}
		h.writeError(w, 500, "INTERNAL_ERROR", "내부 처리 오류입니다.")
	}()
	// ServeMux가 점 경로를 정리하기 전에 원래의 경로 구간을 확인한다.
	path := r.URL.EscapedPath()
	if path == "/health" {
		h.health(w, r)
		return
	}
	if h.authToken != "" && (path == "/keys" || strings.HasPrefix(path, "/keys/")) {
		// Compare fixed-size hashes so token length does not affect comparison time.
		auth := r.Header.Get("Authorization")
		validScheme := len(auth) >= 7 && strings.EqualFold(auth[:7], "Bearer ")
		supplied := ""
		if validScheme {
			supplied = auth[7:]
		}
		got, want := sha256.Sum256([]byte(supplied)), sha256.Sum256([]byte(h.authToken))
		if !validScheme || subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			h.writeError(w, 401, "UNAUTHORIZED", "유효한 Bearer 인증 토큰이 필요합니다.")
			return
		}
	}
	if path != "/keys" && path != "/keys/" {
		segment, ok := strings.CutPrefix(path, "/keys/")
		if !ok || segment == "" || strings.Contains(segment, "/") {
			h.writeError(w, 404, "ROUTE_NOT_FOUND", "정의되지 않은 경로입니다.")
			return
		}
		key, err := url.PathUnescape(segment)
		if err != nil || key == "." || key == ".." {
			h.writeError(w, 400, "INVALID_ARGUMENT", "올바르지 않은 키입니다.")
			return
		}
		// 위 디코딩은 검증용이다. 실제 키는 라우터의 PathValue를 그대로 쓴다.
	}
	if h.debugEnabled {
		h.logger.Debug("HTTP_REQUEST", "method", r.Method, "path", path)
	}
	h.mux.ServeHTTP(w, r)
}

func (h *Handler) ServeKey(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodPut:
		h.Set(w, r)
	case http.MethodGet:
		h.Get(w, r)
	case http.MethodDelete:
		h.Delete(w, r)
	default:
		w.Header().Set("Allow", "GET, PUT, DELETE")
		h.writeError(w, 405, "METHOD_NOT_ALLOWED", "지원하지 않는 메서드입니다.")
	}
}

func (h *Handler) Set(w http.ResponseWriter, r *http.Request) {
	// 헤더만으로 거부할 수 있는 요청은 본문을 읽기 전에 거부한다.
	if values, present := r.Header["Content-Type"]; present {
		if len(values) != 1 {
			h.writeError(w, 415, "UNSUPPORTED_MEDIA_TYPE", "Content-Type이 올바르지 않습니다.")
			return
		}
		mediaType, params, err := mime.ParseMediaType(values[0])
		if err != nil || mediaType != "application/json" ||
			(params["charset"] != "" && !strings.EqualFold(params["charset"], "utf-8")) {
			h.writeError(w, 415, "UNSUPPORTED_MEDIA_TYPE", "application/json을 지정해야 합니다.")
			return
		}
	}
	// 실제 수신량을 먼저 제한하고 끝의 공백까지 본문 전체를 확인한다.
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, h.maxBodyBytes))
	if err != nil {
		var tooLarge *http.MaxBytesError
		if errors.As(err, &tooLarge) {
			h.writeError(w, 413, "LIMIT_EXCEEDED", "본문 크기 상한을 초과했습니다.")
		} else {
			h.writeError(w, 400, "INVALID_ARGUMENT", "본문을 읽지 못했습니다.")
		}
		return
	}
	if !utf8.Valid(body) {
		h.writeError(w, 400, "INVALID_ARGUMENT", "본문은 올바른 UTF-8이어야 합니다.")
		return
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(body, &fields); err != nil || fields == nil {
		h.writeError(w, 400, "INVALID_ARGUMENT", "JSON 객체가 필요합니다.")
		return
	}
	for name := range fields {
		if name != "value" && name != "ttl_ms" {
			h.writeError(w, 400, "INVALID_ARGUMENT", "알 수 없는 필드입니다.")
			return
		}
	}
	if !validStringEscapes(fields["value"]) {
		h.writeError(w, 400, "INVALID_ARGUMENT", "value의 유니코드 이스케이프가 올바르지 않습니다.")
		return
	}
	var value *string
	if err := json.Unmarshal(fields["value"], &value); err != nil || value == nil {
		h.writeError(w, 400, "INVALID_ARGUMENT", "value는 문자열이어야 합니다.")
		return
	}
	var ttl *int64
	if raw, present := fields["ttl_ms"]; present {
		parsed, err := parseTTL(raw)
		if err != nil {
			h.writeError(w, 400, "INVALID_ARGUMENT", "ttl_ms는 허용 범위 내 양의 정수여야 합니다.")
			return
		}
		ttl = &parsed
	}
	if err := h.store.SetContext(r.Context(), r.PathValue("key"), *value, ttl); err != nil {
		h.storeError(w, err)
		return
	}
	h.writeJSON(w, 200, struct {
		OK bool `json:"ok"`
	}{true})
}

// parseTTL validates the exact decimal value without float64 rounding. JSON
// has already validated the number grammar; strings, null and booleans fail.
func parseTTL(raw json.RawMessage) (int64, error) {
	n := string(raw)
	invalid := store.ErrInvalidArgument
	if len(n) == 0 || n[0] < '0' || n[0] > '9' {
		return 0, invalid
	}
	mantissa := n
	exponent := 0
	if at := strings.IndexAny(n, "eE"); at >= 0 {
		mantissa = n[:at]
		parsed, err := strconv.Atoi(n[at+1:])
		// 거대한 지수로 인한 메모리 할당이나 정수 오버플로를 피한다.
		if err != nil || parsed > len(n)+20 || parsed < -len(n)-20 {
			return 0, invalid
		}
		exponent = parsed
	}
	fractionDigits := 0
	if dot := strings.IndexByte(mantissa, '.'); dot >= 0 {
		fractionDigits = len(mantissa) - dot - 1
		mantissa = mantissa[:dot] + mantissa[dot+1:]
	}
	digits := strings.TrimLeft(mantissa, "0")
	if digits == "" {
		return 0, invalid
	}
	trimmed := strings.TrimRight(digits, "0")
	scale := exponent - fractionDigits + len(digits) - len(trimmed)
	if scale < 0 || len(trimmed)+scale > len(strconv.FormatInt(store.MaxTTLMillis, 10)) {
		return 0, invalid
	}
	value, err := strconv.ParseInt(trimmed+strings.Repeat("0", scale), 10, 64)
	if err != nil || value <= 0 || value > store.MaxTTLMillis {
		return 0, invalid
	}
	return value, nil
}

func (h *Handler) Get(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	result, err := h.store.Get(key)
	if err != nil {
		h.storeError(w, err)
		return
	}
	if result.State != store.Live {
		h.writeError(w, 404, "KEY_NOT_FOUND", "키가 없거나 만료되었습니다.")
		return
	}
	h.writeJSON(w, 200, struct {
		Key   string `json:"key"`
		Value string `json:"value"`
		TTL   int64  `json:"ttl_ms"`
	}{key, result.Value, result.TTLMillis})
}

func (h *Handler) Delete(w http.ResponseWriter, r *http.Request) {
	result, err := h.store.DeleteContext(r.Context(), r.PathValue("key"))
	if err != nil {
		h.storeError(w, err)
		return
	}
	deleted := 0
	if result.Deleted {
		deleted = 1
	}
	h.writeJSON(w, 200, struct {
		Deleted int `json:"deleted"`
	}{deleted})
}

func (h *Handler) storeError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, store.ErrCapacityExceeded):
		h.writeError(w, 503, "CAPACITY_EXCEEDED", "데이터 용량 상한을 초과했습니다.")
	case errors.Is(err, store.ErrInvalidArgument):
		h.writeError(w, 400, "INVALID_ARGUMENT", "요청 값이 올바르지 않습니다.")
	case errors.Is(err, store.ErrLimitExceeded):
		h.writeError(w, 413, "LIMIT_EXCEEDED", "키 또는 값의 크기 상한을 초과했습니다.")
	case errors.Is(err, store.ErrOutcomeUnknown):
		h.writeError(w, 503, "PERSISTENCE_OUTCOME_UNKNOWN", "변경 결과가 불명확합니다. 조회로 확인해 주세요.")
	case errors.Is(err, store.ErrPersistence):
		h.writeError(w, 503, "PERSISTENCE_UNAVAILABLE", "영속성 저장이 지연되거나 실패했습니다.")
	default:
		h.writeError(w, 500, "INTERNAL_ERROR", "내부 처리 오류입니다.")
	}
}

func (h *Handler) writeError(w http.ResponseWriter, status int, code, message string) {
	var response errorResponse
	response.Error.Code, response.Error.Message = code, message
	h.writeJSON(w, status, response)
}

func (h *Handler) writeJSON(w http.ResponseWriter, status int, value any) {
	body, err := json.Marshal(value)
	if err != nil {
		status = 500
		body = []byte(`{"error":{"code":"INTERNAL_ERROR","message":"JSON encoding failed"}}`)
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if _, err := w.Write(append(body, '\n')); err != nil && h.logger != nil {
		h.logger.Error("HTTP_WRITE_ERROR", "error", err)
	}
	if h.debugEnabled {
		h.logger.Debug("HTTP_RESPONSE", "status", status)
	}
}

func (h *Handler) health(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", "GET")
		h.writeError(w, 405, "METHOD_NOT_ALLOWED", "GET 메서드를 사용해야 합니다.")
		return
	}
	enabled, healthy := h.store.PersistenceHealth()
	label := "disabled"
	if enabled {
		label = "aof"
	}
	status, code := "ok", 200
	if !healthy {
		status, label, code = "degraded", "error", 503
	}
	h.writeJSON(w, code, struct {
		Status      string `json:"status"`
		Persistence string `json:"persistence"`
	}{status, label})
}
