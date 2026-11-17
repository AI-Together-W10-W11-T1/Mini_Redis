package httpapi

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

func TestRejectInvalidUnicodeBeforeStoreAndAOF(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("AOF is unsupported on Windows")
	}
	path := filepath.Join(t.TempDir(), "data.aof")
	clock := &fakeClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	opts := store.DefaultAOFOptions(path)
	s, err := store.OpenPersistent(clock, nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	h := NewHandler(s, nil)
	expectJSON(t, request(h, "PUT", "/keys/x", `{"value":"original","ttl_ms":60000}`), 200, `{"ok":true}`)
	before, err := s.Get("x")
	if err != nil {
		t.Fatal(err)
	}
	written := s.PersistenceStatus().Written
	originalAOF, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ name, body string }{
		{"invalid_raw_utf8", "{\"value\":\"\xff\"}"},
		{"invalid_utf8_field", "{\"value\":\"new\",\"\xff\":1}"},
		{"lone_high", `{"value":"\ud800"}`},
		{"lone_low", `{"value":"\udfff"}`},
		{"high_then_text", `{"value":"\ud800x"}`},
		{"two_highs", `{"value":"\ud800\ud800"}`},
		{"reversed", `{"value":"\udc00\ud800"}`},
		{"high_then_bmp", `{"value":"\ud800\u0041"}`},
		{"high_then_escaped_backslash", `{"value":"\ud800\\udc00"}`},
		{"high_with_gap", `{"value":"\ud800 \udc00"}`},
		{"trailing_unpaired", `{"value":"\ud83d\ude00\ud800"}`},
		{"escaped_slash_then_high", `{"value":"\\\ud800"}`},
		{"surrogate_with_ttl_change", `{"value":"\ud800","ttl_ms":1}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectError(t, request(h, "PUT", "/keys/x", tc.body), 400, "INVALID_ARGUMENT")
			expectError(t, request(h, "PUT", "/keys/new", tc.body), 400, "INVALID_ARGUMENT")
			expectError(t, request(h, "GET", "/keys/new", ""), 404, "KEY_NOT_FOUND")
			if got, err := s.Get("x"); err != nil || got != before {
				t.Fatalf("record changed: %+v want %+v", got, before)
			}
			if got := s.PersistenceStatus().Written; got != written {
				t.Fatalf("AOF sequence changed: %d want %d", got, written)
			}
			after, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(after, originalAOF) {
				t.Fatal("rejected request changed AOF bytes")
			}
		})
	}
}

func TestAcceptValidUnicodeAndLiteralEscapes(t *testing.T) {
	h, s, _ := testHandler(t, DefaultMaxBodyBytes)
	for _, tc := range []struct{ name, body, want string }{
		{"korean", `{"value":"한글"}`, "한글"},
		{"emoji", `{"value":"😀"}`, "😀"},
		{"replacement_literal", `{"value":"�"}`, "�"},
		{"replacement_escape", `{"value":"\ufffd"}`, "�"},
		{"pair_lower", `{"value":"\ud83d\ude00"}`, "😀"},
		{"pair_upper", `{"value":"\uD83D\uDE00"}`, "😀"},
		{"surrogate_edges", `{"value":"\ud800\udc00\udbff\udfff"}`, "\U00010000\U0010ffff"},
		{"bmp_edges", `{"value":"\ud7ff\ue000"}`, "\ud7ff\ue000"},
		{"literal_high_escape", `{"value":"\\ud800"}`, `\ud800`},
		{"literal_low_escape", `{"value":"\\uDFFF"}`, `\uDFFF`},
		{"backslash_then_pair", `{"value":"\\\ud83d\ude00"}`, "\\😀"},
		{"quote_and_slash", `{"value":"\"/\\"}`, "\"/\\"},
		{"surrounded_pair", `{"value":"a\ud83d\ude00b"}`, "a😀b"},
		{"empty", `{"value":""}`, ""},
		{"whitespace", "{\"value\": \n \"ok\" }", "ok"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			expectJSON(t, request(h, "PUT", "/keys/x", tc.body), 200, `{"ok":true}`)
			if got, err := s.Get("x"); err != nil || got.Value != tc.want {
				t.Fatalf("value=%q want %q", got.Value, tc.want)
			}
		})
	}
}

func TestValueTypeErrorPrecedesUnicodeEscapeCheck(t *testing.T) {
	h, _, _ := testHandler(t, DefaultMaxBodyBytes)
	for _, body := range []string{`{"ttl_ms":5}`, `{"value":null}`, `{"value":5}`, `{"value":true}`, `{"value":[]}`, `{"value":{}}`} {
		w := request(h, "PUT", "/keys/x", body)
		expectError(t, w, 400, "INVALID_ARGUMENT")
		var response errorResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if response.Error.Message != "value는 문자열이어야 합니다." {
			t.Fatalf("body=%s message=%q", body, response.Error.Message)
		}
	}
}
