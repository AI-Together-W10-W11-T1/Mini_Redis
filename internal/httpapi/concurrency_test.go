package httpapi

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/store"
)

// PRD T3 runs C1~C3 through the whole HTTP path. The team profile is 50
// workers x 1,000 iterations within 60 seconds; set MINI_REDIS_T3_FULL=1 to
// run exactly that. The default keeps the same 50 workers with fewer
// iterations so the race-enabled suite stays quick.
func t3Profile(t *testing.T) (workers, iterations int) {
	t.Helper()
	workers, iterations = 50, 100
	if testing.Short() {
		iterations = 20
	}
	if os.Getenv("MINI_REDIS_T3_FULL") == "1" {
		iterations = 1000
	}
	return workers, iterations
}

const (
	t3TTL       = 600_000 // Outlives the test, as T3 requires.
	t3TimeLimit = 60 * time.Second
)

// t3Client records the first violation instead of failing the test directly,
// so the same scenarios can be pointed at a deliberately faulty server and
// be required to notice.
type t3Client struct {
	client    *http.Client
	base      string
	requests  atomic.Int64
	failures  atomic.Int64
	firstFail atomic.Value
}

func newMiniRedis(t *testing.T) http.Handler {
	t.Helper()
	s := store.New(nil, nil)
	t.Cleanup(func() { _ = s.Close() })
	return NewHandler(s, nil)
}

func newT3Client(t *testing.T, handler http.Handler, workers int) *t3Client {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	transport := &http.Transport{MaxIdleConns: workers * 2, MaxIdleConnsPerHost: workers * 2}
	t.Cleanup(transport.CloseIdleConnections)
	return &t3Client{client: &http.Client{Transport: transport, Timeout: 30 * time.Second}, base: server.URL}
}

func (c *t3Client) fail(format string, args ...any) {
	c.failures.Add(1)
	c.firstFail.CompareAndSwap(nil, fmt.Sprintf(format, args...))
}

// failure returns the first recorded violation, or "" when there was none.
func (c *t3Client) failure() string {
	if c.failures.Load() == 0 {
		return ""
	}
	return fmt.Sprintf("%d violations, first: %v", c.failures.Load(), c.firstFail.Load())
}

// call sends one request and returns the status with the body's JSON fields.
// A transport error or a body that is not one JSON object is a violation.
func (c *t3Client) call(method, key, body string) (int, map[string]json.RawMessage, bool) {
	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}
	request, err := http.NewRequest(method, c.base+"/keys/"+url.PathEscape(key), reader)
	if err != nil {
		c.fail("request: %v", err)
		return 0, nil, false
	}
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	c.requests.Add(1)
	response, err := c.client.Do(request)
	if err != nil {
		c.fail("%s %s: %v", method, key, err)
		return 0, nil, false
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		c.fail("%s %s: read: %v", method, key, err)
		return 0, nil, false
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		c.fail("%s %s: status %d with a body that is not a JSON object: %.80q", method, key, response.StatusCode, raw)
		return 0, nil, false
	}
	return response.StatusCode, fields, true
}

// exactFields reports whether the object has exactly the named fields.
func exactFields(fields map[string]json.RawMessage, names ...string) bool {
	if len(fields) != len(names) {
		return false
	}
	for _, name := range names {
		if _, present := fields[name]; !present {
			return false
		}
	}
	return true
}

// put requires 200 {"ok":true}.
func (c *t3Client) put(key, value string, ttl int64) bool {
	body := `{"value":` + strconv.Quote(value) + `}`
	if ttl > 0 {
		body = `{"value":` + strconv.Quote(value) + `,"ttl_ms":` + strconv.FormatInt(ttl, 10) + `}`
	}
	status, fields, ok := c.call(http.MethodPut, key, body)
	if !ok {
		return false
	}
	if status != http.StatusOK || !exactFields(fields, "ok") || string(fields["ok"]) != "true" {
		c.fail("PUT %s: status %d body %v", key, status, describe(fields))
		return false
	}
	return true
}

// get requires either 200 with the requested key, a string value and a
// numeric ttl_ms, or 404 KEY_NOT_FOUND. found is false for the 404.
func (c *t3Client) get(key string) (value string, found, ok bool) {
	status, fields, ok := c.call(http.MethodGet, key, "")
	if !ok {
		return "", false, false
	}
	switch status {
	case http.StatusOK:
		var gotKey, gotValue *string
		var ttl *int64
		if !exactFields(fields, "key", "value", "ttl_ms") ||
			json.Unmarshal(fields["key"], &gotKey) != nil || gotKey == nil || *gotKey != key ||
			json.Unmarshal(fields["value"], &gotValue) != nil || gotValue == nil ||
			json.Unmarshal(fields["ttl_ms"], &ttl) != nil || ttl == nil || (*ttl != -1 && *ttl <= 0) {
			c.fail("GET %s: malformed 200 body %v", key, describe(fields))
			return "", false, false
		}
		return *gotValue, true, true
	case http.StatusNotFound:
		if errorCode(fields) != "KEY_NOT_FOUND" {
			c.fail("GET %s: 404 without KEY_NOT_FOUND: %v", key, describe(fields))
			return "", false, false
		}
		return "", false, true
	default:
		c.fail("GET %s: unexpected status %d %v", key, status, describe(fields))
		return "", false, false
	}
}

// delete requires 200 with deleted exactly 0 or 1.
func (c *t3Client) delete(key string) (deleted int, ok bool) {
	status, fields, ok := c.call(http.MethodDelete, key, "")
	if !ok {
		return 0, false
	}
	raw := string(fields["deleted"])
	if status != http.StatusOK || !exactFields(fields, "deleted") || (raw != "0" && raw != "1") {
		c.fail("DELETE %s: status %d body %v", key, status, describe(fields))
		return 0, false
	}
	return int(raw[0] - '0'), true
}

func errorCode(fields map[string]json.RawMessage) string {
	var detail struct {
		Code string `json:"code"`
	}
	if !exactFields(fields, "error") || json.Unmarshal(fields["error"], &detail) != nil {
		return ""
	}
	return detail.Code
}

func describe(fields map[string]json.RawMessage) string {
	raw, _ := json.Marshal(fields)
	if len(raw) > 120 {
		return string(raw[:120]) + "..."
	}
	return string(raw)
}

// run starts every worker behind one barrier and enforces the time limit.
func (c *t3Client) run(workers int, work func(worker int)) {
	start := make(chan struct{})
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			work(worker)
		}(worker)
	}
	close(start)
	finished := make(chan struct{})
	go func() { wg.Wait(); close(finished) }()
	select {
	case <-finished:
	case <-time.After(t3TimeLimit):
		c.fail("T3 time limit of %s exceeded", t3TimeLimit)
		<-finished // Client timeouts end the workers; do not leak them into other tests.
	}
}

var tagPattern = regexp.MustCompile(`^w(0|[1-9][0-9]*)-i(0|[1-9][0-9]*)$`)

// parseTag accepts only the canonical "w<worker>-i<iteration>" spelling with
// both numbers inside the profile: no sign, no leading zero, nothing after it.
func parseTag(tag string, workers, iterations int) (worker, iteration int, ok bool) {
	match := tagPattern.FindStringSubmatch(tag)
	if match == nil {
		return 0, 0, false
	}
	worker, errW := strconv.Atoi(match[1])
	iteration, errI := strconv.Atoi(match[2])
	if errW != nil || errI != nil || worker >= workers || iteration >= iterations {
		return 0, 0, false
	}
	return worker, iteration, true
}

// --- C1 -------------------------------------------------------------------

func c1Key(worker, i int) string   { return fmt.Sprintf("c1:%d:%d", worker, i) }
func c1Value(worker, i int) string { return fmt.Sprintf("value:%d:%d", worker, i) }

// scenarioC1: every worker owns its keys; each successful operation must be
// visible, and afterwards the whole key list is read back.
func scenarioC1(c *t3Client, workers, iterations int) {
	c.run(workers, func(worker int) {
		for i := 0; i < iterations; i++ {
			key, value := c1Key(worker, i), c1Value(worker, i)
			if !c.put(key, value, t3TTL) {
				return
			}
			got, found, ok := c.get(key)
			if !ok {
				return
			}
			if !found || got != value {
				c.fail("GET %s: found=%v value=%.60q, want %q", key, found, got, value)
				return
			}
			if i%3 == 0 {
				deleted, ok := c.delete(key)
				if !ok {
					return
				}
				if deleted != 1 {
					c.fail("DELETE %s: deleted=%d, want 1", key, deleted)
					return
				}
			}
		}
	})
	if c.failures.Load() != 0 {
		return
	}
	for worker := 0; worker < workers; worker++ {
		for i := 0; i < iterations; i++ {
			key := c1Key(worker, i)
			got, found, ok := c.get(key)
			if !ok {
				return
			}
			if i%3 == 0 && found {
				c.fail("deleted key %s is still present: %.60q", key, got)
				return
			}
			if i%3 != 0 && (!found || got != c1Value(worker, i)) {
				c.fail("kept key %s: found=%v value=%.60q", key, found, got)
				return
			}
		}
	}
}

func TestT3C1DistinctKeysOverHTTP(t *testing.T) {
	workers, iterations := t3Profile(t)
	c := newT3Client(t, newMiniRedis(t), workers)
	began := time.Now()
	scenarioC1(c, workers, iterations)
	if failure := c.failure(); failure != "" {
		t.Fatal(failure)
	}
	t.Logf("%d workers x %d iterations, %d HTTP requests in %s, violations 0", workers, iterations, c.requests.Load(), time.Since(began).Round(time.Millisecond))
}

// --- C2 -------------------------------------------------------------------

const c2Key = "c2:shared"

// c2Value is long and self-describing so a torn or mixed value is detectable.
func c2Value(worker, i int) string {
	tag := fmt.Sprintf("w%d-i%d|", worker, i)
	return strings.Repeat(tag, 1+2000/len(tag))
}

// c2Written reports whether value is exactly one value some worker wrote.
func c2Written(value string, workers, iterations int) (iteration int, ok bool) {
	end := strings.IndexByte(value, '|')
	if end < 0 {
		return 0, false
	}
	worker, iteration, ok := parseTag(value[:end], workers, iterations)
	if !ok || value != c2Value(worker, iteration) {
		return 0, false
	}
	return iteration, true
}

// scenarioC2: all workers write the same key with distinguishable values and
// TTLs. A read must return one complete value that some worker really wrote,
// and the final value must be some worker's last write.
func scenarioC2(c *t3Client, workers, iterations int) {
	c.run(workers, func(worker int) {
		for i := 0; i < iterations; i++ {
			if !c.put(c2Key, c2Value(worker, i), t3TTL+int64(worker)) {
				return
			}
			got, found, ok := c.get(c2Key)
			if !ok {
				return
			}
			if _, written := c2Written(got, workers, iterations); !found || !written {
				c.fail("GET returned found=%v and a value nobody wrote (len %d, starts %.40q)", found, len(got), got)
				return
			}
		}
	})
	if c.failures.Load() != 0 {
		return
	}
	got, found, ok := c.get(c2Key)
	if !ok {
		return
	}
	iteration, written := c2Written(got, workers, iterations)
	if !found || !written {
		c.fail("final value was written by nobody: found=%v starts %.40q", found, got)
		return
	}
	// In any sequential order the last write is the last write of its worker.
	if iteration != iterations-1 {
		c.fail("final value is not any worker's last write: iteration %d of %d", iteration, iterations)
	}
}

func TestT3C2SameKeyWritesOverHTTP(t *testing.T) {
	workers, iterations := t3Profile(t)
	c := newT3Client(t, newMiniRedis(t), workers)
	began := time.Now()
	scenarioC2(c, workers, iterations)
	if failure := c.failure(); failure != "" {
		t.Fatal(failure)
	}
	t.Logf("%d workers x %d iterations, %d HTTP requests in %s, violations 0", workers, iterations, c.requests.Load(), time.Since(began).Round(time.Millisecond))
}

// --- C3 -------------------------------------------------------------------

const c3Key = "c3:shared"

func c3Value(worker, i int) string { return fmt.Sprintf("w%d-i%d", worker, i) }

// c3Writes reports whether (worker, i) is a step that performs a PUT.
func c3Writes(worker, i int) bool { return (worker+i)%3 == 0 }

// c3Written reports whether value is exactly one value the scenario PUTs:
// canonical spelling, both numbers in range, and a step that really writes.
func c3Written(value string, workers, iterations int) bool {
	worker, iteration, ok := parseTag(value, workers, iterations)
	return ok && c3Writes(worker, iteration) && value == c3Value(worker, iteration)
}

// scenarioC3: writes, deletes and reads race on one key. Every read is either
// absent or exactly a written value; after everything finished, a final write
// is visible and a final delete leaves the key absent.
func scenarioC3(c *t3Client, workers, iterations int) {
	var deletedTotal, putTotal atomic.Int64
	c.run(workers, func(worker int) {
		for i := 0; i < iterations; i++ {
			switch {
			case c3Writes(worker, i):
				if !c.put(c3Key, c3Value(worker, i), t3TTL) {
					return
				}
				putTotal.Add(1)
			case (worker+i)%3 == 1:
				deleted, ok := c.delete(c3Key)
				if !ok {
					return
				}
				deletedTotal.Add(int64(deleted))
			default:
				got, found, ok := c.get(c3Key)
				if !ok {
					return
				}
				if found && !c3Written(got, workers, iterations) {
					c.fail("GET returned a value nobody wrote: %.60q", got)
					return
				}
			}
		}
	})
	if c.failures.Load() != 0 {
		return
	}
	// A delete can only succeed against an earlier write.
	if deletedTotal.Load() > putTotal.Load() {
		c.fail("deleted %d keys with only %d writes", deletedTotal.Load(), putTotal.Load())
		return
	}
	if !c.put(c3Key, "final", 0) {
		return
	}
	if got, found, ok := c.get(c3Key); !ok || !found || got != "final" {
		c.fail("after the final write: found=%v value=%.60q", found, got)
		return
	}
	if deleted, ok := c.delete(c3Key); !ok || deleted != 1 {
		c.fail("final DELETE: deleted=%d", deleted)
		return
	}
	if got, found, ok := c.get(c3Key); !ok || found {
		c.fail("after the final delete: found=%v value=%.60q", found, got)
		return
	}
	if deleted, ok := c.delete(c3Key); !ok || deleted != 0 {
		c.fail("repeated DELETE: deleted=%d", deleted)
	}
}

func TestT3C3SetDeleteGetRaceOverHTTP(t *testing.T) {
	workers, iterations := t3Profile(t)
	c := newT3Client(t, newMiniRedis(t), workers)
	began := time.Now()
	scenarioC3(c, workers, iterations)
	if failure := c.failure(); failure != "" {
		t.Fatal(failure)
	}
	t.Logf("%d workers x %d iterations, %d HTTP requests in %s, violations 0", workers, iterations, c.requests.Load(), time.Since(began).Round(time.Millisecond))
}

// --- The checks themselves ---------------------------------------------------

func TestT3ValueChecksAcceptOnlyWrittenValues(t *testing.T) {
	const workers, iterations = 50, 1000
	for _, value := range []string{"w0-i0", "w1-i2", "w49-i998", "w3-i0", "w2-i997"} {
		if !c3Written(value, workers, iterations) {
			t.Errorf("C3 rejected a written value %q", value)
		}
	}
	for _, value := range []string{
		"", "final", "w-1-i-1", "w-1-i-1garbage", "w0-i0garbage", " w0-i0", "w0-i0 ", "w0-i0\n",
		"w00-i0", "w0-i00", "w+0-i0", "w0-i+0", "W0-I0", "w0i0", "w0-i", "w-i0",
		"w50-i1", "w0-i1000", "w99999999999999999999-i0", // out of range, overflow
		"w0-i1", "w1-i0", "w2-i2", // well-formed, in range, but no PUT at that step
		"w0-i0|w0-i0", "w0-i0-i3",
	} {
		if c3Written(value, workers, iterations) {
			t.Errorf("C3 accepted %q", value)
		}
	}
	good := c2Value(7, 12)
	if iteration, ok := c2Written(good, workers, iterations); !ok || iteration != 12 {
		t.Fatalf("C2 rejected a written value: %d %v", iteration, ok)
	}
	for name, value := range map[string]string{
		"empty":         "",
		"truncated":     good[:len(good)-1],
		"extended":      good + "x",
		"mixed":         good[:len(good)/2] + c2Value(8, 12)[:len(good)/2],
		"negative":      strings.Repeat("w-1-i-1|", 300),
		"out_of_range":  c2Value(50, 0),
		"wrong_repeats": strings.Repeat("w7-i12|", 3),
		"no_separator":  "w7-i12",
	} {
		if _, ok := c2Written(value, workers, iterations); ok {
			t.Errorf("C2 accepted %s value", name)
		}
	}
}

// faultyServer answers through the real handler and then corrupts some
// responses for one key. Each fault is something a broken store or HTTP layer
// could plausibly produce; every scenario must notice every one of them.
type faultyServer struct {
	next     http.Handler
	method   string
	key      string
	every    int64 // Corrupt every n-th matching 200 response.
	seen     atomic.Int64
	injected atomic.Int64
	corrupt  func(body map[string]json.RawMessage) (status int, raw []byte)
}

func (f *faultyServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	recorder := httptest.NewRecorder()
	f.next.ServeHTTP(recorder, r)
	status, raw := recorder.Code, recorder.Body.Bytes()
	if r.Method == f.method && strings.HasSuffix(r.URL.EscapedPath(), "/"+url.PathEscape(f.key)) && status == http.StatusOK && f.seen.Add(1)%f.every == 0 {
		var body map[string]json.RawMessage
		if json.Unmarshal(raw, &body) == nil {
			status, raw = f.corrupt(body)
			f.injected.Add(1)
		}
	}
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write(raw)
}

func withField(body map[string]json.RawMessage, name string, value any) (int, []byte) {
	encoded, _ := json.Marshal(value)
	body[name] = encoded
	raw, _ := json.Marshal(body)
	return http.StatusOK, raw
}

func withoutField(body map[string]json.RawMessage, name string) (int, []byte) {
	delete(body, name)
	raw, _ := json.Marshal(body)
	return http.StatusOK, raw
}

func TestT3ScenariosDetectFaultyServers(t *testing.T) {
	const workers, iterations = 12, 60
	type fault struct {
		name     string
		scenario func(*t3Client, int, int)
		method   string
		key      string
		corrupt  func(map[string]json.RawMessage) (int, []byte)
	}
	value := func(v string) func(map[string]json.RawMessage) (int, []byte) {
		return func(body map[string]json.RawMessage) (int, []byte) { return withField(body, "value", v) }
	}
	raw := func(status int, text string) func(map[string]json.RawMessage) (int, []byte) {
		return func(map[string]json.RawMessage) (int, []byte) { return status, []byte(text) }
	}
	faults := []fault{
		// C3: values that were never written.
		{"c3_negative_with_garbage", scenarioC3, "GET", c3Key, value("w-1-i-1garbage")},
		{"c3_negative", scenarioC3, "GET", c3Key, value("w-1-i-1")},
		{"c3_trailing_garbage", scenarioC3, "GET", c3Key, value("w0-i0garbage")},
		{"c3_worker_out_of_range", scenarioC3, "GET", c3Key, value(c3Value(workers, 0))},
		{"c3_iteration_out_of_range", scenarioC3, "GET", c3Key, value(c3Value(0, iterations))},
		{"c3_step_that_does_not_write", scenarioC3, "GET", c3Key, value(c3Value(0, 1))},
		{"c3_empty_value", scenarioC3, "GET", c3Key, value("")},
		// C3: malformed responses.
		{"c3_value_missing", scenarioC3, "GET", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withoutField(b, "value") }},
		{"c3_value_null", scenarioC3, "GET", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "value", nil) }},
		{"c3_value_not_string", scenarioC3, "GET", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "value", 7) }},
		{"c3_wrong_key", scenarioC3, "GET", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "key", "other") }},
		{"c3_not_json", scenarioC3, "GET", c3Key, raw(200, "w0-i0")},
		{"c3_json_array", scenarioC3, "GET", c3Key, raw(200, `["w0-i0"]`)},
		{"c3_truncated_json", scenarioC3, "GET", c3Key, raw(200, `{"key":"c3:shared","value":"w0-i0"`)},
		{"c3_server_error", scenarioC3, "GET", c3Key, raw(500, `{"error":{"code":"INTERNAL_ERROR","message":"x"}}`)},
		{"c3_404_without_code", scenarioC3, "GET", c3Key, raw(404, `{}`)},
		{"c3_delete_count_missing", scenarioC3, "DELETE", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withoutField(b, "deleted") }},
		{"c3_delete_count_two", scenarioC3, "DELETE", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "deleted", 2) }},
		{"c3_delete_count_negative", scenarioC3, "DELETE", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "deleted", -1) }},
		{"c3_put_not_ok", scenarioC3, "PUT", c3Key, func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "ok", false) }},
		{"c3_put_empty_body", scenarioC3, "PUT", c3Key, raw(200, `{}`)},
		// C2: torn, mixed and foreign values.
		{"c2_truncated", scenarioC2, "GET", c2Key, value(c2Value(1, 1)[:1500])},
		{"c2_mixed", scenarioC2, "GET", c2Key, value(c2Value(1, 1)[:1000] + c2Value(2, 2)[:1000])},
		{"c2_out_of_range", scenarioC2, "GET", c2Key, value(c2Value(workers, 0))},
		{"c2_negative", scenarioC2, "GET", c2Key, value(strings.Repeat("w-1-i-1|", 250))},
		{"c2_value_missing", scenarioC2, "GET", c2Key, func(b map[string]json.RawMessage) (int, []byte) { return withoutField(b, "value") }},
		// C1: another key's value, and a delete that reports nothing deleted.
		{"c1_foreign_value", scenarioC1, "GET", c1Key(3, 7), value(c1Value(3, 8))},
		{"c1_delete_reports_zero", scenarioC1, "DELETE", c1Key(3, 6), func(b map[string]json.RawMessage) (int, []byte) { return withField(b, "deleted", 0) }},
	}
	for _, fc := range faults {
		t.Run(fc.name, func(t *testing.T) {
			server := &faultyServer{next: newMiniRedis(t), method: fc.method, key: fc.key, every: 2, corrupt: fc.corrupt}
			if strings.HasPrefix(fc.name, "c1_") {
				server.every = 1 // A single key is requested only once or twice.
			}
			c := newT3Client(t, server, workers)
			fc.scenario(c, workers, iterations)
			if server.injected.Load() == 0 {
				t.Fatal("no fault was injected; the test proved nothing")
			}
			if c.failure() == "" {
				t.Fatalf("scenario passed although %d responses were corrupted", server.injected.Load())
			}
		})
	}
	// The same harness without a fault must pass, or the cases above could be
	// failing for an unrelated reason.
	for name, scenario := range map[string]func(*t3Client, int, int){"c1": scenarioC1, "c2": scenarioC2, "c3": scenarioC3} {
		passthrough := &faultyServer{next: newMiniRedis(t), method: "NONE", every: 1}
		c := newT3Client(t, passthrough, workers)
		scenario(c, workers, iterations)
		if failure := c.failure(); failure != "" {
			t.Fatalf("%s through the unmodified wrapper: %s", name, failure)
		}
	}
}
