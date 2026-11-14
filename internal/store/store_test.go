package store

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

func learningStore(t *testing.T) (*Store, *fakeClock) {
	t.Helper()
	clock := newFakeClock()
	var logger *slog.Logger
	if raw := os.Getenv("MINI_REDIS_DEBUG"); raw != "" {
		enabled, err := strconv.ParseBool(raw)
		if err != nil {
			t.Fatalf("MINI_REDIS_DEBUG는 true/false로 지정하세요: %v", err)
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
	return New(clock, logger), clock
}

func mustSet(t *testing.T, s *Store, key, value string, ttl *int64) {
	t.Helper()
	if err := s.Set(key, value, ttl); err != nil {
		t.Fatal(err)
	}
}

func expectLookup(t *testing.T, s *Store, key string, want LookupResult) {
	t.Helper()
	got, err := s.Get(key)
	if err != nil || got != want {
		t.Fatalf("Get(%q)=(%+v, %v), want %+v", key, got, err, want)
	}
}

func TestStoreTTLBoundaryAndRemaining(t *testing.T) {
	s, clock := learningStore(t)
	ttl := int64(2)
	mustSet(t, s, "user:101", "Minji", &ttl)
	expectLookup(t, s, "user:101", LookupResult{Live, "Minji", 2})
	clock.Advance(1600 * time.Microsecond)
	expectLookup(t, s, "user:101", LookupResult{Live, "Minji", 1})
	// 반복 조회가 TTL을 연장하지 않는지 같은 시각에 다시 확인한다.
	expectLookup(t, s, "user:101", LookupResult{Live, "Minji", 1})
	t.Log("BEFORE: 0.4ms 남음 → Live, ttl_ms=1 (올림, 조회로 연장 없음)")
	clock.Advance(400 * time.Microsecond)
	expectLookup(t, s, "user:101", LookupResult{State: Expired, TTLMillis: -2})
	s.mu.Lock()
	physicalSize := s.table.Len()
	s.mu.Unlock()
	if physicalSize != 0 {
		t.Fatal("expired entry was not physically removed")
	}
	t.Log("AT: 정확히 만료 → Expired, 물리 항목 삭제")
	expectLookup(t, s, "user:101", LookupResult{State: Missing, TTLMillis: -2})
	clock.Advance(time.Millisecond)
	expectLookup(t, s, "user:101", LookupResult{State: Missing, TTLMillis: -2})
	t.Log("AFTER: 정리 후 다시 조회 → Missing")
	mustSet(t, s, "late", "old", &ttl)
	clock.Advance(3 * time.Millisecond)
	expectLookup(t, s, "late", LookupResult{State: Expired, TTLMillis: -2})
}

func TestStoreTTLTransitions(t *testing.T) {
	s, clock := learningStore(t)
	mustSet(t, s, "session", "forever", nil)
	clock.Advance(time.Hour)
	expectLookup(t, s, "session", LookupResult{Live, "forever", -1})
	ttl := int64(10)
	mustSet(t, s, "session", "short", &ttl)
	clock.Advance(3 * time.Millisecond)
	expectLookup(t, s, "session", LookupResult{Live, "short", 7})
	ttl = 20
	mustSet(t, s, "session", "extended", &ttl)
	clock.Advance(8 * time.Millisecond)
	expectLookup(t, s, "session", LookupResult{Live, "extended", 12})
	ttl = 2
	mustSet(t, s, "session", "shortened", &ttl)
	clock.Advance(time.Millisecond)
	expectLookup(t, s, "session", LookupResult{Live, "shortened", 1})
	mustSet(t, s, "session", "", nil)
	clock.Advance(time.Hour)
	expectLookup(t, s, "session", LookupResult{Live, "", -1})
	t.Log("TRANSITIONS: 무기한 → TTL → 연장 → 단축 → 무기한(빈 값), 이전 TTL 제거")
	mustSet(t, s, "session", "old", &ttl)
	clock.Advance(3 * time.Millisecond)
	// 만료 정리 전에 덮어써도 새 값과 TTL을 그대로 보존한다.
	ttl = 5
	mustSet(t, s, "session", "renewed", &ttl)
	expectLookup(t, s, "session", LookupResult{Live, "renewed", 5})
	clock.Advance(5 * time.Millisecond)
	expectLookup(t, s, "session", LookupResult{State: Expired, TTLMillis: -2})
	mustSet(t, s, "session", "reborn", nil)
	expectLookup(t, s, "session", LookupResult{Live, "reborn", -1})
	t.Log("RENEW: 만료 전후 재저장 모두 새 값·새 만료 정책 적용")
}

func TestStoreMissingExpiredAndDelete(t *testing.T) {
	s, clock := learningStore(t)
	checkDelete := func(key string, want DeleteResult) {
		t.Helper()
		got, err := s.Delete(key)
		if err != nil || got != want {
			t.Fatalf("Delete(%q)=(%+v, %v), want %+v", key, got, err, want)
		}
	}
	expectLookup(t, s, "absent", LookupResult{State: Missing, TTLMillis: -2})
	checkDelete("absent", DeleteResult{State: Missing})
	mustSet(t, s, "live", "", nil)
	checkDelete("live", DeleteResult{State: Live, Deleted: true})
	checkDelete("live", DeleteResult{State: Missing})
	ttl := int64(1)
	mustSet(t, s, "expired", "old", &ttl)
	clock.Advance(time.Millisecond)
	checkDelete("expired", DeleteResult{State: Expired})
	checkDelete("expired", DeleteResult{State: Missing})
	mustSet(t, s, "expired", "new", nil)
	expectLookup(t, s, "expired", LookupResult{Live, "new", -1})
	s.mu.Lock()
	count := s.table.Len()
	s.mu.Unlock()
	if count != 1 {
		t.Fatalf("physical size=%d, want 1", count)
	}
	t.Log("DELETE: 유효 키=true, 없음·만료=false; 만료 정리 후 재등록 값 보존")
}

func TestStoreValidationPreservesValueAndTTL(t *testing.T) {
	t.Run("key_rules_and_boundaries", func(t *testing.T) {
		s, _ := learningStore(t)
		invalid := []struct {
			name string
			key  string
			want error
		}{
			{"empty", "", ErrInvalidArgument},
			{"dot", ".", ErrInvalidArgument},
			{"dot_dot", "..", ErrInvalidArgument},
			{"invalid_utf8", string([]byte{0xff}), ErrInvalidArgument},
			{"nul", "a\x00b", ErrInvalidArgument},
			{"newline", "a\nb", ErrInvalidArgument},
			{"unicode_control", "a\u0085b", ErrInvalidArgument},
			{"key_257_bytes", strings.Repeat("k", 257), ErrLimitExceeded},
			{"utf8_key_258_bytes", strings.Repeat("가", 86), ErrLimitExceeded},
			// 길이 확인이 먼저다. 너무 긴 키는 내용을 훑지 않고 크기 초과로 거부한다.
			{"oversized_with_control", strings.Repeat("k", 256) + "\n", ErrLimitExceeded},
			{"oversized_invalid_utf8", strings.Repeat("k", 256) + string([]byte{0xff}), ErrLimitExceeded},
		}
		for _, tc := range invalid {
			t.Run(tc.name, func(t *testing.T) {
				if err := s.Set(tc.key, "value", nil); !errors.Is(err, tc.want) {
					t.Fatalf("Set error=%v, want %v", err, tc.want)
				}
				if _, err := s.Get(tc.key); !errors.Is(err, tc.want) {
					t.Fatalf("Get error=%v, want %v", err, tc.want)
				}
				if _, err := s.Delete(tc.key); !errors.Is(err, tc.want) {
					t.Fatalf("Delete error=%v, want %v", err, tc.want)
				}
			})
		}
		for _, key := range []string{
			strings.Repeat("k", 256), strings.Repeat("가", 85) + "a",
			" ", "a.b", "a..b", "user:101", "민지", "a/b", "a?b#c", "%2F", "a+b", "A", "a",
		} {
			mustSet(t, s, key, key, nil)
			expectLookup(t, s, key, LookupResult{Live, key, -1})
		}
		// 모든 허용 키가 후속 저장 후에도 서로 다른 키로 유지되는지 확인한다.
		for _, key := range []string{"A", "a", "a/b", "%2F"} {
			expectLookup(t, s, key, LookupResult{Live, key, -1})
		}
	})
	t.Run("value_and_ttl_boundaries", func(t *testing.T) {
		s, clock := learningStore(t)
		ascii := strings.Repeat("v", MaxValueBytes)
		mustSet(t, s, "ascii", ascii, nil)
		expectLookup(t, s, "ascii", LookupResult{Live, ascii, -1})
		utf8Value := strings.Repeat("가", 21845) + "a"
		mustSet(t, s, "utf8", utf8Value, nil)
		expectLookup(t, s, "utf8", LookupResult{Live, utf8Value, -1})
		max := MaxTTLMillis
		mustSet(t, s, "max", "largest", &max)
		expectLookup(t, s, "max", LookupResult{Live, "largest", MaxTTLMillis})
		clock.Advance(time.Nanosecond)
		expectLookup(t, s, "max", LookupResult{Live, "largest", MaxTTLMillis})
		clock.Advance(time.Duration(MaxTTLMillis)*time.Millisecond - time.Nanosecond)
		expectLookup(t, s, "max", LookupResult{State: Expired, TTLMillis: -2})
	})
	t.Run("invalid_update_preserves_state", func(t *testing.T) {
		s, clock := learningStore(t)
		ttl := int64(10)
		mustSet(t, s, "keep", "original", &ttl)
		clock.Advance(3 * time.Millisecond)
		for _, invalidTTL := range []int64{0, -1, MaxTTLMillis + 1, int64(1<<63 - 1)} {
			if err := s.Set("keep", "bad", &invalidTTL); !errors.Is(err, ErrInvalidArgument) {
				t.Fatalf("invalid TTL error=%v", err)
			}
			expectLookup(t, s, "keep", LookupResult{Live, "original", 7})
		}
		for _, value := range []string{strings.Repeat("x", MaxValueBytes+1), strings.Repeat("가", 21846)} {
			if err := s.Set("keep", value, nil); !errors.Is(err, ErrLimitExceeded) {
				t.Fatalf("oversized value error=%v", err)
			}
			expectLookup(t, s, "keep", LookupResult{Live, "original", 7})
		}
		clock.Advance(7 * time.Millisecond)
		// 잘못된 갱신은 만료된 항목의 정리나 변경도 하지 않는다.
		if err := s.Set("keep", "bad", new(int64)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
		expectLookup(t, s, "keep", LookupResult{State: Expired, TTLMillis: -2})
		mustSet(t, s, "forever", "original", nil)
		if err := s.Set("forever", "bad", new(int64)); !errors.Is(err, ErrInvalidArgument) {
			t.Fatal(err)
		}
		expectLookup(t, s, "forever", LookupResult{Live, "original", -1})
		t.Log("VALIDATION: 키·값 바이트 경계 및 TTL 범위 확인, 오류 시 기존 값·만료 시각 보존")
	})
}

func TestStoreConcurrentOperations(t *testing.T) {
	// 많은 연산을 수행하므로 이 테스트는 로그를 끄고 동시성에 집중한다.
	s := New(newFakeClock(), nil)
	const workers, keysPerWorker = 16, 80
	start := make(chan struct{})
	errs := make(chan error, workers)
	var wg sync.WaitGroup
	for worker := 0; worker < workers; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			<-start
			for i := 0; i < keysPerWorker; i++ {
				key := fmt.Sprintf("worker:%d:%d", worker, i)
				value := fmt.Sprintf("value:%d:%d", worker, i)
				if err := s.Set(key, value, nil); err != nil {
					errs <- err
					return
				}
				got, err := s.Get(key)
				if err != nil || got != (LookupResult{Live, value, -1}) {
					errs <- fmt.Errorf("own key %s: %+v, %v", key, got, err)
					return
				}
				deleted, err := s.Delete(key)
				if err != nil || deleted != (DeleteResult{Live, true}) {
					errs <- fmt.Errorf("own delete %s: %+v, %v", key, deleted, err)
					return
				}
				got, err = s.Get(key)
				if err != nil || got != (LookupResult{State: Missing, TTLMillis: -2}) {
					errs <- fmt.Errorf("deleted key %s: %+v, %v", key, got, err)
					return
				}
				if err := s.Set(key, value, nil); err != nil {
					errs <- err
					return
				}
				// 같은 키의 값과 TTL이 서로 다른 저장 작업의 것으로 섞이지 않는지 확인한다.
				ttl := int64(50)
				sharedValue := "ttl"
				var expiry *int64 = &ttl
				if worker%2 == 0 {
					sharedValue, expiry = "forever", nil
				}
				if err := s.Set("shared", sharedValue, expiry); err != nil {
					errs <- err
					return
				}
				got, err = s.Get("shared")
				valid := got == (LookupResult{Live, "ttl", 50}) ||
					got == (LookupResult{Live, "forever", -1}) ||
					got == (LookupResult{State: Missing, TTLMillis: -2})
				if err != nil || !valid {
					errs <- fmt.Errorf("mixed value/TTL: %+v, %v", got, err)
					return
				}
				if i%3 == 0 {
					if _, err := s.Delete("shared"); err != nil {
						errs <- err
						return
					}
				}
			}
		}(worker)
	}
	close(start)
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("concurrent operations did not complete: possible deadlock")
	}
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	mustSet(t, s, "shared", "final", nil)
	expectLookup(t, s, "shared", LookupResult{Live, "final", -1})
	for worker := 0; worker < workers; worker++ {
		for i := 0; i < keysPerWorker; i++ {
			key := fmt.Sprintf("worker:%d:%d", worker, i)
			value := fmt.Sprintf("value:%d:%d", worker, i)
			expectLookup(t, s, key, LookupResult{Live, value, -1})
		}
	}
	s.mu.Lock()
	count, capacity := s.table.Len(), s.table.Capacity()
	s.mu.Unlock()
	if count != workers*keysPerWorker+1 || capacity != 2048 {
		t.Fatalf("size=%d capacity=%d, want 1281/2048", count, capacity)
	}
	t.Log("CONCURRENT: 16명 동시 저장·조회·삭제, 1281키 보존, 값·TTL 혼합 없음, capacity=2048")
}

func TestExperimentWithoutExpirationCheck(t *testing.T) {
	if os.Getenv("MINI_REDIS_EXPERIMENT") != "skip-expiration-check" {
		t.Skip("실험 실행: MINI_REDIS_EXPERIMENT=skip-expiration-check 지정")
	}
	s, clock := learningStore(t)
	ttl := int64(1)
	mustSet(t, s, "session", "secret", &ttl)
	clock.Advance(time.Millisecond)
	// 잘못된 조회를 테스트 안에서만 흉내 낸다. 제품 코드는 변경하지 않는다.
	s.mu.Lock()
	raw, found := s.table.Get("session")
	s.mu.Unlock()
	expectLookup(t, s, "session", LookupResult{State: Expired, TTLMillis: -2})
	if !found || raw.Value != "secret" {
		t.Fatal("experiment setup failed")
	}
	t.Fatalf("의도한 실패: 만료 판단 생략 → 만료된 session을 %q로 반환; 정상 Get은 Expired", raw.Value)
}

func TestStoreConcurrentExpirationAndResize(t *testing.T) {
	t.Run("split_check_and_delete_loses_new_value", func(t *testing.T) {
		// 확인과 삭제를 서로 다른 락 구간으로 나누면 어떤 일이 생기는지 순서대로 재현한다.
		s, clock := learningStore(t)
		ttl := int64(1)
		mustSet(t, s, "session", "old", &ttl)
		clock.Advance(time.Millisecond)

		// ① 요청 A: 락 안에서 만료를 확인만 하고 락을 놓는다(읽기 락으로 확인했다고 가정).
		s.mu.Lock()
		record, found := s.table.Get("session")
		sawExpired := found && !clock.Now().Before(record.ExpiresAt)
		s.mu.Unlock()
		// ② 요청 B: 그 틈에 새 값을 무기한으로 저장한다.
		mustSet(t, s, "session", "new", nil)
		// ③ 요청 A: 다시 락을 잡고, 재확인 없이 ①의 판단대로 삭제한다.
		s.mu.Lock()
		if sawExpired {
			s.table.Delete("session")
		}
		s.mu.Unlock()
		expectLookup(t, s, "session", LookupResult{State: Missing, TTLMillis: -2})
		t.Log("SPLIT: 확인과 삭제를 나누면 B가 저장한 새 값이 사라진다 → Store는 둘을 한 락 안에서 끝낸다")

		// 같은 순서를 실제 Store.Get으로 실행하면 새 값이 보존된다.
		mustSet(t, s, "session", "old", &ttl)
		clock.Advance(time.Millisecond)
		mustSet(t, s, "session", "new", nil)
		expectLookup(t, s, "session", LookupResult{Live, "new", -1})
	})

	t.Run("concurrent_expire_overwrite_resize", func(t *testing.T) {
		s := New(newFakeClock(), nil)
		clock := s.clock.(*fakeClock)
		const keys, writers, readers = 200, 4, 8
		ttl := int64(1)
		for i := 0; i < keys; i++ {
			mustSet(t, s, fmt.Sprintf("old:%d", i), "expired", &ttl)
		}
		clock.Advance(time.Millisecond) // 이제 모든 old:* 항목은 만료 상태다.

		start := make(chan struct{})
		var wg sync.WaitGroup
		errs := make(chan error, writers+readers)
		for w := 0; w < writers; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				<-start
				for i := w; i < keys; i += writers {
					// 만료 항목을 새 무기한 값으로 덮어쓰고, 새 키로 리사이징을 유발한다.
					if err := s.Set(fmt.Sprintf("old:%d", i), "fresh", nil); err != nil {
						errs <- err
						return
					}
					if err := s.Set(fmt.Sprintf("grow:%d:%d", w, i), "x", nil); err != nil {
						errs <- err
						return
					}
				}
			}(w)
		}
		for r := 0; r < readers; r++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < keys; i++ {
					// 조회가 만료 정리를 일으킨다. 덮어쓴 값은 지우지 않아야 한다.
					got, err := s.Get(fmt.Sprintf("old:%d", i))
					ok := got.State == Expired || got.State == Missing ||
						got == (LookupResult{Live, "fresh", -1})
					if err != nil || !ok {
						errs <- fmt.Errorf("old:%d = %+v, %v", i, got, err)
						return
					}
				}
			}()
		}
		close(start)
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Fatal(err)
		}
		for i := 0; i < keys; i++ {
			expectLookup(t, s, fmt.Sprintf("old:%d", i), LookupResult{Live, "fresh", -1})
		}
		s.mu.Lock()
		count := s.table.Len()
		s.mu.Unlock()
		if count != keys*2 {
			t.Fatalf("physical size=%d, want %d", count, keys*2)
		}
		t.Log("EXPIRE+RESIZE: 만료 정리·덮어쓰기·확장이 겹쳐도 새 값 400키 모두 보존")
	})
}

// BenchmarkStoreParallel는 락 안에서 보내는 시간(연산 1회 비용)을 측정한다.
// HTTP 요청 1건(수십 µs)과 비교해 단일 락이 병목인지 판단하는 근거로 쓴다.
func BenchmarkStoreParallel(b *testing.B) {
	keys := make([]string, 1024)
	for i := range keys {
		keys[i] = fmt.Sprintf("product:%d", i)
	}
	for _, readPercent := range []int{90, 50} {
		b.Run(fmt.Sprintf("read%d", readPercent), func(b *testing.B) {
			s := New(nil, nil)
			for _, key := range keys {
				if err := s.Set(key, "value", nil); err != nil {
					b.Fatal(err)
				}
			}
			var worker sync.Mutex
			next := 0
			b.RunParallel(func(pb *testing.PB) {
				worker.Lock()
				i := next * 7919
				next++
				worker.Unlock()
				for pb.Next() {
					i++
					key := keys[i&1023]
					if i%100 < readPercent {
						_, _ = s.Get(key)
					} else {
						_ = s.Set(key, "value", nil)
					}
				}
			})
		})
	}
}
