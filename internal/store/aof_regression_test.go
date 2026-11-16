package store

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestMutationGateDeadlineAndCancellation(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	s.mutationTimeout = 30 * time.Millisecond
	mustSet(t, s, "keep", "old", nil)
	s.writeMu.Lock()
	var once sync.Once
	release := func() { once.Do(s.writeMu.Unlock) }
	t.Cleanup(release)
	start := time.Now()
	if err := s.Set("keep", "never-admitted", nil); !errors.Is(err, ErrPersistence) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("gate error=%v", err)
	}
	if time.Since(start) > time.Second {
		t.Fatal("gate wait not bounded")
	}
	if _, err := s.Delete("keep"); !errors.Is(err, ErrPersistence) {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.SetContext(ctx, "keep", "canceled", nil); !errors.Is(err, ErrPersistence) {
		t.Fatal(err)
	}
	expectLookup(t, s, "keep", LookupResult{Live, "old", -1})
	if s.PersistenceStatus().Written != 1 {
		t.Fatal("rejected request wrote to AOF")
	}
	t.Log("DEADLINE: 락 대기는 기한 후 거부, 취소된 대기 요청은 나중에도 실행하지 않음")
}

type blockedAppend struct {
	aofFile
	entered, release chan struct{}
}

func (f blockedAppend) Write(b []byte) (int, error) {
	close(f.entered)
	<-f.release
	return f.aofFile.Write(b)
}

func TestBlockedAppendBoundsCallerAndKeepsOrder(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	s.mutationTimeout = 30 * time.Millisecond
	mustSet(t, s, "keep", "old", nil)
	entered, unblock := make(chan struct{}), make(chan struct{})
	underlying := s.aof.file
	s.aof.file = blockedAppend{underlying, entered, unblock}
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() { result <- s.Set("keep", "late", nil) }()
	<-entered
	select {
	case err := <-result:
		if !errors.Is(err, ErrPersistence) || !errors.Is(err, ErrOutcomeUnknown) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("blocked Write trapped caller")
	}
	expectLookup(t, s, "keep", LookupResult{Live, "old", -1})
	if _, err := s.Delete("keep"); !errors.Is(err, ErrPersistence) {
		t.Fatal(err)
	}
	release()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.writeMu.lockContext(ctx); err != nil {
		t.Fatal(err)
	}
	s.aof.file = underlying
	s.writeMu.Unlock()
	expectLookup(t, s, "keep", LookupResult{Live, "late", -1})
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAOF(t, newFakeClock(), s.aof.opts)
	expectLookup(t, s, "keep", LookupResult{Live, "late", -1})
	t.Log("BLOCKED WRITE: caller는 기한 내 반환·조회 유지, 이미 시작한 쓰기는 나중에 순서대로 완료 가능")
}

func TestRewriteCutoverDeadline(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	s.mutationTimeout = 30 * time.Millisecond
	mustSet(t, s, "keep", "old", nil)
	entered, unblock := make(chan struct{}), make(chan struct{})
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	calls := 0
	s.aof.syncTemp = func(f *os.File) error {
		calls++
		if calls == 2 {
			close(entered)
			<-unblock
		}
		return f.Sync()
	}
	done := make(chan error, 1)
	go func() { done <- s.RewriteAOF() }()
	<-entered
	if err := s.Set("keep", "not-admitted", nil); !errors.Is(err, ErrPersistence) || !strings.Contains(err.Error(), "not admitted") {
		t.Fatal(err)
	}
	if _, err := s.Delete("keep"); !errors.Is(err, ErrPersistence) {
		t.Fatal(err)
	}
	expectLookup(t, s, "keep", LookupResult{Live, "old", -1})
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "keep", "after-cutover", nil)
	t.Log("CUTOVER: 교체 Sync를 멈춰도 새 변경은 기한 후 거부, 조회는 진행, 교체 완료 후 쓰기 재개")
}

func TestDeletionReserveAllowsDeletesWhenSetsAreFull(t *testing.T) {
	opts := testAOFOptions(t)
	opts.MaxBytes = 1500
	opts.RewriteMinBytes = 64
	s := openAOF(t, newFakeClock(), opts)
	var keys []string
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key:%d", i)
		err := s.Set(key, strings.Repeat("v", 80), nil)
		if errors.Is(err, ErrPersistence) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, key)
	}
	if len(keys) == 0 || len(keys) == 100 {
		t.Fatal("capacity setup failed")
	}
	if s.PersistenceStatus().DeleteReserveBytes == 0 {
		t.Fatal("missing delete budget")
	}
	for _, key := range keys {
		r, err := s.Delete(key)
		if err != nil || !r.Deleted {
			t.Fatalf("reserved Delete(%s): %+v %v", key, r, err)
		}
		status := s.PersistenceStatus()
		if status.Bytes+status.DeleteReserveBytes > opts.MaxBytes {
			t.Fatal("capacity exceeded")
		}
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAOF(t, newFakeClock(), opts)
	for _, key := range keys {
		expectLookup(t, s, key, LookupResult{State: Missing, TTLMillis: -2})
	}
	t.Logf("DELETE RESERVE: SET 용량 소진 후에도 %d키 모두 로그를 남겨 삭제, 재시작 후 부활 없음", len(keys))
}

func TestAOFInspectionAndRecoveryPreservesSource(t *testing.T) {
	for _, kind := range []string{"zero-tail", "incomplete", "middle"} {
		t.Run(kind, func(t *testing.T) {
			opts := testAOFOptions(t)
			s := openAOF(t, newFakeClock(), opts)
			mustSet(t, s, "first", "safe", nil)
			mustSet(t, s, "second", "later", nil)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			normal, err := os.ReadFile(opts.Path)
			if err != nil {
				t.Fatal(err)
			}
			damaged := append([]byte{}, normal...)
			switch kind {
			case "zero-tail":
				damaged = append(damaged, make([]byte, 64)...)
			case "incomplete":
				damaged = append(damaged, 0, 0)
			case "middle":
				frame, _ := encodeFrame(aofRecord{Op: "set", Key: "first", Value: []byte("safe")})
				damaged[len(aofMagic)+len(frame)+8] ^= 1
			}
			if err = os.WriteFile(opts.Path, damaged, 0600); err != nil {
				t.Fatal(err)
			}
			report, err := InspectAOF(opts.Path)
			if err != nil {
				t.Fatal(err)
			}
			if report.Problem == "" || report.ValidBytes >= report.Bytes {
				t.Fatalf("bad report: %+v", report)
			}
			if kind == "zero-tail" && !report.ZeroTail {
				t.Fatal("zero tail not reported")
			}
			if kind == "middle" && report.Records != 1 {
				t.Fatal("did not stop at first bad record")
			}
			out := filepath.Join(t.TempDir(), "recovered.aof")
			if _, err = RecoverAOF(opts.Path, out, false); err == nil {
				t.Fatal("loss not acknowledged")
			}
			if _, err = os.Stat(out); !os.IsNotExist(err) {
				t.Fatal("unapproved output created")
			}
			if _, err = RecoverAOF(opts.Path, opts.Path, true); err == nil {
				t.Fatal("source overwrite accepted")
			}
			if _, err = RecoverAOF(opts.Path, out, true); err != nil {
				t.Fatal(err)
			}
			if _, err = RecoverAOF(opts.Path, out, true); err == nil {
				t.Fatal("existing output overwritten")
			}
			after, _ := os.ReadFile(opts.Path)
			if !bytes.Equal(after, damaged) {
				t.Fatal("original modified")
			}
			fixed, err := InspectAOF(out)
			if err != nil || fixed.Problem != "" {
				t.Fatalf("recovered report: %+v %v", fixed, err)
			}
			opts.Path = out
			s = openAOF(t, newFakeClock(), opts)
			expectLookup(t, s, "first", LookupResult{Live, "safe", -1})
			if _, err = InspectAOF(out); err == nil {
				t.Fatal("inspected active server file")
			}
			if kind == "middle" {
				expectLookup(t, s, "second", LookupResult{State: Missing, TTLMillis: -2})
			}
		})
	}
	t.Log("RECOVERY TOOL: 0 꼬리도 기본 보존·거부, 명시 동의 후 새 파일로만 복구, 활성 파일·덮어쓰기 거부")
}

func TestFullLiveDataRewriteBacksOff(t *testing.T) {
	opts := testAOFOptions(t)
	opts.MaxBytes = 1500
	opts.RewriteMinBytes = 1499
	opts.SyncInterval = 10 * time.Millisecond
	opts.MaxUnsyncedAge = time.Second
	s := openAOF(t, newFakeClock(), opts)
	for i := 0; i < 100; i++ {
		err := s.Set(fmt.Sprintf("key:%d", i), strings.Repeat("v", 80), nil)
		if errors.Is(err, ErrPersistence) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	deadline := time.After(3 * time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		s.aof.mu.Lock()
		done := s.aof.rewrites > 0 && !s.aof.backgroundRewrite
		retry := s.aof.retryDelay
		s.aof.mu.Unlock()
		if done {
			if retry <= 0 {
				t.Fatal("no-progress rewrite did not back off")
			}
			break
		}
		select {
		case <-deadline:
			t.Fatal("rewrite did not finish")
		case <-tick.C:
		}
	}
	if r, err := s.Delete("key:0"); err != nil || !r.Deleted {
		t.Fatalf("reserved delete: %+v %v", r, err)
	}
}

func TestHighOccupancyDeletesDoNotRewriteEveryTime(t *testing.T) {
	opts := testAOFOptions(t)
	opts.MaxBytes = 30000
	opts.RewriteMinBytes = 4000
	opts.SyncInterval = 2 * time.Millisecond
	opts.MaxUnsyncedAge = time.Second
	s := openAOF(t, newFakeClock(), opts)
	mustSet(t, s, "live", strings.Repeat("v", 18000), nil)
	if err := s.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	before := s.PersistenceStatus()
	if before.Bytes+before.DeleteReserveBytes < opts.MaxBytes*3/4 {
		t.Fatal("fixture below 75%")
	}
	for i := 0; i < 16; i++ {
		mustSet(t, s, "temporary", "small", nil)
		if r, err := s.Delete("temporary"); err != nil || !r.Deleted {
			t.Fatalf("delete %+v %v", r, err)
		}
		// Allow several background ticks after each DEL: the old condition
		// triggered a complete rewrite here on every iteration.
		time.Sleep(15 * time.Millisecond)
	}
	after := s.PersistenceStatus()
	if after.Rewrites-before.Rewrites > 2 {
		t.Fatalf("16 deletes caused %d rewrites", after.Rewrites-before.Rewrites)
	}
	expectLookup(t, s, "live", LookupResult{Live, strings.Repeat("v", 18000), -1})
}

func TestRewritePressureIncludesDeletionReserve(t *testing.T) {
	j := &journal{opts: AOFOptions{MaxBytes: 10000, RewriteMinBytes: 1000}, size: 6000, baseSize: 5000, deleteReserve: 2000}
	if !j.shouldRewriteLocked(time.Now()) {
		t.Fatal("reservation pressure ignored")
	}
	j.deleteReserve = 0
	if j.shouldRewriteLocked(time.Now()) {
		t.Fatal("unexpected low-pressure rewrite")
	}
	j.deleteReserve = 2000
	j.nextRewrite = time.Now().Add(time.Hour)
	if j.shouldRewriteLocked(time.Now()) {
		t.Fatal("pressure bypassed backoff")
	}
}

func TestDeleteRecordLengthMatchesEncoding(t *testing.T) {
	for _, key := range []string{"plain", `quote"slash\`, "<>&", "한글🙂", "line\u2028", "\u2028\u2029", strings.Repeat("a", MaxKeyBytes)} {
		frame, err := encodeFrame(aofRecord{Op: "del", Key: key})
		if err != nil {
			t.Fatal(err)
		}
		if got := deleteRecordBytes(key); got != int64(len(frame)) {
			t.Fatalf("key=%q length=%d want=%d", key, got, len(frame))
		}
	}
	if n := testing.AllocsPerRun(100, func() { _ = deleteRecordBytes("한글<&>") }); n != 0 {
		t.Fatalf("allocations=%v", n)
	}
}

func TestMutationPanicLogsAndReleasesMemoryLock(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	var logs bytes.Buffer
	s.logger = slog.New(slog.NewTextHandler(&logs, nil))
	err := s.runMutation(context.Background(), func() error {
		s.mu.Lock()
		defer s.mu.Unlock()
		panic("injected-worker-failure")
	})
	if !errors.Is(err, ErrOutcomeUnknown) || !errors.Is(err, ErrPersistence) {
		t.Fatal(err)
	}
	for _, want := range []string{"MUTATION_PANIC", "injected-worker-failure", "goroutine", "TestMutationPanic"} {
		if !strings.Contains(logs.String(), want) {
			t.Fatalf("missing %q in %s", want, logs.String())
		}
	}
	done := make(chan struct{})
	go func() { s.Get("absent"); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("memory lock retained")
	}
	if err := s.Set("later", "v", nil); !errors.Is(err, ErrPersistence) || errors.Is(err, ErrOutcomeUnknown) {
		t.Fatalf("failed worker admitted write: %v", err)
	}
}
