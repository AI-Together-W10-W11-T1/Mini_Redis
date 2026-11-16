package store

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func testAOFOptions(t *testing.T) AOFOptions {
	t.Helper()
	opts := DefaultAOFOptions(filepath.Join(t.TempDir(), "data.aof"))
	// 대부분의 테스트는 기다리지 않고 Sync를 직접 호출한다.
	opts.SyncInterval = 24 * time.Hour
	opts.MaxUnsyncedAge = 48 * time.Hour
	return opts
}

func openAOF(t *testing.T, clock Clock, opts AOFOptions) *Store {
	t.Helper()
	s, err := OpenPersistent(clock, nil, opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestAOFReplayValueDeleteAndTTL(t *testing.T) {
	opts := testAOFOptions(t)
	clock := newFakeClock()
	s := openAOF(t, clock, opts)
	ttl := int64(100)
	mustSet(t, s, "user:101", "Minji", nil)
	mustSet(t, s, "user:101", "Jisoo", nil)
	mustSet(t, s, "empty", "", nil)
	mustSet(t, s, "binary", string([]byte{0xff, 0, 0xfe}), nil)
	mustSet(t, s, "timed", "value", &ttl)
	mustSet(t, s, "deleted", "gone", nil)
	if _, err := s.Delete("deleted"); err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "old-unlimited", "must-not-revive", nil)
	short := int64(1)
	mustSet(t, s, "old-unlimited", "expires", &short)
	max := MaxTTLMillis
	mustSet(t, s, "max", "long", &max)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(25 * time.Millisecond)
	s = openAOF(t, clock, opts)
	expectLookup(t, s, "user:101", LookupResult{Live, "Jisoo", -1})
	expectLookup(t, s, "empty", LookupResult{Live, "", -1})
	expectLookup(t, s, "binary", LookupResult{Live, string([]byte{0xff, 0, 0xfe}), -1})
	expectLookup(t, s, "timed", LookupResult{Live, "value", 75})
	expectLookup(t, s, "deleted", LookupResult{State: Missing, TTLMillis: -2})
	expectLookup(t, s, "old-unlimited", LookupResult{State: Missing, TTLMillis: -2})
	expectLookup(t, s, "max", LookupResult{Live, "long", MaxTTLMillis - 25})
	if err := s.Set("bad", "bad", new(int64)); !errors.Is(err, ErrInvalidArgument) {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAOF(t, clock, opts)
	expectLookup(t, s, "bad", LookupResult{State: Missing, TTLMillis: -2})
	t.Log("REPLAY: 갱신·삭제·빈 값·바이너리 복원, 중단 시간만큼 TTL 감소, 만료된 이전 값 부활 방지")
}

func TestAOFTailRepairAndCorruption(t *testing.T) {
	frame, err := encodeFrame(aofRecord{Op: "set", Key: "partial", Value: []byte("unfinished")})
	if err != nil {
		t.Fatal(err)
	}
	for _, tail := range [][]byte{{0, 0}, {0, 0, 0, 100, '{'}, frame[:10], frame[:len(frame)-2]} {
		opts := testAOFOptions(t)
		s := openAOF(t, newFakeClock(), opts)
		mustSet(t, s, "keep", "safe", nil)
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		before, err := os.ReadFile(opts.Path)
		if err != nil {
			t.Fatal(err)
		}
		if err = os.WriteFile(opts.Path, append(append([]byte{}, before...), tail...), 0600); err != nil {
			t.Fatal(err)
		}
		s = openAOF(t, newFakeClock(), opts)
		expectLookup(t, s, "keep", LookupResult{Live, "safe", -1})
		mustSet(t, s, "after", "new", nil)
		if err = s.Close(); err != nil {
			t.Fatal(err)
		}
		s = openAOF(t, newFakeClock(), opts)
		expectLookup(t, s, "after", LookupResult{Live, "new", -1})
	}
	for _, where := range []string{"header", "length", "checksum", "middle"} {
		t.Run(where, func(t *testing.T) {
			opts := testAOFOptions(t)
			s := openAOF(t, newFakeClock(), opts)
			mustSet(t, s, "a", "first", nil)
			mustSet(t, s, "b", "second", nil)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			data, err := os.ReadFile(opts.Path)
			if err != nil {
				t.Fatal(err)
			}
			switch where {
			case "header":
				data[0] ^= 1
			case "length":
				data[len(aofMagic)+3] ^= 1
			case "checksum":
				data[len(data)-1] ^= 1
			case "middle":
				data[len(aofMagic)+8] ^= 1
			}
			if err = os.WriteFile(opts.Path, data, 0600); err != nil {
				t.Fatal(err)
			}
			if broken, err := OpenPersistent(newFakeClock(), nil, opts); err == nil {
				broken.Close()
				t.Fatal("corrupt file accepted")
			}
			after, _ := os.ReadFile(opts.Path)
			if !bytes.Equal(data, after) {
				t.Fatal("corrupt source modified")
			}
		})
	}
	t.Log("RECOVERY: 마지막 불완전 기록만 제거; 체크섬·헤더·중간 손상은 파일을 보존하고 시작 거부")
}

type failingAOFFile struct {
	aofFile
	writeErr, syncErr error
	short             bool
}

func (f failingAOFFile) Write(b []byte) (int, error) {
	if f.writeErr != nil {
		return 0, f.writeErr
	}
	if f.short {
		return len(b) / 2, nil
	}
	return f.aofFile.Write(b)
}
func (f failingAOFFile) Sync() error {
	if f.syncErr != nil {
		return f.syncErr
	}
	return f.aofFile.Sync()
}

func TestAOFFailureAndSizeLimit(t *testing.T) {
	for _, failure := range []string{"write", "short-write", "sync"} {
		t.Run(failure, func(t *testing.T) {
			s := openAOF(t, newFakeClock(), testAOFOptions(t))
			mustSet(t, s, "keep", "old", nil)
			if err := s.aof.sync(); err != nil {
				t.Fatal(err)
			}
			wrapped := failingAOFFile{aofFile: s.aof.file}
			switch failure {
			case "write":
				wrapped.writeErr = errors.New("disk full")
			case "short-write":
				wrapped.short = true
			case "sync":
				wrapped.syncErr = errors.New("sync error")
			}
			s.aof.file = wrapped
			if failure == "sync" {
				mustSet(t, s, "pending", "accepted-before-sync", nil)
				if err := s.aof.sync(); !errors.Is(err, ErrPersistence) {
					t.Fatal(err)
				}
			}
			if err := s.Set("keep", "bad", nil); !errors.Is(err, ErrPersistence) {
				t.Fatalf("write error=%v", err)
			}
			if _, err := s.Delete("keep"); !errors.Is(err, ErrPersistence) {
				t.Fatalf("delete error=%v", err)
			}
			expectLookup(t, s, "keep", LookupResult{Live, "old", -1})
			if s.PersistenceStatus().Error == "" {
				t.Fatal("failure not exposed")
			}
		})
	}
	t.Run("hard-limit", func(t *testing.T) {
		opts := testAOFOptions(t)
		opts.RewriteMinBytes = 64
		opts.MaxBytes = 300
		s := openAOF(t, newFakeClock(), opts)
		for i := 0; i < 20; i++ {
			err := s.Set("key", strings.Repeat("x", 40), nil)
			if errors.Is(err, ErrPersistence) {
				break
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		if err := s.Set("key", strings.Repeat("x", 40), nil); !errors.Is(err, ErrPersistence) {
			t.Fatalf("limit error=%v", err)
		}
		if s.PersistenceStatus().Bytes > opts.MaxBytes {
			t.Fatal("size limit exceeded")
		}
		if err := s.RewriteAOF(); err != nil {
			t.Fatal(err)
		}
		mustSet(t, s, "key", "after-rewrite", nil)
	})
	t.Run("sync-overdue", func(t *testing.T) {
		s := openAOF(t, newFakeClock(), testAOFOptions(t))
		mustSet(t, s, "keep", "old", nil)
		s.aof.mu.Lock()
		s.aof.dirtySince = time.Now().Add(-72 * time.Hour)
		s.aof.mu.Unlock()
		if err := s.Set("keep", "bad", nil); !errors.Is(err, ErrPersistence) {
			t.Fatal(err)
		}
		expectLookup(t, s, "keep", LookupResult{Live, "old", -1})
		if err := s.aof.sync(); err != nil {
			t.Fatal(err)
		}
		mustSet(t, s, "keep", "resumed", nil)
	})
	t.Log("FAIL-CLOSED: 기록·Sync 오류와 크기·지연 상한에서 변경 거부, 기존 값 조회 유지")
}

type blockingSyncFile struct {
	aofFile
	started, release chan struct{}
}

func (f blockingSyncFile) Sync() error { close(f.started); <-f.release; return f.aofFile.Sync() }

func TestAOFSyncDoesNotBlockReads(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	mustSet(t, s, "key", "value", nil)
	started, release := make(chan struct{}), make(chan struct{})
	underlying := s.aof.file
	s.aof.file = blockingSyncFile{underlying, started, release}
	done := make(chan error, 1)
	go func() { done <- s.aof.sync() }()
	<-started
	readDone := make(chan error, 1)
	go func() {
		r, err := s.Get("key")
		if err == nil && r.Value != "value" {
			err = fmt.Errorf("bad value")
		}
		readDone <- err
	}()
	select {
	case err := <-readDone:
		if err != nil {
			t.Error(err)
		}
	case <-time.After(time.Second):
		t.Error("Get blocked by disk Sync")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	s.aof.file = underlying
	t.Log("READS: 디스크 Sync를 일부러 멈춰도 조회는 계속 완료")
}

func TestAOFRewriteConcurrentChanges(t *testing.T) {
	opts := testAOFOptions(t)
	clock := newFakeClock()
	s := openAOF(t, clock, opts)
	for i := 0; i < 100; i++ {
		mustSet(t, s, "hot", fmt.Sprint(i), nil)
	}
	mustSet(t, s, "gone", "old", nil)
	before := s.PersistenceStatus().Bytes
	started, release := make(chan struct{}), make(chan struct{})
	s.aof.createTemp = func(dir, pattern string) (*os.File, error) {
		close(started)
		<-release
		return os.CreateTemp(dir, pattern)
	}
	done := make(chan error, 1)
	go func() { done <- s.RewriteAOF() }()
	<-started
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := 0; i < 25; i++ {
				if err := s.Set(fmt.Sprintf("worker:%d:%d", w, i), "new", nil); err != nil {
					errs <- err
					return
				}
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	mustSet(t, s, "hot", "latest", nil)
	if _, err := s.Delete("gone"); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if s.PersistenceStatus().Rewrites != 1 {
		t.Fatal("rewrite not recorded")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAOF(t, clock, opts)
	expectLookup(t, s, "hot", LookupResult{Live, "latest", -1})
	expectLookup(t, s, "gone", LookupResult{State: Missing, TTLMillis: -2})
	for worker := 0; worker < 8; worker++ {
		for i := 0; i < 25; i++ {
			expectLookup(t, s, fmt.Sprintf("worker:%d:%d", worker, i), LookupResult{Live, "new", -1})
		}
	}
	if err := s.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	t.Logf("REWRITE: 동시 변경 200키·최신 값·삭제 복원; initial_bytes=%d compact_bytes=%d", before, s.PersistenceStatus().Bytes)
}

func TestAOFRewriteOverflowAndFailureKeepOriginal(t *testing.T) {
	for _, kind := range []string{"overflow", "temp-failure"} {
		t.Run(kind, func(t *testing.T) {
			opts := testAOFOptions(t)
			opts.MaxRewriteDeltaBytes = 1
			s := openAOF(t, newFakeClock(), opts)
			mustSet(t, s, "keep", "old", nil)
			if kind == "temp-failure" {
				s.aof.createTemp = func(string, string) (*os.File, error) { return nil, io.ErrClosedPipe }
				if err := s.RewriteAOF(); err == nil {
					t.Fatal("expected rewrite failure")
				}
			} else {
				started, release := make(chan struct{}), make(chan struct{})
				s.aof.createTemp = func(dir, pattern string) (*os.File, error) {
					close(started)
					<-release
					return os.CreateTemp(dir, pattern)
				}
				done := make(chan error, 1)
				go func() { done <- s.RewriteAOF() }()
				<-started
				mustSet(t, s, "keep", "updated", nil)
				close(release)
				if err := <-done; err == nil {
					t.Fatal("expected delta overflow")
				}
			}
			mustSet(t, s, "after", "still-writable", nil)
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}
			s = openAOF(t, newFakeClock(), opts)
			expectLookup(t, s, "after", LookupResult{Live, "still-writable", -1})
			value := "old"
			if kind == "overflow" {
				value = "updated"
			}
			expectLookup(t, s, "keep", LookupResult{Live, value, -1})
		})
	}
}

func TestAOFBackgroundSyncRewriteAndLock(t *testing.T) {
	opts := testAOFOptions(t)
	opts.SyncInterval = 10 * time.Millisecond
	opts.MaxUnsyncedAge = time.Second
	opts.RewriteMinBytes = 128
	s := openAOF(t, newFakeClock(), opts)
	if duplicate, err := OpenPersistent(newFakeClock(), nil, opts); err == nil {
		duplicate.Close()
		t.Fatal("two owners accepted")
	}
	for i := 0; i < 20; i++ {
		mustSet(t, s, "hot", "value", nil)
	}
	deadline := time.After(3 * time.Second)
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		status := s.PersistenceStatus()
		if status.Written == status.Synced && status.Rewrites > 0 {
			break
		}
		select {
		case <-deadline:
			t.Fatal("background persistence did not run")
		case <-ticker.C:
		}
	}
	if s.PersistenceStatus().Bytes >= 500 {
		t.Fatal("rewrite did not shrink duplicate records")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s = openAOF(t, newFakeClock(), opts)
	expectLookup(t, s, "hot", LookupResult{Live, "value", -1})
	t.Log("BACKGROUND: 주기 Sync·자동 재작성·중복 프로세스 잠금·종료 후 재시작 확인")
}

func TestAOFDebugLogging(t *testing.T) {
	var logs bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&logs, &slog.HandlerOptions{Level: slog.LevelDebug}))
	opts := testAOFOptions(t)
	s, err := OpenPersistent(newFakeClock(), logger, opts)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	mustSet(t, s, "visible-key", "PRIVATE-VALUE", nil)
	if err = s.aof.sync(); err != nil {
		t.Fatal(err)
	}
	if err = s.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "PRIVATE-VALUE") {
		t.Fatal("value leaked into log")
	}
	for _, event := range []string{"AOF_REPLAY", "AOF_APPEND", "AOF_SYNC", "AOF_REWRITE"} {
		if !strings.Contains(logs.String(), event) {
			t.Fatal("missing log: " + event)
		}
	}
	for _, line := range strings.Split(logs.String(), "\n") {
		if strings.Contains(line, "msg=AOF_") {
			t.Log(line)
		}
	}
}

func TestExperimentResetTTLOnReplay(t *testing.T) {
	if os.Getenv("MINI_REDIS_EXPERIMENT") != "reset-ttl-on-replay" {
		t.Skip("실험 실행: MINI_REDIS_EXPERIMENT=reset-ttl-on-replay 지정")
	}
	opts := testAOFOptions(t)
	clock := newFakeClock()
	s := openAOF(t, clock, opts)
	ttl := int64(100)
	mustSet(t, s, "session", "value", &ttl)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	clock.Advance(60 * time.Millisecond)
	s = openAOF(t, clock, opts)
	expectLookup(t, s, "session", LookupResult{Live, "value", 40})
	// 파일 대신 최초 TTL로 다시 Set하는 잘못된 복구를 테스트 안에서만 재현한다.
	bad := New(clock, nil)
	mustSet(t, bad, "session", "value", &ttl)
	r, err := bad.Get("session")
	if err != nil || r.TTLMillis != 100 {
		t.Fatal("experiment setup failed")
	}
	t.Fatalf("의도한 실패: 최초 TTL로 재저장하면 남은 TTL이 %dms로 연장됨; 정상 복구는 40ms", r.TTLMillis)
}
