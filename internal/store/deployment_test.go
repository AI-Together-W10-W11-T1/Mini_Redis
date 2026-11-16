package store

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/hashtable"
)

func TestExpiredKeysDoNotExhaustAOFReservations(t *testing.T) {
	opts := testAOFOptions(t)
	opts.MaxBytes = 64 << 10
	opts.RewriteMinBytes = 4 << 10
	clock := newFakeClock()
	s := openAOF(t, clock, opts)
	ttl := int64(1)
	for i := 0; i < 5000; i++ {
		if err := s.Set(fmt.Sprintf("expired:%d", i), "v", &ttl); err != nil {
			t.Fatalf("key %d: %v", i, err)
		}
		clock.Advance(2 * time.Second)
		if i%200 == 199 {
			if err := s.RewriteAOF(); err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := s.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	n := s.table.Len()
	used := s.usedBytes
	s.mu.Unlock()
	if n != 0 || used != 0 || s.PersistenceStatus().DeleteReserveBytes != 0 {
		t.Fatalf("keys=%d used=%d status=%+v", n, used, s.PersistenceStatus())
	}
	mustSet(t, s, "new", "still-writable", nil)
}

func TestDataCapacityAccounting(t *testing.T) {
	clock := newFakeClock()
	s := NewWithLimit(clock, nil, 140)
	defer s.Close()
	ttl := int64(100)
	mustSet(t, s, "a", "12345", &ttl) // 70 bytes
	mustSet(t, s, "b", "12345", nil)
	if err := s.Set("c", "", nil); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatal(err)
	}
	if err := s.Set("a", "123456", nil); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatal(err)
	}
	expectLookup(t, s, "a", LookupResult{Live, "12345", 100})
	mustSet(t, s, "a", "1", nil)
	if _, err := s.Delete("b"); err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "c", "", nil)
	s.mu.Lock()
	used := s.usedBytes
	s.mu.Unlock()
	if used != 131 {
		t.Fatalf("used=%d", used)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Set("x", "", nil); !errors.Is(err, ErrPersistence) {
		t.Fatal(err)
	}
}

func TestCapacityFailureDoesNotAppendAndReplayCanShrink(t *testing.T) {
	opts := testAOFOptions(t)
	s, err := OpenPersistentWithLimit(nil, nil, opts, 100)
	if err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "a", "12345", nil)
	before := s.PersistenceStatus().Written
	if err := s.Set("b", "12345", nil); !errors.Is(err, ErrCapacityExceeded) {
		t.Fatal(err)
	}
	if s.PersistenceStatus().Written != before {
		t.Fatal("capacity failure appended")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	s, err = OpenPersistentWithLimit(nil, nil, opts, 67)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	expectLookup(t, s, "a", LookupResult{Live, "12345", -1})
	mustSet(t, s, "a", "1", nil)
	if _, err := s.Delete("a"); err != nil {
		t.Fatal(err)
	}
	mustSet(t, s, "b", "1", nil)
}

func TestActiveExpirationMemoryAndClose(t *testing.T) {
	clock := newFakeClock()
	s := NewWithLimit(clock, nil, 100)
	ttl := int64(1)
	mustSet(t, s, "old", "v", &ttl)
	clock.Advance(time.Second)
	deadline := time.Now().Add(2 * time.Second)
	for {
		s.mu.Lock()
		n := s.table.Len()
		used := s.usedBytes
		s.mu.Unlock()
		if n == 0 {
			if used != 0 {
				t.Fatal(used)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("unread expired key not reclaimed")
		}
		time.Sleep(5 * time.Millisecond)
	}
	mustSet(t, s, "new", "v", nil)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-s.maintenanceDone:
	default:
		t.Fatal("maintenance still running")
	}
}

func TestSweepConcurrentMutationReservationInvariant(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	clock := s.clock.(*fakeClock)
	var wg sync.WaitGroup
	for worker := 0; worker < 3; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			ttl := int64(1)
			for i := 0; i < 150; i++ {
				key := fmt.Sprintf("w%d:%d", worker, i%30)
				if err := s.Set(key, "v", &ttl); err != nil {
					t.Error(err)
					return
				}
				clock.Advance(time.Millisecond)
				s.Get(key)
				if i%3 == 0 {
					s.Delete(key)
				}
				s.sweepExpired(context.Background(), 16)
			}
		}(worker)
	}
	wg.Wait()
	if err := s.RewriteAOF(); err != nil {
		t.Fatal(err)
	}
	// An expired physical node removed during rewrite must reserve anew if SET again.
	mustSet(t, s, "w0:0", "fresh", nil)
	s.writeMu.Lock()
	s.mu.Lock()
	var liveReserve int64
	now := clock.Now()
	s.table.Range(func(key string, r hashtable.Record) bool {
		if r.ExpiresAt.IsZero() || now.Before(r.ExpiresAt) {
			liveReserve += deleteRecordBytes(key)
		}
		return true
	})
	s.mu.Unlock()
	status := s.PersistenceStatus()
	s.writeMu.Unlock()
	if status.DeleteReserveBytes < liveReserve || status.Bytes+status.DeleteReserveBytes > s.aof.opts.MaxBytes {
		t.Fatalf("reserve=%d need=%d", status.DeleteReserveBytes, liveReserve)
	}
}

func TestHealthDoesNotBlockBehindJournal(t *testing.T) {
	s := openAOF(t, newFakeClock(), testAOFOptions(t))
	s.aof.mu.Lock()
	enabled, healthy := s.PersistenceHealth()
	s.aof.mu.Unlock()
	if !enabled || !healthy {
		t.Fatal("ordinary metadata contention reported unhealthy")
	}
	s.aof.mu.Lock()
	s.aof.fatal = errors.New("injected failure")
	s.aof.failed.Store(true)
	s.aof.mu.Unlock()
	if _, healthy := s.PersistenceHealth(); healthy {
		t.Fatal("fatal journal healthy")
	}
}

type panicMaintenanceClock struct{}

func (panicMaintenanceClock) Now() time.Time { panic("injected expiration clock panic") }
func TestMaintenancePanicIsContained(t *testing.T) {
	s := New(panicMaintenanceClock{}, nil)
	select {
	case <-s.maintenanceDone:
	case <-time.After(time.Second):
		t.Fatal("maintenance panic not contained")
	}
	if !s.mutationFailed.Load() {
		t.Fatal("panic did not fail closed")
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if s.mu.TryLock() {
		s.mu.Unlock()
	} else {
		t.Fatal("panic retained memory lock")
	}
}

func TestCapacityPressureReclaimsAndReservesReplacement(t *testing.T) {
	opts := testAOFOptions(t)
	clock := newFakeClock()
	s, err := OpenPersistentWithLimit(clock, nil, opts, 140)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	// Disable the timer to prove admission itself reclaims expired entries.
	s.maintenanceOnce.Do(func() { close(s.maintenanceStop); <-s.maintenanceDone })
	ttl := int64(1)
	mustSet(t, s, "a", "12345", &ttl)
	mustSet(t, s, "b", "12345", &ttl)
	clock.Advance(time.Second)
	// Growing a's value initially exceeds the full store. Reclamation removes
	// a itself, so SET must re-read found and allocate a NEW DEL reservation.
	mustSet(t, s, "a", "123456", nil)
	expectLookup(t, s, "a", LookupResult{Live, "123456", -1})
	s.mu.Lock()
	used := s.usedBytes
	s.mu.Unlock()
	if used != 71 {
		t.Fatalf("used=%d", used)
	}
	if r, err := s.Delete("a"); err != nil || !r.Deleted {
		t.Fatalf("delete %+v %v", r, err)
	}
	if got := s.PersistenceStatus().DeleteReserveBytes; got != 0 {
		t.Fatalf("reserve=%d", got)
	}
}

func TestHealthStableDuringConcurrentWrites(t *testing.T) {
	opts := testAOFOptions(t)
	opts.SyncInterval = 10 * time.Millisecond
	opts.MaxUnsyncedAge = 3 * time.Second
	s := openAOF(t, nil, opts)
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for worker := 0; worker < 8; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			key := fmt.Sprintf("worker:%d", worker)
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := s.Set(key, "v", nil); err != nil {
					t.Error(err)
					return
				}
			}
		}(worker)
	}
	unhealthy := 0
	ticker := time.NewTicker(500 * time.Microsecond)
	for i := 0; i < 2000; i++ {
		<-ticker.C
		if _, healthy := s.PersistenceHealth(); !healthy {
			unhealthy++
		}
	}
	ticker.Stop()
	close(stop)
	wg.Wait()
	if unhealthy != 0 {
		t.Fatalf("normal write contention: %d/2000 unhealthy", unhealthy)
	}
}

func TestHealthDetectsStalledFirstWrite(t *testing.T) {
	if testing.Short() {
		t.Skip("real 3-second stalled disk test")
	}
	opts := DefaultAOFOptions(testAOFOptions(t).Path)
	s := openAOF(t, nil, opts)
	entered, unblock := make(chan struct{}), make(chan struct{})
	underlying := s.aof.file
	s.aof.file = blockedAppend{underlying, entered, unblock}
	var once sync.Once
	release := func() { once.Do(func() { close(unblock) }) }
	t.Cleanup(release)
	result := make(chan error, 1)
	go func() { result <- s.Set("blocked", "v", nil) }()
	<-entered
	if _, healthy := s.PersistenceHealth(); !healthy {
		t.Fatal("normal in-progress write unhealthy")
	}
	time.Sleep(3100 * time.Millisecond)
	if _, healthy := s.PersistenceHealth(); healthy {
		t.Fatal("3s stalled Write healthy")
	}
	release()
	<-result
	// Wait for the worker, not just its already timed-out caller.
	s.writeMu.Lock()
	s.writeMu.Unlock()
}

func TestHealthMonotonicDirtyAgeAndSyncRecovery(t *testing.T) {
	s := openAOF(t, nil, testAOFOptions(t))
	mustSet(t, s, "dirty", "v", nil)
	s.aof.dirtySinceTick.Store(s.aof.healthTick(time.Now().Add(-s.aof.opts.MaxUnsyncedAge - time.Second)))
	if _, healthy := s.PersistenceHealth(); healthy {
		t.Fatal("overdue unsynced state healthy")
	}
	if err := s.aof.sync(); err != nil {
		t.Fatal(err)
	}
	if _, healthy := s.PersistenceHealth(); !healthy {
		t.Fatal("sync did not clear dirty health")
	}
}

func TestExpirationSustainedShortTTL(t *testing.T) {
	if testing.Short() {
		t.Skip("10-second real-clock 5000 keys/s expiry workload")
	}
	for _, persistent := range []bool{false, true} {
		name := "memory"
		if persistent {
			name = "aof"
		}
		t.Run(name, func(t *testing.T) {
			var s *Store
			if persistent {
				var err error
				opts := DefaultAOFOptions(testAOFOptions(t).Path)
				s, err = OpenPersistentWithLimit(nil, nil, opts, 1<<20)
				if err != nil {
					t.Fatal(err)
				}
			} else {
				s = NewWithLimit(nil, nil, 1<<20)
			}
			defer s.Close()
			ttl := int64(50)
			start := time.Now()
			schedule := start
			refused := 0
			for batch := 0; batch < 1000; batch++ {
				target := schedule.Add(time.Duration(batch) * 10 * time.Millisecond)
				if wait := time.Until(target); wait > 0 {
					time.Sleep(wait)
				} else if -wait > 50*time.Millisecond {
					// Do not turn a paused CI process into an artificial catch-up flood.
					schedule = time.Now().Add(-time.Duration(batch) * 10 * time.Millisecond)
				}
				for offset := 0; offset < 50; offset++ {
					key := fmt.Sprintf("ttl:%d", batch*50+offset)
					if err := s.Set(key, "v", &ttl); errors.Is(err, ErrCapacityExceeded) {
						refused++
					} else if err != nil {
						t.Fatal(err)
					}
				}
			}
			s.mu.Lock()
			physical := s.table.Len()
			s.mu.Unlock()
			if refused != 0 || physical > 2000 {
				t.Fatalf("refused=%d physical=%d (ideal live ~250)", refused, physical)
			}
			t.Logf("50000 keys, target 5000/s, TTL 50ms: rejected=%d physical=%d elapsed=%s", refused, physical, time.Since(start))
		})
	}
}
