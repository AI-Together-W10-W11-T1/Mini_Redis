package store

import (
	"fmt"
	"testing"
	"time"
)

// After a burst of short-lived keys the bucket array must be given back, not
// kept at its peak size for the rest of the process lifetime.
func TestBucketArrayShrinksAfterExpiredBurst(t *testing.T) {
	clock := newFakeClock()
	s := newStore(clock, nil, DefaultMaxDataBytes) // No background loop: the test drives maintenance.
	ttl := int64(1000)
	for i := 0; i < 20000; i++ {
		mustSet(t, s, fmt.Sprintf("burst:%d", i), "v", &ttl)
	}
	for i := 0; i < 50; i++ {
		mustSet(t, s, fmt.Sprintf("keep:%d", i), "v", nil)
	}
	peak := s.table.Capacity()
	clock.Advance(2 * time.Second)
	for pass := 0; ; pass++ {
		if pass > 1000 {
			t.Fatal("sweep did not finish")
		}
		s.writeMu.Lock()
		result := s.reclaimExpiredHeld(4096)
		s.writeMu.Unlock()
		if result.Done {
			break
		}
	}
	if s.table.Len() != 50 || s.usedBytes != 10*(6+1+64)+40*(7+1+64) {
		t.Fatalf("len=%d used=%d", s.table.Len(), s.usedBytes)
	}
	if got := s.table.Capacity(); got >= peak/16 || got < 64 {
		t.Fatalf("capacity %d -> %d", peak, got)
	}
	for i := 0; i < 50; i++ {
		expectLookup(t, s, fmt.Sprintf("keep:%d", i), LookupResult{Live, "v", -1})
	}
	// The table keeps working and can grow again.
	for i := 0; i < 5000; i++ {
		mustSet(t, s, fmt.Sprintf("again:%d", i), "v", nil)
	}
	if s.table.Len() != 5050 {
		t.Fatalf("len=%d", s.table.Len())
	}
}

// A key stored again while the sweeper is between batches is a new node with
// its own expiry: the sweep must not delete it on behalf of the old one.
func TestSweepDoesNotDeleteValueStoredAgain(t *testing.T) {
	clock := newFakeClock()
	s := newStore(clock, nil, DefaultMaxDataBytes)
	ttl := int64(10)
	for i := 0; i < 2000; i++ {
		mustSet(t, s, fmt.Sprintf("k:%d", i), "old", &ttl)
	}
	clock.Advance(time.Second)
	stored := map[string]bool{}
	for i := 0; ; i++ {
		s.writeMu.Lock()
		result := s.reclaimExpiredHeld(64)
		s.writeMu.Unlock()
		// Re-store keys all over the table, expired ones included.
		key := fmt.Sprintf("k:%d", (i*37)%2000)
		mustSet(t, s, key, "new", nil)
		stored[key] = true
		if result.Done {
			break
		}
	}
	if len(stored) < 10 {
		t.Fatalf("sweep finished too early to interleave: %d", len(stored))
	}
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k:%d", i)
		want := LookupResult{State: Expired, TTLMillis: -2}
		if stored[key] {
			want = LookupResult{Live, "new", -1}
		}
		got, err := s.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		// A key the sweep already removed is Missing rather than Expired.
		if got != want && !(got.State == Missing && !stored[key]) {
			t.Fatalf("%s: %+v, want %+v", key, got, want)
		}
	}
}
