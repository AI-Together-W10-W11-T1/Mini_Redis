package hashtable

import (
	"bytes"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestSeededHashKeepsFNVAndChangesPlacement(t *testing.T) {
	// The unseeded table stays plain FNV-1a so the published vectors hold.
	if got := New(nil).hash("a"); got != Hash("a") || got != 0xaf63dc4c8601ec8c {
		t.Fatalf("unseeded hash changed: %#x", got)
	}
	a, again, b := NewSeeded(nil, 1), NewSeeded(nil, 1), NewSeeded(nil, 2)
	same, moved := 0, 0
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("product:%d", i)
		if a.hash(key) != again.hash(key) {
			t.Fatalf("same seed must give the same hash for %s", key)
		}
		if a.hash(key)%1024 == b.hash(key)%1024 {
			same++
		} else {
			moved++
		}
	}
	// With 1024 buckets about 1 key in 1024 keeps its bucket by chance.
	if same > 20 || moved < 980 {
		t.Fatalf("seed barely changed placement: same=%d moved=%d", same, moved)
	}
}

// Keys that all share one bucket under plain FNV-1a must spread out once the
// table is seeded, including keys that differ only in bytes FNV mixes weakly.
func TestSeededTableSpreadsKeysThatCollideUnseeded(t *testing.T) {
	const buckets = 1024
	var colliding []string
	for i := 0; len(colliding) < 200; i++ {
		key := fmt.Sprintf("k%d", i)
		if Hash(key)%buckets == 7 {
			colliding = append(colliding, key)
		}
	}
	seeded := NewSeeded(nil, 0x9e3779b97f4a7c15)
	used := map[uint64]int{}
	worst := 0
	for _, key := range colliding {
		bucket := seeded.hash(key) % buckets
		used[bucket]++
		if used[bucket] > worst {
			worst = used[bucket]
		}
	}
	if len(used) < 150 || worst > 5 {
		t.Fatalf("200 colliding keys landed in %d buckets, worst bucket %d", len(used), worst)
	}
}

func TestSwapAndTakeReturnPreviousRecord(t *testing.T) {
	table := New(nil)
	expires := time.Unix(100, 0)
	if old, replaced := table.Swap("k", Record{Value: "a", ExpiresAt: expires}); replaced || old != (Record{}) {
		t.Fatalf("first swap: %+v %v", old, replaced)
	}
	if old, replaced := table.Swap("k", Record{Value: "b"}); !replaced || old != (Record{Value: "a", ExpiresAt: expires}) {
		t.Fatalf("second swap: %+v %v", old, replaced)
	}
	if got, ok := table.Take("k"); !ok || got != (Record{Value: "b"}) || table.Len() != 0 {
		t.Fatalf("take: %+v %v len=%d", got, ok, table.Len())
	}
	if _, ok := table.Take("k"); ok {
		t.Fatal("second take must report missing")
	}
}

func chainTable(n int) *Table {
	table := New(nil)
	table.hash = func(string) uint64 { return 0 }
	for i := 0; i < n; i++ {
		table.Put(fmt.Sprint(i), Record{Value: fmt.Sprint(i)})
	}
	return table
}

func reapAll(t *testing.T, table *Table, budget int, remove func(string, Record) bool) (removed []string, calls int) {
	t.Helper()
	var cursor ScanCursor
	for {
		calls++
		if calls > 100000 {
			t.Fatal("reap did not finish")
		}
		done := table.Reap(&cursor, budget, remove, func(key string, _ Record) { removed = append(removed, key) })
		assertInvariants(t, table)
		if done {
			return removed, calls
		}
	}
}

func TestReapRemovesFromOneLongChain(t *testing.T) {
	for _, tc := range []struct {
		name   string
		remove func(i int) bool
		want   int
	}{
		{"all", func(int) bool { return true }, 1000},
		{"none", func(int) bool { return false }, 0},
		{"every_other", func(i int) bool { return i%2 == 0 }, 500},
		{"only_tail", func(i int) bool { return i == 0 }, 1},
		{"only_head", func(i int) bool { return i == 999 }, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			table := chainTable(1000)
			removed, _ := reapAll(t, table, 16, func(key string, _ Record) bool {
				var i int
				fmt.Sscan(key, &i)
				return tc.remove(i)
			})
			if len(removed) != tc.want || table.Len() != 1000-tc.want {
				t.Fatalf("removed=%d len=%d", len(removed), table.Len())
			}
			for i := 0; i < 1000; i++ {
				_, found := table.Get(fmt.Sprint(i))
				if found == tc.remove(i) {
					t.Fatalf("key %d found=%v", i, found)
				}
			}
		})
	}
}

// Between batches the caller may delete, insert and overwrite. Reap must skip
// deleted nodes, judge an overwritten key by its new record, and still unlink
// correctly when the predecessor it remembered is gone.
func TestReapToleratesMutationsBetweenBatches(t *testing.T) {
	table := chainTable(200)
	var cursor ScanCursor
	var removed []string
	expired := func(_ string, r Record) bool { return r.Value != "keep" }
	onRemove := func(key string, _ Record) { removed = append(removed, key) }
	step := 0
	for {
		done := table.Reap(&cursor, 7, expired, onRemove)
		assertInvariants(t, table)
		if done {
			break
		}
		step++
		switch step % 4 {
		case 0: // Overwrite a key that is still ahead of the cursor: it must survive.
			table.Put(fmt.Sprint(step%50), Record{Value: "keep"})
		case 1: // Delete the node the cursor points at and the one it remembers.
			if cursor.next != nil {
				table.Delete(cursor.next.key)
			}
			if cursor.prev != nil {
				table.Delete(cursor.prev.key)
			}
		case 2: // Prepend new nodes: the bucket head changes under the cursor.
			table.Put(fmt.Sprintf("new-%d", step), Record{Value: "keep"})
		}
		if step > 10000 {
			t.Fatal("reap did not finish")
		}
	}
	// One more full pass picks up whatever the first pass could not see.
	reapAll(t, table, 7, expired)
	table.Range(func(key string, r Record) bool {
		if r.Value != "keep" {
			t.Fatalf("expired node %s survived", key)
		}
		return true
	})
	if table.Len() == 0 {
		t.Fatal("kept records were removed")
	}
	seen := map[string]bool{}
	for _, key := range removed {
		if seen[key] {
			t.Fatalf("%s reported removed twice", key)
		}
		seen[key] = true
	}
}

// The work for one chain must grow linearly with its length, not quadratically.
func TestReapOnLongChainNeedsNoRepeatedWalks(t *testing.T) {
	const n = 5000
	table := chainTable(n)
	comparisons := 0
	_, calls := reapAll(t, table, 256, func(key string, _ Record) bool {
		comparisons++
		return len(key)%2 == 1 // roughly half, spread over the chain
	})
	if comparisons != n {
		t.Fatalf("each node must be offered exactly once: %d", comparisons)
	}
	// One probe per bucket plus one visit per node, 256 per call.
	if want := (n+table.Capacity())/256 + 2; calls > want {
		t.Fatalf("calls=%d want<=%d", calls, want)
	}
}

func TestScanSkipsNodesDeletedBetweenBatches(t *testing.T) {
	table := chainTable(100)
	var cursor ScanCursor
	visited := map[string]bool{}
	visit := func(key string, _ Record) { visited[key] = true }
	table.Scan(&cursor, 10, visit)
	doomed := cursor.next.key
	table.Delete(doomed)
	for !table.Scan(&cursor, 10, visit) {
	}
	if visited[doomed] || len(visited) != 99 {
		t.Fatalf("deleted node visited=%v, visited=%d", visited[doomed], len(visited))
	}
}

func TestShrinkAfterMassDeletion(t *testing.T) {
	table := New(nil)
	for i := 0; i < 10000; i++ {
		table.Put(fmt.Sprint(i), Record{Value: fmt.Sprint(i)})
	}
	grown := table.Capacity()
	if table.Shrink() {
		t.Fatal("a well-filled table must not shrink")
	}
	for i := 100; i < 10000; i++ {
		table.Delete(fmt.Sprint(i))
	}
	if !table.Shrink() {
		t.Fatal("sparse table did not shrink")
	}
	capacity := table.Capacity()
	// Halving stops at 512 buckets: 100 keys use at least one bucket in eight there.
	if capacity >= grown || capacity != 512 || table.Len() != 100 {
		t.Fatalf("capacity %d -> %d, len=%d", grown, capacity, table.Len())
	}
	for i := 0; i < 100; i++ {
		assertRecord(t, table, fmt.Sprint(i), Record{Value: fmt.Sprint(i)})
	}
	assertInvariants(t, table)
	if table.Shrink() {
		t.Fatal("second shrink must be a no-op")
	}
	// Inserting right after a shrink must not immediately grow again.
	table.Put("one-more", Record{})
	if table.Capacity() != capacity {
		t.Fatalf("grew right after shrinking: %d", table.Capacity())
	}
	for i := 0; i < 100; i++ {
		table.Delete(fmt.Sprint(i))
	}
	table.Delete("one-more")
	table.Shrink()
	if table.Capacity() != initialCapacity {
		t.Fatalf("empty table capacity=%d", table.Capacity())
	}
}

func TestLongChainIsReportedOncePerDoubling(t *testing.T) {
	var out bytes.Buffer
	table := New(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn})))
	table.hash = func(string) uint64 { return 3 }
	for i := 0; i < 300; i++ {
		table.Put(fmt.Sprintf("secret-key-%d", i), Record{})
	}
	if table.LongestChain() != 300 {
		t.Fatalf("longest chain=%d", table.LongestChain())
	}
	logged := out.String()
	// The table grows while filling, and each resize restarts the thresholds.
	if n := strings.Count(logged, "LONG_CHAIN"); n < 1 || n > 12 {
		t.Fatalf("warnings=%d\n%s", n, logged)
	}
	if strings.Contains(logged, "secret-key") {
		t.Fatal("warning leaked a key name")
	}
	// A well-distributed table never warns.
	out.Reset()
	even := NewSeeded(slog.New(slog.NewTextHandler(&out, &slog.HandlerOptions{Level: slog.LevelWarn})), 42)
	for i := 0; i < 50000; i++ {
		even.Put(fmt.Sprintf("product:%d", i), Record{})
	}
	if out.Len() != 0 || even.LongestChain() > 12 {
		t.Fatalf("longest=%d log=%s", even.LongestChain(), out.String())
	}
}
