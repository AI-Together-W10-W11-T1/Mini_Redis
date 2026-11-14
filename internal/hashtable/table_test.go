package hashtable

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"
)

func learningTable(t *testing.T) *Table {
	t.Helper()
	raw := os.Getenv("MINI_REDIS_DEBUG")
	if raw == "" {
		return New(nil)
	}
	enabled, err := strconv.ParseBool(raw)
	if err != nil {
		t.Fatalf("MINI_REDIS_DEBUG는 true/false로 지정하세요: %v", err)
	}
	if !enabled {
		return New(nil)
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelDebug,
		ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
			if a.Key == slog.TimeKey {
				return slog.Attr{} // Stable learning output without timestamps.
			}
			return a
		},
	}))
	return New(logger)
}

func assertRecord(t *testing.T, table *Table, key string, want Record) {
	t.Helper()
	got, found := table.Get(key)
	if !found || got.Value != want.Value || !got.ExpiresAt.Equal(want.ExpiresAt) {
		t.Fatalf("Get(%q)=(%+v, %t), want=%+v", key, got, found, want)
	}
}

func assertInvariants(t *testing.T, table *Table) {
	t.Helper()
	seenNodes := make(map[*entry]bool) // Reference/checking maps are test-only.
	seenKeys := make(map[string]bool)
	for index, head := range table.buckets {
		for current := head; current != nil; current = current.next {
			if seenNodes[current] {
				t.Fatal("cycle or node shared between chains")
			}
			seenNodes[current] = true
			if seenKeys[current.key] {
				t.Fatalf("duplicate key %q", current.key)
			}
			seenKeys[current.key] = true
			if int(current.hash%uint64(table.Capacity())) != index {
				t.Fatalf("key %q in wrong bucket %d", current.key, index)
			}
		}
	}
	if len(seenNodes) != table.Len() {
		t.Fatalf("node count=%d, Len=%d", len(seenNodes), table.Len())
	}
}

func TestHashFNV1aKnownVectors(t *testing.T) {
	cases := []struct {
		key  string
		want uint64
	}{
		{"", 14695981039346656037},
		{"hello", 11831194018420276491},
		{"user:101", 6856737180763075376},
		{"product:1001", 11619022900221879904},
	}
	for _, tc := range cases {
		if got := Hash(tc.key); got != tc.want {
			t.Errorf("Hash(%q)=%d, want %d", tc.key, got, tc.want)
		}
		t.Logf("key=%q hash=%d bucket(16)=%d", tc.key, tc.want, tc.want%16)
	}
}

func TestTableSetGetUpdateAndEmptyValue(t *testing.T) {
	table := learningTable(t)
	if table.Len() != 0 || table.Capacity() != 16 {
		t.Fatalf("initial size=%d capacity=%d", table.Len(), table.Capacity())
	}
	if _, found := table.Get("missing"); found || table.Delete("missing") {
		t.Fatal("empty table must report missing")
	}
	expires := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	if table.Put("user:101", Record{Value: "Minji", ExpiresAt: expires}) {
		t.Fatal("new key must not be reported as replacement")
	}
	assertRecord(t, table, "user:101", Record{Value: "Minji", ExpiresAt: expires})
	t.Logf("INSERT/GET: user:101=Minji size=%d", table.Len())
	if !table.Put("user:101", Record{Value: "Jisoo"}) || table.Len() != 1 {
		t.Fatal("replacement must preserve key count")
	}
	assertRecord(t, table, "user:101", Record{Value: "Jisoo"})
	t.Logf("UPDATE: user:101=Jisoo size=%d (만료 정보도 새 Record로 교체)", table.Len())
	table.Put("empty", Record{Value: ""})
	assertRecord(t, table, "empty", Record{Value: ""})
	copyOfRecord, _ := table.Get("user:101")
	copyOfRecord.Value = "changed outside"
	assertRecord(t, table, "user:101", Record{Value: "Jisoo"})
	if !table.Delete("user:101") || table.Delete("user:101") || table.Len() != 1 {
		t.Fatal("first deletion succeeds; repeated deletion preserves size")
	}
	if _, found := table.Get("user:101"); found {
		t.Fatal("deleted key must be missing")
	}
	assertRecord(t, table, "empty", Record{})
	assertInvariants(t, table)
	t.Log("EMPTY/MISSING: 빈 값은 found=true, 삭제한 키는 found=false")
}

func TestTableCollisionDeletePositions(t *testing.T) {
	t.Run("real_fnv_bucket_collision", func(t *testing.T) {
		table := learningTable(t)
		if Hash("user:101")%16 != Hash("product:1001")%16 {
			t.Fatal("teaching keys must share bucket 0")
		}
		table.Put("user:101", Record{Value: "Minji"})
		table.Put("product:1001", Record{Value: "Keyboard"})
		assertRecord(t, table, "user:101", Record{Value: "Minji"})
		assertRecord(t, table, "product:1001", Record{Value: "Keyboard"})
		table.Put("user:101", Record{Value: "Jisoo"})
		assertRecord(t, table, "product:1001", Record{Value: "Keyboard"})
		if !table.Delete("user:101") {
			t.Fatal("existing collision key must be deleted")
		}
		assertRecord(t, table, "product:1001", Record{Value: "Keyboard"})
		assertInvariants(t, table)
		t.Log("COLLISION: user:101과 product:1001은 버킷 0 공유, 갱신·삭제 후 다른 키 보존")
	})
	for _, position := range []struct {
		name   string
		target string
	}{
		{"head", "third"}, {"middle", "second"}, {"tail", "first"}, {"missing", "absent"},
	} {
		t.Run(position.name, func(t *testing.T) {
			table := learningTable(t)
			table.hash = func(string) uint64 { return 42 } // Full hash collision, not just bucket collision.
			keys := []string{"first", "second", "third"}
			for _, key := range keys {
				table.Put(key, Record{Value: key})
			}
			// All hashes are equal: this update must still compare the actual key.
			table.Put("second", Record{Value: "updated"})
			wantDeleted := position.target != "absent"
			if got := table.Delete(position.target); got != wantDeleted {
				t.Fatalf("deleted=%t, want %t", got, wantDeleted)
			}
			for _, key := range keys {
				if key == position.target {
					if _, found := table.Get(key); found {
						t.Fatalf("deleted key %q survived", key)
					}
					continue
				}
				value := key
				if key == "second" {
					value = "updated"
				}
				assertRecord(t, table, key, Record{Value: value})
			}
			assertInvariants(t, table)
		})
	}
	t.Run("only_entry_and_reinsert", func(t *testing.T) {
		table := learningTable(t)
		table.Put("only", Record{Value: "before"})
		if !table.Delete("only") || table.Len() != 0 {
			t.Fatal("deleting the only entry must leave an empty table")
		}
		if table.Put("only", Record{Value: "after"}) {
			t.Fatal("reinsertion must be a new key")
		}
		assertRecord(t, table, "only", Record{Value: "after"})
		assertInvariants(t, table)
	})
}

func TestTableResizeThresholdAndPreservation(t *testing.T) {
	table := learningTable(t)
	want := make(map[string]Record)
	base := time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < 12; i++ {
		key := fmt.Sprintf("item:%d", i)
		record := Record{Value: fmt.Sprintf("value:%d", i), ExpiresAt: base.Add(time.Duration(i) * time.Second)}
		table.Put(key, record)
		want[key] = record
	}
	if table.Len() != 12 || table.Capacity() != 16 {
		t.Fatal("12 keys must fit in 16 buckets at load factor 0.75")
	}
	table.Put("item:3", Record{Value: "updated"})
	want["item:3"] = Record{Value: "updated"}
	if table.Len() != 12 || table.Capacity() != 16 {
		t.Fatal("updating the 12th-key state must not resize")
	}
	t.Log("BEFORE: 12키·16버킷. 기존 키 갱신은 확장 없음")
	table.Put("item:12", Record{Value: "value:12", ExpiresAt: base})
	want["item:12"] = Record{Value: "value:12", ExpiresAt: base}
	if table.Len() != 13 || table.Capacity() != 32 {
		t.Fatalf("13th insertion: size=%d capacity=%d", table.Len(), table.Capacity())
	}
	t.Log("AFTER: 13번째 새 키 → 32버킷으로 확장")
	for i := 13; i < 200; i++ {
		key := fmt.Sprintf("item:%d", i)
		record := Record{Value: fmt.Sprintf("value:%d", i), ExpiresAt: base.Add(time.Duration(i) * time.Second)}
		table.Put(key, record)
		want[key] = record
	}
	for key, record := range want {
		assertRecord(t, table, key, record)
	}
	if table.Len() != len(want) || table.Capacity() != 512 {
		t.Fatalf("final size=%d capacity=%d", table.Len(), table.Capacity())
	}
	assertInvariants(t, table)
	t.Logf("PRESERVED: %d개 값·만료 정보 보존, capacity=%d", table.Len(), table.Capacity())
}

// This deliberately failing experiment is test-only and opt-in.
// It demonstrates why matching a hash without comparing the key is incorrect.
func TestExperimentWithoutKeyComparison(t *testing.T) {
	if os.Getenv("MINI_REDIS_EXPERIMENT") != "skip-key-check" {
		t.Skip("실험 실행: MINI_REDIS_EXPERIMENT=skip-key-check 지정")
	}
	table := learningTable(t)
	table.hash = func(string) uint64 { return 42 }
	table.Put("user:101", Record{Value: "Minji"})
	table.Put("product:1001", Record{Value: "Keyboard"})
	assertRecord(t, table, "user:101", Record{Value: "Minji"})
	t.Log("실험용으로 두 키의 전체 해시를 42로 고정했습니다. 실제 Table.Get은 정상입니다.")
	brokenValue := experimentGetWithoutKeyComparison(table, "user:101")
	if brokenValue != "Minji" {
		t.Fatalf("의도한 실패: 키 비교 제거 → user:101 조회가 %q 반환, 정답은 %q", brokenValue, "Minji")
	}
	t.Fatal("실험이 예상한 잘못된 값을 재현하지 못했습니다")
}

func experimentGetWithoutKeyComparison(table *Table, key string) string {
	h := table.hash(key)
	i := int(h % uint64(table.Capacity()))
	for current := table.buckets[i]; current != nil; current = current.next {
		if current.hash == h { // BUG: current.key == key was intentionally omitted.
			return current.record.Value
		}
	}
	return ""
}
