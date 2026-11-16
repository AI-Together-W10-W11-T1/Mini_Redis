package hashtable

import (
	"context"
	"log/slog"
	"time"
)

const initialCapacity = 16

// chainWarnLength is the first chain length reported as a possible hash
// flooding or skew problem. Each report doubles the next threshold.
const chainWarnLength = 64

// Record is copied into and out of the table. A zero ExpiresAt means no expiry.
// The table preserves expiry metadata; only the Store will interpret it.
type Record struct {
	Value     string
	ExpiresAt time.Time
}

type entry struct {
	key    string
	hash   uint64
	record Record
	next   *entry
	// removed is set when the node leaves the table. A scan cursor may still
	// point at such a node; the flag lets it skip the node without a lookup.
	removed bool
}

// Table uses separate chaining and is not concurrency-safe.
// Callers must use New. The Store will serialize access in the next stage.
type Table struct {
	buckets      []*entry
	size         int
	logger       *slog.Logger        // 로그 출력기
	hash         func(string) uint64 // 문자열을 받음 -> uint64 숫자를 반환, 테스트에서 함수 교체 가능
	debugEnabled bool                // Fixed at construction; runtime log-level changes are unsupported.
	longestChain int                 // Longest chain produced by an insert since the last resize.
	chainWarnAt  int                 // Next chain length that is logged as a warning.
}

func New(logger *slog.Logger) *Table {
	return &Table{
		buckets:      make([]*entry, initialCapacity),
		logger:       logger,
		hash:         Hash,
		debugEnabled: logger != nil && logger.Enabled(context.Background(), slog.LevelDebug),
		chainWarnAt:  chainWarnLength,
	}
}

// NewSeeded returns a table whose bucket placement depends on a secret seed.
// The seed replaces the fixed FNV-1a starting state and the result is passed
// through a finalizer, so that every bit of the hash affects the bucket and
// keys chosen to collide for one process do not collide for another.
// This raises the cost of deliberate collisions; it is not a cryptographic
// guarantee the way a keyed hash such as SipHash would be.
func NewSeeded(logger *slog.Logger, seed uint64) *Table {
	t := New(logger)
	t.hash = func(key string) uint64 { return mix64(fnv1a(fnvOffsetBasis^seed, key)) }
	return t
}

const (
	fnvOffsetBasis uint64 = 14695981039346656037
	fnvPrime       uint64 = 1099511628211
)

// Hash implements FNV-1a 64-bit over the key's UTF-8 bytes.
// Bucket allocation, chaining, comparisons, deletion and resizing are our code.
func Hash(key string) uint64 {
	// 고정 초기값에서 시작해 같은 바이트열은 항상 같은 해시값을 얻는다.
	return fnv1a(fnvOffsetBasis, key)
}

func fnv1a(h uint64, key string) uint64 {
	for i := 0; i < len(key); i++ {
		// UTF-8 글자 단위가 아니라 문자열의 각 바이트를 순서대로 섞는다.
		h ^= uint64(key[i]) // FNV-1a는 먼저 XOR하고 다음에 소수를 곱한다.
		h *= fnvPrime       // uint64 오버플로로 하위 64비트를 유지한다.
	}
	return h
}

// mix64 is the 64-bit finalizer of MurmurHash3. FNV-1a only carries changes
// upward, so its low bits ignore the high bits of the state; the bucket index
// uses the low bits, and this step makes them depend on the whole value.
func mix64(h uint64) uint64 {
	h ^= h >> 33
	h *= 0xff51afd7ed558ccd
	h ^= h >> 33
	h *= 0xc4ceb9fe1a85ec53
	h ^= h >> 33
	return h
}

// Put returns true when replacing an existing key, false for a new key.
func (t *Table) Put(key string, record Record) bool {
	_, replaced := t.Swap(key, record)
	return replaced
}

// Swap stores the record and returns the one it replaced, if any, so that a
// caller needing the previous value walks the chain once instead of twice.
func (t *Table) Swap(key string, record Record) (Record, bool) {
	h := t.hash(key)
	i := int(h % uint64(len(t.buckets)))
	if t.debugEnabled {
		t.debug("HASH", "op", "put", "key", key, "hash", h, "bucket", i)
	}

	chain := 0
	for current := t.buckets[i]; current != nil; current = current.next {
		chain++
		match := current.hash == h && current.key == key
		if t.debugEnabled {
			t.debug("COMPARE", "requested", key, "stored", current.key, "match", match)
		}
		if match {
			old := current.record
			current.record = record
			if t.debugEnabled {
				t.debug("UPDATE", "key", key, "size", t.size, "capacity", len(t.buckets))
			}
			return old, true
		}
	}

	// Only a new insertion can cross the 0.75 threshold.
	if (t.size+1)*4 > len(t.buckets)*3 {
		t.resize(len(t.buckets) * 2)
		i = int(h % uint64(len(t.buckets)))
		chain = 0
		for current := t.buckets[i]; current != nil; current = current.next {
			chain++
		}
	}
	if head := t.buckets[i]; head != nil {
		if t.debugEnabled {
			t.debug("COLLISION", "bucket", i, "new_key", key, "existing_key", head.key)
		}
	}
	// Prepend after checking the entire chain for an existing key.
	t.buckets[i] = &entry{key: key, hash: h, record: record, next: t.buckets[i]}
	t.size++
	if t.debugEnabled {
		t.debug("INSERT", "key", key, "bucket", i, "size", t.size, "capacity", len(t.buckets))
	}
	t.noteChain(i, chain+1)
	return Record{}, false
}

// noteChain records the longest chain and reports unusual growth. The key is
// not logged: a warning must not leak stored key names.
func (t *Table) noteChain(bucket, length int) {
	if length > t.longestChain {
		t.longestChain = length
	}
	if length >= t.chainWarnAt {
		for t.chainWarnAt <= length {
			t.chainWarnAt *= 2
		}
		if t.logger != nil {
			t.logger.Warn("LONG_CHAIN", "bucket", bucket, "length", length, "size", t.size, "capacity", len(t.buckets))
		}
	}
}

// LongestChain is the longest chain an insert produced since the last resize.
// Deletions do not lower it; it is an upper bound for diagnosing skew.
func (t *Table) LongestChain() int { return t.longestChain }

// Get distinguishes a missing key from a present record with an empty value.
func (t *Table) Get(key string) (Record, bool) {
	h := t.hash(key)
	i := int(h % uint64(len(t.buckets)))
	if t.debugEnabled {
		t.debug("HASH", "op", "get", "key", key, "hash", h, "bucket", i)
	}
	for current := t.buckets[i]; current != nil; current = current.next {
		match := current.hash == h && current.key == key
		if t.debugEnabled {
			t.debug("COMPARE", "requested", key, "stored", current.key, "match", match)
		}
		if match {
			if t.debugEnabled {
				t.debug("LOOKUP", "key", key, "found", true)
			}
			return current.record, true
		}
	}
	if t.debugEnabled {
		t.debug("LOOKUP", "key", key, "found", false)
	}
	return Record{}, false
}

// Delete removes a physical entry. The Store decides whether it was expired.
func (t *Table) Delete(key string) bool {
	_, deleted := t.Take(key)
	return deleted
}

// Take removes a physical entry and returns its record in one chain walk.
func (t *Table) Take(key string) (Record, bool) {
	h := t.hash(key)
	i := int(h % uint64(len(t.buckets)))
	if t.debugEnabled {
		t.debug("HASH", "op", "delete", "key", key, "hash", h, "bucket", i)
	}
	var previous *entry
	for current := t.buckets[i]; current != nil; current = current.next {
		match := current.hash == h && current.key == key
		if t.debugEnabled {
			t.debug("COMPARE", "requested", key, "stored", current.key, "match", match)
		}
		if match {
			if previous == nil {
				t.buckets[i] = current.next
			} else {
				previous.next = current.next
			}
			current.removed = true
			t.size--
			if t.debugEnabled {
				t.debug("DELETE", "key", key, "deleted", true, "size", t.size)
			}
			return current.record, true
		}
		previous = current
	}
	if t.debugEnabled {
		t.debug("DELETE", "key", key, "deleted", false, "size", t.size)
	}
	return Record{}, false
}

func (t *Table) Len() int      { return t.size }
func (t *Table) Capacity() int { return len(t.buckets) }

// Range visits copies of records. The caller must prevent concurrent access
// and must not mutate the table from visit. Return false to stop early.
func (t *Table) Range(visit func(string, Record) bool) {
	for _, head := range t.buckets {
		for current := head; current != nil; current = current.next {
			if !visit(current.key, current.record) {
				return
			}
		}
	}
}

// ScanCursor is valid while inserts, updates and resizing are excluded.
// Deletions may occur between batches; nodes deleted meanwhile are skipped.
type ScanCursor struct {
	bucket   int
	next     *entry
	prev     *entry // Reap only: last kept node of the current bucket.
	capacity int
}

// Scan bounds bucket probes plus node visits, including a long collision chain.
// The caller protects each invocation and prevents writes across the scan.
func (t *Table) Scan(cursor *ScanCursor, budget int, visit func(string, Record)) bool {
	if budget <= 0 {
		panic("scan budget must be positive")
	}
	for budget > 0 {
		if cursor.next == nil {
			if cursor.bucket >= len(t.buckets) {
				return true
			}
			cursor.next = t.buckets[cursor.bucket]
			cursor.prev = nil
			cursor.bucket++
			budget--
			if cursor.next == nil {
				continue
			}
			if budget == 0 {
				break
			}
		}
		current := cursor.next
		cursor.next = current.next
		budget--
		if current.removed {
			continue // Deleted since the cursor reached it; its next pointer still leads on.
		}
		visit(current.key, current.record)
	}
	return cursor.next == nil && cursor.bucket >= len(t.buckets)
}

// Reap scans like ScanWeak and unlinks every node for which remove returns
// true, reporting it through removed. It is meant for expiry maintenance:
// mutations may happen BETWEEN calls, growth or shrinking restarts the cursor.
//
// A live node is unlinked through the predecessor the cursor remembers, so
// the usual cost is O(1) per node even inside one very long chain. Only when
// that predecessor is stale (it was deleted, or new nodes were inserted at
// the bucket head since) does Reap walk the chain once to find the node.
// Budget counts bucket probes plus node visits, not those rare extra walks.
func (t *Table) Reap(cursor *ScanCursor, budget int, remove func(string, Record) bool, removed func(string, Record)) bool {
	if budget <= 0 {
		panic("scan budget must be positive")
	}
	if cursor.capacity != len(t.buckets) {
		*cursor = ScanCursor{capacity: len(t.buckets)}
	}
	for budget > 0 {
		if cursor.next == nil {
			if cursor.bucket >= len(t.buckets) {
				return true
			}
			cursor.next = t.buckets[cursor.bucket]
			cursor.prev = nil
			cursor.bucket++
			budget--
			if cursor.next == nil {
				continue
			}
			if budget == 0 {
				break
			}
		}
		current := cursor.next
		cursor.next = current.next
		budget--
		if current.removed {
			continue
		}
		if !remove(current.key, current.record) {
			cursor.prev = current
			continue
		}
		bucket := cursor.bucket - 1
		switch {
		case cursor.prev != nil && !cursor.prev.removed && cursor.prev.next == current:
			cursor.prev.next = current.next
		case t.buckets[bucket] == current:
			t.buckets[bucket] = current.next
		default:
			previous := t.buckets[bucket]
			for previous != nil && previous.next != current {
				previous = previous.next
			}
			if previous == nil {
				continue // Not reachable any more; nothing to unlink.
			}
			previous.next = current.next
			cursor.prev = previous
		}
		current.removed = true
		t.size--
		if t.debugEnabled {
			t.debug("REAP", "key", current.key, "size", t.size)
		}
		removed(current.key, current.record)
	}
	return cursor.next == nil && cursor.bucket >= len(t.buckets)
}

// Shrink halves the bucket array while fewer than one in eight buckets would
// be used, never below the initial capacity. The resulting load factor stays
// under 0.25, far from the 0.75 growth threshold, so a table does not bounce
// between growing and shrinking. It reports whether the capacity changed.
// Callers must exclude concurrent scans, exactly as for growth.
func (t *Table) Shrink() bool {
	capacity := len(t.buckets)
	for capacity > initialCapacity && t.size*8 < capacity {
		capacity /= 2
	}
	if capacity == len(t.buckets) {
		return false
	}
	t.resize(capacity)
	return true
}

// ScanWeak permits mutations BETWEEN calls. Inserts at already visited bucket
// heads may wait until the next full cycle. Deleted nodes may still be visited;
// callbacks must recheck membership before acting. Growth restarts the cursor.
// This is suitable for opportunistic maintenance, never a consistent snapshot.
func (t *Table) ScanWeak(cursor *ScanCursor, budget int, visit func(string, Record)) bool {
	if cursor.capacity != len(t.buckets) {
		*cursor = ScanCursor{capacity: len(t.buckets)}
	}
	return t.Scan(cursor, budget, visit)
}

func (t *Table) resize(newCapacity int) {
	if t.debugEnabled {
		t.debug("RESIZE", "old_capacity", len(t.buckets), "new_capacity", newCapacity, "size", t.size)
	}
	newBuckets := make([]*entry, newCapacity)
	for oldIndex, head := range t.buckets {
		for current := head; current != nil; {
			// Keep the old next pointer before linking into the new bucket.
			next := current.next
			newIndex := int(current.hash % uint64(newCapacity))
			current.next = newBuckets[newIndex]
			newBuckets[newIndex] = current
			if t.debugEnabled {
				t.debug("MOVE", "key", current.key, "old_bucket", oldIndex, "new_bucket", newIndex)
			}
			current = next
		}
	}
	t.buckets = newBuckets
	t.longestChain = 0
	t.chainWarnAt = chainWarnLength
	// Moving entries preserves size, values and expiry metadata.
}

func (t *Table) debug(message string, args ...any) {
	t.logger.Debug(message, args...)
}
