package hashtable

import (
	"context"
	"log/slog"
	"time"
)

const initialCapacity = 16

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
}

// Table uses separate chaining and is not concurrency-safe.
// Callers must use New. The Store will serialize access in the next stage.
type Table struct {
	buckets      []*entry
	size         int
	logger       *slog.Logger	// 로그 출력기
	hash         func(string) uint64 // 문자열을 받음 -> uint64 숫자를 반환, 테스트에서 함교수 체 될 수 있음
	debugEnabled bool                // Fixed at construction; runtime log-level changes are unsupported.
}

func New(logger *slog.Logger) *Table {
	return &Table{
		buckets:      make([]*entry, initialCapacity),
		logger:       logger,
		hash:         Hash,
		debugEnabled: logger != nil && logger.Enabled(context.Background(), slog.LevelDebug),
	}
}

// Hash implements FNV-1a 64-bit over the key's UTF-8 bytes.
// Bucket allocation, chaining, comparisons, deletion and resizing are our code.
func Hash(key string) uint64 {
	const offsetBasis uint64 = 14695981039346656037
	const prime uint64 = 1099511628211
	h := offsetBasis // 고정 초기값에서 시작해 같은 바이트열은 항상 같은 해시값을 얻는다.
	for i := 0; i < len(key); i++ {
		// UTF-8 글자 단위가 아니라 문자열의 각 바이트를 순서대로 섞는다.
		h ^= uint64(key[i]) // FNV-1a는 먼저 XOR하고 다음에 소수를 곱한다.
		h *= prime          // uint64 오버플로로 하위 64비트를 유지한다.
	}
	return h
}

// Put returns true when replacing an existing key, false for a new key.
func (t *Table) Put(key string, record Record) bool {
	h := t.hash(key)
	i := int(h % uint64(len(t.buckets)))
	if t.debugEnabled {
		t.debug("HASH", "op", "put", "key", key, "hash", h, "bucket", i)
	}

	for current := t.buckets[i]; current != nil; current = current.next {
		match := current.hash == h && current.key == key
		if t.debugEnabled {
			t.debug("COMPARE", "requested", key, "stored", current.key, "match", match)
		}
		if match {
			current.record = record
			if t.debugEnabled {
				t.debug("UPDATE", "key", key, "size", t.size, "capacity", len(t.buckets))
			}
			return true
		}
	}

	// Only a new insertion can cross the 0.75 threshold.
	if (t.size+1)*4 > len(t.buckets)*3 {
		t.resize(len(t.buckets) * 2)
		i = int(h % uint64(len(t.buckets)))
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
	return false
}

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
			t.size--
			if t.debugEnabled {
				t.debug("DELETE", "key", key, "deleted", true, "size", t.size)
			}
			return true
		}
		previous = current
	}
	if t.debugEnabled {
		t.debug("DELETE", "key", key, "deleted", false, "size", t.size)
	}
	return false
}

func (t *Table) Len() int      { return t.size }
func (t *Table) Capacity() int { return len(t.buckets) }

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
	// Moving entries preserves size, values and expiry metadata.
}

func (t *Table) debug(message string, args ...any) {
	t.logger.Debug(message, args...)
}
