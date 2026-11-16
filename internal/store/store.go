package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/hashtable"
)

const (
	DefaultMaxDataBytes int64 = 256 << 20
	MaxKeyBytes               = 256
	MaxValueBytes             = 65_536
	MaxTTLMillis              = int64(1<<63-1) / int64(time.Millisecond)
)

// 호출자가 HTTP에 의존하지 않고 처리할 수 있도록 입력 오류를 종류로 구분한다.
var (
	ErrCapacityExceeded = errors.New("capacity exceeded")
	ErrInvalidArgument  = errors.New("invalid argument")
	ErrLimitExceeded    = errors.New("limit exceeded")
	ErrPersistence      = errors.New("persistence unavailable")
	ErrOutcomeUnknown   = fmt.Errorf("%w: outcome unknown", ErrPersistence)
)

// Clock lets tests advance time without sleeping. Implementations must be safe
// for concurrent use when shared with other goroutines.
type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (systemClock) Now() time.Time { return time.Now() }

type State uint8

const (
	Missing State = iota
	Live
	Expired
)

func (s State) String() string {
	switch s {
	case Missing:
		return "Missing"
	case Live:
		return "Live"
	case Expired:
		return "Expired"
	default:
		return "Unknown"
	}
}

type LookupResult struct {
	State     State
	Value     string
	TTLMillis int64
}

type DeleteResult struct {
	State   State
	Deleted bool
}

// Store owns its table. Every table access, including reads and lazy deletion,
// must hold mu. Do not copy a Store after construction.
//
// 읽기 전용 락(RWMutex)을 쓰지 않는 이유: Get도 만료 항목을 삭제하는 쓰기 연산이다.
// 읽기 락으로 확인하고 쓰기 락으로 삭제하면 그 사이에 들어온 새 Set 값을 지울 수 있어
// 재확인 로직이 필요하다. 단일 Mutex는 이 규칙을 단순하게 보장한다.
// 성능상 충분한지는 실제 요청 부하로 별도 측정한다.
type Store struct {
	writeMu                          mutationGate // Serializes journal order, writes, rewrite cutover and close.
	mu                               sync.Mutex
	table                            *hashtable.Table
	clock                            Clock
	logger                           *slog.Logger
	debugEnabled                     bool
	aof                              *journal
	mutationTimeout                  time.Duration
	mutationFailed                   atomic.Bool
	maxBytes, usedBytes              int64
	sweepCursor                      hashtable.ScanCursor
	maintenanceStop, maintenanceDone chan struct{}
	maintenanceOnce                  sync.Once
	closed                           atomic.Bool
}

// New uses the real clock when clock is nil. Debug settings are fixed here.
// It starts expiration maintenance; the owner must call Close.
func New(clock Clock, logger *slog.Logger) *Store {
	return NewWithLimit(clock, logger, DefaultMaxDataBytes)
}

func NewWithLimit(clock Clock, logger *slog.Logger, maxBytes int64) *Store {
	s := newStore(clock, logger, maxBytes)
	go s.expirationLoop()
	return s
}

func newStore(clock Clock, logger *slog.Logger, maxBytes int64) *Store {
	if maxBytes <= 0 {
		panic("data limit must be positive")
	}
	if clock == nil {
		clock = systemClock{}
	}
	return &Store{
		table:    hashtable.New(logger),
		maxBytes: maxBytes, maintenanceStop: make(chan struct{}), maintenanceDone: make(chan struct{}),
		clock:           clock,
		logger:          logger,
		debugEnabled:    logger != nil && logger.Enabled(context.Background(), slog.LevelDebug),
		mutationTimeout: defaultMutationTimeout,
	}
}

// Set replaces both the value and expiry. A nil TTL removes any previous expiry.
func (s *Store) Set(key string, value string, ttlMillis *int64) error {
	return s.SetContext(context.Background(), key, value, ttlMillis)
}

// SetContext cancels admission/waiting, not an already-started disk syscall.
// An error after admission may have an unknown outcome; do not blindly retry.
func (s *Store) SetContext(ctx context.Context, key string, value string, ttlMillis *int64) error {
	if err := validateKey(key); err != nil {
		return err
	}
	if len(value) > MaxValueBytes {
		return fmt.Errorf("%w: value exceeds %d bytes", ErrLimitExceeded, MaxValueBytes)
	}
	var ttl int64
	if ttlMillis != nil {
		ttl = *ttlMillis
		if ttl <= 0 || ttl > MaxTTLMillis {
			return fmt.Errorf("%w: TTL must be 1..%d milliseconds", ErrInvalidArgument, MaxTTLMillis)
		}
	}

	hasTTL := ttlMillis != nil
	return s.runMutation(ctx, func() error {
		prepare := func() (hashtable.Record, bool, error) {
			s.mu.Lock()
			defer s.mu.Unlock()
			now := s.clock.Now()
			r := hashtable.Record{Value: value}
			if hasTTL {
				r.ExpiresAt = now.Add(time.Duration(ttl) * time.Millisecond)
			}
			old, found := s.table.Get(key)
			delta := entryBytes(key, r)
			if found {
				delta -= entryBytes(key, old)
			}
			if delta > 0 && delta > s.maxBytes-s.usedBytes {
				return r, found, ErrCapacityExceeded
			}
			return r, found, nil
		}
		record, found, capacityErr := prepare()
		if capacityErr != nil {
			// runMutation already owns writeMu. Do not recursively acquire it.
			// Re-read the old entry afterwards: reclamation may remove this key.
			s.reclaimExpiredHeld(4096)
			record, found, capacityErr = prepare()
		}
		if capacityErr != nil {
			return capacityErr
		}
		if s.aof != nil {
			reserve := int64(0)
			if !found {
				reserve = deleteRecordBytes(key)
			}
			if err := s.aof.append(recordForSet(key, record), reserve); err != nil {
				return err
			}
		}
		s.mu.Lock()
		defer s.mu.Unlock()
		updated := s.putLocked(key, record)
		if s.debugEnabled {
			ttlForLog := int64(-1)
			if hasTTL {
				ttlForLog = ttl
			}
			s.logger.Debug("SET", "key", key, "updated", updated,
				"unlimited", !hasTTL, "ttl_ms", ttlForLog, "size", s.table.Len())
		}
		return nil
	})
}

func (s *Store) Get(key string) (LookupResult, error) {
	if err := validateKey(key); err != nil {
		return LookupResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lookupLocked(key, s.clock.Now(), "get"), nil
}

func (s *Store) Delete(key string) (DeleteResult, error) {
	return s.DeleteContext(context.Background(), key)
}

// DeleteContext has the same uncertain-outcome rules as SetContext.
func (s *Store) DeleteContext(ctx context.Context, key string) (DeleteResult, error) {
	if err := validateKey(key); err != nil {
		return DeleteResult{}, err
	}
	// Result travels through the completion channel; do not share a result
	// pointer with a worker that may outlive the caller's timeout.
	result := make(chan DeleteResult, 1)
	err := s.runMutation(ctx, func() error {
		if s.aof != nil {
			if err := s.aof.health(); err != nil {
				return err
			}
		}
		lookup := func() LookupResult {
			s.mu.Lock()
			defer s.mu.Unlock()
			return s.lookupLocked(key, s.clock.Now(), "delete")
		}()
		deleted := DeleteResult{State: lookup.State}
		if lookup.State == Live {
			if s.aof != nil {
				if err := s.aof.append(aofRecord{Op: "del", Key: key}, -deleteRecordBytes(key)); err != nil {
					return err
				}
			}
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.removeLocked(key)
			}()
			deleted.Deleted = true
		}
		if s.debugEnabled {
			func() {
				s.mu.Lock()
				defer s.mu.Unlock()
				s.logger.Debug("STORE_DELETE", "key", key, "state", deleted.State.String(), "deleted", deleted.Deleted, "size", s.table.Len())
			}()
		}
		result <- deleted
		return nil
	})
	if err != nil {
		return DeleteResult{}, err
	}
	return <-result, nil
}

// lookupLocked must only run with mu held. Reads may physically remove expiry.
func (s *Store) lookupLocked(key string, now time.Time, op string) LookupResult {
	record, found := s.table.Get(key)
	result := LookupResult{State: Missing, TTLMillis: -2}
	cleaned := false
	if found {
		switch {
		case record.ExpiresAt.IsZero():
			result = LookupResult{State: Live, Value: record.Value, TTLMillis: -1}
		case !now.Before(record.ExpiresAt):
			// 만료 정각도 만료다. 같은 락 안에서 확인과 삭제를 끝낸다.
			s.removeLocked(key)
			result.State = Expired
			cleaned = true
		default:
			remaining := record.ExpiresAt.Sub(now)
			millis := int64(remaining / time.Millisecond)
			// 먼저 나누고 나머지를 확인해 올림 덧셈의 오버플로를 피한다.
			if remaining%time.Millisecond != 0 {
				millis++
			}
			result = LookupResult{State: Live, Value: record.Value, TTLMillis: millis}
		}
	}
	if s.debugEnabled {
		s.logger.Debug("EXPIRATION", "op", op, "key", key, "state", result.State.String(),
			"unlimited", result.State == Live && result.TTLMillis == -1,
			"ttl_ms", result.TTLMillis, "cleaned", cleaned)
	}
	return result
}

// validateKey checks O(1) rules first so an oversized key is rejected before
// its bytes are scanned. Order: empty → byte limit → dot key → UTF-8 → control.
func validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("%w: key must be nonempty", ErrInvalidArgument)
	}
	// 길이 확인은 O(1)이다. 매우 긴 키를 문자 단위로 훑기 전에 먼저 거부한다.
	if len(key) > MaxKeyBytes {
		return fmt.Errorf("%w: key exceeds %d bytes", ErrLimitExceeded, MaxKeyBytes)
	}
	if key == "." || key == ".." || !utf8.ValidString(key) {
		return fmt.Errorf("%w: key must be UTF-8 and not a dot key", ErrInvalidArgument)
	}
	for _, r := range key {
		if unicode.IsControl(r) {
			return fmt.Errorf("%w: key contains a control character", ErrInvalidArgument)
		}
	}
	return nil
}

// Accounting is approximate resident data, not an RSS guarantee. Buckets,
// allocator/GC overhead, temporary rewrite buffers and goroutines are separate.
func entryBytes(key string, r hashtable.Record) int64 { return int64(len(key)+len(r.Value)) + 64 }
func (s *Store) putLocked(key string, r hashtable.Record) bool {
	if old, ok := s.table.Get(key); ok {
		s.usedBytes -= entryBytes(key, old)
	}
	updated := s.table.Put(key, r)
	s.usedBytes += entryBytes(key, r)
	return updated
}
func (s *Store) removeLocked(key string) bool {
	old, ok := s.table.Get(key)
	if !ok {
		return false
	}
	if !s.table.Delete(key) {
		return false
	}
	s.usedBytes -= entryBytes(key, old)
	return true
}
