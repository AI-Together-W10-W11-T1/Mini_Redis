package store

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	"mini-redis/internal/hashtable"
)

const (
	MaxKeyBytes   = 256
	MaxValueBytes = 65_536
	MaxTTLMillis  = int64(1<<63-1) / int64(time.Millisecond)
)

// 호출자가 HTTP에 의존하지 않고 처리할 수 있도록 입력 오류를 종류로 구분한다.
var (
	ErrInvalidArgument = errors.New("invalid argument")
	ErrLimitExceeded   = errors.New("limit exceeded")
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
// 재확인 로직이 필요하다. 임계 구역이 0.1µs 수준이라 단일 Mutex로 충분하다.
type Store struct {
	mu           sync.Mutex
	table        *hashtable.Table
	clock        Clock
	logger       *slog.Logger
	debugEnabled bool
}

// New uses the real clock when clock is nil. Debug settings are fixed here.
func New(clock Clock, logger *slog.Logger) *Store {
	if clock == nil {
		clock = systemClock{}
	}
	return &Store{
		table:        hashtable.New(logger),
		clock:        clock,
		logger:       logger,
		debugEnabled: logger != nil && logger.Enabled(context.Background(), slog.LevelDebug),
	}
}

// Set replaces both the value and expiry. A nil TTL removes any previous expiry.
func (s *Store) Set(key string, value string, ttlMillis *int64) error {
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

	s.mu.Lock()
	defer s.mu.Unlock()
	// 락을 얻은 뒤 시간을 읽어, 락 대기 시간이 새 TTL을 줄이지 않게 한다.
	now := s.clock.Now()
	record := hashtable.Record{Value: value}
	if ttlMillis != nil {
		record.ExpiresAt = now.Add(time.Duration(ttl) * time.Millisecond)
	}
	updated := s.table.Put(key, record)
	if s.debugEnabled {
		ttlForLog := int64(-1)
		if ttlMillis != nil {
			ttlForLog = ttl
		}
		s.logger.Debug("SET", "key", key, "updated", updated,
			"unlimited", ttlMillis == nil, "ttl_ms", ttlForLog, "size", s.table.Len())
	}
	return nil
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
	if err := validateKey(key); err != nil {
		return DeleteResult{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	lookup := s.lookupLocked(key, s.clock.Now(), "delete")
	result := DeleteResult{State: lookup.State}
	if lookup.State == Live {
		result.Deleted = s.table.Delete(key)
	}
	if s.debugEnabled {
		s.logger.Debug("STORE_DELETE", "key", key, "state", result.State.String(),
			"deleted", result.Deleted, "size", s.table.Len())
	}
	return result, nil
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
			s.table.Delete(key)
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
