package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unicode/utf8"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/hashtable"
)

const (
	aofMagic = "MINIAOF1"
	maxFrame = 128 * 1024
)

type AOFOptions struct {
	Path                 string
	SyncInterval         time.Duration
	MaxUnsyncedAge       time.Duration
	RewriteMinBytes      int64
	MaxBytes             int64
	MaxRewriteDeltaBytes int
}

func DefaultAOFOptions(path string) AOFOptions {
	return AOFOptions{path, time.Second, 3 * time.Second, 4 << 20, 256 << 20, 8 << 20}
}

type aofRecord struct {
	Op        string     `json:"op"`
	Key       string     `json:"key"`
	Value     []byte     `json:"value,omitempty"` // Base64 preserves arbitrary Go string bytes.
	ExpiresAt *time.Time `json:"expires_at,omitempty"`
}

func recordForSet(key string, record hashtable.Record) aofRecord {
	r := aofRecord{Op: "set", Key: key, Value: []byte(record.Value)}
	if !record.ExpiresAt.IsZero() {
		wall := record.ExpiresAt.Round(0)
		r.ExpiresAt = &wall
	}
	return r
}

type aofFile interface {
	Write([]byte) (int, error)
	Sync() error
	Close() error
}

type journal struct {
	mu                   sync.Mutex
	syncMu               sync.Mutex // Protects file lifetime; never held with Store.mu.
	file                 aofFile
	lock                 *os.File
	opts                 AOFOptions
	logger               *slog.Logger
	debug                bool
	seq, synced          uint64
	size, baseSize       int64
	dirtySince, lastSync time.Time
	fatal                error
	closed               bool
	rewriting            bool
	delta                []byte
	rewriteOverflow      bool
	rewrites             uint64
	stop, done           chan struct{}
	closeOnce            sync.Once
	closeErr             error
	rewriteWG            sync.WaitGroup
	backgroundRewrite    bool
	createTemp           func(string, string) (*os.File, error)
	syncTemp             func(*os.File) error
	nextRewrite          time.Time
	retryDelay           time.Duration
	rewriteRequested     bool
	deleteReserve        int64
	healthOrigin         time.Time // immutable; preserves monotonic elapsed-time checks
	failed               atomic.Bool
	dirtySinceTick       atomic.Int64
	writeStartedTick     atomic.Int64
}

type AOFStatus struct {
	Enabled                   bool
	Bytes                     int64
	Written, Synced, Rewrites uint64
	LastSync                  time.Time
	Error                     string
	DeleteReserveBytes        int64
}

// OpenPersistent replays the file before accepting requests, then starts Sync.
// This format is private to mini-Redis and is not Redis AOF compatible.
func OpenPersistent(clock Clock, logger *slog.Logger, opts AOFOptions) (*Store, error) {
	return OpenPersistentWithLimit(clock, logger, opts, DefaultMaxDataBytes)
}
func OpenPersistentWithLimit(clock Clock, logger *slog.Logger, opts AOFOptions, maxBytes int64) (*Store, error) {
	if maxBytes <= 0 {
		return nil, fmt.Errorf("data limit must be positive")
	}
	if opts.Path == "" || opts.SyncInterval <= 0 || opts.MaxUnsyncedAge < opts.SyncInterval ||
		opts.RewriteMinBytes <= 0 || opts.MaxBytes <= int64(len(aofMagic)) || opts.RewriteMinBytes >= opts.MaxBytes || opts.MaxRewriteDeltaBytes <= 0 {
		return nil, fmt.Errorf("invalid AOF options")
	}
	path, err := filepath.Abs(opts.Path)
	if err != nil {
		return nil, err
	}
	opts.Path = path
	if err = os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = lockFile(lock); err != nil {
		lock.Close()
		return nil, fmt.Errorf("AOF already in use: %w", err)
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0600)
	if err != nil {
		lock.Close()
		return nil, err
	}
	cleanup := func() { f.Close(); lock.Close() }
	info, err := f.Stat()
	if err != nil {
		cleanup()
		return nil, err
	}
	if info.Size() > opts.MaxBytes {
		cleanup()
		return nil, fmt.Errorf("AOF exceeds configured size limit")
	}
	if info.Size() == 0 {
		if err = writeAll(f, []byte(aofMagic)); err == nil {
			err = f.Sync()
		}
		if err == nil {
			err = syncDirectory(filepath.Dir(path))
		}
		if err != nil {
			cleanup()
			return nil, err
		}
	}
	s := newStore(clock, logger, maxBytes)
	recovered, err := replay(f, s)
	if err != nil {
		cleanup()
		return nil, err
	}
	if recovered && logger != nil {
		logger.Warn("AOF_RECOVER_TAIL", "path", path)
	}
	if err = f.Sync(); err == nil {
		err = syncDirectory(filepath.Dir(path))
	}
	if err != nil {
		cleanup()
		return nil, err
	}
	info, err = f.Stat()
	if err != nil {
		cleanup()
		return nil, err
	}
	// 같은 AOF의 잠금을 가진 뒤, 이전 강제 종료로 남은 해당 AOF 임시 파일만 정리한다.
	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		cleanup()
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasPrefix(entry.Name(), rewritePrefix(path)) {
			if err = os.Remove(filepath.Join(filepath.Dir(path), entry.Name())); err != nil {
				cleanup()
				return nil, err
			}
		}
	}
	j := &journal{healthOrigin: time.Now(), file: f, lock: lock, opts: opts, logger: logger, debug: s.debugEnabled, createTemp: os.CreateTemp, syncTemp: func(f *os.File) error { return f.Sync() },
		size: info.Size(), baseSize: info.Size(), lastSync: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	s.aof = j
	j.deleteReserve = s.deletionReserve()
	if j.deleteReserve > opts.MaxBytes-j.size {
		// 이전 버전 파일도 삭제 예산을 확보한 뒤에만 쓰기를 수락한다.
		if err = s.RewriteAOF(); err != nil {
			j.file.Close()
			j.lock.Close()
			return nil, fmt.Errorf("AOF cannot reserve deletion capacity; increase size limit: %w", err)
		}
	}
	if logger != nil {
		logger.Info("AOF_REPLAY", "bytes", j.size, "keys", s.table.Len(), "tail_repaired", recovered, "delete_reserve_bytes", j.deleteReserve)
	}
	if s.usedBytes > s.maxBytes && logger != nil {
		logger.Warn("DATA_LIMIT_EXCEEDED_ON_REPLAY", "used_bytes", s.usedBytes, "max_bytes", s.maxBytes)
	}
	go s.expirationLoop()
	go s.persistenceLoop()
	return s, nil
}

func replay(f *os.File, s *Store) (bool, error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return false, err
	}
	header := make([]byte, len(aofMagic))
	if _, err := io.ReadFull(f, header); err != nil || string(header) != aofMagic {
		return false, fmt.Errorf("invalid or incomplete AOF header")
	}
	offset := int64(len(aofMagic))
	now := s.clock.Now()
	for {
		var length [8]byte
		n, err := io.ReadFull(f, length[:])
		if err == io.EOF && n == 0 {
			return false, nil
		}
		if err != nil {
			return repairTail(f, offset, err)
		}
		if crc32.ChecksumIEEE(length[:4]) != binary.BigEndian.Uint32(length[4:]) {
			return false, fmt.Errorf("AOF frame header checksum mismatch at %d", offset)
		}
		size := binary.BigEndian.Uint32(length[:4])
		if size == 0 || size > maxFrame {
			return false, fmt.Errorf("invalid AOF frame length at %d", offset)
		}
		payload := make([]byte, int(size)+4)
		if _, err = io.ReadFull(f, payload); err != nil {
			return repairTail(f, offset, err)
		}
		if crc32.ChecksumIEEE(payload[:size]) != binary.BigEndian.Uint32(payload[size:]) {
			return false, fmt.Errorf("AOF checksum mismatch at %d", offset)
		}
		r, decodeErr := decodeAOFRecord(payload[:size])
		if decodeErr != nil {
			return false, fmt.Errorf("invalid AOF record at %d: %w", offset, decodeErr)
		}
		switch r.Op {
		case "set":
			record := hashtable.Record{Value: string(r.Value)}
			if r.ExpiresAt != nil {
				if !now.Before(*r.ExpiresAt) {
					// 만료된 덮어쓰기도 이전 값을 지워야 예전 무기한 값이 부활하지 않는다.
					s.removeLocked(r.Key)
					offset += int64(size) + 12
					continue
				}
				// 파일의 벽시계 시각으로 남은 기간을 구하고 현재 단조 시계에 연결한다.
				record.ExpiresAt = now.Add(r.ExpiresAt.Sub(now))
			}
			s.putLocked(r.Key, record)
		case "del":
			if r.ExpiresAt != nil || len(r.Value) != 0 {
				return false, fmt.Errorf("invalid delete record at %d", offset)
			}
			s.removeLocked(r.Key)
		default:
			return false, fmt.Errorf("unknown AOF operation at %d", offset)
		}
		offset += int64(size) + 12
	}
}

func repairTail(f *os.File, offset int64, cause error) (bool, error) {
	if !errors.Is(cause, io.EOF) && !errors.Is(cause, io.ErrUnexpectedEOF) {
		return false, cause
	}
	if err := f.Truncate(offset); err != nil {
		return false, err
	}
	return true, f.Sync()
}

func encodeFrame(r aofRecord) ([]byte, error) {
	body, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(body) > maxFrame {
		return nil, fmt.Errorf("AOF frame too large")
	}
	frame := make([]byte, len(body)+12)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(body)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(frame[:4]))
	copy(frame[8:], body)
	binary.BigEndian.PutUint32(frame[8+len(body):], crc32.ChecksumIEEE(body))
	return frame, nil
}

func writeAll(w io.Writer, data []byte) error {
	n, err := w.Write(data)
	if err == nil && n != len(data) {
		err = io.ErrShortWrite
	}
	return err
}

func (j *journal) healthLocked() error {
	if j.closed {
		return fmt.Errorf("%w: closed", ErrPersistence)
	}
	if j.fatal != nil {
		return fmt.Errorf("%w: %v", ErrPersistence, j.fatal)
	}
	if j.seq > j.synced && time.Since(j.dirtySince) > j.opts.MaxUnsyncedAge {
		return fmt.Errorf("%w: disk sync is overdue", ErrPersistence)
	}
	return nil
}

func (j *journal) health() error { j.mu.Lock(); defer j.mu.Unlock(); return j.healthLocked() }

func (j *journal) append(r aofRecord, reserveDelta int64) error {
	frame, err := encodeFrame(r)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrPersistence, err)
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	if err = j.healthLocked(); err != nil {
		return err
	}
	if reserveDelta > 0 && j.deleteReserve > j.opts.MaxBytes-reserveDelta {
		j.rewriteRequested = true
		return fmt.Errorf("%w: deletion reserve limit reached", ErrPersistence)
	}
	reserve := j.deleteReserve + reserveDelta
	if reserve < 0 {
		return fmt.Errorf("%w: invalid deletion reserve", ErrPersistence)
	}
	if int64(len(frame)) > j.opts.MaxBytes-j.size-reserve {
		j.rewriteRequested = true
		return fmt.Errorf("%w: AOF size limit reached", ErrPersistence)
	}
	j.writeStartedTick.Store(j.healthTick(time.Now()))
	err = writeAll(j.file, frame)
	if err != nil {
		j.fatal = err
		j.failed.Store(true)
		j.writeStartedTick.Store(0)
		if j.logger != nil {
			j.logger.Error("AOF_WRITE_FAILED", "error", err)
		}
		return fmt.Errorf("%w: %v", ErrPersistence, err)
	}
	if j.seq == j.synced {
		j.dirtySince = time.Now()
		j.dirtySinceTick.Store(j.healthTick(j.dirtySince))
	}
	j.writeStartedTick.Store(0)
	j.seq++
	j.size += int64(len(frame))
	j.deleteReserve = reserve
	if r.Op == "del" && j.rewriteRequested {
		j.rewriteRequested = true
		j.nextRewrite = time.Time{}
		j.retryDelay = 0
	}
	if j.rewriting && !j.rewriteOverflow {
		if len(frame) > j.opts.MaxRewriteDeltaBytes-len(j.delta) {
			j.delta = nil
			j.rewriteOverflow = true
		} else {
			j.delta = append(j.delta, frame...)
		}
	}
	if j.debug {
		j.logger.Debug("AOF_APPEND", "op", r.Op, "key", r.Key, "written", j.seq, "synced", j.synced, "bytes", j.size)
	}
	return nil
}

func (j *journal) sync() error {
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	j.mu.Lock()
	if j.closed {
		j.mu.Unlock()
		return fmt.Errorf("%w: closed", ErrPersistence)
	}
	if j.fatal != nil {
		err := j.fatal
		j.mu.Unlock()
		return fmt.Errorf("%w: %v", ErrPersistence, err)
	}
	f, seq := j.file, j.seq
	if seq == j.synced {
		j.mu.Unlock()
		return nil
	}
	started := time.Now()
	j.mu.Unlock()
	err := f.Sync() // 메모리·로그 메타데이터 락을 잡지 않아 조회를 계속 처리한다.
	j.mu.Lock()
	defer j.mu.Unlock()
	if err != nil {
		j.fatal = err
		j.failed.Store(true)
		if j.logger != nil {
			j.logger.Error("AOF_SYNC_FAILED", "error", err)
		}
		return fmt.Errorf("%w: %v", ErrPersistence, err)
	}
	j.synced = seq
	j.lastSync = time.Now()
	if seq == j.seq {
		j.dirtySince = time.Time{}
		j.dirtySinceTick.Store(0)
	} else {
		j.dirtySince = started
		j.dirtySinceTick.Store(j.healthTick(started))
	}
	if j.debug {
		j.logger.Debug("AOF_SYNC", "written", j.seq, "synced", j.synced, "elapsed", time.Since(started))
	}
	return nil
}

// Called with j.mu held. Occupancy includes deletion reservations, but
// ordinary deletes do not bypass the minimum growth or retry backoff.
func (j *journal) shouldRewriteLocked(now time.Time) bool {
	growth := j.size - j.baseSize
	return !j.backgroundRewrite && !now.Before(j.nextRewrite) &&
		(j.rewriteRequested || (j.size >= j.opts.RewriteMinBytes &&
			(growth >= j.baseSize || (growth >= j.opts.RewriteMinBytes && j.size+j.deleteReserve >= j.opts.MaxBytes-j.opts.MaxBytes/4))))
}

func (s *Store) persistenceLoop() {
	j := s.aof
	ticker := time.NewTicker(j.opts.SyncInterval)
	defer ticker.Stop()
	defer func() { j.rewriteWG.Wait(); close(j.done) }()
	for {
		select {
		case <-j.stop:
			return
		case <-ticker.C:
			if j.sync() != nil {
				continue
			}
			j.mu.Lock()
			need := j.shouldRewriteLocked(time.Now())
			if need {
				j.backgroundRewrite = true
			}
			beforeUsed := j.size + j.deleteReserve
			j.mu.Unlock()
			if need {
				j.rewriteWG.Add(1)
				go func() {
					defer j.rewriteWG.Done()
					err := s.RewriteAOF()
					j.mu.Lock()
					j.backgroundRewrite = false
					noSpaceReclaimed := err == nil && j.size+j.deleteReserve >= beforeUsed
					if err != nil || noSpaceReclaimed {
						j.retryDelay *= 2
						if j.retryDelay == 0 {
							j.retryDelay = time.Second
						}
						if j.retryDelay > time.Minute {
							j.retryDelay = time.Minute
						}
						j.nextRewrite = time.Now().Add(j.retryDelay)
					} else {
						j.retryDelay = 0
						j.nextRewrite = time.Time{}
						j.rewriteRequested = false
					}
					j.mu.Unlock()
					if err != nil && j.logger != nil {
						j.logger.Warn("AOF_REWRITE_FAILED", "error", err)
					}
				}()
			}
		}
	}
}

type snapshotEntry struct {
	key    string
	record hashtable.Record
}

// RewriteAOF keeps reads available during file I/O. Writes pause during final
// delta flush/cutover; the old file remains authoritative until rename.
func (s *Store) RewriteAOF() error {
	j := s.aof
	if j == nil {
		return nil
	}
	s.writeMu.Lock()
	j.mu.Lock()
	if err := j.healthLocked(); err != nil {
		j.mu.Unlock()
		s.writeMu.Unlock()
		return err
	}
	if j.rewriting {
		j.mu.Unlock()
		s.writeMu.Unlock()
		return fmt.Errorf("AOF rewrite already running")
	}
	j.rewriting = true
	j.delta = nil
	j.rewriteOverflow = false
	j.mu.Unlock()
	snapshot := s.copyEntries(true)
	s.writeMu.Unlock()
	defer func() { j.mu.Lock(); j.rewriting = false; j.delta = nil; j.mu.Unlock() }()
	f, err := j.createTemp(filepath.Dir(j.opts.Path), rewritePrefix(j.opts.Path))
	if err != nil {
		return err
	}
	defer func() { f.Close(); os.Remove(f.Name()) }()
	if err = writeAll(f, []byte(aofMagic)); err != nil {
		return err
	}
	for _, entry := range snapshot {
		select {
		case <-j.stop:
			return fmt.Errorf("AOF rewrite canceled by shutdown")
		default:
		}
		frame, err := encodeFrame(recordForSet(entry.key, entry.record))
		if err != nil {
			return err
		}
		if err = writeAll(f, frame); err != nil {
			return err
		}
	}
	if err = j.syncTemp(f); err != nil {
		return err
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	j.syncMu.Lock()
	defer j.syncMu.Unlock()
	j.mu.Lock()
	if err = j.healthLocked(); err != nil {
		j.mu.Unlock()
		return err
	}
	if j.rewriteOverflow {
		j.mu.Unlock()
		return fmt.Errorf("rewrite delta limit exceeded; original AOF retained")
	}
	delta := j.delta
	j.mu.Unlock()
	reserve := s.deletionReserve()
	if err = writeAll(f, delta); err != nil {
		return err
	}
	info, err := f.Stat()
	if err != nil {
		return err
	}
	if info.Size() > j.opts.MaxBytes-reserve {
		return fmt.Errorf("rewritten AOF exceeds size limit")
	}
	if err = j.syncTemp(f); err != nil {
		return err
	}
	// 교체 전에 새 추가 기록 핸들을 연다. 교체 후에는 이전 파일에 기록하지 않는다.
	next, err := os.OpenFile(f.Name(), os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	if err = os.Rename(f.Name(), j.opts.Path); err != nil {
		next.Close()
		return err
	}
	dirErr := syncDirectory(filepath.Dir(j.opts.Path))
	j.mu.Lock()
	old := j.file
	j.file = next
	j.size = info.Size()
	j.baseSize = info.Size()
	j.deleteReserve = reserve
	j.synced = j.seq
	j.lastSync = time.Now()
	j.dirtySince = time.Time{}
	j.dirtySinceTick.Store(0)
	j.rewrites++
	if dirErr != nil {
		j.fatal = dirErr
		j.failed.Store(true)
	}
	j.mu.Unlock()
	closeErr := old.Close()
	if dirErr != nil {
		return fmt.Errorf("%w: directory sync: %v", ErrPersistence, dirErr)
	}
	if closeErr != nil {
		return closeErr
	}
	if j.logger != nil {
		j.logger.Info("AOF_REWRITE", "bytes", info.Size(), "keys", len(snapshot))
	}
	return nil
}

func syncDirectory(path string) error {
	d, err := os.Open(path)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

func rewritePrefix(path string) string { return "." + filepath.Base(path) + ".rewrite-" }

func (s *Store) PersistenceStatus() AOFStatus {
	j := s.aof
	if j == nil {
		return AOFStatus{}
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	status := AOFStatus{Enabled: true, Bytes: j.size, Written: j.seq, Synced: j.synced, Rewrites: j.rewrites, LastSync: j.lastSync, DeleteReserveBytes: j.deleteReserve}
	if err := j.healthLocked(); err != nil {
		status.Error = err.Error()
	}
	return status
}

// Exact encoding/json length for a validated UTF-8 key, including default
// HTML escaping. No frame or JSON buffer is allocated to reserve DEL space.
func deleteRecordBytes(key string) int64 {
	n := int64(12 + len(`{"op":"del","key":""}`))
	for _, r := range key {
		switch {
		case r == '"' || r == '\\' || r == '\b' || r == '\f' || r == '\n' || r == '\r' || r == '\t':
			n += 2
		case r < 0x20 || r == '<' || r == '>' || r == '&' || r == '\u2028' || r == '\u2029':
			n += 6
		default:
			n += int64(utf8.RuneLen(r))
		}
	}
	return n
}

// Caller excludes SET/DELETE/resize for the full traversal. Expiration may
// delete nodes between batches; counting such a node only over-reserves.
func (s *Store) deletionReserve() int64 {
	var reserve int64
	var cursor hashtable.ScanCursor
	for {
		done := func() bool {
			s.mu.Lock()
			defer s.mu.Unlock()
			now := s.clock.Now()
			return s.table.Scan(&cursor, 128, func(key string, _ hashtable.Record) {
				// Physically remove expiry BEFORE dropping its reservation. Merely
				// filtering it would under-reserve a later overwrite of the same node.
				r, ok := s.table.Get(key)
				if !ok {
					return
				}
				if !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt) {
					s.removeLocked(key)
					return
				}
				reserve += deleteRecordBytes(key)
			})
		}()
		if done {
			return reserve
		}
		runtime.Gosched()
	}
}

// The caller owns writeMu (or has not published the Store during startup).
// Get may remove expired nodes between batches; no live insertion or resize
// can move unvisited data. Allocation/append is outside the memory lock.
func (s *Store) copyEntries(onlyLive bool) []snapshotEntry {
	s.mu.Lock()
	now := s.clock.Now()
	s.mu.Unlock()
	var result []snapshotEntry
	var cursor hashtable.ScanCursor
	for {
		var batch [128]snapshotEntry
		count := 0
		s.mu.Lock()
		done := s.table.Scan(&cursor, len(batch), func(key string, r hashtable.Record) {
			if !onlyLive || r.ExpiresAt.IsZero() || now.Before(r.ExpiresAt) {
				batch[count] = snapshotEntry{key, r}
				count++
			}
		})
		s.mu.Unlock()
		result = append(result, batch[:count]...)
		if done {
			return result
		}
		runtime.Gosched()
	}
}

// Close stops background work and syncs remaining writes. Disk calls themselves
// cannot be canceled; the server enforces its overall shutdown deadline.
func (s *Store) Close() error {
	s.maintenanceOnce.Do(func() { close(s.maintenanceStop); <-s.maintenanceDone })
	j := s.aof
	if j == nil {
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		s.closed.Store(true)
		return nil
	}
	j.closeOnce.Do(func() {
		close(j.stop)
		<-j.done
		s.writeMu.Lock()
		defer s.writeMu.Unlock()
		j.closeErr = j.sync()
		j.syncMu.Lock()
		defer j.syncMu.Unlock()
		j.mu.Lock()
		defer j.mu.Unlock()
		j.closed = true
		j.failed.Store(true)
		s.closed.Store(true)
		// 기록 오류가 있어도 그 앞의 정상 기록은 최대한 디스크에 반영한다.
		// 최초 오류는 그대로 반환하며 저장 성공으로 바꾸지 않는다.
		if j.closeErr != nil {
			j.closeErr = errors.Join(j.closeErr, j.file.Sync())
		}
		j.closeErr = errors.Join(j.closeErr, j.file.Close(), j.lock.Close())
	})
	return j.closeErr
}

// Elapsed ticks use the immutable monotonic origin, not Unix wall time.
// Zero is reserved for "no outstanding operation".
func (j *journal) healthTick(now time.Time) int64 { return now.Sub(j.healthOrigin).Nanoseconds() + 1 }

// Probe only published atomics: ordinary metadata lock contention is healthy.
func (s *Store) PersistenceHealth() (enabled, healthy bool) {
	if s.aof == nil {
		return false, !s.closed.Load() && !s.mutationFailed.Load()
	}
	j := s.aof
	if j.failed.Load() || s.closed.Load() || s.mutationFailed.Load() {
		return true, false
	}
	now := j.healthTick(time.Now())
	overdue := func(start int64) bool { return start != 0 && now-start > int64(j.opts.MaxUnsyncedAge) }
	return true, !overdue(j.dirtySinceTick.Load()) && !overdue(j.writeStartedTick.Load())
}
