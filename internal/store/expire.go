package store

import (
	"context"
	"fmt"
	"runtime/debug"
	"time"

	"github.com/AI-Together-W10-W11-T1/Mini_Redis/internal/hashtable"
)

// Maintenance adapts scan work to table size and observed expiry density.
// Lock contention does not permanently pin its admission wait to 1ms.
func (s *Store) expirationLoop() {
	defer close(s.maintenanceDone)
	defer func() {
		if cause := recover(); cause != nil {
			s.mutationFailed.Store(true)
			if s.logger != nil {
				s.logger.Error("EXPIRATION_PANIC", "panic", fmt.Sprint(cause), "stack", string(debug.Stack()))
			}
		}
	}()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	wait := time.Millisecond
	for {
		select {
		case <-s.maintenanceStop:
			return
		case <-ticker.C:
			deadline := time.Now().Add(25 * time.Millisecond)
			// Scale the base scan with table size, targeting roughly a full cycle
			// per second. Dense expiry extends the work within this tick's budget.
			s.mu.Lock()
			remaining := (s.table.Capacity() + s.table.Len() + 9) / 10
			s.mu.Unlock()
			if remaining < 256 {
				remaining = 256
			}
			for time.Now().Before(deadline) {
				ctx, cancel := context.WithTimeout(context.Background(), wait)
				result := s.sweepExpired(ctx, 256)
				cancel()
				if !result.Acquired {
					wait = 5 * time.Millisecond
					break
				}
				wait = time.Millisecond
				remaining -= 256
				dense := result.Visited > 0 && result.Removed*4 >= result.Visited
				if result.Done || (remaining <= 0 && !dense) {
					break
				}
				select {
				case <-s.maintenanceStop:
					return
				default:
				}
			}
		}
	}
}

type sweepResult struct {
	Visited, Removed int
	Freed            int64
	Done, Acquired   bool
}

func (s *Store) sweepExpired(ctx context.Context, budget int) sweepResult {
	if err := s.writeMu.lockContext(ctx); err != nil {
		return sweepResult{}
	}
	defer s.writeMu.Unlock()
	return s.reclaimExpiredHeld(budget)
}

// Caller already owns writeMu. Each memory-lock batch is at most 256 scan
// steps; the result describes node callbacks, not empty bucket probes.
func (s *Store) reclaimExpiredHeld(budget int) sweepResult {
	result := sweepResult{Acquired: true}
	for budget > 0 {
		batch := budget
		if batch > 256 {
			batch = 256
		}
		var freed int64
		func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			now := s.clock.Now()
			result.Done = s.table.ScanWeak(&s.sweepCursor, batch, func(key string, _ hashtable.Record) {
				result.Visited++
				// A removed node may still be in the weak cursor. Recheck membership
				// and expiry so a replacement under the same key is never deleted.
				r, ok := s.table.Get(key)
				if ok && !r.ExpiresAt.IsZero() && !now.Before(r.ExpiresAt) && s.removeLocked(key) {
					result.Removed++
					freed += deleteRecordBytes(key)
				}
			})
			if result.Done {
				s.sweepCursor = hashtable.ScanCursor{}
			}
		}()
		if s.aof != nil && freed > 0 {
			s.aof.mu.Lock()
			s.aof.deleteReserve -= freed
			s.aof.mu.Unlock()
		}
		result.Freed += freed
		budget -= batch
		if result.Done {
			break
		}
	}
	return result
}
