package store

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"
)

// mutationGate keeps the familiar Lock/Unlock API for internal maintenance,
// while user requests can stop waiting without acquiring the gate later.
type mutationGate struct {
	once  sync.Once
	token chan struct{}
}

func (g *mutationGate) init() {
	g.once.Do(func() { g.token = make(chan struct{}, 1); g.token <- struct{}{} })
}
func (g *mutationGate) Lock()   { g.init(); <-g.token }
func (g *mutationGate) Unlock() { g.token <- struct{}{} }
func (g *mutationGate) lockContext(ctx context.Context) error {
	g.init()
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-g.token:
		if err := ctx.Err(); err != nil {
			g.Unlock()
			return err
		}
		return nil
	}
}

// runMutation bounds the caller's wait, not the OS disk syscall. Only one
// persistent mutation worker can be active: it owns the gate until completion.
// Once admitted, an operation may commit after its caller times out. This is
// an uncertain outcome, never a claim that the write was rolled back.
func (s *Store) runMutation(parent context.Context, fn func() error) error {
	ctx, cancel := context.WithTimeout(parent, s.mutationTimeout)
	defer cancel()
	if s.closed.Load() {
		return fmt.Errorf("%w: store closed", ErrPersistence)
	}
	if s.mutationFailed.Load() {
		return fmt.Errorf("%w: mutation worker failed; restart required", ErrPersistence)
	}
	if err := s.writeMu.lockContext(ctx); err != nil {
		return fmt.Errorf("%w: mutation not admitted: %w", ErrPersistence, err)
	}
	if s.closed.Load() || s.mutationFailed.Load() {
		s.writeMu.Unlock()
		return fmt.Errorf("%w: restart required", ErrPersistence)
	}
	if s.aof == nil {
		defer s.writeMu.Unlock()
		return fn()
	}
	done := make(chan error, 1)
	go func() {
		var result error
		defer func() {
			if recovered := recover(); recovered != nil {
				s.mutationFailed.Store(true)
				if s.logger != nil {
					s.logger.Error("MUTATION_PANIC", "panic", fmt.Sprint(recovered), "stack", string(debug.Stack()))
				}
				result = fmt.Errorf("%w: mutation worker panic; restart required", ErrOutcomeUnknown)
			}
			s.writeMu.Unlock()
			done <- result
		}()
		result = fn()
	}()
	select {
	case result := <-done:
		return result
	case <-ctx.Done():
		return fmt.Errorf("%w: mutation completion unknown: %w", ErrOutcomeUnknown, ctx.Err())
	}
}

const defaultMutationTimeout = 3 * time.Second
