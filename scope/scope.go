// SPDX-License-Identifier: Apache-2.0 OR MIT

// Package scope implements structured concurrency, modeled on Kotlin's
// coroutineScope.
//
// [Run] starts a scope and waits for every task forked in it before returning.
// If a task returns an error or panics, the scope's context is cancelled and
// Run reports that failure.
//
//	err := scope.Run(ctx, func(ctx context.Context, s *scope.Scope) error {
//		user := s.Fork(func(ctx context.Context) (User, error) {
//			return fetchUser(ctx, id)
//		})
//		orders := s.Fork(func(ctx context.Context) ([]Order, error) {
//			return fetchOrders(ctx, id)
//		})
//
//		u, err := user.Await(ctx)
//		if err != nil {
//			return err
//		}
//		o, err := orders.Await(ctx)
//		if err != nil {
//			return err
//		}
//		return render(u, o)
//	})
package scope

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"sync"
)

// errGoexit is the error for a task that called runtime.Goexit, e.g. via
// t.FailNow in a test.
var errGoexit = errors.New("scope: task exited via runtime.Goexit")

// A Scope tracks the tasks started inside a call to [Run]. Don't use it after
// Run returns.
type Scope struct {
	ctx    context.Context
	cancel context.CancelCauseFunc

	mu     sync.Mutex
	idle   sync.Cond // broadcast when active hits 0
	active int
	closed bool
	err    error       // first error
	panic  *PanicError // first panic
}

// Run calls body with a new scope and waits for all of the scope's tasks to
// finish.
//
// The context passed to body and to each task is cancelled as soon as body or
// a task fails, and when Run returns. Run returns the first error. If anything
// panicked, Run re-panics with a [*PanicError] once all tasks are done.
func Run(ctx context.Context, body func(ctx context.Context, s *Scope) error) error {
	_, err := RunValue(ctx, func(ctx context.Context, s *Scope) (struct{}, error) {
		return struct{}{}, body(ctx, s)
	})
	return err
}

// RunValue is like [Run], but returns body's result. If anything fails, the
// result is the zero value.
func RunValue[T any](ctx context.Context, body func(ctx context.Context, s *Scope) (T, error)) (val T, err error) {
	ctx, cancel := context.WithCancelCause(ctx)
	s := &Scope{ctx: ctx, cancel: cancel}
	s.idle.L = &s.mu

	// If body calls runtime.Goexit, the assignment below never runs and
	// bodyErr stays errGoexit. The defer still runs, so we still wait.
	bodyErr := errGoexit
	defer func() {
		s.fail(bodyErr)
		if err = s.close(); err != nil {
			var zero T
			val = zero
		}
	}()
	bodyErr = catch(func() (err error) {
		val, err = body(ctx, s)
		return err
	})
	return val, nil
}

// Fork runs fn in a new goroutine and returns a [Task] for its result. An
// error or panic from fn cancels the scope.
//
// Fork panics if the scope has already finished.
func (s *Scope) Fork[T any](fn func(ctx context.Context) (T, error)) *Task[T] {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		panic("scope: Fork called after Run returned")
	}
	s.active++
	s.mu.Unlock()

	t := &Task[T]{done: make(chan struct{})}
	go func() {
		defer s.release()
		// Close done before failing the scope. Otherwise an Await on the
		// scope's context could wake up from the cancel and miss our error.
		defer func() { s.fail(t.err) }()
		defer close(t.done)
		// Stays errGoexit if fn calls runtime.Goexit.
		t.err = errGoexit
		t.err = catch(func() (err error) {
			t.val, err = fn(s.ctx)
			return err
		})
	}()
	return t
}

// Go is [Scope.Fork] for functions that only return an error.
func (s *Scope) Go(fn func(ctx context.Context) error) *Task[struct{}] {
	return s.Fork(func(ctx context.Context) (struct{}, error) {
		return struct{}{}, fn(ctx)
	})
}

// catch calls fn and turns a panic into a *PanicError.
func catch(fn func() error) (err error) {
	defer func() {
		if v := recover(); v != nil {
			err = &PanicError{Value: v, Stack: debug.Stack()}
		}
	}()
	return fn()
}

// fail records the first error and cancels the scope. A nil err is a no-op.
func (s *Scope) fail(err error) {
	if err == nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if p, ok := err.(*PanicError); ok && s.panic == nil {
		s.panic = p
	}
	if s.err == nil {
		s.err = err
		s.cancel(err)
	}
}

func (s *Scope) release() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.active--
	if s.active == 0 {
		s.idle.Broadcast()
	}
}

// close waits for the tasks to finish and marks the scope closed, so any
// later Fork panics.
func (s *Scope) close() error {
	s.mu.Lock()
	for s.active > 0 {
		s.idle.Wait()
	}
	s.closed = true
	err, p := s.err, s.panic
	s.mu.Unlock()

	s.cancel(nil)
	if p != nil {
		panic(p)
	}
	return err
}

// A Task is the pending result of a forked function.
type Task[T any] struct {
	done chan struct{}
	val  T
	err  error
}

// Await waits for the task and returns its result. It's safe to call more
// than once; every call returns the same thing.
//
// If ctx is done first, Await returns ctx.Err(). The task keeps running.
func (t *Task[T]) Await(ctx context.Context) (T, error) {
	// If the result is ready, return it even if ctx is done.
	select {
	case <-t.done:
		return t.val, t.err
	default:
	}
	select {
	case <-t.done:
		return t.val, t.err
	case <-ctx.Done():
		// ctx might be the scope's, cancelled by this very task. Tasks close
		// done before cancelling, so check again.
		select {
		case <-t.done:
			return t.val, t.err
		default:
			var zero T
			return zero, ctx.Err()
		}
	}
}

// Wait is Await without the value.
func (t *Task[T]) Wait(ctx context.Context) error {
	_, err := t.Await(ctx)
	return err
}

// PanicError holds a recovered panic value and the stack where it happened.
type PanicError struct {
	Value any
	Stack []byte
}

func (p *PanicError) Error() string {
	return fmt.Sprintf("panic: %v\n\n%s", p.Value, p.Stack)
}

// Unwrap returns Value if it's an error.
func (p *PanicError) Unwrap() error {
	err, _ := p.Value.(error)
	return err
}
