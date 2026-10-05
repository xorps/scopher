// SPDX-License-Identifier: Apache-2.0 OR MIT

package scope

import (
	"context"
	"errors"
	"runtime"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"
)

func TestForkAwait(t *testing.T) {
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		a := s.Fork(func(ctx context.Context) (int, error) { return 1, nil })
		b := s.Fork(func(ctx context.Context) (string, error) { return "two", nil })

		av, err := a.Await(ctx)
		if err != nil || av != 1 {
			t.Errorf("a.Await() = %v, %v; want 1, nil", av, err)
		}
		bv, err := b.Await(ctx)
		if err != nil || bv != "two" {
			t.Errorf("b.Await() = %q, %v; want \"two\", nil", bv, err)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Run() = %v; want nil", err)
	}
}

func TestRunValue(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var finished atomic.Bool
		v, err := RunValue(t.Context(), func(ctx context.Context, s *Scope) (int, error) {
			s.Go(func(ctx context.Context) error {
				time.Sleep(time.Second)
				finished.Store(true)
				return nil
			})
			a := s.Fork(func(ctx context.Context) (int, error) { return 40, nil })
			av, err := a.Await(ctx)
			return av + 2, err
		})
		if v != 42 || err != nil {
			t.Errorf("RunValue() = %v, %v; want 42, nil", v, err)
		}
		if !finished.Load() {
			t.Error("RunValue returned before its task finished")
		}
	})
}

func TestRunValueTaskErrorZeroesValue(t *testing.T) {
	boom := errors.New("boom")
	v, err := RunValue(t.Context(), func(ctx context.Context, s *Scope) (int, error) {
		s.Go(func(ctx context.Context) error { return boom })
		return 42, nil
	})
	if v != 0 || err != boom {
		t.Errorf("RunValue() = %v, %v; want 0, %v", v, err, boom)
	}
}

func TestAwaitIdempotent(t *testing.T) {
	var calls atomic.Int32
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		task := s.Fork(func(ctx context.Context) (int, error) {
			return int(calls.Add(1)), nil
		})
		for range 3 {
			if v, err := task.Await(ctx); v != 1 || err != nil {
				t.Errorf("Await() = %v, %v; want 1, nil", v, err)
			}
		}
		return nil
	})
	if err != nil {
		t.Errorf("Run() = %v; want nil", err)
	}
	if n := calls.Load(); n != 1 {
		t.Errorf("task ran %d times; want 1", n)
	}
}

func TestRunWaitsForTasks(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var finished atomic.Bool
		err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
			s.Go(func(ctx context.Context) error {
				time.Sleep(time.Second)
				finished.Store(true)
				return nil
			})
			return nil
		})
		if err != nil {
			t.Errorf("Run() = %v; want nil", err)
		}
		if !finished.Load() {
			t.Error("Run returned before its task finished")
		}
	})
}

func TestTaskErrorCancelsSiblings(t *testing.T) {
	boom := errors.New("boom")
	var cause error
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		s.Go(func(ctx context.Context) error {
			<-ctx.Done()
			cause = context.Cause(ctx)
			return ctx.Err()
		})
		s.Go(func(ctx context.Context) error { return boom })
		return nil
	})
	if err != boom {
		t.Errorf("Run() = %v; want %v", err, boom)
	}
	if cause != boom {
		t.Errorf("sibling saw cause %v; want %v", cause, boom)
	}
}

func TestBodyErrorCancelsTasks(t *testing.T) {
	boom := errors.New("boom")
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		s.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		return boom
	})
	if err != boom {
		t.Errorf("Run() = %v; want %v", err, boom)
	}
}

func TestAwaitReturnsTaskError(t *testing.T) {
	boom := errors.New("boom")
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		task := s.Fork(func(ctx context.Context) (int, error) { return 0, boom })
		if _, err := task.Await(ctx); err != boom {
			t.Errorf("Await() err = %v; want %v", err, boom)
		}
		return nil
	})
	if err != boom {
		t.Errorf("Run() = %v; want %v", err, boom)
	}
}

func TestGoWait(t *testing.T) {
	boom := errors.New("boom")
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		var ran atomic.Bool
		ok := s.Go(func(ctx context.Context) error {
			ran.Store(true)
			return nil
		})
		if err := ok.Wait(ctx); err != nil || !ran.Load() {
			t.Errorf("Wait() = %v, ran = %v; want nil, true", err, ran.Load())
		}

		failed := s.Go(func(ctx context.Context) error { return boom })
		if err := failed.Wait(ctx); err != boom {
			t.Errorf("Wait() = %v; want %v", err, boom)
		}
		return nil
	})
	if err != boom {
		t.Errorf("Run() = %v; want %v", err, boom)
	}
}

func TestAwaitContextCancelled(t *testing.T) {
	release := make(chan struct{})
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		task := s.Fork(func(ctx context.Context) (int, error) {
			<-release
			return 1, nil
		})

		waitCtx, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := task.Await(waitCtx); err != context.Canceled {
			t.Errorf("Await(cancelled) err = %v; want %v", err, context.Canceled)
		}

		close(release)
		if v, err := task.Await(ctx); v != 1 || err != nil {
			t.Errorf("Await() = %v, %v; want 1, nil", v, err)
		}
		return nil
	})
	if err != nil {
		t.Errorf("Run() = %v; want nil", err)
	}
}

func TestParentCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	err := Run(ctx, func(ctx context.Context, s *Scope) error {
		s.Go(func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		})
		cancel()
		return nil
	})
	if err != context.Canceled {
		t.Errorf("Run() = %v; want %v", err, context.Canceled)
	}
}

func TestNestedFork(t *testing.T) {
	var n atomic.Int32
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		s.Go(func(ctx context.Context) error {
			s.Go(func(ctx context.Context) error {
				n.Add(1)
				return nil
			})
			return nil
		})
		return nil
	})
	if err != nil {
		t.Errorf("Run() = %v; want nil", err)
	}
	if n.Load() != 1 {
		t.Error("task forked from a task did not run before Run returned")
	}
}

func TestNestedScope(t *testing.T) {
	boom := errors.New("boom")
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		s.Go(func(ctx context.Context) error {
			return Run(ctx, func(ctx context.Context, s *Scope) error {
				s.Go(func(ctx context.Context) error { return boom })
				return nil
			})
		})
		return nil
	})
	if err != boom {
		t.Errorf("Run() = %v; want %v", err, boom)
	}
}

func TestTaskPanic(t *testing.T) {
	var siblingCancelled atomic.Bool
	p := catchPanic(func() {
		_ = Run(t.Context(), func(ctx context.Context, s *Scope) error {
			s.Go(func(ctx context.Context) error {
				<-ctx.Done()
				siblingCancelled.Store(true)
				return nil
			})
			task := s.Fork(func(ctx context.Context) (int, error) { panic("oops") })
			if _, err := task.Await(ctx); err == nil {
				t.Error("Await() of panicked task returned nil error")
			}
			return nil
		})
	})
	pe, ok := p.(*PanicError)
	if !ok || pe.Value != "oops" {
		t.Fatalf("Run panicked with %#v; want *PanicError{Value: \"oops\"}", p)
	}
	if !siblingCancelled.Load() {
		t.Error("sibling was not cancelled before Run panicked")
	}
}

func TestBodyPanicWaitsForTasks(t *testing.T) {
	var finished atomic.Bool
	p := catchPanic(func() {
		_ = Run(t.Context(), func(ctx context.Context, s *Scope) error {
			s.Go(func(ctx context.Context) error {
				<-ctx.Done()
				finished.Store(true)
				return nil
			})
			panic("oops")
		})
	})
	if pe, ok := p.(*PanicError); !ok || pe.Value != "oops" {
		t.Fatalf("Run panicked with %#v; want *PanicError{Value: \"oops\"}", p)
	}
	if !finished.Load() {
		t.Error("Run panicked before its task finished")
	}
}

func TestPanicErrorUnwrap(t *testing.T) {
	boom := errors.New("boom")
	p := catchPanic(func() {
		_ = Run(t.Context(), func(ctx context.Context, s *Scope) error {
			s.Go(func(ctx context.Context) error { panic(boom) })
			return nil
		})
	})
	if err, _ := p.(error); !errors.Is(err, boom) {
		t.Errorf("errors.Is(%v, boom) = false; want true", p)
	}
}

func TestTaskGoexit(t *testing.T) {
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		task := s.Fork(func(ctx context.Context) (int, error) {
			runtime.Goexit()
			return 0, nil
		})
		if _, err := task.Await(ctx); err != errGoexit {
			t.Errorf("Await() err = %v; want %v", err, errGoexit)
		}
		return nil
	})
	if err != errGoexit {
		t.Errorf("Run() = %v; want %v", err, errGoexit)
	}
}

func TestForkAfterRunPanics(t *testing.T) {
	var leaked *Scope
	err := Run(t.Context(), func(ctx context.Context, s *Scope) error {
		leaked = s
		return nil
	})
	if err != nil {
		t.Errorf("Run() = %v; want nil", err)
	}
	if p := catchPanic(func() { leaked.Go(func(ctx context.Context) error { return nil }) }); p == nil {
		t.Error("Fork after Run returned did not panic")
	}
}

func catchPanic(fn func()) (p any) {
	defer func() { p = recover() }()
	fn()
	return nil
}
