// SPDX-License-Identifier: Apache-2.0 OR MIT

package scope_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"

	"github.com/xorps/scopher/scope"
)

func ExampleRun() {
	err := scope.Run(context.Background(), func(ctx context.Context, s *scope.Scope) error {
		user := s.Fork(func(ctx context.Context) (string, error) {
			return "gopher", nil
		})
		orders := s.Fork(func(ctx context.Context) (int, error) {
			return 3, nil
		})

		u, err := user.Await(ctx)
		if err != nil {
			return err
		}
		o, err := orders.Await(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("%s has %d orders\n", u, o)
		return nil
	})
	if err != nil {
		fmt.Println("error:", err)
	}
	// Output: gopher has 3 orders
}

func ExampleRunValue() {
	sum, err := scope.RunValue(context.Background(), func(ctx context.Context, s *scope.Scope) (int, error) {
		a := s.Fork(func(ctx context.Context) (int, error) { return 40, nil })
		b := s.Fork(func(ctx context.Context) (int, error) { return 2, nil })

		av, err := a.Await(ctx)
		if err != nil {
			return 0, err
		}
		bv, err := b.Await(ctx)
		if err != nil {
			return 0, err
		}
		return av + bv, nil
	})
	fmt.Println(sum, err)
	// Output: 42 <nil>
}

func ExampleScope_Go() {
	var sent atomic.Int32
	err := scope.Run(context.Background(), func(ctx context.Context, s *scope.Scope) error {
		for range 3 {
			s.Go(func(ctx context.Context) error {
				sent.Add(1)
				return nil
			})
		}
		return nil
	})
	fmt.Println(sent.Load(), err)
	// Output: 3 <nil>
}

func ExampleRun_cancellation() {
	errNotFound := errors.New("not found")
	err := scope.Run(context.Background(), func(ctx context.Context, s *scope.Scope) error {
		s.Go(func(ctx context.Context) error {
			<-ctx.Done()
			fmt.Println("cancelled because:", context.Cause(ctx))
			return ctx.Err()
		})
		s.Go(func(ctx context.Context) error {
			return errNotFound
		})
		return nil
	})
	fmt.Println("Run returned:", err)
	// Output:
	// cancelled because: not found
	// Run returned: not found
}
