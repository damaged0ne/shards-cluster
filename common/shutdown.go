package common

import (
	"context"

	"k8s.io/klog"
)

// RunWithContext runs f and waits for it to return or for ctx to be done, whichever comes first.
// It is used to bound shutdown steps that can't be canceled themselves; if ctx expires, f keeps
// running in the background (the process is about to exit anyway). It reports whether f completed.
func RunWithContext(ctx context.Context, name string, f func()) bool {
	done := make(chan struct{})
	go func() {
		defer close(done)
		f()
	}()
	select {
	case <-done:
		return true
	case <-ctx.Done():
		klog.Warningf("%s: did not complete before the shutdown deadline: %s", name, ctx.Err())
		return false
	}
}
