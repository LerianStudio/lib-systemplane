//go:build unit

package group

import (
	"context"
	"slices"
	"sync"
	"testing"
)

func statusTenants(c *Coordinator[coordDoc]) []string {
	var tenants []string
	for _, st := range c.Status() {
		tenants = append(tenants, st.Tenant)
	}

	return tenants
}

func TestCoordinatorDropScopeForgetsTheTenant(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
	defer unsubscribe()

	c.Publish(ctx, publication("t1", 1, "one"))
	c.Publish(ctx, publication("t2", 2, "two"))
	c.Publish(ctx, publication("t1", 3, "three"))

	c.DropScope("t1")
	c.DropScope("t1")
	c.DropScope("never-seen")

	if got := statusTenants(c); !slices.Equal(got, []string{"t2"}) {
		t.Fatalf("Status tenants after dropping t1 = %v, want [t2]", got)
	}

	if got := applierScopeKeys(t, c); !slices.Equal(got, []string{"t2"}) {
		t.Fatalf("applier bookkeeping after dropping t1 = %v, want [t2]", got)
	}

	c.Publish(ctx, publication("t1", 4, "four"))

	got := rec.all()
	if last := got[len(got)-1]; last.current.Revision != 4 || last.previous != nil {
		t.Fatalf("first delivery after the drop = rev %d previous %v, want rev 4 with previous nil", last.current.Revision, last.previous)
	}

	if got := statusTenants(c); !slices.Equal(got, []string{"t1", "t2"}) {
		t.Fatalf("Status tenants after t1 published again = %v, want [t1 t2]", got)
	}
}

func TestCoordinatorDropScopeDuringADeliveryRecordsNothing(t *testing.T) {
	c := newCoordinator(t)
	ctx := context.Background()

	var (
		rec     recorder
		once    sync.Once
		entered = make(chan struct{})
		release = make(chan struct{})
	)

	unsubscribe := mustRegister(t, c, func(fnCtx context.Context, current Decoded[coordDoc], previous *Decoded[coordDoc]) error {
		once.Do(func() {
			close(entered)
			<-release
		})

		return rec.apply(fnCtx, current, previous)
	})
	defer unsubscribe()

	done := make(chan struct{})

	go func() {
		defer close(done)

		c.Publish(ctx, publication("t1", 1, "one"))
	}()

	waitFor(t, entered, "the applier to block inside the t1 delivery")
	c.DropScope("t1")
	close(release)
	waitFor(t, done, "the in-flight delivery to finish")

	if got := rec.names(); !slices.Equal(got, []string{"one"}) {
		t.Fatalf("deliveries = %v, want the in-flight one to finish", got)
	}

	if got := applierScopeKeys(t, c); len(got) != 0 {
		t.Fatalf("applier bookkeeping after the drop = %v, want none: the finished delivery must record nothing", got)
	}

	c.Publish(ctx, publication("t1", 2, "two"))

	got := rec.all()
	if last := got[len(got)-1]; last.current.Revision != 2 || last.previous != nil {
		t.Fatalf("first delivery after the drop = rev %d previous %v, want rev 2 with previous nil", last.current.Revision, last.previous)
	}
}
