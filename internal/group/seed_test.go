//go:build unit

package group

import (
	"context"
	"strings"
	"testing"
)

// seeder stands in for D-G7's synchronous read through the Client: it hands the
// coordinator one publication and counts how often it was consulted, so a test
// can prove the seed is taken exactly once, retried while the scope is
// untracked, and never taken for a scope a publication already covered.
//
// No mutex: every test here registers from its own goroutine only, and the
// coordinator consults the seed under its state mutex.
type seeder struct {
	pub   Publication
	ok    bool
	calls int
}

func (s *seeder) read() (Publication, bool) {
	s.calls++

	return s.pub, s.ok
}

func newSeedingCoordinator(t *testing.T, seed *seeder) *Coordinator[coordDoc] {
	t.Helper()

	return NewCoordinator[coordDoc](nil, Decode[coordDoc], seed.read)
}

func TestCoordinatorSeedsWhenNothingWasObserved(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	got := rec.all()
	if len(got) != 1 {
		t.Fatalf("deliveries = %v, want exactly the seeded one", rec.names())
	}

	if got[0].current.Value.Name != "seeded" || got[0].current.Revision != 7 {
		t.Fatalf("delivery = %+v, want the seeded document at revision 7", got[0].current)
	}

	if got[0].previous != nil {
		t.Fatalf("previous = %+v, want nil on the first delivery", got[0].previous)
	}

	if seed.calls != 1 {
		t.Fatalf("seed consulted %d times, want exactly 1", seed.calls)
	}

	st := statusOf(t, c, "t1")
	if st.Desired != 7 || st.Applied != 7 || st.LastErr != nil {
		t.Fatalf("Status() = %+v, want desired and applied 7 with no error", st)
	}
}

func TestCoordinatorDoesNotSeedWhenTheClientDoesNotTrackTheScope(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded")} // ok false: the scope is not tracked yet
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	if names := rec.names(); len(names) != 0 {
		t.Fatalf("deliveries = %v, want none while the scope is untracked", names)
	}

	if st := c.Status(); len(st) != 0 {
		t.Fatalf("Status() = %+v, want no scope recorded", st)
	}

	// The seed stays available: a later registration tries again.
	seed.ok = true

	var second recorder

	unsubscribeSecond := c.Register(second.apply)
	defer unsubscribeSecond()

	if seed.calls != 2 {
		t.Fatalf("seed consulted %d times, want 2: once refused, once taken", seed.calls)
	}

	if names := second.names(); len(names) != 1 || names[0] != "seeded" {
		t.Fatalf("deliveries = %v, want the seeded document once", names)
	}
}

func TestCoordinatorDropsTheDuplicatePublicationAfterASeed(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	c.Publish(context.Background(), publication("t1", 7, "seeded"))

	if names := rec.names(); len(names) != 1 {
		t.Fatalf("deliveries = %v, want the seed and the publication it anticipates to count once", names)
	}

	st := statusOf(t, c, "t1")
	if st.Desired != 7 || st.Applied != 7 || st.LastErr != nil {
		t.Fatalf("Status() = %+v, want desired and applied 7 with no error", st)
	}
}

func TestCoordinatorKeepsAHigherRevisionAfterASeed(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	c.Publish(context.Background(), publication("t1", 8, "newer"))

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("deliveries = %v, want the seed and the newer revision", rec.names())
	}

	if got[1].current.Revision != 8 || got[1].current.Value.Name != "newer" {
		t.Fatalf("second delivery = %+v, want the newer document at revision 8", got[1].current)
	}

	if got[1].previous == nil || got[1].previous.Value.Name != "seeded" {
		t.Fatalf("previous = %+v, want the seeded document", got[1].previous)
	}
}

func TestCoordinatorKeepsADifferentValueAtTheSameRevisionAfterASeed(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	c.Publish(context.Background(), publication("t1", 7, "changed"))

	if names := rec.names(); len(names) != 2 || names[1] != "changed" {
		t.Fatalf("deliveries = %v, want revision 7 delivered again for a different document", names)
	}
}

func TestCoordinatorDeliversARevisionZeroDeleteAfterADroppedPublication(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	ctx := context.Background()

	c.Publish(ctx, publication("t1", 7, "seeded")) // dropped: the seed already covered it
	c.Publish(ctx, publication("t1", 0, "gone"))   // a delete, after the watermark is spent

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("deliveries = %v, want the seed and the delete", rec.names())
	}

	if got[1].current.Revision != 0 || got[1].current.Value.Name != "gone" {
		t.Fatalf("second delivery = %+v, want the delete at revision 0", got[1].current)
	}

	st := statusOf(t, c, "t1")
	if st.Desired != 0 || st.Applied != 0 || st.LastErr != nil {
		t.Fatalf("Status() = %+v, want the delete converged at revision 0", st)
	}
}

func TestCoordinatorNeverSeedsAScopeItAlreadyObserved(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	c.Publish(context.Background(), publication("t1", 3, "published"))

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	if seed.calls != 0 {
		t.Fatalf("seed consulted %d times, want 0 once a publication was observed", seed.calls)
	}

	if names := rec.names(); len(names) != 1 || names[0] != "published" {
		t.Fatalf("deliveries = %v, want the published document replayed once", names)
	}
}

func TestCoordinatorSeedThatFailsToDecodeIsRecorded(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "bad"), ok: true}
	logger := newRecordingLogger()
	c := NewCoordinator[coordDoc](logger, rejectingDecode("bad"), seed.read)

	var rec recorder

	unsubscribe := c.Register(rec.apply)
	defer unsubscribe()

	if names := rec.names(); len(names) != 0 {
		t.Fatalf("deliveries = %v, want a document that fails to decode never delivered", names)
	}

	st := statusOf(t, c, "t1")
	if st.Desired != 7 || st.Applied != 0 || st.LastErr == nil {
		t.Fatalf("Status() = %+v, want revision 7 desired, nothing applied and the decode error", st)
	}

	lines := logger.recorded()
	if len(lines) != 1 || !strings.Contains(lines[0], "failed to decode") {
		t.Fatalf("logged = %v, want the decode failure at error level", lines)
	}

	// The scope counts as observed for seeding purposes: the seed is not retried.
	var second recorder

	unsubscribeSecond := c.Register(second.apply)
	defer unsubscribeSecond()

	if seed.calls != 1 {
		t.Fatalf("seed consulted %d times, want 1: a failed decode is not retried", seed.calls)
	}
}
