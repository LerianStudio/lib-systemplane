//go:build unit

package group

import (
	"context"
	"errors"
	"slices"
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
	err   error
	calls int
}

func (s *seeder) read() (Publication, bool, error) {
	s.calls++

	return s.pub, s.ok, s.err
}

func newSeedingCoordinator(t *testing.T, seed *seeder) *Coordinator[coordDoc] {
	t.Helper()

	return NewCoordinator[coordDoc](nil, Decode[coordDoc], seed.read)
}

func TestCoordinatorSeedsWhenNothingWasObserved(t *testing.T) {
	seed := &seeder{pub: publication("t1", 7, "seeded"), ok: true}
	c := newSeedingCoordinator(t, seed)

	var rec recorder

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribeSecond := mustRegister(t, c, second.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
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

	unsubscribe := mustRegister(t, c, rec.apply)
	defer unsubscribe()

	if names := rec.names(); len(names) != 0 {
		t.Fatalf("deliveries = %v, want a document that fails to decode never delivered", names)
	}

	st := statusOf(t, c, "t1")
	if st.Desired != 7 || st.Applied != 0 || st.LastErr == nil {
		t.Fatalf("Status() = %+v, want revision 7 desired, nothing applied and the decode error", st)
	}

	lines := logger.recorded()
	if len(lines) != 1 || !strings.Contains(lines[0].msg, "failed to decode") {
		t.Fatalf("logged = %v, want the decode failure at error level", lines)
	}

	// A6 parity with the published-document branch: the rejection IS an
	// observation, so the observed flag and not just the spent-seed flag says
	// the coordinator has been heard from, and anyObservedLocked stays the one
	// truth a later guard can read.
	if !scopeObserved(t, c, "t1") {
		t.Error("the seeded scope is not marked observed, want it observed: a rejection at decode is still an observation")
	}

	// The scope counts as observed for seeding purposes: the seed is not retried.
	var second recorder

	unsubscribeSecond := mustRegister(t, c, second.apply)
	defer unsubscribeSecond()

	if seed.calls != 1 {
		t.Fatalf("seed consulted %d times, want 1: a failed decode is not retried", seed.calls)
	}

	if names := second.names(); len(names) != 0 {
		t.Errorf("deliveries to the second registration = %v, want none: nothing decodable was ever seeded", names)
	}

	if keys := applierScopeKeys(t, c); !slices.Equal(keys, []string{"t1"}) {
		t.Errorf(`applier scope keys = %q, want ["t1"]: bookkeeping is keyed by the scope, never by a payload`, keys)
	}

	// A document that DOES decode now delivers to both registrations, which is
	// the write-back half of the same rule: an acceptance must land under the
	// scope's own tenant and leave no second key behind.
	c.Publish(context.Background(), publication("t1", 8, "good"))

	if names := second.names(); len(names) != 1 || names[0] != "good" {
		t.Errorf("deliveries to the second registration = %v, want the decodable document delivered once", names)
	}

	if keys := applierScopeKeys(t, c); !slices.Equal(keys, []string{"t1"}) {
		t.Errorf(`applier scope keys after a delivery = %q, want ["t1"]`, keys)
	}
}

// TestCoordinatorSeedReadErrorReachesTheRegistrant separates "nothing to seed"
// from "could not look". A read that FAILS leaves the registration with no
// initial delivery and — on a key nobody writes again, which is the ordinary
// case for a runtime knob — no delivery at all, so the error goes back to the
// registrant instead of into silence. Nothing is observed, no seed is spent,
// and the next registration reads again.
func TestCoordinatorSeedReadErrorReachesTheRegistrant(t *testing.T) {
	failed := errors.New("the client is closed")
	seed := &seeder{err: failed}
	c := newSeedingCoordinator(t, seed)

	var first recorder

	unsubscribe, err := c.Register(first.apply)
	if !errors.Is(err, failed) {
		t.Fatalf("Register = %v, want the seed read's own error", err)
	}

	if unsubscribe == nil {
		t.Fatal("unsubscribe is nil on the error return, so a caller cannot defer it before checking err")
	}

	defer unsubscribe()

	if got := first.all(); len(got) != 0 {
		t.Fatalf("deliveries = %v, want none: the read failed", first.names())
	}

	if status := c.Status(); len(status) != 0 {
		t.Fatalf("Status = %+v, want empty: a failed read observes no scope", status)
	}

	seed.err = nil
	seed.pub = publication("t1", 4, "seeded")
	seed.ok = true

	var second recorder

	unsubscribeSecond := mustRegister(t, c, second.apply)
	defer unsubscribeSecond()

	if seed.calls != 2 {
		t.Fatalf("seed consulted %d times, want 2: a failed read spends nothing", seed.calls)
	}

	got := second.all()
	if len(got) != 1 || got[0].current.Revision != 4 {
		t.Fatalf("deliveries to the second registration = %v, want the seeded revision 4", second.names())
	}
}

// TestCoordinatorDeliversAfterASeedItCannotProveIdentical pins the watermark's
// "not proven identical means deliver" rule on the only two ways the proof can
// be impossible: the seeded document refuses to marshal, so there are no bytes
// to compare against, or the publication that follows refuses to, so there is
// nothing to compare. Either way the seed and the publication are two
// observations, not one. Spending the watermark on an unproven match would drop
// a document silently, at a revision no later write has to beat, and the
// consumer would run the seeded configuration believing the published one is in
// force.
func TestCoordinatorDeliversAfterASeedItCannotProveIdentical(t *testing.T) {
	cases := []struct {
		name      string
		seeded    any
		published any
	}{
		{name: "the seed cannot be marshalled", seeded: refuseMarshal{}, published: refuseMarshal{}},
		{name: "the publication cannot be marshalled", seeded: document(unmarshallableName), published: refuseMarshal{}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			seed := &seeder{pub: Publication{Tenant: "t1", Revision: 1, Value: tc.seeded}, ok: true}
			c := NewCoordinator[coordDoc](nil, decodeRefusing, seed.read)

			var rec recorder

			unsubscribe := mustRegister(t, c, rec.apply)
			defer unsubscribe()

			c.Publish(context.Background(), Publication{Tenant: "t1", Revision: 1, Value: tc.published})

			if names := rec.names(); len(names) != 2 {
				t.Fatalf("deliveries = %v, want two: the seed, and the publication the watermark could not prove identical to it", names)
			}

			if st := statusOf(t, c, "t1"); st.Desired != 1 || st.Applied != 1 || st.LastErr != nil {
				t.Errorf("Status() = %+v, want desired and applied 1 with no error", st)
			}
		})
	}
}
