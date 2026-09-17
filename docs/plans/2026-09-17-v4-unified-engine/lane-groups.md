# lib-systemplane v4 — Typed Groups — Implementation Plan

> **For implementers:** Use ring-default:executing-plans (rolling-phase: elaborate the
> current phase against the real code, execute its tasks in review-checkpointed
> batches, then elaborate the next phase — repeat),
> ring-default:dispatching-workflows to run each phase as a reviewed multi-agent
> workflow (review + contrarian baked in), or ring-dev-team:running-dev-cycle for the
> full subagent-orchestrated workflow.
> This document is the living source of truth — task elaboration for later
> phases is written back into it during execution.

**Goal:** A consumer declares one typed configuration document, gets it back as a Go value with its revision and freshness, writes it atomically, and receives a serialized hot-reload hook — without writing decode, cache or event-identity code.

**Architecture:** A group is exactly one registered key whose stored value is the JSON document of `T` (D5), so the atomicity of a group is the atomicity of one row. Everything in this lane sits on the public facade of the root `systemplane` package — `Register`, `GetEntry`, `Set`, `OnChange`, `IsRegistered` — and never reaches into the engine or the store. The generic surface required by FC-7 lives at the root in `api_group.go`; the non-generic machinery that FC-7's semantics actually live in (per-scope publication cache, revision dedupe, trailing-edge coalescing, per-applier `Previous` and status) lives in `internal/group`, where it is unit-testable with synthetic revisions that the wave-1 facade shim cannot yet produce.

**Tech Stack:** Go 1.26 generics, `encoding/json`, `github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core` (tenant id from context — already a direct dependency, no `go.mod` change), `go.uber.org/goleak` for the new package's `TestMain`.

**Lane:** groups
**Depends on:** contracts
**Worktree:** `/srv/worktrees/v4-groups` on branch `feat/v4-groups`

## Phase Overview

| Phase | Milestone | Epics | Status |
|-------|-----------|-------|--------|
| 1 | A consumer binds a typed document, reads it back as `T` with revision/tenant/staleness, and writes it; an invalid document is rejected at every ingress the facade owns | 1.1, 1.2 | Detailed |
| 2 | The same group delivers hot reload: `OnApply` fires serialized, coalesced, never twice for the same revision, and `Status` reports desired vs applied per tenant | 2.1, 2.2, 2.3 | Epic-level |

---

## Design decisions (frozen for this lane)

These are decided here, not during implementation. A task that contradicts one is wrong.

**D-G1 — Encoding is `encoding/json`, and the value in force is always the canonical JSON form.**
`Bind` does NOT register the caller's `T` value as the default. It marshals `defaults` and unmarshals the bytes back into an `any`, and registers THAT canonical form. Three reasons, each load-bearing:
1. Uniformity. A stored row, a `Set` write and the registered default then all have the same shape (`map[string]any`, `[]any`, scalars), so one decode path serves every read.
2. Clone safety. The facade's `Register` rejects a default that is not safely cloneable, and a consumer struct carrying an unexported mutable field (a `sync.Mutex`, a lazily-built map) fails that check. Canonicalizing removes the whole failure class: a JSON document is always cloneable.
3. Operator surface. Admin `GET`, `List` and `Get` render a group identically whether the default or a stored row is in force.
Consequence to state in the godoc: `validate` sees the round-tripped document, so a field the consumer excluded with `json:"-"` is absent when its own validator runs. That is the honest semantics — it validates what will actually be in force.

**D-G2 — Decoding `any` into `T` is: typed assertion first, JSON round-trip as the fallback.**
Try `v.(T)` with the comma-ok form (never a bare assertion — `forcetypeassert` is on). On failure, `json.Marshal(v)` then `json.Unmarshal` into a `T`. The assertion path exists because a caller can hand a `T` straight to `Set`; the round-trip path handles everything that came out of the store or the cache. Unknown fields are ACCEPTED (no `DisallowUnknownFields`): during a rolling deploy an old binary must keep reading a document a new binary widened, and rejecting it would take the old pods' config back to defaults. Required-field enforcement is the consumer's `validate`, not the decoder's.

**D-G3 — One validator covers every ingress FC-7 names.**
`Bind` builds a single `func(any) error` that decodes into `T` and then calls `validate` (when non-nil), and passes it to `Register` via `WithValidator`. That one function is what the facade calls when `Register` checks the default, what `Set` calls before persisting, and what the engine calls on hydration, refresh and reconcile once engine-core lands. There is no second validation mechanism in this lane.
Ordering matters: `Bind` appends its own `WithValidator` AFTER the caller's `opts`, so a consumer passing `WithValidator` cannot silently disable type checking on its own group.

**D-G4 — An invalid stored row never reaches a group, and the group never re-validates on read.**
The engine rejects an undecodable or validator-failing row at its `decode → validate → publish` ingress: nothing is published, the value already in force stays in force, and the rejection is logged and counted by the engine. A group sits downstream of that ingress, so `Snapshot` cannot receive the invalid row and cannot surface its validation error. What `Snapshot` returns for a key whose stored row is invalid is the last value that DID pass — or the registered default, when nothing valid was ever published for that key. The rejection is observable only through the engine's log and telemetry, never through the group's API.
Two consequences the implementation must honor:
1. `Snapshot` does NOT run the consumer's `validate`. Whatever is in force already passed it at ingress — the registered validator IS this group's decode-plus-validate closure (D-G3) — so a second call would be a consumer callback per read that can never fail. There is no `validate` field on `Group[T]`.
2. `Snapshot` still returns an error rather than a half-filled `T` if a decode fails. Through an engine-backed Client that path is unreachable, because a document that cannot decode into `T` cannot pass the registered validator either; it is defense-in-depth against a facade bug, not the specified handling of an invalid stored row, and no test asserts it as the latter.
On this lane's base the wave-1 facade still hydrates without validating, so a seeded invalid row can reach a reader. The lane does not compensate for that (the same stance D-G5 takes on revisions): the end-to-end "an invalid row keeps the last valid value" assertion belongs to engine-core's ingress tests and to the integration lane.

**D-G5 — `Snapshot` field derivation.**
`Value` ← decoded `Entry.Value`. `Revision` ← `Entry.Revision`. `Stale` ← `Entry.Stale`, unchanged. `Tenant` ← `tmcore.GetTenantIDContext(ctx)` from `lib-commons/v7/commons/tenant-manager/core`, which is the same helper the client already uses to identify a request's tenant; it returns `""` in single-tenant mode and for a context no middleware has touched, which is exactly the FC-7 contract (`"" in single-tenant mode`).
On the contracts base the single-tenant read path reports `Revision 0` for every cached row and `Stale false` always — FC-5 already documents that as the wave-1 shim's limitation. This lane maps the fields faithfully; it does not compensate, and it does not assert non-zero revisions through the single-tenant facade (see D-G9).

**D-G6 — A delivered `Applied` always carries `Stale: false`.**
Staleness describes a read, not a publication: the engine publishes a value it has just observed, while `Stale` reports that a scope's changefeed is down or has not yet reconciled. A subscriber that needs to know whether its scope is currently converged calls `Snapshot`. The godoc on `Applied` says this in one sentence.

**D-G7 — The group owns one subscription, taken at `Bind`, plus a per-scope publication cache that a synchronous read seeds when a delivery has not landed yet. That pair is what makes `OnApply` correct before AND after `Start`.**
`Bind` registers a single `OnChange` for the group's key (the facade permits `OnChange` before `Start`). Every publication updates `latest[scope]` inside the group and fans out to the registered appliers. `OnApply` appends its function and synchronously replays `latest` for every scope already observed — FC-7's "delivers the current snapshot of every scope the Client already tracks", read literally.
The replay alone is not enough, and this is the gap that forces the seed: `Client.Start` returns once the scope's first reconcile has completed (FC-11), but the engine hands each publication to a per-(scope, key) dispatch worker that invokes `OnChange` on its own goroutine. For a moment after `Start` returns, `latest` is therefore still empty, and an `OnApply` landing in that window would replay nothing — while FC-7 promises the current snapshot before the call returns.
The fallback: when `OnApply` runs and the group has observed no publication for a scope the Client already tracks, the group reads that scope's current entry synchronously with `Client.GetEntry` (FC-5), decodes it with the group codec, and seeds `latest` from it under the group's state mutex before replaying. The replay then always has a value.
"A scope the Client already tracks" is FC-7's own phrase, and `Entry.Stale` is how the group asks it: `Stale == false` means the scope has completed a reconcile, so the entry is a published value worth seeding; `Stale == true` means it has not, which is exactly the pre-`Start` case, and no seed is taken.
- Called **before** `Start`: no scope is tracked, the read reports `Stale`, so nothing is seeded and nothing is delivered now. The initial delivery is the publication the engine makes when it reconciles the scope at `Start` (**R1** in `## Requests to index.md`, frozen as FC-11) — which is FC-7 verbatim: "Before `Start`, `OnApply` registers and the initial delivery happens during `Start`."
- Called **after** `Start`: either the scope's publication is already cached and the replay delivers it, or that publication is still in flight and the seed supplies the same value out of the engine's own state. Either way the applier runs before `OnApply` returns.
A seed and the in-flight publication it anticipates are ONE observation, so the group must not deliver both. A seed records the revision it read as that scope's seeded watermark, and the FIRST publication arriving for that scope afterwards is dropped when its revision is less than or equal to the watermark (revision monotonicity, D3). Only that first one: after it, FC-4's normal rules resume, including "Revision 0 is never deduplicated", so a later delete still delivers. A publication newer than the seed is delivered as usual, which is why no revision can fall between the seed and the subscription — the subscription was taken at `Bind`, before any publication existed.
The seed reads with `context.Background()`, so a seeded snapshot carries `Tenant: ""` — correct in single-tenant mode, and multi-tenant `OnApply` is refused on this lane's base anyway. `Snapshot` needs no seeding: it already reads through `GetEntry` on every call, which is the same read.
Multi-tenant on the contracts base: `OnChange` returns `ErrNotSupportedInMultiTenant`. `Bind` records that error instead of failing (so `Snapshot` and `Set` still work for a multi-tenant consumer) and `OnApply` returns it.
Wave-1 caveat: the shim reports `Stale false` always (D-G5), so the pre-`Start` gate cannot be exercised through the facade on this lane's base. The coordinator takes the seed as an injected function and its tests drive both outcomes directly (D-G9's pattern); end to end it is deferred to the integration lane.

**D-G8 — Serialization and coalescing use two locks and no goroutines.**
Per scope: a delivery mutex held while applier functions run, plus a state mutex guarding `latest`, a monotonically increasing publication sequence number, and the per-applier bookkeeping. A publication records itself under the state mutex, then takes the delivery mutex and drains: if the sequence number has not advanced since the last fan-out it exits, which is precisely the trailing-edge coalescing FC-7 asks for. The sequence number, not the revision, drives the drain, because Revision 0 repeats legitimately and would otherwise look like "nothing new".
The fan-out runs on the publishing goroutine. FC-4 already licenses that: a subscriber of one key may occupy that key's dispatch worker, and different keys deliver independently. Applier functions run WITHOUT the state mutex held, so calling `Status()` or `Snapshot()` from inside an applier works.
Documented constraint: an applier function must not call `OnApply` or `Set` for its own group synchronously — deliveries are serialized per scope and re-entering blocks. `internal/group` carries a `goleak.VerifyTestMain`, which is what proves the no-goroutines claim rather than asserting it in prose.

**D-G9 — Revision semantics are tested in `internal/group`, not through the single-tenant facade.**
The wave-1 facade zeroes `Revision` on both the single-tenant read path and the single-tenant `Change`, so dedupe-by-revision — the core semantic of FC-7 — is untestable end to end from the root package on this lane's base. The coordinator in `internal/group` therefore takes publications as plain values with explicit revisions, and its tests drive revision 0, repeated revisions and advancing revisions directly. Root tests cover the wiring, the ordering and the end-to-end shape; they assert delivery counts and values, never a non-zero revision arriving through the single-tenant facade.

**D-G10 — Nil-receiver behavior, matching the rest of the package.**
`Bind` on a nil `*Client` returns `(nil, ErrClosed)`. On a nil `*Group[T]`: `Snapshot` returns `(zero, ErrClosed)`, `Set` returns `ErrClosed`, `OnApply` returns `(no-op, ErrClosed)`, `Status` returns `nil`. `Status` returns its slice sorted by `Tenant`, so a test can compare it without ordering flake.

**D-G11 — Parameter naming trap.** The root package is scanned by an AST test that flags exported parameters whose NAME suggests a logger or telemetry sink: `logger`, `log`, `l`, `recorder`, `factory`, `metrics`, `metricsfactory`, `telemetry`, `t`. No exported function or method added by this lane may name a parameter any of those. `T` as a type parameter is fine; a value parameter called `t` is not.

---

## Phase 1: Typed read and write

At the end of Phase 1 a consumer can declare a typed configuration document, read it back as a Go value with its revision, tenant and staleness, and write it — with an invalid document rejected at registration and at write. No hot reload yet.

### Epic 1.1: The typed document — codec and `Bind`

**Goal:** `Bind` registers a group's key with the canonical JSON form of its defaults and a validator that turns "is this a valid `T`?" into the facade's `func(any) error` ingress hook. A `*Group[T]` handle exists and carries what the read and write paths need.
**Scope:** `internal/group/` (new), root `api_group.go` (new).
**Dependencies:** none.
**Done when:** `Bind` before `Start` registers the key; the registered default round-trips through JSON; invalid defaults are rejected through the caller's `validate`; a caller-supplied `WithValidator` cannot displace the type check; `Bind` on a nil Client returns `ErrClosed`; `make test-unit` green.
**Status:** Pending

#### Task 1.1.1: Build the typed codec in `internal/group`

- [ ] Done

**Context:** Every path in this lane converts between a consumer's `T` and the untyped JSON document the facade stores. There is no such helper anywhere in the repository today — the facade handles `any` end to end and leaves decoding to the caller. This task creates the single conversion point that Phase 1 and Phase 2 both build on, in a package that did not exist before, so nothing else in the repo changes.

**Implementation vision:** Create the package `group` under `internal/group` with two exported generic functions and no dependency on the root package (the root imports this package, so the reverse would be an import cycle).

```go
package group

// Canonical returns the JSON document form of v: the value produced by
// marshaling v and unmarshaling the bytes back into an any. Registered
// defaults, written values and stored rows all take this shape, so one decode
// path serves every read.
func Canonical[T any](v T) (any, error)

// Decode converts a stored or published value into T. It accepts a value that
// is already a T and falls back to a JSON round-trip for the untyped document
// shapes the store and cache produce. Unknown fields are accepted so a rolling
// deploy can widen a document without taking older pods back to defaults.
func Decode[T any](v any) (T, error)
```

`Decode` uses the comma-ok assertion `if t, ok := v.(T); ok` first — a bare assertion fails the `forcetypeassert` linter and, worse, panics on the map shape that is the common case. The fallback marshals `v` and unmarshals into a `T`. A `nil` input decodes to the zero `T` with no error, because a JSON `null` is a legitimate document for a pointer-shaped `T`; a value that cannot be marshaled, or whose JSON cannot be unmarshaled into `T`, returns an error wrapping the `encoding/json` error so `errors.As` still reaches `*json.UnmarshalTypeError`.

Do not use `DisallowUnknownFields` (D-G2). Do not add a decode cache — a group read is not a hot loop, and a cache keyed by a shape would be the kind of speculative machinery this repo's rules forbid.

Add `internal/group/main_test.go` with `goleak.VerifyTestMain`, mirroring the existing `internal/client` test main: the coordinator landing in Phase 2 claims to spawn no goroutines, and this is what will hold it to that.

**Files:**
- Create: `internal/group/codec.go`
- Create: `internal/group/main_test.go`
- Test: `internal/group/codec_test.go`

**Verification:** `go test -tags=unit -race -run 'TestCanonical|TestDecode' ./internal/group/...` — all cases pass.

**Done when:** RED-first tests named `TestCanonicalRoundTripsStructToDocument`, `TestDecodeAcceptsTypedValue`, `TestDecodeAcceptsUntypedDocument`, `TestDecodeAcceptsUnknownFields`, `TestDecodeRejectsWrongShape` and `TestDecodeNilYieldsZeroValue` cover: a struct with nested struct/slice/map fields becoming `map[string]any`; a `T` passed straight through; a `map[string]any` decoding into the struct with an integer field arriving as `float64` and landing as an `int`; a document with an extra field decoding cleanly; a JSON string where the struct is expected returning an error that `errors.As` resolves to `*json.UnmarshalTypeError`; `nil` yielding the zero `T`.

**Estimated size:** ~15 turns.

---

#### Task 1.1.2: Add `Bind`, `Group[T]` and `Snapshot[T]` to the root package

- [ ] Done

**Context:** FC-7 fixes the public shape of the typed group API verbatim. The facade methods it must sit on — `Register`, `IsRegistered` and the key options `WithDescription`, `WithValidator`, `WithRedaction`, `WithCatalogMetadata` — already exist on `*Client` in the root package. Nothing in the repository declares a generic exported type yet, though the boundary AST test already handles generic receivers, so no test needs adjusting.

**Implementation vision:** Create `api_group.go` in package `systemplane` declaring `Group[T any]` (unexported fields: the `*Client`, the namespace, the key, and — from Phase 2 — the coordinator), `Snapshot[T any]` with exactly the four fields FC-7 names, and:

```go
func Bind[T any](c *Client, namespace, key string, defaults T, validate func(T) error, opts ...KeyOption) (*Group[T], error)
```

`Bind` in order: reject a nil `c` with `ErrClosed`; produce the canonical document with `group.Canonical(defaults)` and wrap a marshal failure as `ErrValidation`; build the ingress validator as a closure that calls `group.Decode[T]` and then `validate` when non-nil; call `c.Register(namespace, key, canonicalDefaults, append(opts, WithValidator(ingress))...)`; return the handle. Appending the validator LAST is the decision from D-G3 and needs a one-line comment saying why, because it looks like an ordering accident otherwise.

Do not re-validate the defaults inside `Bind`. `Register` already runs the registered validator against the registered default, so the caller's `validate` rejecting the defaults surfaces as `Register`'s `ErrValidation`, wrapped once. Adding a second check would produce two different error strings for one condition.

Copy the `append` carefully: `append(opts, ...)` may write into the caller's backing array. Build a fresh slice of `len(opts)+1` instead.

The doc comment on `Bind` is FC-7's verbatim, plus the two consequences this lane decided: the default in force is the round-tripped document (so `validate` sees the round-trip), and `Bind` must be called before `Start`.

Watch the parameter names against D-G11 — `defaults`, `validate`, `opts`, `c`, `namespace`, `key` are all safe; do not rename any of them to a single letter.

**Files:**
- Create: `api_group.go`
- Test: `api_group_test.go`

**Verification:** `go test -tags=unit -race -run 'TestGroupBind' ./...` and `go vet ./...` — the untagged vet run must still compile the root package.

**Done when:** `api_group_test.go` exists in package `systemplane_test` with `//go:build unit`, declares its own fake store named `groupMemoryStore` (every helper in this file prefixed `group` so it cannot collide with the `apiMemoryStore` helpers engine-core owns in `api_client_test.go`), and RED-first tests named `TestGroupBindRegistersCanonicalDefaults`, `TestGroupBindRejectsInvalidDefaults`, `TestGroupBindRejectsAfterStart`, `TestGroupBindRejectsDuplicateKey`, `TestGroupBindOnNilClientReturnsErrClosed` and `TestGroupBindValidatorSurvivesCallerWithValidator` prove: after `Bind`, `IsRegistered` is true and `Get` before `Start` returns the canonical document (a `map[string]any`, not the struct); a `validate` that rejects the defaults makes `Bind` return an error matching `ErrValidation`; `Bind` after `Start` matches `ErrRegisterAfterStart`; a second `Bind` on the same key matches `ErrDuplicateKey`; `Bind(nil, ...)` matches `ErrClosed`; and a caller passing `WithValidator(func(any) error { return nil })` still gets a wrong-shaped `Set` rejected.

**Estimated size:** ~30 turns.

---

### Epic 1.2: The typed read and write paths

**Goal:** `Snapshot` returns the document as `T` with its revision, tenant and staleness; `Set` persists a `T` after validation. Both resolve the caller's scope exactly as the underlying facade does.
**Scope:** root `api_group.go`, `api_group_test.go`.
**Dependencies:** Epic 1.1.
**Done when:** `Snapshot` decodes and maps all four fields WITHOUT re-running the consumer's `validate` (D-G4); a document that cannot decode into `T` surfaces as an error instead of a half-filled value; `Set` rejects an invalid value before it reaches the store; both are nil-receiver safe; `make test-unit` green.
**Status:** Pending

#### Task 1.2.1: Implement `Group[T].Snapshot`

- [ ] Done

**Context:** `GetEntry` on `*Client` is the scope-resolving read the contracts lane landed; it returns a struct carrying the decoded value, the revision, provenance and a staleness flag, and reports `ok == false` only for an unregistered key. A group's key is registered by construction, so `!ok` means the Client was torn down underneath the group.

**Implementation vision:** `func (g *Group[T]) Snapshot(ctx context.Context) (Snapshot[T], error)`.

Nil receiver returns `(Snapshot[T]{}, ErrClosed)`. Call `g.client.GetEntry(ctx, g.namespace, g.key)` and return its error untouched — `ErrClosed`, `ErrNilContext` and any store error must reach the caller as themselves, not re-wrapped, so `errors.Is` keeps working. `!ok` returns an error wrapping `ErrUnknownKey` naming the namespace and key.

Decode with `group.Decode[T]` and stop there: do NOT run the consumer's `validate` (D-G4). The engine's ingress rejects anything that would fail it before publication, so whatever is in force always decodes, and the group's job on read is to hand back a `T` — not to re-litigate a document the engine already accepted. A decode failure returns an error wrapping `ErrValidation` that names the namespace and key, with a zero `Value`; that is the defensive path, not the specified handling of an invalid stored row. On success build the `Snapshot[T]` per D-G5: `Value` from the decode, `Revision` and `Stale` copied from the entry, `Tenant` from `tmcore.GetTenantIDContext(ctx)`.

Do NOT store the caller's `validate` on the `Group[T]`. It lives inside the ingress closure `Bind` hands to `Register`, which is where every ingress FC-7 names calls it (D-G3); no read path in this lane needs it, and a field nothing reads is an invitation to grow a second validation mechanism.

The import of `github.com/LerianStudio/lib-commons/v7/commons/tenant-manager/core` adds no module: it is already a direct requirement and already imported elsewhere in the repo. Do not touch `go.mod` or `go.sum`; if `go mod tidy` wants a change, stop and report to the orchestrator.

**Files:**
- Modify: `api_group.go`
- Test: `api_group_test.go`

**Verification:** `go test -tags=unit -race -run 'TestGroupSnapshot' ./...`

**Done when:** RED-first tests named `TestGroupSnapshotReturnsDefaultsBeforeAnyWrite`, `TestGroupSnapshotReturnsStoredDocument`, `TestGroupSnapshotReturnsDecodeErrorNotPartialValue`, `TestGroupSnapshotDoesNotRunConsumerValidate`, `TestGroupSnapshotCarriesTenantFromContext` and `TestGroupSnapshotOnNilGroupReturnsErrClosed` prove: before any write, `Snapshot` returns the defaults as a populated `T`; after the fake store is seeded with a document and the Client started, `Snapshot` returns it decoded with nested fields intact; a row holding a JSON string where the struct belongs returns an error matching `ErrValidation` with a zero `Value` — never a half-filled `T` — which is D-G4's defensive path, not a claim that an invalid row reaches a group; a group whose `validate` counts its calls records ZERO calls across a hundred `Snapshot`s while that same `validate` still rejects a bad `Set`, proving the read path does not re-validate and the ingress still does (D-G4); a context carrying a tenant id yields that id in `Snapshot.Tenant` while a bare `context.Background()` yields `""`; a nil `*Group[T]` returns `ErrClosed`.

**Estimated size:** ~25 turns.

---

#### Task 1.2.2: Implement `Group[T].Set`

- [ ] Done

**Context:** `Client.Set` already validates through the key's registered validator, marshals, persists and — in single-tenant mode — updates the cache with the JSON-canonical value. The group's registered validator (Task 1.1.2) is exactly the type check, so the typed write needs no validation of its own.

**Implementation vision:** `func (g *Group[T]) Set(ctx context.Context, value T, actor string) error` delegates to `g.client.Set(ctx, g.namespace, g.key, value, actor)` and returns the error untouched.

Pass `value` itself, not the canonical form: the facade marshals it, and the registered validator's decode path accepts a typed `T` directly (D-G2). Canonicalizing here would double the marshal work and change nothing observable.

Do not add a "validate first for a better message" branch. The facade already wraps the validator's error with `ErrValidation`, and a second wrap would produce two error strings for one condition.

Nil receiver returns `ErrClosed`. Document the inherited behavior explicitly in the godoc: `Set` before `Start` returns `ErrNotStarted`, and the write is last-write-wins across the whole document, which is what makes a group atomic (D5).

**Files:**
- Modify: `api_group.go`
- Test: `api_group_test.go`

**Verification:** `go test -tags=unit -race -run 'TestGroupSet' ./...`

**Done when:** RED-first tests named `TestGroupSetPersistsAndIsReadableBack`, `TestGroupSetRejectsValueFailingValidate`, `TestGroupSetBeforeStartReturnsErrNotStarted`, `TestGroupSetRecordsActor` and `TestGroupSetOnNilGroupReturnsErrClosed` prove: `Set` then `Snapshot` in the same goroutine returns the written document; a value the consumer's `validate` rejects returns an error matching `ErrValidation` and the fake store receives no write; `Set` before `Start` matches `ErrNotStarted`; the actor string reaches the fake store's recorded entry; a nil `*Group[T]` returns `ErrClosed`.

**Estimated size:** ~15 turns.

---

#### Task 1.2.3: Prove the whole-document semantics end to end

- [ ] Done

**Context:** D5 says the atomicity of a group is the atomicity of one row, and the index's integration scenario 8 asserts that a concurrent reader never observes a mix of old and new fields. That scenario runs against real backends in the integration lane; this task pins the same property at the level this lane owns, where the guarantee actually comes from — one key, one JSON document, one write.

**Implementation vision:** Add a test that binds a group whose `T` has at least three fields of different kinds (a string, an integer, a nested slice), starts the Client on the fake store, then runs `Set` with all three fields changed on one goroutine while another goroutine loops on `Snapshot`, and asserts that every observed snapshot is either wholly the old document or wholly the new one. Run it under `-race`.

The fake store must fire its subscriber callback from inside `Set`, as `apiMemoryStore` does — that is the shape that makes the single-tenant cache update and the change echo both land, and it is also what Phase 2 relies on to simulate a publication during `Start`. Guard the fake's map with a mutex: the existing `apiMemoryStore` is unsynchronized and would trip `-race` the moment two goroutines touch it.

Also add the "a broken document never becomes a partial `T`" case at this level: seed the store with a document whose nested slice holds an object of the wrong type, start, and assert `Snapshot` errors rather than returning a `T` with a half-filled slice. This is D-G4's defensive decode assertion, not a statement about invalid rows in the shipped system: once engine-core's ingress lands, such a row is rejected before publication and the group keeps the last valid value, or the registered default when nothing valid was ever published. It is assertable here only because the wave-1 facade hydrates without validating, which is the sole reason the row can reach a reader at all.

**Files:**
- Modify: `api_group_test.go`

**Verification:** `make test-unit` — the whole unit suite green, including the new package — followed by `go test -tags=unit -race -count=10 -run 'TestGroupDocument' ./...` to shake the concurrency case.

**Done when:** tests named `TestGroupDocumentIsAtomicAcrossFields` and `TestGroupDocumentPartialDecodeIsRejected` pass repeatedly under `-race`, and `make test-unit` is green on the lane branch.

**Estimated size:** ~20 turns.

---

## Phase 2: Hot reload — `OnApply` and `Status`

At the end of Phase 2 a consumer registers an apply function once and receives every published revision of its group, serialized per tenant, coalesced while it is busy, never twice for the same non-zero revision, with `Status` reporting what is desired and what is actually applied.

### Epic 2.1: The publication coordinator in `internal/group`

**Goal:** The non-generic semantics of FC-7 exist and are tested with explicit revisions: per-scope publication cache, the seed that covers a publication still in flight, per-applier revision dedupe, trailing-edge coalescing, `Previous` tracking, rejection recording, and status aggregation.
**Scope:** `internal/group/` (new files alongside the codec).
**Dependencies:** Phase 1.
**Done when:** the coordinator's tests drive revision 0, a repeated non-zero revision, an advancing revision, a publication arriving while an applier is running, an applier returning an error, two scopes interleaved, and a `Register` with nothing observed yet that seeds from the injected read and then drops the publication following it at the same revision — all with no goroutine surviving `goleak`.
**Status:** Pending

The contract between this epic and Epic 2.2 is the coordinator's surface, written here so the two cannot disagree:

```go
package group

// Publication is one published revision of a group's document in one scope.
type Publication struct {
	Tenant   string
	Revision int64
	Value    any // the raw published document; the coordinator decodes it
}

// Coordinator holds the per-scope publication cache and the registered
// appliers of one group. It spawns no goroutines: a fan-out runs on the
// goroutine that published.
type Coordinator[T any] struct { /* unexported */ }

// NewCoordinator builds a coordinator. decode converts a published document
// into T. seed reads the group's current entry through the Client and reports
// ok=false when the Client does not yet track the scope (Entry.Stale, FC-5);
// it is consulted only by a Register that finds no observed publication at
// all, which is the window between Start returning and the first dispatch
// delivery landing (D-G7).
func NewCoordinator[T any](
	decode func(any) (T, error),
	seed func() (Publication, bool),
) *Coordinator[T]

// Publish records pub as the newest state of its scope and delivers it to
// every registered applier, serialized per scope. A publication that arrives
// while a fan-out for the same scope is running replaces the pending one
// rather than queueing behind it.
func (c *Coordinator[T]) Publish(ctx context.Context, pub Publication)

// Register adds fn and synchronously delivers the cached publication of every
// scope already observed. When no scope has been observed it takes one seed
// and delivers that instead, so a caller registering between Start and the
// first dispatch delivery is never left without a value. Returns a function
// that removes fn.
func (c *Coordinator[T]) Register(fn func(ctx context.Context, snap, prev Publication, value T) error) func()

// Status reports desired and applied revisions per scope, sorted by tenant.
func (c *Coordinator[T]) Status() []Status

type Status struct {
	Tenant  string
	Desired int64
	Applied int64
	LastErr error
}
```

Semantics this epic implements, each of them an FC-7 sentence:
- Dedupe is per (applier, scope) on the last DELIVERED revision. A non-zero revision equal to the last delivered one is skipped; Revision 0 is always delivered.
- `Desired` is the newest revision observed for the scope, advancing even when coalescing meant no applier saw the intermediate ones.
- `Applied` is the newest revision every registered applier has accepted — the minimum across appliers — so "applied" means the document is in force everywhere. With the single-applier case, which is the normal one, this is just that applier's last accepted revision.
- An applier returning an error records the revision as rejected: `Desired` advances, that applier's `Applied` does not, `LastErr` holds the error, and nothing is retried. `LastErr` is nil exactly when `Desired == Applied`.
- A publication whose value fails `decode` is recorded identically to an applier rejection, and logged by the caller — the coordinator must never hand a garbage `T` to an applier.
- `Previous` is the last snapshot that applier ACCEPTED for that scope, nil on its first delivery and unchanged by a rejection.
- Coalescing is driven by an internal sequence number, not by the revision, because Revision 0 legitimately repeats.
- A seed records its revision as that scope's seeded watermark, and the FIRST publication arriving for the scope afterwards is dropped when its revision is less than or equal to the watermark: the seed and that publication are one observation, not two (D-G7). Only the first is droppable — afterwards the ordinary rules resume, so a later Revision 0 (a delete) still delivers.
- A seed is an observation like any other for status purposes: a seeded delivery an applier accepts sets that scope's `Desired` and `Applied` to the seeded revision.
- `seed` returning ok=false (the Client does not track the scope yet, which is the pre-`Start` case) delivers nothing and records nothing. The coordinator never calls `seed` again once any publication has been observed for that scope.

### Epic 2.2: `OnApply`, `Status` and the subscription wiring

**Goal:** The FC-7 surface is complete at the root and wired to the coordinator through the facade's `OnChange`.
**Scope:** root `api_group.go`, `api_group_test.go`.
**Dependencies:** Epic 2.1.
**Done when:** `Bind` takes the group's single `OnChange` subscription and feeds the coordinator; `OnApply` before `Start` registers and receives its initial delivery when the engine publishes at `Start`; `OnApply` after `Start` delivers the current snapshot before returning, seeding it from `GetEntry` when the publication has not been delivered yet (D-G7); the same non-zero revision is never delivered twice; a slow applier does not lose a revision, only intermediate ones; `Status` reports per tenant; multi-tenant `OnApply` returns `ErrNotSupportedInMultiTenant` until engine-tenants lands. The root test that pins the pre-`Start` case uses a fake store whose `Subscribe` fires an event for the seeded key, so the publication lands DURING `Start` — the same shape the engine's reconcile will take.
**Status:** Pending

The RED test for the seed, `TestGroupOnApplyAfterStartSeedsUndeliveredPublication`: the fake store is seeded with a row and its `Subscribe` HOLDS the publication instead of firing it during `Start`, standing in for a dispatch worker that has not run yet. `OnApply` called immediately after `Start` returns fires the applier exactly once, with the stored document rather than the registered default. Releasing the held publication at the same revision does NOT fire it again. The matching coordinator-level tests in Epic 2.1 drive the revision arithmetic the wave-1 facade cannot express (D-G9): a released publication at a HIGHER revision does fire, and a Revision 0 delete arriving after the dropped first publication fires too.

### Epic 2.3: Compiling package example and godoc sweep

**Goal:** A reader of the package documentation sees a complete group in one screen, and it compiles.
**Scope:** root `example_group_test.go` (new), doc comments in `api_group.go`.
**Dependencies:** Epic 2.2.
**Done when:** `example_group_test.go` is in package `systemplane_test` with NO build tag and NO `// Output:` comment, so it compiles in the untagged build (`go vet ./...`, `go build`) as well as under `-tags=unit`, and is never executed. It therefore must not reference `NewForTesting`, which is build-tag gated — it constructs through `NewPostgres` inside a function that never runs. It shows: declaring the config struct, `Bind` with a validator, `OnApply`, `Start`, `Snapshot` and `Set`. Every FC-7 doc comment in `api_group.go` matches the behavior that landed.
**Status:** Pending

---

## Requests to index.md

One promise this lane needs from a sibling, written so the orchestrator can freeze it verbatim.

**R1 — engine-core: the reconcile at `Start` publishes every registered key, and a publication dispatches to subscribers registered before `Start`.**

Proposed wording for `## Frozen Contracts`:

> **FC-11 Initial publication at `Start`.** The reconcile the engine performs in
> response to the `OpResync` that `Store.Subscribe` emits when a scope's
> changefeed first connects publishes EVERY registered key of that scope, and a
> publication dispatches to `OnChange` subscribers registered before `Start`
> exactly as a later one does. A key with a row publishes its stored value and
> revision; a key with no row publishes the registered default with Revision 0.
> A subscriber registered before `Start` therefore receives exactly one delivery
> per registered key during `Start`, and `Client.Start` returns only after that
> scope's first reconcile has completed.

Why this is cheap for engine-core rather than new work: D2 already requires the engine to answer every `OpResync` by reloading the scope and republishing, and requires those republications to fire callbacks — that is the whole mechanism behind "converge without a second write". R1 only says the FIRST reconcile behaves like every other one. It removes the v3 special case in which hydration wrote the cache without firing subscribers; it does not add one.

Why this lane needs it: FC-7 states that `OnApply` called before `Start` registers and has its initial delivery during `Start`. This lane makes `OnApply` deliver from the group's own record of what the engine has published (D-G7). Without R1 the engine publishes nothing at `Start`, so a group bound and subscribed before `Start` has nothing to replay and its applier receives no initial delivery until the first real change.

Consequence the orchestrator should route to the docs lane: a per-key `OnChange` subscriber registered before `Start` now fires once at `Start`. That is a visible v4 behavior change for every consumer that calls `OnChange` (br-sfn has 17), and a benign one — it removes the read-then-subscribe race each of them currently hand-rolls — but `MIGRATION-v4.md` should name it.

---

## Self-review

**No blockers.** Nothing in this lane requires changing FC-7. FC-7's pre-`Start` `OnApply` clause requires R1 from engine-core, which is a promise about engine behavior, not a change to the frozen group API.

### FC-7 coverage

| FC-7 item or semantic | Delivered by |
|---|---|
| `Bind[T]` signature and "must be called before `c.Start`" | Task 1.1.2 |
| `Group[T]` type | Task 1.1.2 |
| `Snapshot[T]` type and its four fields | Task 1.1.2 (type), Task 1.2.1 (population, D-G5) |
| "`defaults` as the value in force when no row exists" | Task 1.1.2 (D-G1) |
| "`validate` runs on every ingress: defaults at `Bind`, `Set`, hydration, refresh, reconcile" | Task 1.1.2 — one `func(any) error` registered through `WithValidator` is what the facade and the engine both call (D-G3) |
| `Snapshot(ctx)` | Task 1.2.1 |
| `Set(ctx, value, actor)` | Task 1.2.2 |
| Group atomicity is one row | Task 1.2.3 |
| `Applied[T]` with embedded `Snapshot[T]` and `Previous *Snapshot[T]` | Epic 2.2 |
| "`Previous` is the snapshot fn last accepted for that scope, nil on the first delivery" | Epic 2.1 |
| `OnApply` signature and unsubscribe | Epic 2.2 |
| "subscribes first and then delivers the current snapshot of every scope the Client already tracks" | Epic 2.1 (`Coordinator.Register` replay plus the seed for a publication still in flight), Epic 2.2 (subscription taken at `Bind`, D-G7) |
| "so no revision can fall between the initial delivery and the subscription" | D-G7 — structural: the subscription predates every publication, and the seed's watermark drops the one publication it duplicates |
| "the same non-zero revision is never delivered twice; Revision 0 is never deduplicated" | Epic 2.1 |
| "serialized and coalesced per scope of this group" | Epic 2.1 (D-G8) |
| "`Status.Desired` always names the newest published revision even when fn has not seen intermediate ones" | Epic 2.1 |
| "fn returning an error records that revision as rejected, keeps the previously applied revision as current; the engine does not retry" | Epic 2.1 |
| "Before `Start`, `OnApply` registers and the initial delivery happens during `Start`" | Epic 2.2 + request R1 |
| `ApplyStatus` fields and `LastErr` nil when `Desired == Applied` | Epic 2.1 |
| `Status() []ApplyStatus` | Epic 2.2 (sorted by tenant, D-G10) |

### Vagueness scan

Every Phase 1 task names its edge cases and their handling: wrong-shaped row (decode error, never a half-filled `T` — and through an engine-backed Client it never reaches the group at all, D-G4), unknown fields (accepted, with the rolling-deploy reason), nil input (zero `T`), caller-supplied `WithValidator` (cannot displace the type check), nil receiver (`ErrClosed`), `Set` before `Start` (`ErrNotStarted` inherited), `append` aliasing the caller's option slice, unsynchronized fake store under `-race`. No "appropriate", "TBD" or unnamed edge case remains in Phase 1. Deferrals exist only in Phase 2 epics, which is what rolling detail is for.

### File disjointness

This lane creates and owns, and touches nothing else:

- `api_group.go`
- `api_group_test.go`
- `example_group_test.go`
- `internal/group/codec.go`, `internal/group/codec_test.go`, `internal/group/main_test.go`, plus the coordinator files Phase 2 adds under the same directory

All are new. The index assigns `api_group*.go` to this lane explicitly and excludes them from engine-core's scope; `internal/group/` is named by no other lane; the docs lane owns `examples/groups/`, a directory, not `example_group_test.go`. `boundary_test.go` is engine-core's and needs no edit from here — it is an AST scanner, not an inventory of exported symbols, and it already handles generic receivers. No `file:line` reference appears anywhere in this plan; every reference into existing code is by symbol name, per lane-cut rule 3.

`go.mod` and `go.sum` are not edited. The one new external import, `lib-commons/v7/commons/tenant-manager/core`, is already a direct requirement of the module. If `go mod tidy` disagrees, the lane stops and reports to the orchestrator rather than editing either file.

### Deferred to the integration lane

Two assertions this lane cannot make green on its own base, both because they depend on engine behavior that has not landed:

1. A non-zero revision arriving through the single-tenant facade — `Snapshot.Revision` and `Applied.Revision` end to end. The wave-1 shim zeroes both (FC-5 documents this). Covered here at coordinator level with synthetic revisions (D-G9) and end to end by integration scenario 4.
2. The real (non-fake) pre-`Start` initial delivery under R1, which is integration scenario 4's `Group.OnApply` `Status()` assertion. The pre-`Start` gate on the seed rides with it: the wave-1 shim reports `Stale false` always, so only a real engine distinguishes "not started yet" from "tracked and reconciled" (D-G7).
3. The end-to-end rule that an invalid stored row keeps the last valid value in force — or the registered default when nothing valid was ever published — and is visible only in the engine's log and telemetry (D-G4). The rejection happens in engine-core's ingress, which is not on this lane's base; engine-core asserts it directly and the integration lane's "invalid external row keeps last valid" scenario covers it against real backends.
