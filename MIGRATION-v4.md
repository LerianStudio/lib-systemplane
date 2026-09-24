# Migrating to lib-systemplane v4

v4 has one job: **replace the two runtime-configuration engines v3 carried with
one**, and give the surface the vocabulary that engine needs — a revision, a
freshness flag, an error on a write that landed but was not published.

---

## Why v4 exists

v3 implemented one policy twice. The single-tenant `Client` kept an in-process
cache fed by one changefeed loop; the multi-tenant `Manager` kept a second cache
fed by a second loop, with its own warm load, its own NOTIFY handling and its
own callback dispatch. Two code paths answered the same questions — what does a
delete publish, what happens when the connection drops, what does a callback
receive — and answered them differently. Every defect the 2026-09 audit raised
was one bug that existed in one engine and had already been fixed in the other.

v4 has one engine. It tracks scopes (the single-tenant scope is one of them),
reconciles a whole scope against the store after every changefeed connect and
reconnect, and stamps every value it publishes with the revision the store
assigned. A key is fenced individually: a re-read that fails marks that key
stale and nothing else, and each subscribed key is delivered by its own worker.

What a single-tenant consumer gets from that today: a value written while the
feed was down becomes visible after the reconnect **without a second write**;
a subscriber that blocks on one key no longer delays delivery on another; and a
read reports whether anything is currently confirming the value it just handed
back. That is a behaviour upgrade, not only a rename — so § Behaviour changes
has to be read even where your code compiles unchanged.

<!-- NOT-YET(engine-tenants): the same guarantees per tenant scope -->

---

## The surface diff

**Removed.**

| Gone in v4 | What to do |
|---|---|
| `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold` | The type and its constructor no longer exist. A single-tenant process never needed them. A multi-tenant process configures the one `Client` instead: `WithLogger`, `WithTelemetry`, `WithMultiTenantEnabled`, `WithModule`. |
| `(*Manager).Drain` | `Client.Close()`. It now waits, bounded by `WithCloseTimeout`, for subscriber callbacks whose context it has cancelled. |
| `(*Manager).IsClosed` | No replacement. Calls on a closed `Client` return `ErrClosed`; that is the answer the flag was read for. |
| `(*Manager).OnTenantActivated`, `OnTenantSuspended`, `OnTenantDeleted`, `OnTenantCredentialsRotated`, `HandleTenantLifecycle` | <!-- NOT-YET(engine-tenants): Client.HandleTenantLifecycle, WithAggregateTenantThreshold, tenant-scope teardown in Close --> |
| `DefaultSeedSQL` | No replacement. Defaults live in code, at `Register` / `Bind`. A value an operator must be able to override before first boot is a row your own migration pipeline inserts, not something this library seeds. |
| `WithTable`, `WithListenChannel`, `WithCollection` | No replacement: the names are fixed. Postgres table `systemplane_entries`, Postgres channel `systemplane_changes`, MongoDB collection `systemplane_entries`. Drop the option; if you renamed an object, rename it back before upgrading. |

**Added.**

| New in v4 | What it is for |
|---|---|
| `WithCloseTimeout`, `ErrCloseTimeout` | Bound the wait `Close` gives subscriber callbacks (default 30s) and name the (scope, key) still running when the bound elapses. |
| `GetEntry`, `Entry` | Read the value together with its revision, `UpdatedAt`, `UpdatedBy` and a per-key `Stale` flag. |
| `Bind`, `Group[T]`, `Snapshot[T]`, `Applied[T]`, `ApplyStatus`, `Group.OnApply`, `Group.Status`, `ErrApplyPanicked` | Declare a whole typed configuration document as one key, read it as `T`, and apply it through a serialized hook that records what is desired, what is applied and what last failed. |
| `MigrationV3ToV4SQL()` | The v3 → v4 Postgres delta as an importable artifact for your migration pipeline. See § The database and operator contract. |

The tenant-manager additions ride the same placeholder as the removed `Manager`
lifecycle row above; they are not a second thing to wait for.

**Changed shape.**

| Symbol | v3 | v4 |
|---|---|---|
| `OnChange` callback | `func(ctx context.Context, ns, key string, newValue any)` | `func(ctx context.Context, ch Change)`, where `Change{Tenant, Namespace, Key, Revision, Value}`. `Tenant` is `""` in single-tenant mode; `Revision` is 0 when no row exists and `Value` is then the registered default. |
| `Close` | `func() error`, returned once the backend was released | Same signature. It now cancels the callback context and waits for callbacks, bounded by `WithCloseTimeout`, before returning. |
| `SchemaSQL()` | v3 DDL | v4 DDL: the revision column, its sequence and the three triggers. |

Everything else in the facade is unchanged: `NewPostgres`, `NewMongoDB`,
`NewForTesting`, `Register`, `Start`, `Close`, `Get`, `GetString`, `GetInt`,
`GetBool`, `GetFloat64`, `GetDuration`, `Set`, `Delete`, `List`, `Catalog`,
`CatalogKey`, `CatalogService`, `KeyDescription`, `KeyRedaction`,
`IsRegistered`, `Logger`, the key options `WithDescription`, `WithValidator`,
`WithContextValidator`, `WithRedaction`, `WithCatalogMetadata`, the client
options `WithLogger`, `WithTelemetry`, `WithDebounce`, `WithPollInterval`,
`WithMultiTenantEnabled`, `WithModule`, `WithCatalogService`, and
`admin.Mount` / `admin.MountCatalog` with their options.

So a single-tenant consumer that registers keys and reads them edits its import
line — and then reads § Behaviour changes, because numeric defaults and
validators now see `float64` rather than `int`, and `Set` and `Delete` can
return errors they never returned in v3.

---

## The module and dependency hop

| You are on | Module path | lib-commons | lib-observability | Also required |
|---|---|---|---|---|
| `/v3` | `/v3` → `/v4` | `/v7`, already | `/v4`, already | Nothing. The import line is the whole hop. |
| `/v2` | `/v2` → `/v4` | `lib-commons/v6` → `/v7` | `/v2` → `/v4` | The observability boundary, described in [MIGRATION-v3.md](MIGRATION-v3.md). |
| v1.6.x | unsuffixed → `/v4` | v5 → `/v7` | v1 → `/v4` | Fiber v2 → v3 first; see below. |

**lib-commons' major is not optional.** It is part of this library's contract by
construction, which is why `boundary_test.go` leaves it out of the denylist it
enforces on lib-observability: the tenant connector takes a concrete lib-commons
connection-pool handle, and no interface can stand in for it. Concretely, today:
multi-tenant mode resolves the per-tenant database through lib-commons v7's
`tmcore.GetPGContext` / `tmcore.GetMBContext`. A process whose tenant-manager
middleware is still on lib-commons v6 sets a context key this library never
looks at, so every read and write fails with `ErrTenantConnectionMissing`.

**Fiber v2 → v3 is a precondition, not part of this upgrade.** It is what makes
v1.6.x the hardest starting point: it changes `admin.WithAuthorizer` from
`func(*fiber.Ctx, string) error` to `func(fiber.Ctx, string) error`, and it
touches your whole HTTP layer rather than this library alone. This repository
publishes no v1 → v2 document; migration notes start at v3.

**lib-observability stays on `/v4` in every row.** `go mod tidy` raises its minor
to whatever this library requires. Do not pin it yourself.

---

## Behaviour changes

These are ordered by how quietly each one changes a running service: the first
breaks no build and can change what your process serves on its next boot; the
last is a log query.

### A stored row your validator rejects no longer reaches a read

**Affects:** every single-tenant consumer that registered a validator.

v3 handed a stored row straight to `Get`. v4 grades every value on the way in —
the first reconcile at `Start`, every later reconcile, every changefeed re-read
— with the same validator that grades a `Set`. A row the validator refuses
never comes into force: at `Start` the registered default stays in force, on a
later refresh the last valid value stays, and a WARN names the namespace, the
key and the validator's error. The refused value itself is never logged.

So a row an older binary wrote, or an operator wrote by hand, or that a
validator you have since tightened would now refuse, stops being served the
next time the process boots — with nothing failing at build time to say so.

**Do:** before deploying, query the store for rows your validators would
refuse. Those keys revert to their registered default on the next start, so fix
the rows or widen the validator first.

Multi-tenant per-request reads (`Get`, `List`) still read through ungraded.

<!-- NOT-YET(engine-tenants): tenant scopes graded at ingress -->

### Validators and defaults see the canonical JSON shape

**Affects:** every consumer with a validator that type-asserts, and every
numeric or structured default.

A value reaches a validator — and a reader — in the shape the store hands back:
numbers as `float64`, objects as `map[string]any`, arrays as `[]any`. That now
holds at `Register` too, where the default is marshaled and decoded before it
enters the registry, so `Get` of a numeric key with no row returns `float64`
and never the Go value you passed. A validator that asserts the Go type it
registered fails with `ErrValidation` — at `Register` for the default, at `Set`
for a write:

~~~go
// v3 accepted this. v4 refuses it at Register and at Set.
systemplane.WithValidator(func(v any) error {
	n, ok := v.(int)
	if !ok || n < 1 {
		return errors.New("want a positive whole number")
	}
	return nil
})

// v4: grade the canonical shape.
systemplane.WithValidator(func(v any) error {
	n, ok := v.(float64)
	if !ok || n < 1 || n != math.Trunc(n) {
		return errors.New("want a positive whole number")
	}
	return nil
})
~~~

Grading one shape on every ingress has two further consequences. Validators
must be **deterministic**, because read-back grades the stored row again in
that same shape and a validator answering differently on that pass pins the
last valid value. And a validator that **panics** refuses the write, or the
row, instead of unwinding into the caller's goroutine: it comes back as
`ErrValidation`, and the panic is reported through lib-observability's recovery
pipeline — for a key registered redacted, carrying the panic value's dynamic
type rather than the value.

`WithContextValidator` sees the `Set` caller's own context on a write, but
read-back grades with the client's lifecycle context: no request values, no
tenant. A context validator that refuses when it cannot find a tenant therefore
refuses every stored row on read-back and pins the last valid value in force.
Treat a context that lacks the scope you expect as "cannot verify" and decide
by your own policy.

**Do:** audit every validator for Go-type assertions and for a dependency on
request scope.

### `Set` and `Delete` can return an error for a change that landed

**Affects:** every caller that reads a nil error as "persisted".

A single-tenant `Set` persists the row and then publishes it into this
process's own cache. v3 returned nil when that publication was dropped. v4
returns an error, with the row already in the store:

- `ErrClosed`, when the Client closed under the write;
- an error wrapping `ErrNotStarted` that names the key and says it "was
  written but not published" — the engine holds no live scope, because `Start`
  never brought one up or it was dropped under the write;
- otherwise an error naming the key and the publication failure.

`Delete` reports the same three, worded "was deleted but not published". The
narrow case worth knowing: a `Set` racing `Start` can persist its row and still
report `ErrNotStarted`, because the Client counts as started from the moment
its first reconcile begins.

**Do:** read a non-nil error from `Set` or `Delete` as "persisted, but this
process does not serve it yet", never as "not persisted". What it asks for is
that you stop reporting the write as lost, not that you retry it.

### Read-your-writes

**Affects:** every single-tenant consumer that reads back what it just wrote.

`Set` publishes the value into the cache with the revision the store assigned
before it returns, and the changefeed echo of that same write arrives at the
same revision and is deduplicated — refreshing provenance, firing no callback.
In v3 a `Set` followed immediately by a `Get` could return the old value until
the NOTIFY came back. **Do:** delete the workarounds for that window — the
sleep, the retry loop, the second read.

### Redaction fails closed

**Affects:** consumers that render configuration values, and anything that
reads `KeyRedaction`.

`KeyRedaction` reports `RedactFull` for a closed or nil Client, where v3
reported `RedactNone`. A Client that can no longer read its own registry
withholds rather than discloses: the admin GET and list handlers look the
policy up after their read, so a `Close` landing in that window would otherwise
render a redacted key in clear into a response body. On an open Client an
unregistered key still reports `RedactNone`.

A redacted key's value is also withheld from every report it could ride out on:
the decode-failure and validator-rejection log lines, a validator or apply-hook
panic, the typed getters' errors (`GetInt` and `GetDuration` used to quote the
value they could not convert) and the group decode and apply lines. Each
carries what failed and the value's dynamic type instead.

**Do:** expect masked output wherever a consumer renders values after `Close`,
and stop parsing values back out of these errors and log lines.

### Operational: primary pinning and the `keyname` log field

**Affects:** operators, and anyone running Postgres behind a resolver that
carries replicas.

Every systemplane statement is pinned to the primary, reads included — and to
the first primary deterministically when a resolver reports several. A standby
could otherwise serve a revision older than the one `Set` just returned, and
older than the NOTIFY the changefeed is reconciling against, since the feed
LISTENs on the primary DSN. What this gives up is read spreading, and only on a
resolver reporting more than one primary.

Every log line that named a configuration key in field `key` names it in
`keyname` instead. `key` is an exact entry in lib-observability's default
sensitive-field list, so those lines shipped `key=[REDACTED]` to an operator
hunting a rejected row — a line that reads correctly in the source and is wrong
only in production.

**Do:** re-point any log query, dashboard or alert that matches on field `key`.

### Every callback registered before `Start` fires once at `Start`

**Affects:** every single-tenant consumer with an `OnChange` subscriber.

`Start` reconciles every registered key against the store and hands the value
in force to every subscriber registered beforehand — once, per key. Keys the
store had no row for, and keys whose stored row the validator refused, are
announced as the registered default at `Revision 0`. The announcement is queued
while `Start` runs and delivered on the key's own goroutine, so a callback may
run just after `Start` returns; what `Start` itself guarantees is that every
read taken after it already serves the value that announcement carries.

What it buys is that a consumer can put its whole reload path in `OnChange` and
be correct from boot, instead of reading each key once at start-up and
subscribing for the rest. What it costs is that everything a callback does as a
side effect now happens on every boot.

**Do:** make callbacks idempotent, or compare the announced value against the
one last applied. A callback that posts a notification, rotates a credential or
restarts a worker now does it once per process start.

This holds for the single-tenant scope today.

<!-- NOT-YET(engine-tenants): multi-tenant callbacks fire once per tenant at activation -->

### `OnChange`'s signature changed, and an unregistered key is refused

**Affects:** every `OnChange` caller. It is the one change in this section that
breaks the build, which makes it the easy one.

~~~go
// v3
unsub, _ := c.OnChange(ns, key, func(ctx context.Context, ns, key string, v any) {
	reload(v)
})

// v4
unsub, err := c.OnChange(ns, key, func(ctx context.Context, ch systemplane.Change) {
	reload(ch.Value)
})
~~~

`Change{Tenant, Namespace, Key, Revision, Value}` carries two things the four
arguments could not. `Revision` is the store revision behind the value, and 0
means no row: the registered default is in force because the key was deleted,
was never written, or its stored row was refused. `Tenant` names the tenant
whose row changed and is `""` in single-tenant mode — it is what a callback
reads to learn the tenant, never the context.

Never the context, because the context a callback receives is the engine's own
lifecycle context: no request values, no tenant. v3's godoc promised a
tenant-scoped context and handed over the LISTEN goroutine's. Cancellation is
the one thing to read out of it, and honouring it is what lets `Close` finish.

`OnChange` for a key nothing registered returns `ErrUnknownKey` in both modes,
where v3 logged a debug line and handed back a no-op unsubscribe — a
subscription that could never deliver anything now surfaces the typo at wiring
time. In multi-tenant mode a registered key then returns
`ErrNotSupportedInMultiTenant`: no scope is tracked and no changefeed runs, so
nothing could fire. For a consumer that only reads per-request configuration
that refusal is permanent, and reading through on each request is the whole
design.

<!-- NOT-YET(engine-tenants): multi-tenant OnChange with a tenant manager, Change.Tenant set -->

**Do:** rewrite the signature, and check the error — v3 callers routinely
discarded it because it only ever reported a closed Client.

### Deliveries are coalesced per key and independent across keys

**Affects:** subscribers that count callbacks or accumulate what they receive.

One goroutine per subscribed key delivers that key's changes, serially and off
the changefeed goroutine. While a callback runs, a newer revision of the same
key replaces the pending one: the callback may skip intermediate revisions,
always receives the newest, and never sees revisions out of order. Different
keys deliver independently, so a subscriber blocking on one key delays nothing
else. v3's `Manager` ran every callback synchronously on the LISTEN goroutine,
where one slow subscriber stalled every key of every tenant.

Two deliveries are suppressed rather than coalesced. A row republished at the
same non-zero revision with the same bytes refreshes `UpdatedAt` and
`UpdatedBy` and fires no callback — that is how a `Set`'s own changefeed echo
is absorbed. `Revision 0` is never deduplicated, so a delete always delivers.

**Do:** read a delivery as "the current value is this", never as "one change
happened". A consumer that counted events, or that applied each value in turn
to build up state, reconciles to the newest value instead.

### `Close` is bounded, and names the callback that would not stop

**Affects:** every consumer that closes a Client, and anyone with a
long-running callback.

`Close` cancels the changefeed and the context every callback holds, then waits
for deliveries still running — up to `WithCloseTimeout`, 30 seconds by default.
A callback that honours its context ends and `Close` returns nil. One that
ignores it survives `Close`, which returns `ErrCloseTimeout` naming every
`(scope, key)` still inside a delivery; the single-tenant scope renders as
`single-tenant`. That goroutine is the subscriber's leak, reported rather than
hidden.

An `ErrCloseTimeout` that names no key at all is a different diagnosis, not a
missing one: no callback is running, so what held shutdown is engine work
inside the store — a reconcile whose `List` has not answered, or a debounced
re-read. That is a backend or network fault, and looking for it in consumer
code wastes the outage.

`WithCloseTimeout` bounds the engine's wait alone. Releasing the backend is not
covered by it, and `Close` returns the engine's timeout joined with the store's
own error so neither hides the other. A second `Close` replays the first one's
result.

**Do:** honour the context a callback is handed. Raising the timeout is the
second-best answer to a callback that cannot be interrupted.

### A failed `Start` is retriable

**Affects:** consumers that start against a database that may not be up yet.

A `Start` that fails leaves the Client usable and every subscription registered
before it intact, so the answer to a database that is not up is to call `Start`
again rather than to rebuild the Client. After a failed reconcile the next
`Start` reconciles from nothing. After a context expiry it waits on the
reconcile already pending, and `Register` stays refused with
`ErrRegisterAfterStart` — that reconcile may already have read the registry, so
the registry cannot grow underneath it.

**Do:** register every key before the first `Start` attempt, and retry `Start`
itself on failure.

### `GetEntry` reports revision, provenance and freshness

**Affects:** consumers that need to know whether the value they just read is
being confirmed by anything.

`GetEntry` returns `Entry{Value, Revision, UpdatedAt, UpdatedBy, Stale}`.
`Revision`, `UpdatedAt` and `UpdatedBy` describe the persisted row behind the
value and are zero when the registered default is in force. `Stale` reports
that nothing is currently confirming **this** key: before `Start`; while the
changefeed is disconnected, or connected but not yet reconciled; and while this
key's last change could not be re-read. Reads keep serving the last published
value throughout — `Stale` is about confidence, not absence — and a sibling key
that could not be re-read does not make this one stale.

Three rules produce that per-key answer:

- A changefeed delete is counted the moment it arrives, ahead of the re-read
  that answers it, so nothing can publish the removed row back over the
  removal. An empty re-read publishes the registered default at `Revision 0`;
  a row recreated in the meantime wins at its own revision.
- A re-read that fails is retried once, off the changefeed goroutine, after a
  wait drawn from [125ms, 250ms) — floored so the pool that refused the first
  read is not hammered, jittered so a scope-wide failure does not send every
  key back at the same instant. A second failure marks that one key `Stale`.
- A key is confirmed only by an ingress that read it back. A reconcile snapshot
  does not clear the mark, because that photograph may predate the very change
  the failed re-read was sent for.

The admin GET and list responses render the same four fields: `revision`,
`updatedAt` (JSON null when no row backs the value), `updatedBy` and `stale`.

**Do:** use `GetEntry` where a decision depends on freshness — gating a risky
action, or answering a health endpoint. `Get` is unchanged for everything else.

### Panics, and your logger

**Affects:** everyone.

An `OnChange` callback that panics is recovered per subscriber: the other
subscribers of that key still run, the delivery worker survives, and the panic
is reported through lib-observability's recovery pipeline under component
`systemplane.engine`, name `onchange`. For a key registered redacted the report
carries the panic value's dynamic type instead of the value.

The logger you pass in is guarded. A logger that panics can neither take a
library goroutine down nor unwind out of a library call — in multi-tenant mode
the error lines are written on the caller's own goroutine, so an unguarded one
took a `Get` or a `List` with it. `Logger()` still hands back the logger you
passed, unwrapped. Multi-tenant error lines stamp `tenant.id` from the context,
or `unresolved` when the context carries none.

<!-- NOT-YET(panic-posture): every recovered panic reported with a log line, panic_recovered_total and a span event; the counter needs runtime.InitPanicMetrics in the host -->

**Do:** nothing, unless a panicking callback was what failed your boot. It no
longer does.

### Revisions are opaque

**Affects:** anything that persists, exports or compares a `Revision`.

Compare revisions of one key, for ordering, and nothing else: what a revision
is, how it advances and what it refuses to promise is in
[§ The database and operator contract](#the-database-and-operator-contract).

---

## The database and operator contract

<!-- filled by Task 1.1.3 -->

---

## Per consumer

Find your row and read only it. Every section below assumes § Behaviour changes
has already been read: it is where the changes that break a service without
breaking its build are written down, and no per-consumer section repeats them.

<!-- filled by Tasks 1.1.4, 1.1.5 -->

---

## Why the module path moved in the same commit as the break

This repository does not auto-major: `.releaserc.yml` maps a breaking commit to
a **minor** bump, guarded in both directions by `admin/release_policy_test.go`,
because a `major` rule on a `/vN` line computes a `/vN+1` version whose tag Go
cannot consume and whose release run dies at `git tag`. The path rename and the
API break are therefore one change, since Go rejects a module whose path says
`/v4` under a `v3.x` tag outright. The `v4.0.0` cut follows a semantic-release
dry-run against `main`, which decides whether the run cuts the tag itself or a
hand tag plus its channel note is needed — never a reflex tag.
