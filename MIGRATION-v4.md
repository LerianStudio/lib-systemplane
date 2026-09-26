# Migrating to lib-systemplane v4

v4 has one job: **replace the two runtime-configuration engines v3 carried with
one**, and give the surface the vocabulary that engine needs — a revision, a
freshness flag, an error on a write that landed but was not published.

---

## Why v4 exists

v3 implemented one policy twice. The single-tenant `Client` kept an in-process
cache fed by one changefeed loop; the multi-tenant `Manager` kept a second cache
fed by a second loop, with its own warm load, its own NOTIFY handling and its
own callback dispatch. The two engines answered the same questions — what does
a delete publish, what happens when the connection drops, what does a callback
receive — differently.

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

A multi-tenant Client built with `WithPostgresTenantManager` or
`WithMongoTenantManager` gets the same per tenant: a tenant's first read brings
up that tenant's own scope, reconciled and fed by its own changefeed, and later
reads are served in process.

---

## The surface diff

**Removed.**

| Gone in v4 | What to do |
|---|---|
| `Manager`, `ManagerOption`, `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, `WithManagerAggregateTenantThreshold` | The type and its constructor no longer exist. A single-tenant process never needed them. A multi-tenant process configures the one `Client` instead: `WithLogger`, `WithTelemetry`, `WithMultiTenantEnabled`, `WithModule`, and `WithAggregateTenantThreshold` in place of `WithManagerAggregateTenantThreshold`, with the same meaning and default (`DefaultAggregateTenantThreshold`, 1000). The metrics moved: see § Metrics moved to meter `systemplane.engine`. |
| `(*Manager).Drain` | `Client.Close()`. It now waits, bounded by `WithCloseTimeout`, for subscriber callbacks whose context it has cancelled. |
| `(*Manager).IsClosed` | No replacement. Calls on a closed `Client` return `ErrClosed`; that is the answer the flag was read for. |
| `(*Manager).OnTenantActivated`, `OnTenantSuspended`, `OnTenantDeleted`, `OnTenantCredentialsRotated`, `HandleTenantLifecycle` | `Client.HandleTenantLifecycle`, with the same `tmevent.EventHandler` signature, on a Client built with `WithPostgresTenantManager` or `WithMongoTenantManager`. It routes all four events, so the `On*` methods have no replacement of their own; chain it after the dispatcher's own `HandleEvent`. `tenant.activated` no longer warms the tenant: its first read does. It returns `ErrClosed` after `Close` and `ErrValidation` for an event with no `TenantID`, where v3 returned nil: the tenant-manager listener logs the error and moves on, and a dispatcher that stops on an error must handle both. `Client.Close()` tears every tenant scope down: it closes each tenant's feed and waits, bounded by `WithCloseTimeout`, for activations still in flight. |
| `DefaultSeedSQL` | No replacement. Defaults live in code, at `Register` / `Bind`. A value an operator must be able to override before first boot is a row your own migration pipeline inserts, not something this library seeds. |
| `RedactPolicy`, `RedactNone`, `RedactMask`, `RedactFull` | No replacement. |
| `WithRedaction`, `ApplyRedaction`, `(*Client).KeyRedaction` | No replacement; drop the option from every `Register` / `Bind` call, including `WithRedaction(RedactNone)`. |
| `WithTable`, `WithListenChannel`, `WithCollection` | No replacement: the names are fixed, and v4 reads nothing else. Postgres: rename or copy a custom table to `systemplane_entries` first, then apply `MigrationV3ToV4SQL()`, which names the table unqualified, refuses when `search_path` reaches none, and recreates the notification triggers on `systemplane_changes`, so a custom channel needs no step of its own. MongoDB: copy a custom collection to `systemplane_entries` before starting v4; v4 moves no data. |

**Added.**

| New in v4 | What it is for |
|---|---|
| `WithCloseTimeout`, `ErrCloseTimeout` | Bound the wait `Close` gives subscriber callbacks (default 30s) and name the (scope, key) still running when the bound elapses. |
| `GetEntry`, `Entry` | Read the value together with its revision, `UpdatedAt`, `UpdatedBy` and a per-key `Stale` flag. |
| `Bind`, `Group[T]`, `Snapshot[T]`, `Applied[T]`, `ApplyStatus`, `Group.Snapshot`, `Group.Set`, `Group.OnApply`, `Group.Status`, `ErrApplyPanicked` | Declare a whole typed configuration document as one key, read it as `T`, and apply it through a serialized hook that records what is desired, what is applied and what last failed. `Bind`, `Snapshot` and `Set` work in both modes; on a multi-tenant Client without a tenant manager `OnApply` returns `ErrNotSupportedInMultiTenant`, and `Snapshot` reads through graded like `Get`. |
| `MigrationV3ToV4SQL()` | The v3 → v4 Postgres delta as an importable artifact for your migration pipeline. See § The database and operator contract. |
| `WithPostgresTenantManager`, `WithMongoTenantManager`, `ErrTenantManagerBackendMismatch` | Cache and push each tenant's configuration the way the single-tenant scope is: a tenant's first read activates its scope, `OnChange` delivers per tenant with `Change.Tenant` set, and `Client.HandleTenantLifecycle` drops, blocks and rebuilds the scope. Each option implies `WithMultiTenantEnabled`; the constructor of the other backend refuses it with `ErrTenantManagerBackendMismatch`. |
| `WithContextValidator` | Validate a value against the `Set` caller's context, so a validator can use the tenant that call carried. The registered default is still validated with `context.Background()`. |
| `TestScope` | The scope every `TestStore` method now takes: `TestScope{Tenant}`, with `Tenant` `""` for the single-tenant scope. |

**Changed shape.**

| Symbol | v3 | v4 |
|---|---|---|
| `OnChange` callback | `func(ctx context.Context, ns, key string, newValue any)` | `func(ctx context.Context, ch Change)`, where `Change{Tenant, Namespace, Key, Revision, Value}`. `Tenant` is `""` in single-tenant mode; `Revision` is 0 when no row exists and `Value` is then the registered default. |
| `Close` | `func() error`, returned once the backend was released | Same signature. It now cancels the callback context and waits for callbacks, bounded by `WithCloseTimeout`, before returning. |
| `SchemaSQL()` | v3 DDL | v4 DDL: the revision column, its sequence and the three triggers. |
| `TestStore`, the store `NewForTesting` takes | `Get(ctx, ns, key)`, `Set(ctx, e) error`, `Delete(ctx, ns, key, actor)`, `List(ctx)`, `Subscribe(ctx, fn)` | Every method but `Start` and `Close` takes a `TestScope` after `ctx`, and `Set` returns `(int64, error)`: the revision the store assigned. |
| `TestEntry` | `{Namespace, Key, Value, UpdatedAt, UpdatedBy}` | Gains `Revision`. |
| `TestEvent` | `{Namespace, Key, Op}` | Gains `Scope` and `Revision`. |

Everything else in the facade is unchanged: `NewPostgres`, `NewMongoDB`,
`Register`, `Start`, `Close`, `Get`, `GetString`, `GetInt`,
`GetBool`, `GetFloat64`, `GetDuration`, `Set`, `Delete`, `List`, `Catalog`,
`CatalogKey`, `CatalogService`, `KeyDescription`,
`IsRegistered`, `Logger`, the key options `WithDescription`, `WithValidator`,
`WithCatalogMetadata`, the client options `WithLogger`,
`WithTelemetry`, `WithDebounce`, `WithPollInterval`,
`WithMultiTenantEnabled`, `WithModule`, `WithCatalogService`, and
`admin.Mount` / `admin.MountCatalog` with their options.

So a single-tenant consumer that registers keys and reads them applies
`MigrationV3ToV4SQL()` (Postgres: v4 reads and writes the `revision` column, so
without it the first `Start` fails), edits its import line, and then reads
§ Behaviour changes, because numeric defaults and validators now see `float64`
rather than `int`, and `Set` and `Delete` can return errors they never returned
in v3.

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
breaks no build and can change what your process serves on its next boot.

### A stored row your validator rejects no longer reaches a read

**Affects:** every consumer that registered a validator: single-tenant, and multi-tenant on every per-request read.

v3 handed a stored row straight to `Get`. v4 grades every value on the way in —
the first reconcile at `Start`, every later reconcile, every changefeed re-read
— with the same validator that grades a `Set`. A row the validator refuses
never comes into force: at `Start` the registered default stays in force, on a
later refresh the last valid value stays, and a WARN names the namespace, the
key and the validator's error. The refused value itself is never logged.

So a row an older binary wrote, or an operator wrote by hand, or that a
validator you have since tightened would now refuse, stops being served the
next time the process boots — with nothing failing at build time to say so.

Multi-tenant per-request reads (`Get`, `List`, `Snapshot`) are graded too, with
the reader's own context: a refused row reads as the registered default, and
every such read logs the WARN.

**Do:** before deploying, query the store for rows your validators would
refuse. Those keys revert to their registered default on the next start, or on
the first multi-tenant read after the deploy, so fix the rows or widen the
validator first.

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
pipeline.

`WithContextValidator` sees the `Set` caller's own context on a write, and a
multi-tenant per-request read grades with the reader's. Every other read-back
grades with the client's lifecycle context: no request values, and a tenant
only on a tenant-managed Client's tenant scope. A context validator that
refuses when it cannot find a tenant therefore refuses every single-tenant row
on read-back and pins the last valid value in force. Treat a context that lacks
the scope you expect as "cannot verify" and decide by your own policy.

**Do:** audit every validator for Go-type assertions and for a dependency on
request scope.

### `Set` and `Delete` can return an error for a change that landed

**Affects:** every caller that reads a nil error as "persisted".

A single-tenant `Set` persists the row and then publishes it into this
process's own cache. v3 wrote its cache directly and could not fail after
persisting. v4 returns an error, with the row already in the store:

- `ErrClosed`, when the Client closed under the write;
- an error wrapping `ErrNotStarted` that names the key and says it "was
  written but not published" — the engine holds no live scope, because `Start`
  never brought one up or it was dropped under the write;
- otherwise an error naming the key and the publication failure.

`Delete` reports the same three, worded "was deleted but not published". The
narrow case worth knowing: a `Set` racing `Start` can persist its row and still
report `ErrNotStarted`, because the Client counts as started from the moment
its first reconcile begins.

**Do:** classify the error by cause.
`ErrValidation`, `ErrUnknownKey`, `ErrNilContext` and `ErrTenantConnectionMissing`
are returned before the store is touched: nothing was persisted. `ErrClosed` and
`ErrNotStarted` come from either side of the write, so they do not say whether it
landed. Any other error is a store failure (outcome unknown) or a publication
failure (persisted); no sentinel tells the two apart, and `Get` cannot settle it:
in single-tenant mode it reports what this process serves, the published value,
not the row. Treat the outcome as unresolved. Retry only when the side effect is
acceptable: a retried `Set` overwrites any newer write (last-write-wins), and a
retried `Delete` removes a key recreated since the first attempt.

### A write's own changefeed echo no longer fires a callback

**Affects:** every single-tenant consumer that subscribes to a key it also
writes.

`Set` publishes the value into the cache with the revision the store assigned
before it returns, and the changefeed echo of that same write arrives at the
same revision and is deduplicated — refreshing provenance, firing no callback.
In v3 the echo fired the subscriber. **Do:** move anything a writer relied on
that callback for onto the write path itself.

### Nothing is masked any more

**Affects:** every consumer that registered a key with `WithRedaction`, and
every operator who can reach the admin surface.

Admin GET, list and catalog detail return values and defaults in clear, and
the catalog loses its `redaction` field. Values are served in clear to every
caller the admin authorizer allows, so mount `/system` behind an
operator/admin permission. Decode and validator lines carry the error as
produced, and a panic report carries the panic value
([README § Panic recovery](README.md#panic-recovery)).

**Do:** drop every `WithRedaction` call; a value that stays here is served in
clear.

### Operational: primary pinning and the `keyname` log field

**Affects:** operators, and anyone running Postgres behind a resolver that
carries replicas.

Where a resolver supplies the handle — multi-tenant mode, or a tenant connector
— every systemplane statement is pinned to the primary, reads included, and to
the first primary deterministically when a resolver reports several. A standby
could otherwise serve a revision older than the one `Set` just returned, and
older than the NOTIFY the changefeed is reconciling against, since the feed
LISTENs on the primary DSN. What this gives up is read spreading, and only on a
resolver reporting more than one primary. A single-tenant Client uses the
`*sql.DB` you hand `NewPostgres` unchanged, so read routing there is yours.

Every log line that named a configuration key in field `key` names it in
`keyname` instead. `key` is an exact entry in lib-observability's default
sensitive-field list, so those lines shipped `key=[REDACTED]` to an operator
hunting a rejected row — a line that reads correctly in the source and is wrong
only in production.

**Do:** re-point any log query, dashboard or alert that matches on field `key`.

### Metrics moved to meter `systemplane.engine`

**Affects:** every dashboard and alert on meter `systemplane.manager`, and every
Client built with `WithTelemetry`, single-tenant ones included.

| v3, meter `systemplane.manager` | v4, meter `systemplane.engine` |
|---|---|
| `systemplane.manager.tenants_active` | `systemplane.scopes_active` |
| `systemplane.manager.cache_entries` | `systemplane.cache_entries` |
| `systemplane.manager.notify_received_total` | `systemplane.changefeed_events_total` |
| `systemplane.manager.listen_disconnects_total` | `systemplane.changefeed_disconnects_total` |
| `systemplane.manager.warmload_latency_seconds` | `systemplane.activation_latency_seconds` |
| `systemplane.manager.get_cache_hits_total` | `systemplane.cache_reads_total`, attribute `result` = `hit` \| `miss` |

`systemplane.scopes_active` is one unlabelled count, as `tenants_active` was.
On every other instrument a tenant scope's points carry `tenant_id`, which reads
the literal `aggregate` for every tenant once more than
`WithAggregateTenantThreshold` tenant scopes are active, and the single-tenant
scope's points carry no `tenant_id`. v3's attribute `outcome` is now `result`;
`op` on `notify_received_total` and `reason` on `listen_disconnects_total` have
no replacement, and `systemplane.changefeed_events_total` also counts every
disconnect and every resync. `systemplane.cache_reads_total` counts every read
of an active scope, one made while that scope is still activating included: it
counts a miss and reads the tenant database. A read of a tenant with no active
scope counts nothing, as in v3.

### Every callback registered before `Start` fires once at `Start`

**Affects:** every `OnChange` subscriber of a single-tenant or tenant-managed
Client.

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

On a Client built with `WithPostgresTenantManager` or `WithMongoTenantManager`
`Start` announces nothing, because no tenant scope exists yet. Each tenant
announces every registered key once when its first read activates its scope,
and again each time that scope is rebuilt: after `tenant.credentials.rotated`,
and on the first read after a `tenant.activated` that follows a suspension or
deletion.

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
where v3 logged a debug line (single-tenant) and handed back a no-op unsubscribe
— a subscription that could never deliver anything now surfaces the typo at
wiring time. On a multi-tenant Client without a tenant manager a registered key
then returns `ErrNotSupportedInMultiTenant`: no scope is tracked and no
changefeed runs, so nothing could fire. For a consumer that only reads
per-request configuration that refusal is permanent, and reading through on
each request is the whole design. With `WithPostgresTenantManager` or
`WithMongoTenantManager` one subscription covers every tenant: `Change.Tenant`
names the tenant of each delivery, and deliveries are serialized and coalesced
per (tenant, key).

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
same non-zero revision with an equal decoded value refreshes `UpdatedAt` and
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

An `ErrCloseTimeout` that names no key means engine work inside the store held
shutdown — a reconcile whose `List` has not answered, or a debounced re-read.

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
that could not be re-read does not make this one stale. A read a tenant's
scope serves reports `Stale` the same way. A read that goes to the tenant
database reports false, because no cache can lag: every read of a multi-tenant
Client without a tenant manager, and a tenant-managed read while that tenant's
scope is not up.

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
`systemplane.engine`, name `onchange`.

The logger you pass in is guarded. A logger that panics can neither take a
library goroutine down nor unwind out of a library call — in multi-tenant mode
the error lines are written on the caller's own goroutine, so an unguarded one
took a `Get` or a `List` with it. `Logger()` still hands back the logger you
passed, unwrapped. Multi-tenant error lines stamp `tenant.id` from the context,
or `unresolved` when the context carries none.

Every panic the library recovers is reported the same way: a `panic recovered`
line at ERROR, a span event when the context carries a recording span, and an
increment of `panic_recovered_total`. The counter exists only after your host
calls `runtime.InitPanicMetrics(factory)` once at startup; the library never
calls it. Components and names: [README § Panic recovery](README.md#panic-recovery).

**Do:** nothing, unless a panicking callback was what failed your boot. It no
longer does.

---

## The database and operator contract

### Postgres

**The row carries a `revision`.** `revision BIGINT NOT NULL` is drawn from a
table-level sequence, `systemplane_revision_seq`, and only ever by the
`BEFORE INSERT OR UPDATE` trigger `systemplane_bump_revision_trigger`, whose
function is `SECURITY DEFINER`; once the DDL finishes the column carries no
`DEFAULT`. The sequence is therefore advanced with the privileges of the role
that applied the DDL, and **the runtime role needs plain DML on
`systemplane_entries` and no grant at all on `systemplane_revision_seq`**.

**Prefer `MigrationV3ToV4SQL()` for an existing install: it creates no table, so
it cannot fork one.** It adds `revision` at 1 for every row already stored,
creates and seeds the sequence past the highest revision present, installs
`systemplane_bump_revision_v4()` and `systemplane_notify_v4()`, drops
`systemplane_notify_v3()`, and installs three triggers: the new
`systemplane_bump_revision_trigger`, plus `systemplane_notify_trigger` and
`systemplane_notify_update_trigger`, which keep the names they had in v3. The
NOTIFY payload gains a fourth field — `{namespace, key, op, revision}` — and a
delete publishes `revision: 0`. It creates no table, so it upgrades the install
wherever `search_path` finds it; it is idempotent; and lib-systemplane never
executes it for you. `SchemaSQL()` is also idempotent and upgrades a v3 table in
place, but its `CREATE TABLE IF NOT EXISTS` lands in the first schema of
`search_path`; it is the artifact for a database that has no install yet.

The pipeline needs the SQL, not the library. Print it from a throwaway `main`:

~~~go
package main

import (
    "fmt"

    systemplane "github.com/LerianStudio/lib-systemplane/v4"
)

func main() { fmt.Print(systemplane.MigrationV3ToV4SQL()) }
~~~

Save it as `cmd/print-ddl/main.go`; `go run ./cmd/print-ddl >
004_systemplane_v4.sql` then hands the pipeline its file. Fetching the module to
emit SQL is not deploying it: the migration runs first, the binary boots after.

**The rolling-deploy window, and rollback.** A v1.6, v2 or v3 binary keeps
working on the migrated schema, so pods still on the old binary need not drain
first: their `INSERT` and `UPDATE` name no `revision`, and the
`BEFORE INSERT OR UPDATE` trigger fills that `NOT NULL` column on every write;
their reads name their own columns; and they decode the NOTIFY payload into
three fields and ignore the fourth. Rolling the binary back is therefore safe,
on the migrated schema; the migration has no down step and needs none for
that. Two exceptions. A pod listening on a custom channel goes deaf the moment
the migration runs, because the recreated triggers notify `systemplane_changes`
only: it keeps serving its cache, unrefreshed, until it is replaced. And a pod
reading a custom table fails once that table is renamed to
`systemplane_entries`, so the rename and the new binary land in one deploy.

The migration guards itself, because every statement in it names
`systemplane_entries` unqualified. It refuses when `search_path` reaches no
`systemplane_entries` at all — put the schema holding the install first in
`search_path` and re-run — and when a second `systemplane_entries` exists in
another user schema, where it would upgrade whichever one `search_path`
resolves first and leave the other on v3, reading v3 payloads through a v4
runtime; drop or rename the stray table, or narrow `search_path` to the schema
holding the install you mean.

`SchemaSQL()` carries the opposite guard, which fires when any non-system
schema other than `current_schema()` already holds the table:

> systemplane_entries already exists in schema %, but this role would provision
> into %; applying the full schema here would fork the install into a second,
> empty table and orphan the populated one

`CREATE TABLE IF NOT EXISTS` only ever looks at the first schema of
`search_path`, so without that guard the case exits 0 and leaves every
registered key serving its default out of a second, empty table. The fix is to
drop the stray copy if that is what it is, to put the schema holding the real
install first in `search_path`, or to upgrade that install with
`MigrationV3ToV4SQL()`, which creates no table and so follows `search_path` to
wherever the table actually is.

**One database per tenant, never one schema per tenant inside a shared
database.** Two reasons, both structural: NOTIFY is database-wide and every
feed listens on the single `systemplane_changes` channel, so two installs in
one database each receive the other's events; and the unqualified
`DROP FUNCTION` of the v3 notify function resolves through the applying role's
whole `search_path`, so applying the DDL in one schema can drop another
schema's function. Nothing in the database enforces this — it is the
operator's responsibility. A Client built with `WithPostgresTenantManager`
catches one case inside its own process: a tenant whose LISTEN connection
reaches a database another tenant's feed of that Client already listens on is
refused when its feed opens. That tenant's activation logs a WARN, its reads
stay per request and a later read retries; no root error reports it. A pinned
`search_path` alone is not refused, and two processes sharing one database
cannot see each other.

A single-tenant Client holds one LISTEN connection, opened from `listenDSN` and
held for the life of the Client, separate from the `*sql.DB` pool you hand
`NewPostgres`. A Client built with `WithPostgresTenantManager` holds one per
active tenant instead, opened from that tenant's primary DSN on top of the
tenant manager's pools, so size `max_connections` against active tenants ×
replicas. A tenant is active from the first read that brings its scope up until
`tenant.suspended`, `tenant.deleted` or `Close` drops it.

**Revisions are opaque and monotonic per `(namespace, key)`**, including across
a delete and a recreate: the counter is table-level, so a key that comes back
always lands above every revision it ever had. They may skip numbers, they
start at 2 on a fresh database rather than 1, and their magnitude differs
between backends — compare two revisions of one key for ordering and nothing
else. Writing the same value again keeps the revision: `updated_at` and
`updated_by` move, the row's provenance is refreshed, and no callback fires.

### MongoDB

**`Delete` writes a tombstone; it does not remove the document.** The document
stays, carrying `deleted: true`, no `value`, a bumped `revision` and the
provenance of the delete — which is what keeps a revision monotonic across a
delete and a recreate on a backend with no table-level counter. `Get` reports
the key as not found and `List` skips it, so nothing about the library's own
API changes.

**Anything that reads `systemplane_entries` directly — a report, a dashboard, a
support query, your own migration — must filter `deleted: {$ne: true}`.** `$ne`
rather than `$exists: false`: a document written before v4 carries no `deleted`
field at all and must stay visible, and `$ne` matches a missing field. The
library's own reads use exactly that filter.

Tombstones are never purged. Their count is bounded by the set of keys you
register, so there is nothing to schedule: a key deleted a thousand times
leaves one document.

**Change streams need a replica set.** Against a standalone server, pass
`WithPollInterval`: the fallback reads the same collection on a timer and obeys
the same resync and revision rules, costing latency rather than correctness. In
multi-tenant mode every tenant database — resolved from the request context, or
by `WithMongoTenantManager` for a tenant's scope — is materialized on first use
with `createCollection`, so the runtime role needs that privilege in every
tenant database; the single-tenant collection is left to be created by its
first write. A tenant-managed Client refuses a tenant's feed when another
tenant's feed of that Client already watches the same database and collection:
that tenant's activation logs a WARN and its reads stay per request. Tenants on
distinct databases of one server are admitted.

---

## Per consumer

Find your row and read only it. Every section below assumes § Behaviour changes
has already been read: it is where the changes that break a service without
breaking its build are written down, and no per-consumer section repeats them.
plugin-br-pix-lerian has no section: it does not depend on this library.
Every consumer below but product-console registers keys with `WithRedaction`:
drop the option from each call ([§ The surface diff](#the-surface-diff)).

### matcher

**From:** v2.0.0 — `/v2`, lib-commons `/v6`, lib-observability `/v2`.
**Mode:** single-tenant, Postgres.
**Breaks:** all three module paths (the `/v2` row of [§ The module and dependency hop](#the-module-and-dependency-hop)); the `OnChange` signature; every validator that asserts a Go type; `Set` and `Delete`, which now return an error for a change that landed.
**Do:**

1. Apply `MigrationV3ToV4SQL()` through your migration pipeline before the new binary boots.
2. Bump the three module paths in one change. lib-commons `/v7` is not optional.
3. Rewrite each `OnChange` callback to `func(ctx context.Context, ch Change)`; namespace, key, revision and value all come off `ch`.
4. Audit every validator for Go-type assertions — a whole number arrives as `float64` — and for a dependency on request scope.
5. Stop reading a non-nil `Set`/`Delete` error as "not persisted".
6. Drop `WithRedaction` from the four `RedactFull` keys.

Moving matcher's glue — the code that decodes a namespace of scalar keys into a
struct, validates it and re-applies it on change — onto `Bind`, `Group[T]` and
`Group.OnApply` is optional and not part of the v4 hop. `Bind` stores the whole
document under one new key, so it changes the storage shape: without a data
migration from the scalar rows into the group row, every operator override
reverts to its default. Keep the scalar keys, or migrate the rows first.

### billing-worker

**From:** v2.0.0 — `/v2`, lib-commons `/v6`, lib-observability `/v2`.
**Mode:** multi-tenant flag, per-request dispatch, Postgres.
**Breaks:** the three module paths; the `DefaultSeedSQL()` DDL generator, which no longer exists; the fixed Postgres names; the canonical JSON shape and the new `Set`/`Delete` errors.
**Do:**

1. Delete the seed-DDL generator. Defaults belong at `Register` / `Bind` in code; a row an operator must be able to override before first boot is one your own migration pipeline inserts.
2. Rename the table to `systemplane_entries` ([§ The surface diff](#the-surface-diff)), then drop the table and channel overrides. `MigrationV3ToV4SQL()` refuses when it finds no `systemplane_entries`, so skipping the rename fails loudly, not silently. The table is `systemplane_entries` and the channel is `systemplane_changes`, both fixed; a name collision is now a reason for the install to have its own database, not a reason to rename an object.
3. Apply `MigrationV3ToV4SQL()` per tenant database, then bump the three module paths.
4. Audit the validators for the canonical shape, and the `Set`/`Delete` call sites for the new errors.

`OnChange` still returns `ErrNotSupportedInMultiTenant` on this shape — a
documented refusal, not a regression: no scope is tracked and no changefeed
runs, so no callback could fire.

### finance-hub

**From:** v1.6.0 — unsuffixed module, Fiber v2, lib-commons v5, lib-observability v1. The hardest starting point in the matrix.
**Mode:** single-tenant, Postgres.
**Breaks:** everything the `/v2` consumers above are hit by, plus the v1.6.x preconditions.
**Do, in this order:**

1. **Fiber v2 → v3 first, as its own change.** It is not part of this upgrade: it retypes `admin.WithAuthorizer` from `func(*fiber.Ctx, string) error` to `func(fiber.Ctx, string) error` and touches your whole HTTP layer.
2. Take the observability boundary — lib-observability v1 → `/v4`, lib-commons v5 → `/v7` — as described in [MIGRATION-v3.md](MIGRATION-v3.md).
3. Delete the `DefaultSeedSQL()` generator; see billing-worker above for what replaces it.
4. Then the single-tenant steps: `MigrationV3ToV4SQL()`, the module path, the callback signature, the validator audit, the `Set`/`Delete` errors.

### br-consignado-gw

**From:** v2.0.0 — `/v2`, lib-commons `/v6`, lib-observability `/v2`.
**Mode:** single-tenant, Postgres.
**Breaks:** the three module paths, and nothing this library asks you to redesign.
**Do:** the cheapest path in the matrix — apply `MigrationV3ToV4SQL()`, bump the three module paths, rewrite the `OnChange` callbacks if you have any. Still audit every validator against
[§ Validators and defaults see the canonical JSON shape](#validators-and-defaults-see-the-canonical-json-shape): a validator that asserts `int` now refuses its own registered default, at `Register`, on boot.

### go-boilerplate-ddd

Same as br-consignado-gw.

### product-console

**From:** new adopter — no version to leave; take `/v4` directly.
**Mode:** multi-tenant, MongoDB, through its Go service. The first MongoDB consumer.
**Breaks:** nothing — there is no earlier version to leave.
**Do:**

1. Mount `admin.MountCatalog` before the tenant-manager middleware, `admin.Mount` after it, so value reads and writes receive the resolved tenant database and catalog metadata does not need one.
2. Render `revision`, `updatedAt` (JSON null when no row backs the value) and `updatedBy` from every admin GET and list response as the Console's provenance fields. `stale` is always false on this shape.
3. Filter `deleted: {$ne: true}` in **every** direct read of `systemplane_entries`. A delete writes a tombstone rather than removing the document; see [§ MongoDB](#mongodb).

The Console's tenant shape is the per-request one: the database resolved from
the request context on every read and write, no in-process cache, no
changefeed, and `OnChange` refused with `ErrNotSupportedInMultiTenant`. To cache
each tenant in process and subscribe per tenant, construct with
`WithMongoTenantManager(mbMgr)`, passing the `*tmmongo.Manager` the
tenant-manager middleware registers under the `WithModule` name. Each tenant
then needs a database of its own and a replica set, or `WithPollInterval`
([§ MongoDB](#mongodb)); `stale` reports on reads a tenant's scope serves; and
the lifecycle wiring is the one [§ notifications](#notifications) shows.

### notifications

**From:** v1.6.1 — unsuffixed module, Fiber v2, lib-commons v5, lib-observability v1, and a `Manager`.
**Mode:** multi-tenant, Postgres.
**Breaks:** the v1.6.x row of [§ The module and dependency hop](#the-module-and-dependency-hop), and the `Manager` on top of it: `NewManager`, `WithManagerLogger`, `WithManagerTelemetry`, the four `OnTenant*` handlers and `Drain` all stop existing in the same change. The two cannot be split — one import line cannot be on v1.6.x and on `/v4` at once — which makes this the largest single upgrade in the matrix. Budget it as two.
**Do:**

1. Fiber v2 → v3 first, as its own change, then the observability boundary. Both are in the hop table above.
2. Apply `MigrationV3ToV4SQL()` to every tenant database before the new binary boots.
3. Delete the `Manager` and move what it configured onto one `Client`: `WithPostgresTenantManager(pgMgr)`, which implies `WithMultiTenantEnabled()`, plus `WithModule`, `WithLogger` and `WithTelemetry`. `pgMgr` must be the `*tmpostgres.Manager` the tenant-manager middleware registers under the `WithModule` name: writes and uncached reads use the database the middleware resolved, cached reads use `pgMgr`'s.
4. Replace the four `OnTenant*` handlers with `c.HandleTenantLifecycle`, chained after the dispatcher's own `HandleEvent`. It returns two errors the handlers never did, `ErrClosed` after `Close` and `ErrValidation` for an event with no `TenantID`, and no longer the one they did: a tenant database that cannot be reached fails that tenant's activation in the background with a WARN, and a later read retries.
5. Replace `Drain(ctx)` with `Close()`, which takes no context — see [§ `Close` is bounded, and names the callback that would not stop](#close-is-bounded-and-names-the-callback-that-would-not-stop).

| v1.6.1 handler | v4 event through `HandleTenantLifecycle` |
|---|---|
| `OnTenantActivated` warmed the tenant | `tenant.activated` clears the tenant's blocked marker and opens nothing: the tenant's next read brings its scope up. |
| `OnTenantSuspended`, `OnTenantDeleted` | `tenant.suspended` and `tenant.deleted` drop the scope and block the tenant: its reads go to the tenant database per request, and none brings the scope back until the next `tenant.activated`. |
| `OnTenantCredentialsRotated` | `tenant.credentials.rotated` rebuilds an active tenant's scope on a fresh feed and leaves a blocked tenant blocked. |

~~~go
// v1.6.1 — construction, lifecycle registration, shutdown
c, err := systemplane.NewPostgres(db, listenDSN, systemplane.WithMultiTenantEnabled())
m := systemplane.NewManager(c, pgMgr, systemplane.WithManagerLogger(lg))
// m.OnTenantActivated / OnTenantSuspended / OnTenantDeleted /
// OnTenantCredentialsRotated wired into the lifecycle dispatcher
defer m.Drain(ctx)
~~~

~~~go
// v4 — construction, lifecycle registration, shutdown
c, err := systemplane.NewPostgres(nil, "",
	systemplane.WithPostgresTenantManager(pgMgr), systemplane.WithLogger(lg))
if err != nil {
	return err
}
// dispatcher.HandleEvent first: on a rotation it reloads the pools the rebuild resolves
lifecycle := func(ctx context.Context, evt tmevent.TenantLifecycleEvent) error {
	return errors.Join(dispatcher.HandleEvent(ctx, evt), c.HandleTenantLifecycle(ctx, evt))
}
// lifecycle is the tmevent.EventHandler the tenant event listener is built with
return c.Close() // at shutdown, in place of m.Drain(ctx)
~~~

### plugin-br-pix-jd

**From:** v3.0.0 — `/v3`, already on lib-commons `/v7` and lib-observability `/v4`.
**Mode:** multi-tenant, Postgres.
**Breaks:** the `Manager` — `NewManager`, `Drain`, and `HandleTenantLifecycle` as a method on it; the `DefaultSeedSQL()` DDL generator; the channel override.
**Do:**

1. Bump the module path. That is the whole dependency hop for this consumer — the `/v3` row of the hop table, no lib-commons and no lib-observability move.
2. Delete the seed-DDL generator and the channel override; the name-override row of [§ The surface diff](#the-surface-diff) says what each needs.
3. Apply `MigrationV3ToV4SQL()` to every tenant database before the new binary boots.
4. Delete the `Manager` and build the Client with `WithPostgresTenantManager(pgMgr)`, passing the `*tmpostgres.Manager` the tenant-manager middleware registers under the `WithModule` name.
5. Register `c.HandleTenantLifecycle` where `m.HandleTenantLifecycle` was: same signature, chained after the dispatcher's own `HandleEvent`. What each event now does is the table in [§ notifications](#notifications). `tenant.activated` no longer warms the tenant, and the handler returns `ErrClosed` after `Close` and `ErrValidation` for an event with no `TenantID`, where v3 returned nil for every event.
6. Replace `Drain(ctx)` with `Close()` — the same contrast notifications carries above.

~~~go
// v3.0.0 — construction, lifecycle registration, shutdown
c, err := systemplane.NewPostgres(db, listenDSN, systemplane.WithMultiTenantEnabled()) // plus a WithListenChannel call (set to the default name)
m := systemplane.NewManager(c, pgMgr)
// m.HandleTenantLifecycle registered as the tmevent handler
defer m.Drain(ctx)
~~~

~~~go
// v4 — construction, lifecycle registration, shutdown
c, err := systemplane.NewPostgres(nil, "", systemplane.WithPostgresTenantManager(pgMgr))
if err != nil {
	return err
}
// registered as the tmevent handler, where m.HandleTenantLifecycle was
lifecycle := func(ctx context.Context, evt tmevent.TenantLifecycleEvent) error {
	return errors.Join(dispatcher.HandleEvent(ctx, evt), c.HandleTenantLifecycle(ctx, evt))
}
return c.Close() // at shutdown, in place of m.Drain(ctx)
~~~

### br-sfn

**From:** v3.0.0-beta.2 — `/v3`, already on lib-commons `/v7` and lib-observability `/v4`.
**Mode:** multi-tenant, Postgres, with 17 `OnChange` callbacks registered before `Start`.
**Breaks:** the `Manager`, and when the 17 callbacks fire. v3 dispatched them through the bound `Manager`, once per NOTIFY across any active tenant, on the LISTEN goroutine's context and never with the tenant on it. On v4 they fire only on a Client built with `WithPostgresTenantManager`: without it multi-tenant `OnChange` returns `ErrNotSupportedInMultiTenant` for every registered key, so the 17 hot-reloads either fail at boot, where the error is checked, or silently stop, where it is discarded.
**Do:**

1. Bump the module path; nothing else in `go.mod`.
2. Apply `MigrationV3ToV4SQL()` to every tenant database.
3. Delete the `Manager` and build the Client with `WithPostgresTenantManager(pgMgr)`, passing the `*tmpostgres.Manager` the tenant-manager middleware registers under the `WithModule` name; `Close()` replaces `Drain(ctx)`.
4. Rewrite the 17 callbacks to `func(ctx context.Context, ch Change)` and key each reload by `ch.Tenant`: a delivery carries one tenant's value. Namespace, key, revision and value come off `ch`; the ctx is the engine's own lifecycle context and carries no request values and no tenant.
5. Make each callback idempotent per tenant. `Start` announces nothing on this Client; each tenant instead delivers every registered key once when its first read activates its scope, and again when that scope is rebuilt: 17 deliveries per tenant activation that v3 never made. A tenant this process never reads delivers nothing. Deliveries coalesce per (tenant, key), as [§ Deliveries are coalesced per key and independent across keys](#deliveries-are-coalesced-per-key-and-independent-across-keys) describes.
6. Register `c.HandleTenantLifecycle` as [§ notifications](#notifications) shows. Without it a suspended or deleted tenant keeps its scope and its LISTEN connection until `Close`.

~~~go
// v3.0.0-beta.2 — construction, subscriptions, shutdown
c, err := systemplane.NewPostgres(db, listenDSN, systemplane.WithMultiTenantEnabled())
m := systemplane.NewManager(c, pgMgr)
// 17x c.OnChange(ns, key, func(ctx context.Context, ns, key string, v any) { ... })
// registered before c.Start(ctx) and dispatched through m, once per NOTIFY
defer m.Drain(ctx)
~~~

~~~go
// v4 — construction, the 17 subscriptions before c.Start(ctx), lifecycle, shutdown
c, err := systemplane.NewPostgres(nil, "", systemplane.WithPostgresTenantManager(pgMgr))
if err != nil {
	return err
}
if _, err := c.OnChange(ns, key, func(ctx context.Context, ch systemplane.Change) {
	reload(ch.Tenant, ch.Value) // once per tenant activation, then that tenant's changes
}); err != nil {
	return err
}
lifecycle := func(ctx context.Context, evt tmevent.TenantLifecycleEvent) error {
	return errors.Join(dispatcher.HandleEvent(ctx, evt), c.HandleTenantLifecycle(ctx, evt))
}
return c.Close() // at shutdown, in place of m.Drain(ctx)
~~~
