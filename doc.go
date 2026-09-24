// Copyright 2025 Lerian Studio.

// Package systemplane provides a dual-backend runtime configuration library.
//
// It allows services to hot-reload a small set of operational knobs (log
// levels, feature flags, rate limits, circuit-breaker thresholds) without a pod
// restart. The values live in Postgres or MongoDB and change while the service
// runs.
//
// Construct a Client with [NewPostgres] or [NewMongoDB]; declare each key with
// [Client.Register], or bind a typed document with [Bind]; call [Client.Start];
// read with the nil-receiver-safe typed accessors ([Client.GetString],
// [Client.GetInt], etc.) or with [Group.Snapshot]; react to live updates with
// [Client.OnChange] or [Group.OnApply]; release the backend with [Client.Close].
//
// In single-tenant mode every value is decoded and graded by the key's
// registered validator before it enters the cache — on the write path for a
// local write, at the changefeed and reconcile ingress for every other row — so
// a row written by an older binary, or straight into the table, cannot put a
// value in force that the write path would refuse. The scope reconciles against
// the store after every changefeed reconnect, so a value written while the feed
// was down becomes visible without a second write. [Client.GetEntry] reports
// the revision, provenance and staleness behind the value it returns. In
// multi-tenant mode there is no cache: every read resolves the tenant database
// from ctx and reads through.
//
// Settings that imply resource teardown (DB DSNs, secrets, TLS material, listen
// addresses) belong in environment variables, not in the runtime config plane.
package systemplane
