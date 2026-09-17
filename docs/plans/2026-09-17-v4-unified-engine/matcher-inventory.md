<!-- Read-only inventory of the matcher service (commit 653c237b, 2026-09-09) produced 2026-09-17 by an explorer agent as input for lane matcher-pilot. file:symbol references, no line numbers. -->

# matcher — lib-systemplane & Environment Inventory

Repo: `/Users/fredamaral/repos/lerianstudio/matcher` (read-only). Dependency today: `github.com/LerianStudio/lib-systemplane/v2 v2.0.0` (vendored; comments and `cmd/systemplane-ddl:libVersion` still say "v1.6.0"/"v5", a stale naming layer).

**Measured counts** (from grep/wc, not estimates):

| Quantity | Count | How measured |
|---|---|---|
| `matcherKeyDef{...}` entries | 71 | `grep -c "matcherKeyDef{" internal/bootstrap/systemplane_keys_defs.go` |
| `client.Register(...)` call sites | 2 (one loop over 71 defs + 1 explicit) → **72 keys registered per boot** | `grep -rn "client.Register("` |
| Keys watched for `OnChange` | 71 | `buildWatchedSystemplaneKeys` entry count |
| Keys with a validator | 22 | `grep -o "validator: [a-zA-Z]*"` → 16 `validatePositiveInt`, 1 each of `corsProductionValidator`, `validateBodyLimitBytes`, `validateMaxUploadBytes`, `validateMaxExtractionBytes`, 2 `enumKey`-derived |
| Keys with `RedactFull` | 4 | `grep -c "redact: systemplane.RedactFull"` |
| Keys with catalog metadata | 2 | `enumKey` call sites |
| Production `env:` struct tags | 206 (196 in `config.go`, 10 in `rest_connectors_config.go`) | `grep -o 'env:"[^"]*"' \| sort -u \| wc -l` |
| Entries in `configEnvVarKeys` | 208 (= 206 tags + 2 hand-parsed) | list length |
| `MATCHER_*` override names declared | 71 | `matcherOverrideEnvVarKeys` length |
| Active / commented vars in `config/.config-map.example` | 32 / 86 | `grep -E '^[A-Z]...=' ` and `grep -E '^# ?[A-Z]...='` |
| `STREAMING_*` names owned by lib-streaming | 38 | `vendor/.../lib-streaming/v3/.env.reference` |
| Helm chart in repo | **0** | no `Chart.yaml` / `values.yaml` outside `ui/node_modules` |

---

## 1. Systemplane keys registered today

All keys live in one namespace: `matcher` (`internal/bootstrap/systemplane_keys.go:systemplaneNamespace`). All 71 defs are built by `internal/bootstrap/systemplane_keys_defs.go:matcherKeyDefs`, registered by `internal/bootstrap/systemplane_keys.go:RegisterMatcherKeys`, and mirrored back into `*Config` by `internal/bootstrap/config_manager.go:applySystemplaneOverrides` (driven per-key by `internal/bootstrap/config_manager.go:ConfigManager.WatchSystemplane`).

Because of that mirror, **every** key has one "read" that is the mirror itself. The column below names the *consumer* read — the one that decides hot-read vs apply-hook. Defaults are taken from `cfg` (the env-resolved snapshot); the compile-time value shown is the `envDefault` tag / `internal/bootstrap/config_defaults.go:defaultConfig` value.

Legend for the last column: **HOT** = re-resolved on every request/call; **APPLY** = captured in a live resource, refreshed only by a reconcile hook; **INERT** = registered but nothing re-reads it after boot (footgun).

### Server / HTTP

| Key | Go type | Default | Validator | Description | Redact | Consumer read (`file:symbol`) | Mode |
|---|---|---|---|---|---|---|---|
| `server.body_limit_bytes` | `int` | `104857600` | `validateBodyLimitBytes` — positive and ≤ 128 MiB | Max buffered request body | none | `internal/bootstrap/fiber_middleware_runtime.go:runtimeBodyLimitMiddleware` → `:effectiveRuntimeBodyLimit` | HOT (per request) |
| `cors.allowed_origins` | `string` | `http://localhost:3000` | `corsProductionValidator(envName)` — rejects exact `*` when `ENV_NAME` is production; nil validator otherwise | CSV allowed origins | none | `internal/bootstrap/fiber_middleware_runtime.go:runtimeCORSMiddleware` | HOT |
| `cors.allowed_methods` | `string` | `GET,POST,PUT,PATCH,DELETE,OPTIONS` | none | CSV allowed methods | none | same as above | HOT |
| `cors.allowed_headers` | `string` | `Origin,Content-Type,Accept,Authorization,X-Request-ID` | none | CSV allowed headers | none | same as above | HOT |

### Tenancy (manager-shaping)

All nine feed `internal/bootstrap/dynamic_infrastructure_multi_tenant.go:dynamicMultiTenantKey`; a changed key makes `internal/bootstrap/dynamic_infrastructure_provider.go` rebuild via `:buildCanonicalTenantManager` and close the old manager.

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `tenancy.multi_tenant_url` | `string` | `""` | none | Tenant-manager service URL | none | `dynamic_infrastructure_multi_tenant.go:buildCanonicalTenantManager` | APPLY |
| `tenancy.multi_tenant_max_tenant_pools` | `int` | `100` | none | Max tenant pools | none | `:tenantManagerPostgresOptions` | APPLY |
| `tenancy.multi_tenant_idle_timeout_sec` | `int` | `300` | none | Pool idle timeout | none | `:tenantManagerPostgresOptions` | APPLY |
| `tenancy.multi_tenant_timeout` | `int` | `30` | none | Tenant-manager API timeout | none | `config_env.go:Config.MultiTenantTimeoutDuration` → `:buildTenantManagerClientOptions` | APPLY |
| `tenancy.multi_tenant_circuit_breaker_threshold` | `int` | `5` | none | CB failure threshold | none | `:buildTenantManagerClientOptions` | APPLY |
| `tenancy.multi_tenant_circuit_breaker_timeout_sec` | `int` | `30` | none | CB open window | none | `:buildTenantManagerClientOptions` | APPLY |
| `tenancy.multi_tenant_service_api_key` | `string` | `""` | none | Tenant-manager service key | **RedactFull** | `:buildTenantManagerClientOptions` (fingerprinted into the rebuild key) | APPLY |
| `tenancy.multi_tenant_cache_ttl_sec` | `int` | `120` | none | Tenant-config cache TTL | none | `config_env.go:Config.MultiTenantCacheTTL` | APPLY |
| `tenancy.multi_tenant_connections_check_interval_sec` | `int` | `30` | none | Pool settings revalidation interval | none | `config_env.go:Config.MultiTenantConnectionsCheckInterval` | APPLY |

### Postgres / Infrastructure

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `postgres.query_timeout_sec` | `int` | `30` | none | Per-query timeout | none | `internal/bootstrap/fiber_middleware_runtime.go:dbQueryTimeoutMiddleware` → `:currentQueryTimeout` (`config_env.go:Config.QueryTimeout`) | HOT |
| `infrastructure.health_check_timeout_sec` | `int` | `5` | none | Legacy per-check probe timeout | none | `internal/bootstrap/health_huma.go:healthHumaHandler.readiness` via `config.go:InfrastructureConfig.HealthCheckTimeout` | HOT (250 ms result cache) |
| `infrastructure.health_check_timeout_ms` | `int` | `800` | none | Per-check probe timeout (preferred) | none | same as above | HOT |

### Telemetry / Swagger — registered but nothing re-reads them

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `telemetry.deployment_env` | `string` | `development` | `enumKey("development","staging","production")` + catalog `Kind:"enum"` | OTel deployment environment | none | `internal/bootstrap/observability.go:InitTelemetry` — boot only | **INERT** |
| `swagger.enabled` | `bool` | `false` | none | Enable Swagger UI | none | `internal/bootstrap/routes.go` (spec mount decided once at route registration) | **INERT** |
| `swagger.host` | `string` | `""` | none | Swagger host override | none | `internal/bootstrap/init_aggregator_webhook.go` (boot) | **INERT** |
| `swagger.schemes` | `string` | `https` | none | Swagger URL schemes | none | `internal/bootstrap/init_aggregator_webhook.go:parseSchemes` (boot) | **INERT** |

### Rate limit (all HOT)

Read per request through `internal/bootstrap/rate_limiter.go:settingsBackedRateLimitHandler` → `:currentRateLimitConfig` → `internal/bootstrap/runtime_settings.go:runtimeSettingsResolver.rateLimit`, which re-reads all eleven keys on each call.

| Key | Go type | Default | Validator | Description | Redact |
|---|---|---|---|---|---|
| `rate_limit.enabled` | `bool` | `true` | none | Enable rate limiting | none |
| `rate_limit.max` | `int` | `100` | `validatePositiveInt` | Global tier max | none |
| `rate_limit.expiry_sec` | `int` | `60` | `validatePositiveInt` | Global tier window | none |
| `rate_limit.export_max` | `int` | `10` | none | Export tier max | none |
| `rate_limit.export_expiry_sec` | `int` | `60` | none | Export tier window | none |
| `rate_limit.dispatch_max` | `int` | `50` | none | Dispatch tier max | none |
| `rate_limit.dispatch_expiry_sec` | `int` | `60` | none | Dispatch tier window | none |
| `rate_limit.admin_max` | `int` | `30` | `validatePositiveInt` | `/system` tier max | none |
| `rate_limit.admin_expiry_sec` | `int` | `60` | `validatePositiveInt` | `/system` tier window | none |
| `rate_limit.signup_max` | `int` | `5` | `validatePositiveInt` | `/v1/signup` per-IP max | none |
| `rate_limit.signup_expiry_sec` | `int` | `3600` | `validatePositiveInt` | Signup window | none |

The admin tier additionally re-reads through `internal/bootstrap/systemplane_mount.go:MountSystemplaneAPI` → `internal/bootstrap/rate_limiter.go:NewAdminRateLimit`.

### Idempotency / dedupe / callbacks / exception / webhook (all HOT)

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `idempotency.retry_window_sec` | `int` | `300` | none | Failed-key cooldown | none | closures in `init_storage.go` + `init_exception.go` → `init_runtime_settings.go:resolveIdempotencyRetryWindow` → `runtime_settings.go:runtimeSettingsResolver.idempotencyRetryWindow` | HOT |
| `idempotency.success_ttl_hours` | `int` | `168` | none | Cached-response TTL | none | `init_runtime_settings.go:resolveIdempotencySuccessTTL` | HOT |
| `idempotency.hmac_secret` | `string` | `""` | none | Key-signing secret | **RedactFull** | `init_runtime_settings.go:resolveIdempotencyHMACSecret` | HOT |
| `deduplication.ttl_sec` | `int` | `3600` | none | Dedupe key TTL | none | `init_ingestion.go` closure → `init_runtime_settings.go:resolveDedupeTTL` | HOT |
| `callback_rate_limit.per_minute` | `int` | `60` | none | Per-external-system callback budget | none | `init_exception.go` closure → `init_runtime_settings.go:resolveCallbackRateLimit` | HOT |
| `exception.dispute_lock_timeout_ms` | `int` | `2000` | none | Dispute row-lock wait before retryable 409 | none | `init_exception.go` closure → `init_runtime_settings.go:resolveDisputeLockTimeoutMS`; consumed by `internal/exception/adapters/postgres/exception/exception.postgresql.go` as the `lock_timeout` GUC | HOT (per request) |
| `webhook.timeout_sec` | `int` | `30` | none | Webhook HTTP timeout (clamped to `maxWebhookTimeoutSec` = 300) | none | `init_exception.go` closure → `init_runtime_settings.go:resolveWebhookTimeout` | HOT |

### Ingestion / Fetcher engine (all HOT)

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `ingestion.max_upload_bytes` | `int64` | `1073741824` (1 GiB) | `validateMaxUploadBytes` — `[1 MiB, 8 GiB]` | Streaming-upload ceiling, counted against the **whole** multipart body | none | `init_ingestion.go` → `ingestionHTTP.WithMaxUploadBytesGetter` → `internal/ingestion/adapters/http/handlers.go:Handlers.uploadLimitBytes` | HOT (once per upload; in-flight uploads keep their limit) |
| `fetcher.discovery_interval_sec` | `int` | `60` | none | Discovery refresh interval | none | `init_discovery.go` lock-TTL getter → `config_env.go:Config.FetcherDiscoveryInterval` | HOT |
| `fetcher.schema_cache_ttl_sec` | `int` | `300` | none | Schema cache TTL | none | `init_discovery.go` ttl resolver → `config_env.go:Config.FetcherSchemaCacheTTL`; consumed by `internal/discovery/schemacache/cache.go` | HOT |
| `fetcher.extraction_timeout_sec` | `int` | `600` | none | Engine extraction timeout | none | `init_discovery.go` timeout getter → `config_env.go:Config.FetcherExtractionTimeout` | HOT |
| `fetcher.max_extraction_bytes` | `int64` | `2147483648` (2 GiB) | `validateMaxExtractionBytes` — `[1 MiB, 16 GiB]` | Engine extraction payload cap | none | `init_discovery.go` → `WithMaxExtractionBytesGetter` → `internal/discovery/services/command/engine_ingestion_handoff.go:EngineIngestionHandoffImpl` | HOT (per `Handle`) |

### Object storage (mixed)

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `object_storage.endpoint` | `string` | `http://localhost:8333` | none | S3 endpoint | none | `internal/bootstrap/init_storage.go:newRuntimeReportingStorageClient` (dynamic delegate, per call) **and** `worker_manager_runtime.go:extractWorkerConfig`/`:applyArchivalRuntimeConfig` (archival S3 rebuild) | HOT + APPLY |
| `object_storage.region` | `string` | `us-east-1` | none | S3 region | none | same | HOT + APPLY |
| `object_storage.bucket` | `string` | `matcher-exports` | none | Export bucket | none | same | HOT + APPLY |
| `object_storage.access_key_id` | `string` | `""` | none | S3 access key | **RedactFull** | same | HOT + APPLY |
| `object_storage.secret_access_key` | `string` | `""` | none | S3 secret | **RedactFull** | same | HOT + APPLY |
| `object_storage.use_path_style` | `bool` | `true` | none | Path-style addressing | none | same | HOT + APPLY |
| `object_storage.allow_insecure_endpoint` | `bool` | `false` | none | Permit plaintext endpoint | none | same; forced `false` in production by `config_loading.go:Config.enforceProductionSecurityDefaults` at boot only | HOT + APPLY |

### Workers (all APPLY, via `ConfigManager.OnReload` → `WorkerManager` reconcile)

Reconcile path: `config_manager.go:ConfigManager.notifyReloadSubscribers` → `worker_manager.go` → `worker_manager_runtime.go:reconcileSlotLocked` → `:workerConfigChanged`/`:extractWorkerConfig` → `:applyWorkerRuntimeConfig`.

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `export_worker.enabled` | `bool` | `true` | none | Enable export worker | none | `worker_manager_runtime.go:reconcileSlotLocked` (start/stop) | APPLY |
| `export_worker.poll_interval_sec` | `int` | `5` | `validatePositiveInt` | Poll interval | none | `worker_manager_runtime.go:applyExportRuntimeConfig`; boot copy `init_reporting.go` | APPLY |
| `export_worker.page_size` | `int` | `1000` | `validatePositiveInt` | Page size | none | `:applyExportRuntimeConfig` | APPLY |
| `export_worker.presign_expiry_sec` | `int` | `3600` | none | Presigned URL expiry (clamped to `maxPresignExpirySec` = 604800) | none | `init_reporting.go` → `init_runtime_settings.go:resolveExportPresignExpiry` | **HOT** |
| `match_run_worker.enabled` | `bool` | `true` | none | Enable async run execution | none | `:reconcileSlotLocked` | APPLY |
| `match_run_worker.poll_interval_sec` | `int` | `3` | `validatePositiveInt` | Poll interval | none | `worker_manager_runtime.go:applyMatchRunRuntimeConfig` | APPLY |
| `cleanup_worker.enabled` | `bool` | `true` | none | Enable cleanup worker | none | `:reconcileSlotLocked` | APPLY |
| `cleanup_worker.interval_sec` | `int` | `3600` | `validatePositiveInt` | Sweep interval | none | `worker_manager_runtime.go:applyCleanupRuntimeConfig` | APPLY |
| `cleanup_worker.grace_period_sec` | `int` | `3600` | `validatePositiveInt` | File-delete grace period | none | `:applyCleanupRuntimeConfig` | APPLY |
| `cleanup_worker.batch_size` | `int` | `100` | `validatePositiveInt` | Batch size | none | `:applyCleanupRuntimeConfig` | APPLY |
| `scheduler.interval_sec` | `int` | `60` | `validatePositiveInt` | Scheduler tick | none | `worker_manager_runtime.go:applySchedulerRuntimeConfig`; boot copy `init_runtime_settings.go:schedulerInterval` | APPLY |

### Archival (APPLY, except presign)

Reconciled by `worker_manager_runtime.go:applyArchivalRuntimeConfig` (the only apply hook that also takes an `InfraConnector`, because it rebuilds the S3 client).

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `archival.enabled` | `bool` | `false` | none | Enable archival worker | none | `:reconcileSlotLocked` | APPLY |
| `archival.interval_hours` | `int` | `24` | `validatePositiveInt` | Sweep interval | none | `:applyArchivalRuntimeConfig`; boot copy `init_status_archival.go` | APPLY |
| `archival.batch_size` | `int` | `5000` | `validatePositiveInt` | Batch size | none | `:applyArchivalRuntimeConfig` | APPLY |
| `archival.partition_lookahead` | `int` | `3` | `validatePositiveInt` | Partitions pre-created | none | `:applyArchivalRuntimeConfig`; `internal/bootstrap/dynamic_partition_manager.go` | APPLY |
| `archival.hot_retention_days` | `int` | `90` | none | Hot tier retention | none | `:applyArchivalRuntimeConfig` | APPLY |
| `archival.warm_retention_months` | `int` | `24` | none | Warm tier retention | none | `:applyArchivalRuntimeConfig` | APPLY |
| `archival.cold_retention_months` | `int` | `84` | none | Cold tier retention | none | `:applyArchivalRuntimeConfig` | APPLY |
| `archival.storage_bucket` | `string` | `matcher-archives` | none | Archive bucket | none | `:applyArchivalRuntimeConfig` | APPLY |
| `archival.storage_class` | `string` | `GLACIER` | `enumKey(STANDARD, STANDARD_IA, ONEZONE_IA, INTELLIGENT_TIERING, GLACIER_IR, GLACIER, DEEP_ARCHIVE)` + catalog `Kind:"enum"` | AWS storage tier | none | `:applyArchivalRuntimeConfig` | APPLY |
| `archival.presign_expiry_sec` | `int` | `3600` | none | Presigned URL expiry (clamped to 604800) | none | `init_status_archival.go` → `runtime_settings.go:runtimeSettingsResolver.archivalPresignExpiry` | **HOT** |

### Per-tenant AI gates (registered outside the defs loop)

| Key | Go type | Default | Validator | Description | Redact | Consumer read | Mode |
|---|---|---|---|---|---|---|---|
| `doc_extraction.tenant_opt_in` | `bool` | `false` | none | Per-tenant opt-in for the AI document-extraction lane (EGRESS gate, fail-closed) | none | `internal/bootstrap/init_document_extraction.go:systemplaneDocExtractionGate.AllowedForTenant`, wired at `init_ingestion.go` | HOT (per request, tenant resolved from ctx) |
| `rule_advisor.tenant_opt_in` | `bool` | *(never registered)* | — | Per-tenant opt-in for the AI rule-suggestion lane | — | `internal/bootstrap/init_rule_suggestion.go:systemplaneRuleSuggestionGate.AllowedForTenant`, wired at `init_matching.go` | HOT — **but see below** |

> **Live defect worth carrying into v4.** `systemplane_keys.go:ruleSuggestionTenantOptInKey` is declared and read, but `RegisterMatcherKeys` only registers `docExtractionTenantOptInKey`. lib-systemplane returns `ErrUnknownKey` for an unregistered key (`vendor/.../lib-systemplane/v2/api_errors.go`), which `systemplane_init.go:systemplaneRead` downgrades to "not found" → the fallback `false`. Net effect: the rule-suggestion lane is permanently denied for every tenant, an operator PUT to that key is rejected, and the only signal is a WARN line. A typed group with one field per lane makes this class of drift structurally impossible.

### Registry hygiene notes

- `systemplane_keys_defaults.go:matcherKeyDefsCapacity = 110` is a stale preallocation hint against 71 actual defs.
- `systemplane_init.go:reclassifiedBootstrapOnlyKeys` carries 8 names (`app.log_level`, `tenancy.default_tenant_id`, `tenancy.default_tenant_slug`, `app.env_name`, `auth.enabled`, `auth.host`, `outbox.retry_window_sec`, `outbox.dispatch_interval_sec`) purely to emit a startup WARN when a restored DB row still holds a value for a key that was demoted to bootstrap-only.

---

## 2. Proposed groups

Eight groups. Grouping criterion used: **one group = one live resource or one enforcement point**, so an apply hook has exactly one target and a cross-field invariant never spans two groups.

### `http` — HOT only
Fields: `body_limit_bytes`, `cors_allowed_origins`, `cors_allowed_methods`, `cors_allowed_headers`, `query_timeout_sec`, `health_check_timeout_ms`, `health_check_timeout_sec`.
No apply hook: every field is resolved inside middleware per request (`fiber_middleware_runtime.go`, `health_huma.go`).
Invariants that justify the grouping: `body_limit_bytes` > 0 and ≤ the Fiber ceiling (128 MiB) — a single-field bound the `http` group owns outright. The disjointness with the streaming upload ceiling is **not** an invariant and needs no cross-group rule: the upload route is exempt from `body_limit_bytes` by construction. `fiber_server.go` sets `StreamRequestBody: true`, under which fasthttp prefetches 8 KiB and does not reject a body larger than Fiber's `BodyLimit` during the read, and `fiber_middleware_runtime.go:runtimeBodyLimitMiddleware` returns `Next()` for that route (`:isStreamingUploadRoute`) before it ever reads the field. `ingestion.max_upload_bytes`, enforced by the handler's counting reader, is the route's only ceiling. State it as a routing fact in the group's doc comment; do not write a validator for it. `health_check_timeout_ms` supersedes `health_check_timeout_sec` (`InfrastructureConfig.HealthCheckTimeout` already encodes the precedence); the two belong in one struct so the fallback rule is a method on the group, not a free function. `query_timeout_sec * 1000` ≥ `health_check_timeout_ms` — stated with the conversion because the two fields carry different units (seconds and milliseconds) and a direct `>=` would compare `30` against `800` and reject the default configuration. Unit rule for the whole document: the pilot's group validator receives the raw `int` fields, not `time.Duration`, so every cross-field time invariant here must spell out its unit conversion.

### `rate_limit` — HOT only
Fields: `enabled`, plus five `(max, expiry_sec)` pairs: global, export, dispatch, admin, signup.
No apply hook — `rate_limiter.go:settingsBackedRateLimitHandler` reads the whole group per request. This is the single strongest argument for typed groups in the repo: eleven separate `SystemplaneGetInt` round trips happen on **every** request today (`runtime_settings.go:runtimeSettingsResolver.rateLimit`), where a group read would be one.
Invariants: every `expiry_sec` > 0 (a zero window makes the limiter a no-op); `admin_max` ≤ `max` (the admin plane must never be able to starve tenant traffic — the stated design intent, unenforced today); `signup_max` ≤ `max`; in production a write of `enabled=false` must be rejected with `ErrValidation`, not silently corrected (today `config_loading.go:Config.enforceProductionSecurityDefaults` forces it back to `true` at boot only, so a runtime PUT of `false` in production is accepted and takes effect — a security regression a group-level validator closes).
**`enabled` is not live in both directions today, and the pilot must make it so.** `rate_limiter.go:NewLibRateLimiter` delegates to `ratelimit.New`, which reads the `RATE_LIMIT_ENABLED` *process env var* itself (`commons.RateLimitEnabled()` in `vendor/.../lib-commons/v6/commons/net/http/ratelimit/middleware.go:New`) and returns `nil` when it is not truthy; a nil limiter is a pass-through. `rate_limiter.go:settingsBackedRateLimitHandler` then reads only the group's `enabled` field per request. So with `RATE_LIMIT_ENABLED=false` at boot there is no limiter to enforce with, and flipping `rate_limit.enabled` to `true` through `/system` changes nothing — the request still passes unlimited, with no error to the operator. The pilot must construct the limiter unconditionally at boot (its existence must not depend on any enabled flag, env or group) and let the group's `enabled` field be the only thing that decides enforcement per request. The production guard narrows that in one direction only: in production the group validator **rejects a write of `enabled=false`** with `ErrValidation`, so `true → false` is refused there and the stored value stays `true`; outside production both transitions are live. Required tests for the pilot lane, three of them: `false → true` live in every environment, `true → false` live outside production, and `true → false` rejected with `ErrValidation` in production with the stored value unchanged.

### `ingestion` — HOT only
Fields: `max_upload_bytes`, `dedupe_ttl_sec`, `idempotency_retry_window_sec`, `idempotency_success_ttl_hours`, `idempotency_hmac_secret` (redacted).
No apply hook — all resolved per request through `init_ingestion.go` / `init_storage.go` / `init_exception.go` closures.
Invariants: `retry_window_sec` < `success_ttl_hours × 3600` (a retry window longer than the success TTL means a failed key can never be retried against a still-cached success); `dedupe_ttl_sec` ≥ `retry_window_sec`; `max_upload_bytes` ∈ [1 MiB, 8 GiB] and > `http.body_limit_bytes` is the *expected* relation, not an error — worth stating so nobody "fixes" it.

### `matching` — APPLY (match-run worker) + one bootstrap escapee
Fields: `match_run_worker_enabled`, `match_run_worker_poll_interval_sec`, `scheduler_interval_sec`, `dispute_lock_timeout_ms`, and — the open question — `candidate_batch_size`.
Apply hook: yes, `worker_manager_runtime.go:applyMatchRunRuntimeConfig` + `:applySchedulerRuntimeConfig`.
`dispute_lock_timeout_ms` is a HOT read inside the group (per-request GUC), so the group mixes both modes. That is fine and is exactly why v4's group struct should mark hookability per group, with the hook receiving the whole group and ignoring the fields it does not own.
`candidate_batch_size` is currently **bootstrap-only on purpose** (`MATCHING_CANDIDATE_BATCH_SIZE`, snapshotted once via `WithCandidateBatchSize`, clamped to `maxMatchingCandidateBatchSize` = 100000 by `config_env.go:Config.CandidateBatchSize`) because the matching use case has no reload seam. It belongs in this group the moment a seam exists.
Invariants: `poll_interval_sec` > 0; `dispute_lock_timeout_ms` > 0 (zero means "wait forever" to PostgreSQL — the exact failure `runtime_settings.go:runtimeSettingsResolver.disputeLockTimeoutMS` guards against by falling back rather than passing through); `candidate_batch_size` ≤ 100000.

### `reporting` — APPLY (export + cleanup workers) + object storage
Fields: `export_enabled`, `export_poll_interval_sec`, `export_page_size`, `export_presign_expiry_sec`, `cleanup_enabled`, `cleanup_interval_sec`, `cleanup_grace_period_sec`, `cleanup_batch_size`.
Apply hook: yes — `worker_manager_runtime.go:applyExportRuntimeConfig` and `:applyCleanupRuntimeConfig`.
Invariants: `cleanup_grace_period_sec` ≥ `export_presign_expiry_sec` — otherwise the cleanup worker can delete a file while a presigned URL for it is still valid, which is a live correctness bug that no single-key validator can catch. This invariant alone justifies the group. Also `export_presign_expiry_sec` ≤ 604800 (S3 hard ceiling; clamped today in `runtime_settings.go:runtimeSettingsResolver.exportPresignExpiry`).

### `archival` — APPLY (archival worker + its S3 client)
Fields: `enabled`, `interval_hours`, `batch_size`, `partition_lookahead`, `hot_retention_days`, `warm_retention_months`, `cold_retention_months`, `storage_bucket`, `storage_class` (enum), `presign_expiry_sec`.
Apply hook: yes, and the heaviest one — `worker_manager_runtime.go:applyArchivalRuntimeConfig` takes an `InfraConnector` because a change rebuilds the S3 client.
Invariants (the clearest case in the repo): `hot_retention_days` ≤ `warm_retention_months * 30` ≤ `cold_retention_months * 30` — multiplied rather than dividing the days, because integer division on the raw `int` truncates (`89 / 30 = 2`) and would accept a hot tier that outlives the warm tier. Today all three are separate `int` keys with no validator at all, so an operator can PUT `warm=1, cold=0` and silently create a tier gap that destroys data. `partition_lookahead` × `interval_hours` must cover at least one sweep cycle or the worker archives into a partition that does not exist yet. `presign_expiry_sec` ≤ 604800.

### `object_storage` — APPLY (shared S3 resource) + HOT delegate
Fields: `endpoint`, `region`, `bucket`, `access_key_id` (redacted), `secret_access_key` (redacted), `use_path_style`, `allow_insecure_endpoint`.
Apply hook: yes. This group is consumed by **two** resources — `init_storage.go:newRuntimeReportingStorageClient` (a dynamic delegate that resolves per call, so effectively HOT) and the archival worker's S3 client (rebuilt by `applyArchivalRuntimeConfig`). It must be its own group rather than folded into `archival` or `reporting` precisely because two owners share it; today the sharing is expressed by copying six fields into `worker_manager_runtime.go:archivalWorkerComparableConfig`.
Invariants: `allow_insecure_endpoint` must be false whenever `endpoint` is not `https://` in a production environment (boot-time-only today); `bucket` non-empty when either worker is enabled (`init_storage.go:reportingStorageRequired`); `archival.storage_bucket` ≠ `object_storage.bucket` is a *cross-group* invariant that argues for keeping both under one parent validator.

### `tenancy` — APPLY (tenant-manager rebuild)
Fields: the nine `multi_tenant_*` keys.
Apply hook: yes — this group already *is* a hand-rolled apply hook. `dynamic_infrastructure_multi_tenant.go:dynamicMultiTenantKey` serialises nine fields plus a SHA-256 fingerprint of the API key into a string, compares it, and rebuilds via `:buildCanonicalTenantManager`. A typed group with struct equality plus a `Rebuild` hook deletes that entire fingerprinting apparatus.
Invariants: `circuit_breaker_timeout_sec` ≥ `multi_tenant_timeout` (both seconds; a breaker that reopens faster than a single call can complete never closes); `cache_ttl_sec` ≥ `connections_check_interval_sec` (both seconds). The connection-budget rule is **not** in this list — see "Startup-only invariants" below. Note the group deliberately *excludes* `multi_tenant_max_open_conns_per_tenant`, `multi_tenant_max_idle_conns_per_tenant` and `multi_tenant_allow_insecure_http`, which are bootstrap-only; that exclusion is load-bearing and documented at `dynamic_infrastructure_multi_tenant.go:dynamicMultiTenantKey`.

### Groups requested in the brief that should **not** exist as typed groups

- **`document_extraction`** and **`rule_suggestion`**: each is a single per-tenant boolean read fail-closed per request. The rest of each lane (`Enabled`, `APIKey`, `Model`) is genuinely bootstrap — the Anthropic client is built once. A one-field group is a struct with no invariant. Better shape: one `ai_gates` group with two booleans (`doc_extraction_tenant_opt_in`, `rule_advisor_tenant_opt_in`), which also makes the unregistered-key defect above impossible to reintroduce.
- **A `telemetry`/`swagger` group**: both are INERT today. They should be *deregistered*, not regrouped, unless someone commits to an apply hook that re-creates the OTel exporter and re-mounts the spec route. Registering a knob with no reload path is the exact footgun `systemplane_keys_defs.go:matcherKeyDefs` spends 40 lines of comments warning against, and these four keys violate it.

### Startup-only invariants (env, not systemplane)

One invariant does not belong to any group, because no group validator can see both of its operands.

- **Tenant connection budget**: `MULTI_TENANT_MAX_TENANT_POOLS` × `MULTI_TENANT_MAX_OPEN_CONNS_PER_TENANT` ≤ `POSTGRES_MAX_OPEN_CONNS`. The comment in `dynamic_infrastructure_multi_tenant.go:tenantManagerPostgresOptions` spells this out and nothing enforces it. It cannot become a `tenancy` group invariant: a group validator receives only the decoded `tenancy` value, and that value excludes `MULTI_TENANT_MAX_OPEN_CONNS_PER_TENANT` (bootstrap-only, deliberately outside the rebuild key) while `POSTGRES_MAX_OPEN_CONNS` belongs to no group at all. Enforce it **once at boot**, in the config loader, where all three env values are in hand — `config_loading.go:Config.Validate` is the natural home. Changing any of the three requires a restart anyway, so a startup check loses nothing.

**Apply-hook summary**: `tenancy`, `matching` (partial), `reporting`, `archival`, `object_storage` need hooks. `http`, `rate_limit`, `ingestion`, `ai_gates` are hot reads only.

---

## 3. Environment variables

**Loader**: `internal/bootstrap/config_loading.go:LoadConfigWithLogger` → `:loadConfigFromEnvForStartup` → `:normalizeLegacyAuthEnvVars` → `:sanitizeEnvVarsForConfig` → `:loadConfigFromEnv` → `libCommons.SetConfigFromEnvVars` per sub-struct → `config_helpers.go:restoreZeroedFields` → `:enforceProductionSecurityDefaults` → `Config.Validate`.

### Precedence today

1. `internal/bootstrap/config_defaults.go:defaultConfig` seeds compile-time defaults.
2. `libCommons.SetConfigFromEnvVars` overlays env — **but only for the 22 sub-structs explicitly passed** (see the gap below).
3. `config_helpers.go:restoreZeroedFields` restores any field the overlay zeroed, *unless* `config_helpers.go:hasExplicitEnvOverride` sees the var present (present-but-empty counts as an explicit override).
4. That env-resolved `*Config` becomes the **registered default** for each systemplane key (`systemplane_keys_defs.go:matcherKeyDefs` reads every default off `cfg`). So env wins at boot.
5. Any stored systemplane value overwrites it on hydrate and on every PUT (`config_manager.go:applySystemplaneOverrides`). So systemplane wins after boot, permanently — an env var set in the pod spec is silently ignored from the first PUT onward.
6. `MATCHER_*` prefixed overrides: **dead**. The vendored lib states "lib-systemplane reads ZERO environment variables directly" (`vendor/.../lib-systemplane/v2/.env.reference`) and `grep -rn "Getenv\|LookupEnv" vendor/.../lib-systemplane/v2/` returns nothing. The 71 names in `config_override_env_keys_test.go:matcherOverrideEnvVarKeys` exist only so `Makefile:MATCHER_OVERRIDE_KEYS` can `env -u` them before test runs — a scrub list for a mechanism that no longer exists.

### The loader gap

`config_loading.go:loadConfigFromEnv` calls `SetConfigFromEnvVars` on 22 of the 38 sub-structs. `vendor/.../lib-commons/v6/commons/os.go:SetConfigFromEnvVars` is **non-recursive** (it walks only the top-level fields of the struct it is handed). The 16 sub-structs never passed are:

`Outbox`, `MatchRunWorker`, `TrialWorker`, `ProvisioningWorker`, `OverageDebitWorker`, `DunningWorker`, `Matching`, `BindingScheduler`, `Governance`, `Exception`, `Advisor`, `DocExtraction`, `RuleAdvisor`, `RESTConnectors`, `HubWebhook`, `Onboarding`.

Their ~60 `env:` tags are decorative: the values come from `defaultConfig()` and nothing else. `grep -rn "…" --include=*_test.go | grep t.Setenv` finds **no** test that sets any of them, so nothing catches it. Concretely this means `DOC_EXTRACTION_ENABLED`, `RULE_ADVISOR_ENABLED`, `MATCHING_CANDIDATE_BATCH_SIZE`, `EXCEPTION_DISPUTE_LOCK_TIMEOUT_MS`, every `ONBOARDING_*`, every billing-worker knob, and both Anthropic API keys **cannot be set from the environment today**. Some of these have systemplane keys (`exception.dispute_lock_timeout_ms`, `match_run_worker.*`) so they are reachable via `/system`; the AI kill-switches and `ONBOARDING_*` secrets are reachable by nothing at all.

### Tables

Classification: `bootstrap` (DSN/port/identity, restart required by nature) · `secret` · `hot-read` (could be a systemplane knob read on use) · `controlled-apply` (could be a knob but needs a resource rebuild) · `removable` (nobody should tune it). The **Dup** column flags env vars that duplicate a systemplane key.

#### AppConfig + top-level (5)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `ENV_NAME` | string | `development` | `config_loading.go:LoadConfigWithLogger`; also read raw in `cmd/matcher/main.go` and `init.go` before Config exists | bootstrap | no (deliberately unregistered) |
| `LOG_LEVEL` | string | `info` | same; `logger_environment.go:ResolveLoggerLevel` | bootstrap | no |
| `DEPLOYMENT_MODE` | string | `local` | `/readyz` envelope + logger defaults | bootstrap | no |
| `SHUTDOWN_GRACE_PERIOD_SEC` | int (hand-parsed) | 5 s | `config_loading.go:loadConfigFromEnv` | bootstrap | no |
| `VERSION` | string | `0.0.0` | `health_huma.go:healthHumaHandler.version`, `health_check.go` (per request, via `GetenvOrDefault`) | bootstrap | no |

#### ServerConfig (10)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `SERVER_ADDRESS` | string | `:4018` | `fiber_server_runtime.go:Server.Run` | bootstrap | no |
| `HTTP_READ_TIMEOUT` | duration (hand-parsed) | `defaultHTTPReadTimeout` | `config_loading.go:loadConfigFromEnv` | bootstrap | no |
| `HTTP_BODY_LIMIT_BYTES` | int | `104857600` | seeds `server.body_limit_bytes` | hot-read | **YES → `server.body_limit_bytes`** |
| `CORS_ALLOWED_ORIGINS` | string | `http://localhost:3000` | seeds key | hot-read | **YES → `cors.allowed_origins`** |
| `CORS_ALLOWED_METHODS` | string | `GET,POST,PUT,PATCH,DELETE,OPTIONS` | seeds key | hot-read | **YES** |
| `CORS_ALLOWED_HEADERS` | string | `Origin,Content-Type,Accept,Authorization,X-Request-ID` | seeds key | hot-read | **YES** |
| `SERVER_TLS_CERT_FILE` | string | `""` | `fiber_server_runtime.go:Server.Run` (ListenTLS) | bootstrap | no |
| `SERVER_TLS_KEY_FILE` | string | `""` | same | secret | no |
| `TLS_TERMINATED_UPSTREAM` | bool | `false` | `fiber_server.go:NewFiberApp` (HSTS) | bootstrap | no |
| `TRUSTED_PROXIES` | string | `""` | `fiber_server.go:NewFiberApp` (ProxyHeader) | bootstrap | no |

#### TenancyConfig (20)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `DEFAULT_TENANT_ID` | string | `11111111-…-111111111111` | `init_multi_tenant.go` tenant extractor (auth globals, boot) | bootstrap | no (reclassified) |
| `DEFAULT_TENANT_SLUG` | string | `default` | same | bootstrap | no (reclassified) |
| `MULTI_TENANT_ENABLED` | bool | `false` | `dynamic_infrastructure_provider.go:setMultiTenantStrict` (boot authority) | bootstrap | no |
| `MULTI_TENANT_URL` | string | `""` | `:buildCanonicalTenantManager` | controlled-apply | **YES → `tenancy.multi_tenant_url`** |
| `MULTI_TENANT_ENVIRONMENT` | string | falls back to `ENV_NAME` | `config_env.go:Config.effectiveMultiTenantEnvironment` | bootstrap | no |
| `MULTI_TENANT_REDIS_HOST` | string | `""` | boot Redis wiring | bootstrap | no |
| `MULTI_TENANT_REDIS_PORT` | string | `6379` | same | bootstrap | no |
| `MULTI_TENANT_REDIS_PASSWORD` | string | `""` | same | secret | no |
| `MULTI_TENANT_REDIS_TLS` | bool | `false` | same | bootstrap | no |
| `MULTI_TENANT_MAX_TENANT_POOLS` | int | `100` | `:tenantManagerPostgresOptions` | controlled-apply | **YES** |
| `MULTI_TENANT_IDLE_TIMEOUT_SEC` | int | `300` | same | controlled-apply | **YES** |
| `MULTI_TENANT_TIMEOUT` | int | `30` | `:buildTenantManagerClientOptions` | controlled-apply | **YES** |
| `MULTI_TENANT_CIRCUIT_BREAKER_THRESHOLD` | int | `5` | same | controlled-apply | **YES** |
| `MULTI_TENANT_CIRCUIT_BREAKER_TIMEOUT_SEC` | int | `30` | same | controlled-apply | **YES** |
| `MULTI_TENANT_SERVICE_API_KEY` | string | `""` | same | secret | **YES (redacted key)** |
| `MULTI_TENANT_CACHE_TTL_SEC` | int | `120` | `config_env.go:Config.MultiTenantCacheTTL` | controlled-apply | **YES** |
| `MULTI_TENANT_CONNECTIONS_CHECK_INTERVAL_SEC` | int | `30` | `config_env.go:Config.MultiTenantConnectionsCheckInterval` | controlled-apply | **YES** |
| `MULTI_TENANT_MAX_OPEN_CONNS_PER_TENANT` | int | `0` | `dynamic_infrastructure_multi_tenant.go:perTenantConnLimits` | bootstrap | no (deliberately excluded from the rebuild key) |
| `MULTI_TENANT_MAX_IDLE_CONNS_PER_TENANT` | int | `0` | same | bootstrap | no |
| `MULTI_TENANT_ALLOW_INSECURE_HTTP` | bool | `false` | `config_validation_tenancy.go` | bootstrap | no |

#### PostgresConfig (20)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `POSTGRES_HOST` | string | `localhost` | `config_env.go:Config.PrimaryDSN`; `systemplane_init.go:buildSystemplaneDSN` | bootstrap | no |
| `POSTGRES_PORT` | string | `5432` | same | bootstrap | no |
| `POSTGRES_USER` | string | `matcher` | same | bootstrap | no |
| `POSTGRES_PASSWORD` | string | `matcher_dev_password` | same | secret | no |
| `POSTGRES_DB` | string | `matcher` | same | bootstrap | no |
| `POSTGRES_SSLMODE` | string | `disable` | same | bootstrap | no |
| `POSTGRES_TLS_REQUIRED` | bool | `false` | `tls_enforcement.go:ValidateRequiredTLS` (pre-connection) | bootstrap | no |
| `POSTGRES_REPLICA_HOST` | string | `""` | `config_env.go:Config.ReplicaDSN` | bootstrap | no |
| `POSTGRES_REPLICA_PORT` | string | `""` | same | bootstrap | no |
| `POSTGRES_REPLICA_USER` | string | `""` | same | bootstrap | no |
| `POSTGRES_REPLICA_PASSWORD` | string | `""` | same | secret | no |
| `POSTGRES_REPLICA_DB` | string | `""` | same | bootstrap | no |
| `POSTGRES_REPLICA_SSLMODE` | string | `""` | same | bootstrap | no |
| `POSTGRES_REPLICA_TLS_REQUIRED` | bool | `false` | `tls_enforcement.go:ValidateRequiredTLS` | bootstrap | no |
| `POSTGRES_MAX_OPEN_CONNS` | int | `25` | pool built once; also feeds `dynamicMultiTenantKey` | controlled-apply | no |
| `POSTGRES_MAX_IDLE_CONNS` | int | `5` | same | controlled-apply | no |
| `POSTGRES_CONN_MAX_LIFETIME_MINS` | int | `30` | `config_env.go:Config.ConnMaxLifetime` (pool build) | controlled-apply | no |
| `POSTGRES_CONN_MAX_IDLE_TIME_MINS` | int | `5` | `config_env.go:Config.ConnMaxIdleTime` | controlled-apply | no |
| `POSTGRES_CONNECT_TIMEOUT_SEC` | int | `10` | DSN build | bootstrap | no |
| `POSTGRES_QUERY_TIMEOUT_SEC` | int | `30` | seeds key | hot-read | **YES → `postgres.query_timeout_sec`** |

#### RedisConfig (12) — every one bootstrap-only by design

| Env var | Type | Default | Applied at | Class |
|---|---|---|---|---|
| `REDIS_HOST` | string | `localhost:6379` | `init_infra_connect.go` (boot) | bootstrap |
| `REDIS_MASTER_NAME` | string | `""` | same | bootstrap |
| `REDIS_PASSWORD` | string | `""` | same | secret |
| `REDIS_DB` | int | `0` | same | bootstrap |
| `REDIS_TLS` | bool | `false` | same | bootstrap |
| `REDIS_TLS_REQUIRED` | bool | `false` | `tls_enforcement.go:ValidateRequiredTLS` | bootstrap |
| `REDIS_CA_CERT` | string | `""` | same | secret |
| `REDIS_POOL_SIZE` | int | `10` | client build | controlled-apply |
| `REDIS_MIN_IDLE_CONNS` | int | `2` | client build | controlled-apply |
| `REDIS_READ_TIMEOUT_MS` | int | `3000` | `config_env.go:Config.RedisReadTimeout` | controlled-apply |
| `REDIS_WRITE_TIMEOUT_MS` | int | `3000` | `config_env.go:Config.RedisWriteTimeout` | controlled-apply |
| `REDIS_DIAL_TIMEOUT_MS` | int | `5000` | `config_env.go:Config.RedisDialTimeout` | bootstrap |

#### RabbitMQConfig (9) — all bootstrap

| Env var | Type | Default | Applied at | Class |
|---|---|---|---|---|
| `RABBITMQ_URI` | string | `amqp` | `config_env.go:Config.RabbitMQDSN` | bootstrap |
| `RABBITMQ_HOST` | string | `localhost` | same | bootstrap |
| `RABBITMQ_PORT` | string | `5672` | same | bootstrap |
| `RABBITMQ_USER` | string | `matcher_admin` | same | bootstrap |
| `RABBITMQ_PASSWORD` | string | `matcher_dev_password` | same | secret |
| `RABBITMQ_VHOST` | string | `/` | same | bootstrap |
| `RABBITMQ_HEALTH_URL` | string | `http://localhost:15672` | `health_check_resolvers.go` | bootstrap |
| `RABBITMQ_ALLOW_INSECURE_HEALTH_CHECK` | bool | `false` | same | bootstrap |
| `RABBITMQ_TLS_REQUIRED` | bool | `false` | `tls_enforcement.go:ValidateRequiredTLS` | bootstrap |

#### Auth / Swagger / Telemetry (15)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `PLUGIN_AUTH_ENABLED` | bool | `false` | `init_telemetry_auth.go` (boot); legacy alias `AUTH_ENABLED` handled by `config_loading.go:normalizeLegacyAuthEnvVars` | bootstrap | no (reclassified) |
| `PLUGIN_AUTH_ADDRESS` | string | `""` | same; legacy alias `AUTH_SERVICE_ADDRESS` | bootstrap | no (reclassified) |
| `AUTH_PROVIDER` | string | `""` | `config_validation_auth.go:ResolveAuthProvider` | bootstrap | no |
| `AUTH_ENABLED` | bool | — | legacy alias → `PLUGIN_AUTH_ENABLED`; conflicting non-empty values are a boot error (`errConflictingEnvAliasValues`) | removable | no |
| `AUTH_SERVICE_ADDRESS` | string | — | legacy alias → `PLUGIN_AUTH_ADDRESS` | removable | no |
| `SWAGGER_ENABLED` | bool | `false` | route mount (boot); forced `false` in production | controlled-apply | **YES → `swagger.enabled` (INERT key)** |
| `SWAGGER_HOST` | string | `""` | `init_aggregator_webhook.go` | controlled-apply | **YES (INERT)** |
| `SWAGGER_SCHEMES` | string | `https` | `init_aggregator_webhook.go:parseSchemes` | controlled-apply | **YES (INERT)** |
| `ENABLE_TELEMETRY` | bool | `false` | `observability.go:InitTelemetry` | bootstrap | no |
| `OTEL_RESOURCE_SERVICE_NAME` | string | `matcher` | same | bootstrap | no |
| `OTEL_LIBRARY_NAME` | string | `github.com/LerianStudio/matcher` | same | bootstrap | no |
| `OTEL_RESOURCE_SERVICE_VERSION` | string | `1.1.0` | same | bootstrap | no |
| `OTEL_RESOURCE_DEPLOYMENT_ENVIRONMENT` | string | `development` | same | bootstrap | **YES → `telemetry.deployment_env` (INERT key)** |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | string | `localhost:4317` | same | bootstrap | no |
| `DB_METRICS_INTERVAL_SEC` | int | `15` | `db_metrics.go` collector (boot) | bootstrap | no |

#### RateLimitConfig (11) — every one duplicates a hot systemplane key

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `RATE_LIMIT_ENABLED` | bool | `true` | seeds key; also gates `ratelimit.New` at boot (returns nil when false, which makes the systemplane key un-flippable — see the docker-compose comment) | hot-read | **YES → `rate_limit.enabled`** |
| `RATE_LIMIT_MAX` | int | `100` | seeds key | hot-read | **YES** |
| `RATE_LIMIT_EXPIRY_SEC` | int | `60` | seeds key | hot-read | **YES** |
| `EXPORT_RATE_LIMIT_MAX` | int | `10` | seeds key | hot-read | **YES** |
| `EXPORT_RATE_LIMIT_EXPIRY_SEC` | int | `60` | seeds key | hot-read | **YES** |
| `DISPATCH_RATE_LIMIT_MAX` | int | `50` | seeds key | hot-read | **YES** |
| `DISPATCH_RATE_LIMIT_EXPIRY_SEC` | int | `60` | seeds key | hot-read | **YES** |
| `ADMIN_RATE_LIMIT_MAX` | int | `30` | seeds key | hot-read | **YES** |
| `ADMIN_RATE_LIMIT_EXPIRY_SEC` | int | `60` | seeds key | hot-read | **YES** |
| `SIGNUP_RATE_LIMIT_MAX` | int | `5` | seeds key | hot-read | **YES** |
| `SIGNUP_RATE_LIMIT_EXPIRY_SEC` | int | `3600` | seeds key | hot-read | **YES** |

#### Infrastructure / Idempotency / Outbox / Ingestion / Dedupe / Webhook / Exception / Governance (13)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `INFRA_CONNECT_TIMEOUT_SEC` | int | `30` | `config_env.go:Config.InfraConnectTimeout` (boot connections) | bootstrap | no |
| `HEALTH_CHECK_TIMEOUT_SEC` | int | `5` | seeds key | hot-read | **YES → `infrastructure.health_check_timeout_sec`** |
| `HEALTH_CHECK_TIMEOUT_MS` | int | `800` | seeds key | hot-read | **YES** |
| `IDEMPOTENCY_RETRY_WINDOW_SEC` | int | `300` | seeds key | hot-read | **YES** |
| `IDEMPOTENCY_SUCCESS_TTL_HOURS` | int | `168` | seeds key | hot-read | **YES** |
| `IDEMPOTENCY_HMAC_SECRET` | string | `""` | seeds key | secret | **YES (redacted key)** |
| `OUTBOX_RETRY_WINDOW_SEC` | int | `300` | **never loaded** (Outbox not in `loadConfigFromEnv`); `config_env.go:Config.OutboxRetryWindow` | controlled-apply | no (reclassified out) |
| `OUTBOX_DISPATCH_INTERVAL_SEC` | int | `2` | **never loaded**; `config_env.go:Config.OutboxDispatchInterval` | controlled-apply | no (reclassified out) |
| `INGESTION_MAX_UPLOAD_BYTES` | int64 | `1073741824` | seeds key | hot-read | **YES → `ingestion.max_upload_bytes`** |
| `DEDUPE_TTL_SEC` | int | `3600` | seeds key | hot-read | **YES** |
| `WEBHOOK_TIMEOUT_SEC` | int | `30` | seeds key | hot-read | **YES** |
| `EXCEPTION_DISPUTE_LOCK_TIMEOUT_MS` | int | `2000` | **never loaded**; systemplane-only in practice | hot-read | **YES → `exception.dispute_lock_timeout_ms`** |
| `ACTOR_PII_ENCRYPTION_KEY` | string | `""` | **never loaded**; `config_validation_actor_pii.go`, `init_governance.go` | secret | no |

#### ObjectStorage (8)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `OBJECT_STORAGE_ENDPOINT` | string | `http://localhost:8333` | seeds key; `init_storage.go:createObjectStorage` | controlled-apply | **YES** |
| `OBJECT_STORAGE_REGION` | string | `us-east-1` | same | controlled-apply | **YES** |
| `OBJECT_STORAGE_BUCKET` | string | `matcher-exports` | same | controlled-apply | **YES** |
| `OBJECT_STORAGE_ACCESS_KEY_ID` | string | `""` | same | secret | **YES (redacted key)** |
| `OBJECT_STORAGE_SECRET_ACCESS_KEY` | string | `""` | same | secret | **YES (redacted key)** |
| `OBJECT_STORAGE_USE_PATH_STYLE` | bool | `true` | same | controlled-apply | **YES** |
| `OBJECT_STORAGE_ALLOW_INSECURE_ENDPOINT` | bool | `false` | same; forced `false` in production at boot only | controlled-apply | **YES** |
| `OBJECT_STORAGE_TLS_REQUIRED` | bool | `false` | `tls_enforcement.go:ValidateRequiredTLS` | bootstrap | no |

#### Workers (22)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `EXPORT_WORKER_ENABLED` | bool | `true` | seeds key | controlled-apply | **YES** |
| `EXPORT_WORKER_POLL_INTERVAL_SEC` | int | `5` | seeds key | controlled-apply | **YES** |
| `EXPORT_WORKER_PAGE_SIZE` | int | `1000` | seeds key | controlled-apply | **YES** |
| `EXPORT_PRESIGN_EXPIRY_SEC` | int | `3600` | seeds key | hot-read | **YES** |
| `MATCH_RUN_WORKER_ENABLED` | bool | `true` | **never loaded** | controlled-apply | **YES** |
| `MATCH_RUN_WORKER_POLL_INTERVAL_SEC` | int | `3` | **never loaded** | controlled-apply | **YES** |
| `TRIAL_WORKER_ENABLED` | bool | `true` | **never loaded**; `worker_manager.go` | controlled-apply | no |
| `TRIAL_WORKER_INTERVAL_SEC` | int | `3600` | **never loaded**; `config_env.go:Config.TrialWorkerInterval` | controlled-apply | no |
| `PROVISIONING_WORKER_ENABLED` | bool | `true` | **never loaded** | controlled-apply | no |
| `PROVISIONING_WORKER_INTERVAL_SEC` | int | `15` | **never loaded**; `config_env.go:Config.ProvisioningWorkerInterval` | controlled-apply | no |
| `OVERAGE_DEBIT_WORKER_ENABLED` | bool | `true` | **never loaded** | controlled-apply | no |
| `OVERAGE_DEBIT_WORKER_INTERVAL_SEC` | int | `86400` | **never loaded** | controlled-apply | no |
| `DUNNING_WORKER_ENABLED` | bool | `true` | **never loaded** | controlled-apply | no |
| `DUNNING_WORKER_INTERVAL_SEC` | int | `86400` | **never loaded** | controlled-apply | no |
| `CLEANUP_WORKER_ENABLED` | bool | `true` | seeds key | controlled-apply | **YES** |
| `CLEANUP_WORKER_INTERVAL_SEC` | int | `3600` | seeds key | controlled-apply | **YES** |
| `CLEANUP_WORKER_BATCH_SIZE` | int | `100` | seeds key | controlled-apply | **YES** |
| `CLEANUP_WORKER_GRACE_PERIOD_SEC` | int | `3600` | seeds key | controlled-apply | **YES** |
| `SCHEDULER_INTERVAL_SEC` | int | `60` | seeds key | controlled-apply | **YES** |
| `BINDING_SCHEDULER_ENABLED` | bool | `false` | **never loaded**; `init_binding_scheduler.go` | controlled-apply | no |
| `BINDING_SCHEDULER_INTERVAL_SEC` | int | `60` | **never loaded**; `config_env.go:Config.BindingSchedulerInterval` | controlled-apply | no |
| `MATCHING_CANDIDATE_BATCH_SIZE` | int | `10000` | **never loaded**; `config_env.go:Config.CandidateBatchSize` (clamped to 100000) | controlled-apply | no (explicitly bootstrap-only) |

#### Archival (10) — all duplicate a systemplane key

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `ARCHIVAL_WORKER_ENABLED` | bool | `false` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_WORKER_INTERVAL_HOURS` | int | `24` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_HOT_RETENTION_DAYS` | int | `90` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_WARM_RETENTION_MONTHS` | int | `24` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_COLD_RETENTION_MONTHS` | int | `84` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_BATCH_SIZE` | int | `5000` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_STORAGE_BUCKET` | string | `matcher-archives` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_STORAGE_CLASS` | string | `GLACIER` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_PARTITION_LOOKAHEAD` | int | `3` | seeds key | controlled-apply | **YES** |
| `ARCHIVAL_PRESIGN_EXPIRY_SEC` | int | `3600` | seeds key | hot-read | **YES** |

#### Fetcher / AI lanes (14)

| Env var | Type | Default | Applied at | Class | Dup |
|---|---|---|---|---|---|
| `FETCHER_DISCOVERY_INTERVAL_SEC` | int | `60` | seeds key | hot-read | **YES** |
| `FETCHER_SCHEMA_CACHE_TTL_SEC` | int | `300` | seeds key | hot-read | **YES** |
| `FETCHER_EXTRACTION_TIMEOUT_SEC` | int | `600` | seeds key | hot-read | **YES** |
| `FETCHER_MAX_EXTRACTION_BYTES` | int64 | `2147483648` | seeds key | hot-read | **YES** |
| `APP_ENC_KEY` | string | `""` | `init_crypto.go` (derived keys cached for process lifetime) | secret | no |
| `ADVISOR_ENABLED` | bool | `false` | **never loaded**; `init_ingestion.go:buildMappingAdvisor` | bootstrap | no |
| `ADVISOR_ANTHROPIC_API_KEY` | string | `""` | **never loaded** | secret | no |
| `ADVISOR_MODEL` | string | `claude-sonnet-4-6` | **never loaded** | bootstrap | no |
| `DOC_EXTRACTION_ENABLED` | bool | `false` | **never loaded**; `init_document_extraction.go:buildDocumentExtractor` | bootstrap | no (tenant gate is the key) |
| `DOC_EXTRACTION_ANTHROPIC_API_KEY` | string | `""` | **never loaded** | secret | no |
| `DOC_EXTRACTION_MODEL` | string | `claude-sonnet-4-6` | **never loaded** | bootstrap | no |
| `RULE_ADVISOR_ENABLED` | bool | `false` | **never loaded**; `init_rule_suggestion.go:buildRuleAdvisorSuggester` | bootstrap | no |
| `RULE_ADVISOR_ANTHROPIC_API_KEY` | string | `""` | **never loaded** | secret | no |
| `RULE_ADVISOR_MODEL` | string | `claude-sonnet-4-6` | **never loaded** | bootstrap | no |

#### RESTConnectors (10) — `rest_connectors_config.go`, all never loaded from env

| Env var | Type | Default | Class |
|---|---|---|---|
| `PAGBANK_CONNECTOR_BASE_URL` | string | `https://api.pagseguro.com` | bootstrap |
| `STRIPE_CONNECTOR_BASE_URL` | string | `https://api.stripe.com` | bootstrap |
| `STRIPE_CONNECTOR_FILES_BASE_URL` | string | `https://files.stripe.com` | bootstrap |
| `PIX_CONNECTOR_BASE_URL` | string | `""` | bootstrap |
| `PLUGGY_CONNECTOR_BASE_URL` | string | `https://api.pluggy.ai` | bootstrap |
| `PLUGGY_WEBHOOK_HMAC_SECRET` | string | `""` | secret |
| `PLUGGY_WEBHOOK_ALLOWED_IPS` | string | `""` | controlled-apply |
| `BELVO_CONNECTOR_BASE_URL` | string | `https://api.belvo.com` | bootstrap |
| `BELVO_WEBHOOK_HMAC_SECRET` | string | `""` | secret |
| `BELVO_WEBHOOK_ALLOWED_IPS` | string | `""` | controlled-apply |

#### HubWebhook (6) — never loaded from env

| Env var | Type | Default | Applied at | Class |
|---|---|---|---|---|
| `HUB_WEBHOOK_ENABLED` | bool | `false` | `init_hub_webhook.go` | bootstrap |
| `HUB_WEBHOOK_FRESHNESS_TOLERANCE_SEC` | int | `300` | `init_hub_webhook.go` (clock-skew window) | hot-read |
| `HUB_WEBHOOK_DRAIN_INTERVAL_SEC` | int | `30` | `init_hub_drain.go` | controlled-apply |
| `HUB_WEBHOOK_DRAIN_BATCH_SIZE` | int | `100` | same | controlled-apply |
| `HUB_WEBHOOK_DRAIN_MAX_ATTEMPTS` | int | `0` (unbounded) | same | controlled-apply |
| `HUB_WEBHOOK_DRAIN_ROUTES` | string | `""` | same | controlled-apply |

#### Onboarding / billing (25) — never loaded from env; the largest secret cluster in the service

| Env var | Type | Default | Applied at | Class |
|---|---|---|---|---|
| `ONBOARDING_SMTP_HOST` | string | `""` | `init_onboarding.go` | bootstrap |
| `ONBOARDING_SMTP_PORT` | int | `1025` | same | bootstrap |
| `ONBOARDING_SMTP_USERNAME` | string | `""` | same | bootstrap |
| `ONBOARDING_SMTP_PASSWORD` | string | `""` | same | secret |
| `ONBOARDING_SMTP_FROM` | string | `""` | same | bootstrap |
| `ONBOARDING_SMTP_TIMEOUT_SECONDS` | int | `10` | same (fd/goroutine-leak guard) | hot-read |
| `ONBOARDING_VERIFY_BASE_URL` | string | `""` | same | bootstrap |
| `ONBOARDING_TENANT_SERVICE_TOKEN` | string | `""` | same | secret |
| `ONBOARDING_PENDING_TTL_HOURS` | int | `168` | same (native TTL, no purge worker) | controlled-apply |
| `ONBOARDING_TRIAL_DURATION_HOURS` | int | `720` | `config_env.go:Config.TrialDuration` | hot-read |
| `ONBOARDING_TRIAL_GRACE_HOURS` | int | `24` | `config_env.go:Config.TrialGrace` | hot-read |
| `BILLING_GATE_CACHE_TTL_SEC` | int | `5` | `init_billing_fulfilment.go` (suspend-enforcement latency floor) | hot-read |
| `ONBOARDING_FREE_TIER_MATCHED_LINE_LIMIT` | int64 | `1000000` | `init_modules.go:wireQuotaResolver` (boot refuses on disagreement with `pkg/onboarding`) | bootstrap |
| `ONBOARDING_IDP_ADMIN_URL` | string | `""` | `init_onboarding.go` | bootstrap |
| `ONBOARDING_IDP_ADMIN_CLIENT_ID` | string | `""` | same | bootstrap |
| `ONBOARDING_IDP_ADMIN_CLIENT_SECRET` | string | `""` | same | secret |
| `ONBOARDING_CONSOLE_REDIRECT_URIS` | string | `""` | same | controlled-apply |
| `ONBOARDING_IDP_ISSUER_URL` | string | `""` | same | bootstrap |
| `ONBOARDING_IDENTITY_URL` | string | `""` | same | bootstrap |
| `ONBOARDING_IDENTITY_CLIENT_ID` | string | `""` | same | bootstrap |
| `ONBOARDING_IDENTITY_CLIENT_SECRET` | string | `""` | same | secret |
| `ONBOARDING_IDENTITY_ADMIN_GROUP` | string | `matcher-customer-admin` | same; must match `permissions.yaml` | bootstrap |
| `ONBOARDING_BILLING_SECRET_KEY` | string | `""` | `init_billing_fulfilment.go` | secret |
| `ONBOARDING_BILLING_WEBHOOK_SECRET` | string | `""` | `init_billing_webhook.go` | secret |
| `ONBOARDING_CONSOLE_BASE_URL` | string | `""` | `init_onboarding.go` | bootstrap |

#### Library-owned env, outside matcher's `Config` (44)

- **`SYSTEMPLANE_POSTGRES_DSN`** — string, empty. Read directly by `systemplane_init.go:buildSystemplaneDSN`; when set it overrides the DSN composed from `POSTGRES_*`. Class: **bootstrap**. This is the only env var lib-systemplane integration reads itself, and v4 should keep it (or take the DSN as a parameter).
- **38 `STREAMING_*` vars** — consumed by `streaming.LoadConfig()` at `internal/bootstrap/init_modules.go`, never by matcher's `Config`. `STREAMING_TLS_ENABLED` is additionally asserted in production by `config_validation_production.go`. Class: **bootstrap** (broker identity, SASL/TLS) and **secret** (`STREAMING_SASL_PASSWORD`, `STREAMING_TLS_CA_CERT`). Not in scope for matcher's groups.
- **`ALLOW_INSECURE_TLS`** — read by `vendor/.../lib-commons/v6/commons/security.go:AllowInsecureTLS`, set to `"true"` in `docker-compose.yml`. Class: **bootstrap**.
- **`OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG`** — documented (commented) in `config/.config-map.example`, consumed by the OTel SDK. Class: **bootstrap**.
- **`HEALTH_PROBE_URL`** — `cmd/health-probe/main.go`. Class: **bootstrap** (a separate binary).
- **`GOMEMLIMIT`** — read in `init_phases.go` only to emit a warning via `gomemlimit_warn.go:warnOnMissingGOMEMLIMIT`. Class: **bootstrap**.

#### Dead / stale env surface — `removable`

| Name | Where it appears | Why removable |
|---|---|---|
| 71 × `MATCHER_*` | `config_override_env_keys_test.go:matcherOverrideEnvVarKeys`, `Makefile:MATCHER_OVERRIDE_KEYS` | The override mechanism no longer exists in lib-systemplane v2. The list survives only as a test scrub list. |
| `FETCHER_URL` | `docker-compose.yml` (app service) | No `env:` tag, no `os.Getenv`. Points at a removed out-of-process fetcher. |
| `FETCHER_ALLOW_PRIVATE_IPS` | `docker-compose.yml` | Same. |
| `OTEL_EXPORTER_ENDPOINT` | `docker-compose.yml` | Misspelling of `OTEL_EXPORTER_OTLP_ENDPOINT`; matches nothing. |
| `AUTH_ENABLED`, `AUTH_SERVICE_ADDRESS` | `config_loading.go:legacyAuthEnvAliases` | Legacy aliases; keep only as long as a deployment still sets them. |
| `MATCHING_CANDIDATE_BATCH_SIZE`, `EXCEPTION_DISPUTE_LOCK_TIMEOUT_MS`, `MATCH_RUN_WORKER_*` and the other ~55 tags on the 16 unloaded sub-structs | `config.go` | Either wire them into `loadConfigFromEnv` or delete the tags. Today they promise an override that does not happen. |

---

## 4. Glue that v4 should delete

Measured with `wc -l`. **Production glue total: 2,449 lines. Test glue total: 4,637 lines.**

### Disappears entirely

| File / symbol | Lines | Why v4 removes it |
|---|---|---|
| `internal/bootstrap/systemplane_keys_defs.go:matcherKeyDefs` | 325 | The whole file is a hand-rolled key registry: 71 struct literals plus ~120 lines of prose explaining which keys were deliberately *not* registered. A typed group struct is the registry; the "why not registered" prose becomes "the field is not in a group". |
| `internal/bootstrap/systemplane_keys.go:RegisterMatcherKeys` + `:matcherKeyDef` | 112 | The per-key option-building loop (`WithDescription`/`WithValidator`/`WithRedaction`/`WithCatalogMetadata`) is exactly what group registration does once. The `matcherKeyDef` struct is a hand-rolled reflection of what a typed group already carries. |
| `internal/bootstrap/systemplane_init.go:SystemplaneGetString/Int/Int64/Bool` + `:systemplaneRead` | ~120 of 302 | Four typed getters with identical type-switch + fallback + WARN bodies. Typed groups return the typed struct; no `any` unwrapping, no per-call WARN. |
| `internal/bootstrap/config_manager.go:buildWatchedSystemplaneKeys` + `:watchedSystemplaneKeys` + `:watchedSystemplaneKeysCache` | ~130 of 556 | A 71-entry string list that must be kept in sync by hand with `matcherKeyDefs` (the sync is asserted by `TestWatchedSystemplaneKeys_CoversMatcherDefs`). Group-level change subscription removes both the list and its drift test. |
| `internal/bootstrap/config_manager.go:applySystemplaneOverrides` | ~155 of 556 | 71 hand-written assignment lines mirroring systemplane values back into `*Config`. With typed groups the group *is* the value; nothing is mirrored. |
| `internal/bootstrap/runtime_settings.go` (whole file: `runtimeSettingsResolver` + 10 methods) | 178 | Every method is "read N keys, clamp, fall back". Clamping (`maxWebhookTimeoutSec`, `maxPresignExpirySec`) moves into group validators; reading becomes one group read. |
| `internal/bootstrap/init_runtime_settings.go` (whole file) | 154 | Three generic `resolveRuntime{Duration,Int,String}Setting` helpers plus eight one-line wrappers, all existing to thread `(cfg, configGetter, settingsResolver)` through a three-way fallback chain. A group handle collapses this to a single accessor. |
| `internal/bootstrap/systemplane_keys_validators.go:validatePositiveInt`, `:validateBodyLimitBytes`, `:validateMaxUploadBytes`, `:validateMaxExtractionBytes`, `:enumKey` | 218 | All five exist because the v2 validator signature is `func(any) error`, forcing a type switch over `int`/`int64`/`float64` in every validator (JSON round-tripping). Typed fields remove the switches; `enumKey`'s dual validator+catalog return exists only to stop the advertised set from drifting from the enforced set, which a typed enum does structurally. |
| `cmd/systemplane-ddl/main.go` + `main_test.go` | 89 + 80 | Exists only because lib-systemplane v1.6.0 stopped creating its own schema, forcing matcher to vendor `SchemaSQL()` into `migrations/000001_init_schema.up.sql`. If v4 restores managed schema (or ships a migration), this command, the `Makefile:systemplane-ddl` target, the 63-line DDL block in the baseline migration, and `migrations/systemplane_drift_test.go` (31) all go. |
| `internal/bootstrap/systemplane_init.go:warnOnReclassifiedOrphanKeys` + `:reclassifiedBootstrapOnlyKeys` | ~85 of 302 | A raw `information_schema` probe plus a positional-placeholder query, purely to WARN about rows for 8 demoted keys. With typed groups, a key not in any group cannot be read, and orphan rows are a store-level concern. |
| `internal/shared/testutil/goleak.go:LeakOptionsWithSystemplane` | ~12 of 95 | Three `IgnoreAnyFunction` entries for `postgres.(*Store).Subscribe`, `.listenLoop`, `(*Client).Close.func2`. Only needed because v2's Subscribe goroutine outlives the client's `Close`. If v4's built-in lifecycle joins its goroutines, this helper and its callers in `internal/bootstrap/leak_test.go` (52) and `cmd/matcher/leak_test.go` (29) drop back to plain `LeakOptions`. |

### Shrinks substantially

| File / symbol | Lines | What remains |
|---|---|---|
| `internal/bootstrap/systemplane_init.go:InitSystemplane` + `:buildSystemplaneDSN` | 302 total | The constructor, DSN assembly, and cleanup-on-error stay (~60 lines). Everything else listed above goes. |
| `internal/bootstrap/config_manager.go:ConfigManager` (`Get`/`Update`/`Stop`/`OnReload`/`notifyReloadSubscribers`/`WatchSystemplane`) | 556 total | The atomic-pointer config holder and the `OnReload` fan-out to `WorkerManager` stay — they are matcher's own reload plumbing, not systemplane glue. `WatchSystemplane`'s per-key subscription loop and the `unsubscribes` bookkeeping (added because v1.1.0 leaked them) become one group subscription. Expect ~150 lines surviving. |
| `internal/bootstrap/systemplane_mount.go:MountSystemplaneAPI` | 194 | The auth chain (`:buildSystemplaneAuthChain`, `:buildSystemplaneAuthorizer`, `:requireSystemplaneAdminContext`) is matcher's RBAC and stays. The `nonHandlerSlots` capacity arithmetic, the `useArgs []any` variadic assembly, and the MountCatalog-before-Mount ordering comment are workarounds for the v2 mount API and should shrink. |
| `internal/bootstrap/init_phases.go:initSystemplaneStage` | ~76 of the file | The production/non-production degradation policy is a matcher product decision and stays. The "register keys → Start → WatchSystemplane → register shutdown closure" choreography should collapse into the lib's own lifecycle. |
| `internal/bootstrap/systemplane_keys_defaults.go` | 216 | A constant block duplicating every `envDefault` tag, kept in sync by `config_drift_test.go:TestDefaults_EnvDefaultTagsMatchConstants`. Shrinks to whatever v4 groups cannot express as field defaults. `matcherKeyDefsCapacity = 110` (stale against 71) disappears with the slice. |
| `internal/bootstrap/dynamic_infrastructure_multi_tenant.go:dynamicMultiTenantKey` | ~28 of 174 | The 14-field `fmt.Sprintf` fingerprint plus SHA-256 hashing of the API key exists solely to detect "did any tenancy knob change". A typed group makes that struct equality. |
| `internal/bootstrap/worker_manager_runtime.go:extractWorkerConfig` + the five `*ComparableConfig` structs | ~60 of the file | Same pattern: hand-built comparable views of Config sub-structs so `reflect.DeepEqual` can detect change. Typed groups are already the comparable unit. `:applyExportRuntimeConfig`, `:applyMatchRunRuntimeConfig`, `:applyCleanupRuntimeConfig`, `:applyArchivalRuntimeConfig`, `:applySchedulerRuntimeConfig` remain — they *are* the apply hooks v4 formalises. |

### Stays

| File / symbol | Lines | Why |
|---|---|---|
| `internal/bootstrap/v4_deprecation_middleware.go` | 105 | Answers `410 Gone` on the retired `/v1/system/*` paths with a migration hint. A product/compat decision, unrelated to which config library is underneath. (The `v4_` name refers to matcher's own API v4, not lib-systemplane v4 — worth renaming to avoid confusion during the migration.) |
| `internal/bootstrap/streaming_manifest_mount.go` + `:skipSystemplaneMiddlewareForStreamingManifest` | 196 (+ ~10 in the mount) | A deliberate M2M carve-out: the manifest route must bypass the `/system` auth chain and admin rate limiter. The bypass helper is coupled to whatever middleware guards `/system`, so it survives in some form. |
| `internal/bootstrap/init_document_extraction.go:systemplaneDocExtractionGate` / `init_rule_suggestion.go:systemplaneRuleSuggestionGate` | ~25 each | Thin per-request fail-closed adapters over one boolean. They shrink to a group-field read but the port implementations stay — the fail-closed semantics are product policy. |
| `internal/bootstrap/config_manager.go:OnReload` + `worker_manager.go` reconcile | — | Matcher's own hot-reload fan-out. v4's apply hooks plug into it; they do not replace it. |
| `migrations/000001_init_schema.up.sql` systemplane section | 63 | Stays if v4 still declines to manage its own schema; disappears with `cmd/systemplane-ddl` if it does. |

---

## 5. Open product questions

1. If an operator changes the environment name the service reports to monitoring, should that take effect immediately, or only on the next restart — and is it acceptable that today it appears to succeed but does nothing?
2. Should turning the API documentation page on or off be a live switch an administrator can flip, or is "restart to change" the right answer for something we deliberately keep off in production?
3. When someone raises a retention window on a live archive, should the change apply to data already archived under the old policy, or only to what gets archived from that moment on?
4. Is there ever a legitimate reason for warm retention to be shorter than hot retention, or should the system simply refuse such a combination?
5. Should an administrator be able to switch off rate limiting in production at runtime, given that we refuse that setting at startup today?
6. Should the two AI features (statement reading and rule suggestion) share a single per-customer consent switch, or must a customer be able to enable one without the other?
7. When a customer has never been asked about AI features, is "off" always the right answer, or should some plans default to on?
8. Who is allowed to flip a customer's AI consent — the customer's own administrator, or only Lerian operations?
9. Should the cleanup schedule and the lifetime of download links be tunable independently, or should the system guarantee that a link never outlives the file it points to?
10. Are per-customer values expected for any of these settings, or is a single service-wide value per deployment always sufficient outside the AI consent switches?
11. Which of these settings should a customer see and change themselves in the product, versus which are strictly operations-only?
12. Should a setting an operator typed into the deployment configuration ever win over a value an administrator set in the running system, or is "whatever was set last through the admin surface wins, forever" the intended rule?
13. Is it acceptable that a deployment currently has no way to set the AI feature switches, the email sender, or the billing credentials through deployment configuration, only through code defaults?
14. Should the tenancy connection settings be changeable while the service is serving traffic, given that a change closes and rebuilds every customer's connection pool?
15. Which of the roughly 200 deployment settings should an operations team actually be expected to know about, versus which exist only because someone once needed them for a specific customer?
