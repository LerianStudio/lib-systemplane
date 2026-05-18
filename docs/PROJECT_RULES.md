# Project Rules - lib-systemplane

This document defines the coding standards, architecture patterns, and development guidelines for `lib-systemplane` — a standalone Go library providing dual-backend (PostgreSQL / MongoDB) hot-reload runtime configuration, with LISTEN/NOTIFY and change-stream subscriptions, admin HTTP routes, and tenant-scoped overrides.

## Table of Contents

| # | Section | Description |
|---|---------|-------------|
| 1 | [Architecture Patterns](#architecture-patterns) | Package structure and organization |
| 2 | [Code Conventions](#code-conventions) | Go coding standards |
| 3 | [Error Handling](#error-handling) | Error handling patterns |
| 4 | [Testing Requirements](#testing-requirements) | Test coverage and patterns |
| 5 | [Documentation Standards](#documentation-standards) | Code documentation requirements |
| 6 | [Dependencies](#dependencies) | Dependency management rules |
| 7 | [Security](#security) | Security requirements |
| 8 | [DevOps](#devops) | CI/CD and tooling |

---

## Architecture Patterns

### Package Structure

```text
lib-systemplane/
├── (root package: systemplane)     # Client, register/get/set, subscribers, tenant-scoped accessors
├── admin/                          # Fiber HTTP handlers for runtime config management
├── systemplanetest/                # Backend-equivalence contract test suite
└── internal/
    ├── store/                      # Backend-agnostic Store interface (private)
    ├── postgres/                   # PostgreSQL LISTEN/NOTIFY implementation
    ├── mongodb/                    # MongoDB change-stream (+ polling fallback) implementation
    └── debounce/                   # Trailing-edge debouncer for changefeed coalescing
```

### Package Design Principles

1. **Single Responsibility**: Each package should have one clear purpose
2. **Minimal Dependencies**: Packages should minimize external dependencies
3. **Interface-Driven**: Define interfaces for testability and flexibility
4. **Zero Business Logic**: This is a utility library - no domain/business logic
5. **Nil-Safe and Concurrency-Safe**: Keep behavior safe by default
6. **Explicit Error Returns**: Prefer error returns over panic paths

### Naming Conventions

| Type | Convention | Example |
|------|------------|---------|
| Package | lowercase, single word preferred | `postgres`, `redis`, `circuitbreaker` |
| Files | snake_case or camelCase matching content | `pool_manager_pg.go`, `stringUtils.go` |
| Public Functions | PascalCase, descriptive | `NewClient`, `ServeReverseProxy` |
| Private Functions | camelCase | `validateConfig` |
| Interfaces | -er suffix or descriptive | `Logger`, `Manager`, `LockManager` |
| Constants | PascalCase | `DefaultTimeout`, `LevelInfo` |

---

## Code Conventions

### Go Version

- **Minimum**: Go 1.26.3
- Keep `go.mod` updated with latest stable Go version
- Module path: `github.com/LerianStudio/lib-systemplane`

### Build Tags

- Unit test files **MUST** have `//go:build unit` as the first line
- Integration test files **MUST** have `//go:build integration` as the first line

```go
//go:build unit

package mypackage

import "testing"

func TestMyFunc(t *testing.T) { ... }
```

### Imports Organization

```go
import (
    // Standard library
    "context"
    "fmt"
    "time"

    // Third-party packages
    "github.com/jackc/pgx/v5"
    "go.uber.org/zap"

    // Internal packages
    "github.com/LerianStudio/lib-observability/log"
)
```

### Function Design

1. **Context First**: Functions that may block should accept `context.Context` as first parameter
2. **Options Pattern**: Use functional options for configurable constructors
3. **Error Last**: Return errors as the last return value
4. **Named Returns**: Avoid named returns except for documentation

```go
// Good
func NewClient(ctx context.Context, opts ...Option) (*Client, error)

// Avoid
func NewClient(opts ...Option) (client *Client, err error)
```

### Struct Design

```go
type Config struct {
    Host     string        `json:"host"`
    Port     int           `json:"port"`
    Timeout  time.Duration `json:"timeout"`
    MaxConns int           `json:"max_conns"`
}

func (c *Config) Validate() error {
    if c.Host == "" {
        return ErrEmptyHost
    }
    return nil
}
```

### Constants and Variables

```go
const (
    DefaultTimeout  = 30 * time.Second
    DefaultMaxConns = 10
)

var (
    ErrNotFound     = errors.New("not found")
    ErrInvalidInput = errors.New("invalid input")
)
```

---

## Error Handling

### Error Definition

1. **Sentinel Errors**: Define package-level errors for expected conditions
2. **Error Wrapping**: Use `fmt.Errorf` with `%w` for context
3. **Custom Types**: Use custom error types when additional context is needed

```go
var (
    ErrConnectionFailed = errors.New("connection failed")
    ErrTenantNotFound   = errors.New("tenant not found")
)

// Wrapping
return fmt.Errorf("failed to connect to %s: %w", host, err)

// Custom type
type ValidationError struct {
    Field   string
    Message string
}

func (e *ValidationError) Error() string {
    return fmt.Sprintf("validation failed for %s: %s", e.Field, e.Message)
}
```

### Error Handling Rules

1. **NEVER use panic()** - Always return errors
2. **NEVER ignore errors** - Handle or propagate all errors
3. **Log at boundaries** - Log errors at service boundaries, not in library code
4. **Provide context** - Wrap errors with meaningful context

```go
// Good
if err != nil {
    return fmt.Errorf("failed to execute query: %w", err)
}

// Bad - panics
if err != nil {
    panic(err)
}

// Bad - ignores error
result, _ := doSomething()
```

---

## Testing Requirements

### Coverage Requirements

- **Minimum Coverage**: 80% for new packages
- **Critical Paths**: 100% coverage for error handling paths
- **Run Coverage**: `make coverage-unit` or `make coverage-integration`
- **Coverage Exclusions**: Defined in `.ignorecoverunit` (e.g., `*_mock.go`)

### Build Tags

All test files **MUST** include the appropriate build tag as the first line:

| Type | Build Tag | Example |
|------|-----------|---------|
| Unit Tests | `//go:build unit` | All `_test.go` files |
| Integration Tests | `//go:build integration` | All `_integration_test.go` files |

### Test File Naming

| Type | Pattern | Example |
|------|---------|---------|
| Unit Tests | `{file}_test.go` | `config_test.go` |
| Integration | `{file}_integration_test.go` | `postgres_integration_test.go` |
| Examples | `{feature}_example_test.go` | `cursor_example_test.go` |
| Benchmarks | In `_test.go` or `benchmark_test.go` | `BenchmarkXxx` |

### Integration Test Conventions

- Test function names **MUST** start with `TestIntegration_` (e.g., `TestIntegration_MyFeature_Works`)
- Integration tests use `testcontainers-go` to spin up ephemeral containers
- Docker is required to run integration tests
- Integration tests run sequentially (`-p=1`) to avoid Docker container conflicts

### Test Patterns

```go
func TestConfig_Validate(t *testing.T) {
    tests := []struct {
        name    string
        config  Config
        wantErr bool
    }{
        {
            name:    "valid config",
            config:  Config{Host: "localhost", Port: 5432},
            wantErr: false,
        },
        {
            name:    "empty host",
            config:  Config{Host: "", Port: 5432},
            wantErr: true,
        },
    }

    for _, tt := range tests {
        t.Run(tt.name, func(t *testing.T) {
            err := tt.config.Validate()
            if (err != nil) != tt.wantErr {
                t.Errorf("Validate() error = %v, wantErr %v", err, tt.wantErr)
            }
        })
    }
}
```

### Test Data

- Use realistic but fake data (e.g., `"pass"`, `"secret"` for passwords in tests)
- Never use real credentials in tests
- Use test fixtures for complex data structures

### Mocking

- Use `go.uber.org/mock` for interface mocking
- Define interfaces at point of use for testability
- Prefer dependency injection over global state
- Mock files follow the `{type}_mock.go` pattern

---

## Documentation Standards

### Package Documentation

Every package MUST have a `doc.go` file or package comment:

```go
// Package postgres provides PostgreSQL connection management utilities.
//
// It supports connection pooling, migrations, and read-replica configurations
// for high-availability deployments.
package postgres
```

### Function Documentation

Public functions MUST have documentation:

```go
// Connect establishes a connection to the PostgreSQL database.
// It validates the configuration before attempting to connect.
//
// Returns an error if the configuration is invalid or connection fails.
func (c *Client) Connect(ctx context.Context) error {
```

### README Updates

- Update `README.md` API Reference when adding public APIs
- Include usage examples for new packages

### README Awareness

- If a task changes package-level behavior or API expectations, update `README.md`

---

## Dependencies

### Lerian Library Boundaries

Lerian shared-library ownership is split intentionally:

- `github.com/LerianStudio/lib-commons/v5` — non-observability shared primitives. This repo uses `commons/tenant-manager/core`, `commons/net/http`, and `commons/backoff`.
- `github.com/LerianStudio/lib-observability` — canonical observability stack. This repo uses `log`, `tracing`, and `runtime` for structured logging, telemetry, span helpers, redaction, and panic recovery.
- `github.com/LerianStudio/lib-systemplane` — runtime-mutable configuration. Do not duplicate its functionality in service repositories.
- `github.com/LerianStudio/lib-streaming` — tenant-scoped event streaming. Do not add it to this repo unless a task explicitly requires streaming integration.

Do not reintroduce observability packages from `lib-commons`; they are being removed from that module. New observability code must use `lib-observability`.

### Key Third-Party Dependencies

- `github.com/gofiber/fiber/v2` — HTTP framework for the admin routes
- `github.com/jackc/pgx/v5` — PostgreSQL driver with LISTEN/NOTIFY support
- `go.mongodb.org/mongo-driver/v2` — MongoDB driver with change streams
- `github.com/hashicorp/golang-lru/v2` — Bounded LRU for lazy tenant cache
- `github.com/google/uuid` — UUID generation
- `github.com/stretchr/testify` — Test assertions and suites
- `github.com/testcontainers/testcontainers-go` — Ephemeral containers for integration tests
- OpenTelemetry SDK — tracing and metrics instrumentation, normally reached through `lib-observability`

### Adding Dependencies

1. Check if functionality exists in standard library
2. Check if existing dependency provides the functionality
3. Evaluate package maintenance and security
4. Add to `go.mod` with specific version

---

## Security

### Credential Handling

1. **Never hardcode credentials** - Use environment variables
2. **Never log credentials** - Use the `Redactor` for sensitive fields
3. **Mask in errors** - Never include credentials in error messages

```go
// Use the built-in lib-observability Redactor for sensitive data
redactor := tracing.NewDefaultRedactor()
safeValue := redactor.Redact(sensitiveField)
```

### Sensitive Field Detection

- Use `lib-observability/tracing.Redactor` with `RedactionRule` patterns for telemetry attributes
- Use `lib-observability/log` safe logging helpers when emitting external errors
- Constructors: `NewDefaultRedactor()` and `NewRedactor(rules, mask)`

### Input Validation

1. Validate all external inputs
2. Use parameterized queries - never string concatenation
3. Sanitize user-provided identifiers
4. Use `go-playground/validator/v10` for struct validation

### Log Injection Prevention

- Use `lib-observability/log` for production-safe logging and log-injection prevention
- Never interpolate untrusted input into log messages without sanitization

### Environment Variables

- Use `LOG_OBFUSCATION_DISABLED` to control HTTP body obfuscation (default: disabled)
- Sensitive field detection uses `commons/security.IsSensitiveField()` with a hardcoded set
- Document required environment variables
- Provide sensible defaults where safe

---

## DevOps

### Linting

- **Tool**: `golangci-lint` v2
- **Config**: `.golangci.yml`
- **Run**: `make lint` (read-only check) or `make lint-fix` (auto-fix)
- **Performance**: Optional `perfsprint` checks (install separately)

### Enabled Linters

**Existing linters:**
`bodyclose`, `depguard`, `dogsled`, `dupword`, `errchkjson`, `gocognit`, `gocyclo`, `loggercheck`, `misspell`, `nakedret`, `nilerr`, `nolintlint`, `prealloc`, `predeclared`, `reassign`, `revive`, `staticcheck`, `unconvert`, `unparam`, `usestdlibvars`, `wastedassign`, `wsl_v5`

**Tier 1 — Safety & Correctness:**
`errorlint`, `exhaustive`, `fatcontext`, `forcetypeassert`, `gosec`, `nilnil`, `noctx`

**Tier 2 — Code Quality & Modernization:**
`goconst`, `gocritic`, `inamedparam`, `intrange`, `mirror`, `modernize`, `perfsprint`

**Tier 3 — Zero-Issue Guards:**
`asasalint`, `copyloopvar`, `durationcheck`, `exptostd`, `gocheckcompilerdirectives`, `makezero`, `musttag`, `nilnesserr`, `recvcheck`, `rowserrcheck`, `spancheck`, `sqlclosecheck`, `testifylint`

### Formatting

- **Tool**: `gofmt`
- **Run**: `make format`
- All code MUST be formatted before commit

### Testing Commands

```bash
make ci                    # Local fix + verify pipeline
make test                  # Run unit tests (with -tags=unit)
make test-unit             # Run unit tests (excluding integration)
make test-integration      # Run integration tests with testcontainers (requires Docker)
make test-all              # Run all tests (unit + integration)
make coverage-unit         # Unit tests with coverage report
make coverage-integration  # Integration tests with coverage report
make coverage              # All coverage targets
```

### Testing Options

| Option | Description | Example |
|--------|-------------|---------|
| `RUN` | Specific test name pattern | `make test-integration RUN=TestIntegration_MyFeature` |
| `PKG` | Specific package to test | `make test-integration PKG=./commons/postgres/...` |
| `LOW_RESOURCE` | Low-resource mode (no race, -p=1) | `make test LOW_RESOURCE=1` |
| `RETRY_ON_FAIL` | Retry failed tests once | `make test RETRY_ON_FAIL=1` |

### Code Quality Commands

```bash
make lint                  # Run linters (read-only)
make lint-fix              # Run linters with auto-fix
make format                # Format code
make tidy                  # Clean dependencies
make check-tests           # Verify test coverage for packages
make vet                   # Run go vet on all packages
make sec                   # Security scan with gosec
make sec SARIF=1           # Security scan with SARIF output
make build                 # Build all packages
make clean                 # Clean all build artifacts
```

### Git Hooks

- Pre-commit hooks available in `.githooks/`
- Setup: `make setup-git-hooks`
- Verify: `make check-hooks`
- Environment check: `make check-envs`

### CI/CD

- All PRs must pass linting
- All PRs must pass tests
- Coverage must not decrease
- Security scan must pass

---

## API Invariants

Key API contracts that must be preserved:

| Area | Invariant |
|------|-----------|
| Client construction | `NewPostgres(db, listenDSN, opts...) (*Client, error)` and `NewMongoDB(client, database, opts...) (*Client, error)` — consumer picks backend at construction time. |
| Lifecycle | Construct → `Register`/`RegisterTenantScoped` → `Start(ctx)` → runtime ops → `Close()`. `Register` after `Start` returns `ErrRegisterAfterStart`. |
| Read paths | `Get`, `GetString`, `GetInt`, `GetBool`, `GetFloat64`, `GetDuration` are nil-receiver safe and return zero values on miss. |
| Write path | `Set(ctx, ns, key, value, actor)` — last-write-wins with write-through cache; subscribers fire via changefeed echo, not synchronously. |
| Subscriptions | `OnChange(ns, key, fn)` returns an `unsubscribe` func. Callbacks invoked serially with panic recovery via `lib-observability/runtime.RecoverAndLog`. |
| Tenant-scoped keys | `RegisterTenantScoped` declares per-tenant eligibility; legacy `Get`/`OnChange`/`List` continue observing only the shared `_global` row (non-breaking addition). |
| Tenant access | `GetForTenant`, `SetForTenant`, `DeleteForTenant`, `ListTenantsForKey`, `OnTenantChange` plus typed accessor mirrors. Fail-closed — no silent fallback to global. |
| Tenant validation | Tenant ID extracted via `core.GetTenantIDContext`, validated by `core.IsValidTenantID`. `_global` is reserved and rejected as a tenant ID. |
| Resolution order | `GetForTenant`: per-tenant cache → legacy global cache → registered default. |
| Delete semantics | Idempotent. Removing an existing row fires `OnTenantChange` with `newValue = registered default`. No-op delete emits no changefeed event. |
| Tenant ctx propagation | `OnTenantChange` callback ctx is pre-scoped to `tenantID` via `core.ContextWithTenantID` — subscribers can directly call tenant-aware facilities (DLQ, idempotency, webhook). |
| Admin HTTP surface | `Mount(router, client, opts...)` registers six routes (three legacy global, three tenant-scoped) at a configurable prefix (default `/system`). |
| Admin authorization | `WithAuthorizer` covers legacy routes only. Tenant routes require `WithTenantAuthorizer` — default-deny when absent. Library does NOT silently fall back. |
| Storage evolution | Postgres: `tenant_id TEXT NOT NULL DEFAULT '_global'` with composite unique index on `(namespace, key, tenant_id)`. MongoDB: compound `_id` `{namespace, key, tenant_id}` with idempotent backfill migration run during `NewMongoDB` (inside `ensureSchema`). |
| Internal Store | `internal/store` defines the backend-agnostic contract. `internal/postgres` (LISTEN/NOTIFY, pgx/v5) and `internal/mongodb` (change streams + polling fallback, mongo-driver/v2) implement it. Both satisfy `systemplanetest.Run(t, factory)`. |
| Sentinel errors | `ErrClosed`, `ErrNotStarted`, `ErrRegisterAfterStart`, `ErrUnknownKey`, `ErrValidation`, `ErrDuplicateKey`, `ErrMissingTenantContext`, `ErrInvalidTenantID`, `ErrTenantScopeNotRegistered`, `ErrTenantSchemaNotEnabled`. |
| Test helper | `NewForTesting(s TestStore, opts...)` is an explicit out-of-package test helper, not a promised production API. |
| Scope | Runtime-mutable knobs only. Bootstrap-only config (DB DSNs, secrets, TLS paths, telemetry init, server identity) should live in env vars / secret manager, not systemplane. |

---

## Checklist

Before submitting code:

- [ ] Code follows naming conventions
- [ ] All public APIs are documented
- [ ] Tests achieve 80%+ coverage
- [ ] Test files have correct build tag (`//go:build unit` or `//go:build integration`)
- [ ] No panics - all errors handled
- [ ] No hardcoded credentials
- [ ] `make lint` passes
- [ ] `make test` passes
- [ ] `make build` passes
- [ ] Dependencies are justified
- [ ] `README.md` updated if public API changed
