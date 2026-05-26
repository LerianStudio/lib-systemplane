<table border="0" cellspacing="0" cellpadding="0">
  <tr>
    <td><img src="https://github.com/LerianStudio.png" width="72" alt="Lerian" /></td>
    <td><h1>Lib SystemPlane</h1></td>
  </tr>
</table>

---

## Description

<!-- Summarize what this PR changes and why. Mention the package(s) affected
     (admin, client, debounce, mongodb, postgres, store, systemplanetest,
     core/public API, ...). Note the operating mode touched (single-tenant vs
     multi-tenant) when relevant. -->

## Type of Change

- [ ] `feat`: New feature or capability
- [ ] `fix`: Bug fix
- [ ] `perf`: Performance improvement
- [ ] `refactor`: Internal restructuring with no behavior change
- [ ] `docs`: Documentation only (README, docs/, inline comments)
- [ ] `style`: Formatting, whitespace, naming (no logic change)
- [ ] `test`: Adding or updating tests
- [ ] `ci`: CI pipeline or workflow changes
- [ ] `build`: Build system, Go module dependencies
- [ ] `chore`: Maintenance, config, tooling
- [ ] `revert`: Reverts a previous commit
- [ ] `BREAKING CHANGE`: Consumers must update their integration

## Breaking Changes

<!-- If applicable, describe exactly what breaks (public API signatures,
     exported types, storage shape, default behaviors, configuration keys) and
     how downstream services should migrate. Remove this section if not
     applicable. -->

None.

## Testing

- [ ] `make test-unit` passes
- [ ] `make test-integration` passes (testcontainers — requires Docker) if storage paths are exercised
- [ ] `make lint` passes
- [ ] `make sec` passes (gosec)
- [ ] AC15 perf gate (`go test -tags=unit -run=^TestPerf_ ./...`) passes
- [ ] Coverage threshold respected (see Go Combined Analysis)

**Test evidence / Actions run:** <!-- Optional: link to a CI run or screenshot -->

## Architectural Checklist

- [ ] No `panic()` in production paths — explicit error returns instead
- [ ] Errors wrapped with `%w`, never swallowed
- [ ] Nil-safe and concurrency-safe by default
- [ ] Timestamps use `time.Now().UTC()`
- [ ] Public API contracts preserved (or `BREAKING CHANGE` flagged above)
- [ ] Single-tenant and multi-tenant modes both considered
- [ ] Storage shape invariants respected (no `tenant_id` column/field)
- [ ] Structured logging only via `lib-observability/log` (`Log(ctx, level, msg, fields...)`)
- [ ] No high-cardinality telemetry labels introduced
- [ ] Exported docs match behavior (godoc + CLAUDE.md if user-facing)

## Related Issues

Closes #
