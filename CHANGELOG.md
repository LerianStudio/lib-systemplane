# Changelog

All notable changes to this project will be documented in this file.

## [1.0.0] - 2026-05-18

### Breaking Changes

- Promote standalone `lib-systemplane` to the stable v1 release line.
- Migrate the public observability surface from `lib-commons/v5` to `lib-observability`.
- `WithLogger` now accepts `lib-observability/log.Logger`.
- `WithTelemetry` now accepts `*lib-observability/tracing.Telemetry`.
- Subscriber panic recovery now uses `lib-observability/runtime`.

### Changed

- Update the minimum Go version to `1.26.3`.
- Keep `lib-commons/v5` for non-observability primitives: tenant context, admin HTTP helpers, and backoff.

## [0.1.0] - 2026-04-21

Initial extraction from lib-commons v5.0.2. First standalone release.
