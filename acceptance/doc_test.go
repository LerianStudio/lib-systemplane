// Package acceptance drives the public root API against live Postgres and
// MongoDB through the single-tenant audit scenarios; the multi-tenant ones run
// on the tenant harness in internal/client. Untagged so go list sees the package.
package acceptance
