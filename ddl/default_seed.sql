-- systemplane universal default seed — published by lib-systemplane.
--
-- A neutral baseline for the `runtime_config` namespace that becomes every
-- consumer's default. Values are intentionally NEUTRAL (no app-specific
-- tuning, no dev origins). Every row uses
-- ON CONFLICT (namespace, "key") DO NOTHING so operator-set values are never
-- overwritten on re-application.
--
-- JSONB encoding mirrors the lib's runtime seedDefaults (json.Marshal):
--   string -> '"x"'::jsonb   bool -> 'true'/'false'::jsonb   int -> '5'::jsonb

INSERT INTO systemplane_entries (namespace, "key", value, updated_at, updated_by)
VALUES
	('runtime_config', 'app.log_level',                        '"info"'::jsonb,                                   now(), 'systemplane.manager'),
	('runtime_config', 'cors.allowed_origins',                 '""'::jsonb,                                        now(), 'systemplane.manager'),
	('runtime_config', 'cors.allowed_methods',                 '"GET,POST,PUT,PATCH,DELETE,OPTIONS"'::jsonb,       now(), 'systemplane.manager'),
	('runtime_config', 'cors.allowed_headers',                 '"Origin,Content-Type,Accept,Authorization"'::jsonb, now(), 'systemplane.manager'),
	('runtime_config', 'rate_limit.enabled',                   'false'::jsonb,                                     now(), 'systemplane.manager'),
	('runtime_config', 'rate_limit.max',                       '100'::jsonb,                                       now(), 'systemplane.manager'),
	('runtime_config', 'rate_limit.expiry_sec',                '60'::jsonb,                                        now(), 'systemplane.manager'),
	('runtime_config', 'idempotency.require_redis',            'false'::jsonb,                                     now(), 'systemplane.manager'),
	('runtime_config', 'idempotency.duplicate_guard_ttl_seconds', '300'::jsonb,                                   now(), 'systemplane.manager')
ON CONFLICT (namespace, "key") DO NOTHING;
