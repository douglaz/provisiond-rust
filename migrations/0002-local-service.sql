-- STO-12/13: offline forward release; preserves release 1 meaning.
-- STO-34/36/47, API-35/57: credentials, status, and automatic assignment.
CREATE TABLE provisiond.enrolments(handle uuid PRIMARY KEY,tenant_id text NOT NULL UNIQUE,
 created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),expires_at timestamptz NOT NULL);
CREATE TABLE provisiond.provider_accounts(account text PRIMARY KEY,assignable boolean NOT NULL,
 descriptor jsonb NOT NULL CHECK(jsonb_typeof(descriptor)='object'));
CREATE TABLE provisiond.provider_account_status(provider_account text PRIMARY KEY REFERENCES provisiond.provider_accounts,
 status text NOT NULL DEFAULT 'healthy' CHECK(status IN ('healthy','account_unreachable','credentials_rejected','terminated')),
 source text NOT NULL DEFAULT 'configuration',observed_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 operator_ref text,updated_at timestamptz NOT NULL DEFAULT transaction_timestamp());
CREATE FUNCTION provisiond.initialize_account_status() RETURNS trigger LANGUAGE plpgsql SET search_path=pg_catalog AS $$
BEGIN INSERT INTO provisiond.provider_account_status(provider_account) VALUES(NEW.account) ON CONFLICT DO NOTHING; RETURN NEW; END $$;
CREATE TRIGGER account_configured AFTER INSERT ON provisiond.provider_accounts FOR EACH ROW EXECUTE FUNCTION provisiond.initialize_account_status();
CREATE TABLE provisiond.tenant_provider_accounts(tenant_id text NOT NULL,provider_account text NOT NULL REFERENCES provisiond.provider_accounts,
 assigned_at timestamptz NOT NULL DEFAULT transaction_timestamp(),policy_version text NOT NULL,PRIMARY KEY(tenant_id,provider_account));
CREATE TABLE provisiond.offer_listings(provider_account text PRIMARY KEY REFERENCES provisiond.provider_accounts,
 offers jsonb NOT NULL,read_at timestamptz NOT NULL);
CREATE TABLE provisiond.channel_closures(provider_account text NOT NULL,channel text NOT NULL,closed_until timestamptz NOT NULL,PRIMARY KEY(provider_account,channel));
CREATE TABLE provisiond.rates(currency text PRIMARY KEY,value jsonb NOT NULL,available boolean NOT NULL);
CREATE TABLE provisiond.funding_health(singleton boolean PRIMARY KEY CHECK(singleton),solvency_halt boolean NOT NULL);
INSERT INTO provisiond.funding_health VALUES(true,false);
-- SEC-39/STO-21: increments share the guarded request's transaction.
CREATE TABLE provisiond.ceiling_counters(principal text NOT NULL,ceiling text NOT NULL,interval_start bigint NOT NULL,
 used bigint NOT NULL CHECK(used>=0),PRIMARY KEY(principal,ceiling,interval_start));
CREATE SEQUENCE provisiond.deposit_derivation_index;
ALTER TABLE provisiond.deposits ADD COLUMN invoice text, ADD COLUMN lightning_cancelled boolean NOT NULL DEFAULT false;
ALTER TABLE provisiond.operations ADD COLUMN correlator_kind text,ADD COLUMN correlator_value text,
 ADD COLUMN admitted_sats bigint,ADD COLUMN requested_by text NOT NULL DEFAULT 'caller',ADD COLUMN system_reason text,
 ADD COLUMN episode_id uuid;
CREATE UNIQUE INDEX operation_correlator ON provisiond.operations(provider_account,correlator_kind,correlator_value) WHERE correlator_value IS NOT NULL;
ALTER TABLE provisiond.machines ADD COLUMN name text,ADD COLUMN region text,ADD COLUMN public_ips jsonb NOT NULL DEFAULT '[]',
 ADD COLUMN state_observed_at timestamptz, ADD COLUMN runway_until timestamptz,
 ADD COLUMN customer_rate_num bigint,ADD COLUMN customer_rate_den bigint,ADD COLUMN protected_sats bigint,
 ADD COLUMN meter_seed_at timestamptz;
-- OPS-27: retained cancellation obligation for a suspended attach, never a fake completion.
CREATE TABLE provisiond.episodes(id uuid PRIMARY KEY DEFAULT gen_random_uuid(),machine_id uuid NOT NULL REFERENCES provisiond.machines,
 key text NOT NULL,reasons jsonb NOT NULL,state text NOT NULL,created_at timestamptz NOT NULL DEFAULT transaction_timestamp());
CREATE UNIQUE INDEX episode_open ON provisiond.episodes(machine_id,key) WHERE state='open';
-- STO-35: response snapshots can be purged, fingerprints cannot silently disappear.
CREATE TRIGGER receipts_retained BEFORE DELETE OR TRUNCATE ON provisiond.idempotency_records FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();
GRANT SELECT,INSERT,UPDATE ON ALL TABLES IN SCHEMA provisiond TO app;
REVOKE UPDATE ON provisiond.ledger_entries, provisiond.tenant_identities FROM app;
GRANT USAGE ON SEQUENCE provisiond.deposit_derivation_index TO app;
GRANT DELETE ON provisiond.tenants,provisiond.enrolments,provisiond.operations TO app;
-- WIR-1/10/11: UTC seconds and explicit public projections; raw identities stay private.
CREATE FUNCTION provisiond.wire_time(t timestamptz) RETURNS text LANGUAGE sql IMMUTABLE STRICT SET search_path=pg_catalog
AS $$ SELECT to_char(t AT TIME ZONE 'UTC','YYYY-MM-DD"T"HH24:MI:SS"Z"') $$;
CREATE VIEW provisiond.operation_views AS SELECT o.id,o.tenant_id,o.created_at,
 jsonb_build_object('id',o.id,'kind',CASE WHEN o.kind='create' THEN 'create_machine' WHEN o.kind='delete' THEN 'delete_machine' ELSE o.kind END,
 'tenant',o.tenant_id,'status',o.status,'terminal',o.status IN ('succeeded','failed'),'revision',o.revision,
 'retryable',CASE WHEN o.status='needs_reconciliation' THEN false ELSE COALESCE((o.error->>'retryable')::boolean,false) END,
 'requested_by',o.requested_by,'system_reason',o.system_reason,'episode_id',o.episode_id,
 'machine_id',o.machine_id,'provider_account',o.provider_account,'idempotency_key',o.idempotency_key,
 'committed_sats',COALESCE(o.admitted_sats,c.reserved_sats),'result',CASE WHEN o.status='succeeded' AND o.kind='create' THEN jsonb_build_object('machine_id',o.machine_id) ELSE o.result END,
 'error',o.error,'created_at',provisiond.wire_time(o.created_at),'updated_at',provisiond.wire_time(o.updated_at),
 'correlation_id',o.correlation_id) || CASE WHEN o.status IN ('succeeded','failed') THEN '{}'::jsonb ELSE jsonb_build_object('poll_after_ms',CASE WHEN o.status='queued' THEN 1000 ELSE 2000 END) END AS body
 FROM provisiond.operations o LEFT JOIN provisiond.commitments c ON c.id=o.commitment_id;
CREATE VIEW provisiond.machine_views AS SELECT m.id,m.tenant_id,m.created_at,
 jsonb_build_object('id',m.id,'name',m.name,'kind',m.kind,'state',m.state,'state_observed_at',provisiond.wire_time(m.state_observed_at),
 'region',m.region,'public_ips',m.public_ips,'provider_account',m.provider_account,
 'committed_sats',COALESCE(c.reserved_sats,0),'runway_until',provisiond.wire_time(m.runway_until),
 'network_restriction',jsonb_build_object('status','unknown','source',NULL,'observed_at',NULL),
 'install_strategies',m.install_strategies,'last_install',NULL,'rate_outage_deadline',NULL,
 'effective_cancellation_date',NULL,'earliest_cancellation_date',provisiond.wire_time(m.earliest_cancellation_date),
 'created_at',provisiond.wire_time(m.created_at),'updated_at',provisiond.wire_time(m.updated_at)) AS body
 FROM provisiond.machines m LEFT JOIN provisiond.commitments c ON c.machine_id=m.id AND c.state='open';
GRANT SELECT ON provisiond.operation_views,provisiond.machine_views TO app;
-- LDG-72: only the first-seed subset; ongoing increments remain unsupported.
CREATE TABLE provisiond.meter_totals(subject_kind text NOT NULL,subject_id uuid NOT NULL,billing_period text NOT NULL,
 high_water_mark timestamptz NOT NULL,PRIMARY KEY(subject_kind,subject_id,billing_period));
GRANT SELECT,INSERT,UPDATE ON provisiond.meter_totals TO app;
ALTER TABLE provisiond.operations DROP CONSTRAINT operations_kind_check;
ALTER TABLE provisiond.operations ADD CHECK(kind IN ('create','refresh','power','delete'));
