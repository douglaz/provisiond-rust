-- STO-12/13: offline transactional release. Stop all older components and their
-- sessions before applying; do not restart them. No rollback past this contract.
-- Fresh schema only: no legacy about_to_order_at values exist to backfill.
-- STO-59: exactly one UUID, protected from deletion and truncation.
CREATE TABLE provisiond.startup_generation (
    singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
    generation uuid NOT NULL DEFAULT gen_random_uuid()
);
INSERT INTO provisiond.startup_generation DEFAULT VALUES;
CREATE FUNCTION provisiond.forbid_removal() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$ BEGIN RAISE EXCEPTION 'retained state cannot be removed'; END $$;
CREATE TRIGGER generation_retained BEFORE DELETE OR TRUNCATE ON provisiond.startup_generation
FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();

-- DOM-1: identities are server-minted, random, permanent even after tenant purge.
CREATE TABLE provisiond.tenant_identities (id text PRIMARY KEY DEFAULT gen_random_uuid()::text);
CREATE TRIGGER identities_retained BEFORE UPDATE OR DELETE OR TRUNCATE ON provisiond.tenant_identities
FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();
CREATE TABLE provisiond.tenants (
    id text PRIMARY KEY REFERENCES provisiond.tenant_identities,
    credential_digest text NOT NULL, recovery_digest text NOT NULL,
    credential_generation bigint NOT NULL DEFAULT 0,
    status text NOT NULL CHECK(status IN ('pending','active','suspended')),
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(), activated_at timestamptz
);
-- STO-17: account/external identity is globally unique, including tombstones.
CREATE TABLE provisiond.machines (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id text NOT NULL,
    provider_account text NOT NULL, external_id text NOT NULL,
    offer_id text NOT NULL, kind text NOT NULL CHECK(kind IN ('virtual','bare_metal')),
    state text NOT NULL, install_strategies jsonb NOT NULL DEFAULT '[]',
    billing_unit_seconds bigint NOT NULL CHECK(billing_unit_seconds>=0),
    declared_cancellation_bound timestamptz, earliest_cancellation_date timestamptz,
    first_unit_debit_id uuid, metadata jsonb NOT NULL DEFAULT '{}',
    correlator_kind text, correlator_value text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE(provider_account,external_id)
);
-- STO-3/9/10/50/51: only the represented operation kinds can enter this slice.
CREATE TABLE provisiond.operations (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id text NOT NULL,
    idempotency_key text NOT NULL, kind text NOT NULL CHECK(kind IN ('create','refresh','power')),
    status text NOT NULL CHECK(status IN ('queued','running','succeeded','failed','needs_reconciliation')),
    machine_id uuid REFERENCES provisiond.machines, provider_account text,
    claim_number bigint NOT NULL DEFAULT 0 CHECK(claim_number>=0),
    available_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    yielded_at timestamptz, about_to_order_at timestamptz, write_started_at timestamptz,
    rescue_exited_cleanly boolean, request jsonb, request_summary jsonb NOT NULL,
    correlation_id text NOT NULL, result jsonb, error jsonb,
    commitment_id uuid, revision bigint NOT NULL DEFAULT 1,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    updated_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    CHECK (kind='create' OR about_to_order_at IS NULL),
    CHECK (status IN ('queued','running') OR request IS NULL),
    CHECK (jsonb_typeof(request_summary)='object' AND
      request_summary - ARRAY['kind','machine_id','provider_account','external_id','correlator',
        'offer','setup_fee','first_unit','write_started_at','rescue_exited_cleanly'] = '{}')
);
CREATE UNIQUE INDEX operation_machine_running ON provisiond.operations(machine_id)
WHERE status='running' AND yielded_at IS NULL;
CREATE INDEX operation_claim ON provisiond.operations(status,available_at,created_at,id);
CREATE INDEX operation_tenant ON provisiond.operations(tenant_id,created_at DESC);
-- STO-27/58, PRV-42: unresolved resource offers have no invented expiry.
CREATE TABLE provisiond.resource_exclusions (
    provider_account text NOT NULL, ordering_channel text NOT NULL, offer_id text NOT NULL,
    operation_id uuid NOT NULL UNIQUE,
    PRIMARY KEY(provider_account,ordering_channel,offer_id)
);
-- LDG-30, STO-23/28: reservations are separate from money entries.
CREATE TABLE provisiond.commitments (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id text NOT NULL,
    machine_id uuid REFERENCES provisiond.machines, operation_id uuid,
    reserved_sats bigint NOT NULL CHECK(reserved_sats>=0),
    state text NOT NULL CHECK(state IN ('open','closed')), version bigint NOT NULL DEFAULT 0,
    opened_at timestamptz NOT NULL DEFAULT transaction_timestamp(), closed_at timestamptz,
    CHECK ((state='closed')=(closed_at IS NOT NULL))
);
CREATE UNIQUE INDEX commitment_machine_open ON provisiond.commitments(machine_id) WHERE state='open';
CREATE INDEX commitment_tenant ON provisiond.commitments(tenant_id,state);
ALTER TABLE provisiond.operations ADD FOREIGN KEY(commitment_id) REFERENCES provisiond.commitments;
-- STO-26/29: no tenant FK, no tenant cascade, bindings outlive tenant deletion.
CREATE TABLE provisiond.deposits (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id text NOT NULL,
    requested_sats bigint NOT NULL CHECK(requested_sats>0),
    payment_hash text NOT NULL UNIQUE, address text NOT NULL UNIQUE,
    derivation_index bigint NOT NULL UNIQUE CHECK(derivation_index>=0),
    idempotency_key text NOT NULL, expires_at timestamptz NOT NULL,
    attributed_tenant_id text, created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE(tenant_id,idempotency_key)
);
CREATE TRIGGER deposits_retained BEFORE DELETE OR TRUNCATE ON provisiond.deposits
FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();
-- LDG-1/6/7/8/70, STO-22: signed exact amounts and indexed authoritative balance.
CREATE TABLE provisiond.ledger_entries (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), tenant_id text NOT NULL,
    seq bigint NOT NULL CHECK(seq>0), kind text NOT NULL
      CHECK(kind IN ('topup','usage_debit','setup_fee_debit','operation_fee_debit','first_unit_debit','correction')),
    amount_sats bigint NOT NULL CHECK(amount_sats<>0), balance_after bigint NOT NULL CHECK(balance_after>=0),
    idempotency_key text NOT NULL, operation_id uuid, machine_id uuid, commitment_id uuid,
    deposit_id uuid, native_minor bigint, native_currency text,
    rate_num bigint, rate_den bigint, rate_source text, rate_observed_at timestamptz,
    haircut_bps bigint, rounding_rule_version text,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    UNIQUE(tenant_id,idempotency_key), UNIQUE(tenant_id,seq),
    CHECK(kind<>'topup' OR deposit_id IS NOT NULL)
);
CREATE INDEX ledger_latest ON provisiond.ledger_entries(tenant_id,seq DESC);
CREATE TRIGGER ledger_append_only BEFORE UPDATE OR DELETE OR TRUNCATE ON provisiond.ledger_entries
FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();
ALTER TABLE provisiond.machines ADD FOREIGN KEY(first_unit_debit_id) REFERENCES provisiond.ledger_entries;
-- STO-30/31/46: payment identity, actual value, and exactly one linked credit.
CREATE TABLE provisiond.payments (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), deposit_id uuid NOT NULL REFERENCES provisiond.deposits,
    rail text NOT NULL CHECK(rail IN ('lightning','onchain')), payment_ref text NOT NULL UNIQUE,
    amount_sats bigint NOT NULL CHECK(amount_sats>0),
    ledger_entry_id uuid NOT NULL UNIQUE REFERENCES provisiond.ledger_entries,
    credited_tenant_id text NOT NULL, observed_at timestamptz NOT NULL, settled_at timestamptz NOT NULL,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
CREATE INDEX payment_deposit ON provisiond.payments(deposit_id);
-- STO-4/35: only principal scope owns receipt uniqueness, never operations.
CREATE TABLE provisiond.idempotency_records (
    scope_kind text NOT NULL CHECK(scope_kind IN ('tenant','operator')),
    scope_id text NOT NULL, key text NOT NULL, fingerprint text NOT NULL,
    resource_kind text NOT NULL, resource_id uuid NOT NULL, status integer NOT NULL,
    response_body jsonb NOT NULL, created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
    expires_at timestamptz NOT NULL, PRIMARY KEY(scope_kind,scope_id,key)
);
-- LDG-31: represented clamp overflow remains an operator cost.
CREATE TABLE provisiond.operator_deficiencies (
    id uuid PRIMARY KEY DEFAULT gen_random_uuid(), provider_account text NOT NULL,
    machine_id uuid NOT NULL, operation_id uuid NOT NULL, cause text NOT NULL,
    native_minor bigint NOT NULL, currency text NOT NULL, clamped_sats bigint NOT NULL,
    rate_num bigint, rate_den bigint, idempotency_key text NOT NULL UNIQUE,
    created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
);
-- Recovery incidents are not implemented. Their presence blocks ordinary startup.
CREATE TABLE provisiond.recovery_incidents (id uuid PRIMARY KEY, closed_at timestamptz);
-- LDG-70: durable fail-closed result of the explicit ledger audit path.
CREATE TABLE provisiond.spending_health(singleton boolean PRIMARY KEY CHECK(singleton),ledger_valid boolean NOT NULL);
INSERT INTO provisiond.spending_health VALUES(true,true);
CREATE TRIGGER spending_health_retained BEFORE DELETE OR TRUNCATE ON provisiond.spending_health
FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();
GRANT USAGE ON SCHEMA provisiond TO app;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA provisiond TO app;
REVOKE UPDATE ON provisiond.ledger_entries, provisiond.tenant_identities FROM app;

-- STO-58: known outcomes release their resource exclusions. Money history is retained.
GRANT DELETE ON provisiond.resource_exclusions TO app;
