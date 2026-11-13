-- STO-12/13: offline forward release; retain the meaning of earlier releases.
-- STO-3/19: resolution is made by no worker and its evidence is write-once.
ALTER TABLE provisiond.operations
 ADD COLUMN resolution text CHECK(resolution IN ('observed','absent','applied','not_applied','abandoned')),
 ADD COLUMN resolved_at timestamptz,
 ADD COLUMN resolved_by text,
 ADD COLUMN resolution_evidence jsonb,
 ADD COLUMN mutation_wait_ended_at timestamptz,
 ADD COLUMN resolution_attempted_at timestamptz;
ALTER TABLE provisiond.operations ADD CHECK
 ((resolution IS NULL) = (resolved_at IS NULL) AND
  (resolution IS NULL) = (resolved_by IS NULL) AND
  (resolution IS NULL) = (resolution_evidence IS NULL));
-- OPS-23/35/45: schema 2 queued suspended-attach cleanups retained only '{}'.
-- Upgrade that exact supported system shape from its authoritative linked
-- machine. Mismatched ownership/accounts and unrelated malformed rows are not
-- repaired into dispatchable operations.
UPDATE provisiond.operations o
 SET provider_account=m.provider_account,
 request_summary=jsonb_build_object('kind','delete','machine_id',m.id,
   'provider_account',m.provider_account,'external_id',m.external_id)
 FROM provisiond.machines m,provisiond.provider_accounts a
 WHERE o.kind='delete' AND o.status IN ('queued','running')
 AND o.requested_by='system' AND o.system_reason='tenant_suspended'
 AND o.episode_id IS NOT NULL AND o.request_summary='{}'
 AND m.id=o.machine_id AND m.tenant_id=o.tenant_id
 AND (o.provider_account IS NULL OR o.provider_account=m.provider_account)
 AND a.account=m.provider_account
 AND EXISTS(SELECT 1 FROM provisiond.episodes e WHERE e.id=o.episode_id
   AND e.machine_id=m.id AND e.key='delete' AND e.state='open');
CREATE FUNCTION provisiond.retain_resolution() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$ BEGIN
 IF OLD.resolution IS NOT NULL AND
    ROW(NEW.resolution,NEW.resolved_at,NEW.resolved_by,NEW.resolution_evidence)
    IS DISTINCT FROM ROW(OLD.resolution,OLD.resolved_at,OLD.resolved_by,OLD.resolution_evidence)
 THEN RAISE EXCEPTION 'resolution evidence is write-once'; END IF;
 RETURN NEW;
END $$;
CREATE TRIGGER resolution_retained BEFORE UPDATE ON provisiond.operations
 FOR EACH ROW EXECUTE FUNCTION provisiond.retain_resolution();
-- STO-53/PRV-36: append-only dispatch-to-observation samples, not clock origins.
CREATE TABLE provisiond.provider_observations (
 provider_account text NOT NULL, operation_id uuid PRIMARY KEY,
 dispatched_at timestamptz NOT NULL, observed_at timestamptz NOT NULL,
 CHECK(observed_at>=dispatched_at));
CREATE INDEX provider_observation_account ON provisiond.provider_observations(provider_account);
CREATE TRIGGER observations_retained BEFORE UPDATE OR DELETE OR TRUNCATE ON provisiond.provider_observations
 FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();
GRANT SELECT,INSERT ON provisiond.provider_observations TO app;
-- STO-52/OPS-48: upgrade the retained placeholder obligation without re-enqueue.
DROP INDEX provisiond.episode_open;
ALTER TABLE provisiond.episodes RENAME COLUMN created_at TO opened_at;
ALTER TABLE provisiond.episodes ADD COLUMN current_operation_id uuid,
 ADD COLUMN closed_at timestamptz, ADD COLUMN close_reason text,
 ADD COLUMN kept_by text, ADD COLUMN keep_evidence jsonb;
UPDATE provisiond.episodes SET state='attempting' WHERE state='open';
UPDATE provisiond.episodes e SET current_operation_id=(
 SELECT o.id FROM provisiond.operations o WHERE o.episode_id=e.id
 ORDER BY o.created_at DESC,o.id DESC LIMIT 1);
ALTER TABLE provisiond.episodes ADD CHECK(state IN ('attempting','uncertain','stalled','scheduled','closed')),
 ADD CHECK((state='closed')=(closed_at IS NOT NULL)),
 ADD CHECK((state='closed')=(close_reason IS NOT NULL)),
 ADD CHECK(close_reason IN ('resource_gone','funded','abandoned','kept')),
 ADD CHECK((close_reason IS NOT DISTINCT FROM 'kept')=(kept_by IS NOT NULL)),
 ADD CHECK((kept_by IS NULL)=(keep_evidence IS NULL));
CREATE UNIQUE INDEX episode_open ON provisiond.episodes(machine_id,key) WHERE state<>'closed';
CREATE INDEX episode_state ON provisiond.episodes(state);
CREATE INDEX operation_episode ON provisiond.operations(episode_id);
ALTER TABLE provisiond.machines ADD COLUMN destroy_committed uuid REFERENCES provisiond.episodes,
 ADD COLUMN meter_stopped_at timestamptz;
-- LDG-67: the accepted snapshot remains the sole amount source after release.
CREATE VIEW provisiond.pending_order_obligations AS
 SELECT id AS operation_id,provider_account,request_summary->'setup_fee' AS setup_fee,
 request_summary->'first_unit' AS first_unit FROM provisiond.operations
 WHERE kind='create' AND status='needs_reconciliation' AND resolution IS NULL;
GRANT SELECT ON provisiond.pending_order_obligations TO app;
-- STO-37: a charged but absent order has no machine subject.
ALTER TABLE provisiond.operator_deficiencies DROP CONSTRAINT operator_deficiencies_subject_kind_check;
ALTER TABLE provisiond.operator_deficiencies ADD CHECK(subject_kind IN ('machine','operation'));
ALTER TABLE provisiond.operator_deficiencies ALTER COLUMN machine_id DROP NOT NULL;
CREATE VIEW provisiond.episode_views AS SELECT e.id,m.tenant_id,e.opened_at,e.state,
 jsonb_build_object('id',e.id,'machine_id',e.machine_id,'key',e.key,'reasons',e.reasons,
 'state',e.state,'opened_at',provisiond.wire_time(e.opened_at),'current_operation_id',e.current_operation_id,
 'closed_at',provisiond.wire_time(e.closed_at),'close_reason',e.close_reason,
 'kept_by',e.kept_by,'keep_evidence',e.keep_evidence) AS body
 FROM provisiond.episodes e JOIN provisiond.machines m ON m.id=e.machine_id;
GRANT SELECT ON provisiond.episode_views TO app;
-- LDG-38/72: exact exit increments, including no-entry tails and period splits.
ALTER TABLE provisiond.meter_totals ADD COLUMN credit_num numeric NOT NULL DEFAULT 0,
 ADD COLUMN credit_den numeric NOT NULL DEFAULT 1 CHECK(credit_den>0);
ALTER TABLE provisiond.machines ADD COLUMN native_terms jsonb,
 ADD COLUMN meter_quarantined boolean NOT NULL DEFAULT false;
UPDATE provisiond.machines m SET native_terms=o.request_summary->'offer'->'accepted_terms'
 FROM provisiond.operations o WHERE o.machine_id=m.id AND o.kind='create';
-- LDG-72: older rows without retained pricing evidence can still be deleted,
-- but cannot invent a usage charge or meter history during this upgrade.
UPDATE provisiond.machines SET meter_quarantined=true WHERE native_terms IS NULL;
-- Only fixtures supply rates in this profile. Retain their acceptance boundaries
-- so an exit cannot reprice earlier elapsed time using the last fixture value.
CREATE TABLE provisiond.fixture_rate_history (
 sequence bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,currency text NOT NULL,
 accepted_at timestamptz NOT NULL DEFAULT clock_timestamp(),value jsonb NOT NULL,available boolean NOT NULL);
INSERT INTO provisiond.fixture_rate_history(currency,value,available)
 SELECT currency,value,available FROM provisiond.rates;
CREATE FUNCTION provisiond.retain_fixture_rate() RETURNS trigger LANGUAGE plpgsql
SET search_path=pg_catalog AS $$ BEGIN
 INSERT INTO provisiond.fixture_rate_history(currency,value,available) VALUES(NEW.currency,NEW.value,NEW.available);
 RETURN NEW;
END $$;
CREATE TRIGGER fixture_rate_changed AFTER INSERT OR UPDATE ON provisiond.rates
 FOR EACH ROW EXECUTE FUNCTION provisiond.retain_fixture_rate();
GRANT SELECT,INSERT ON provisiond.fixture_rate_history TO app;
GRANT USAGE ON SEQUENCE provisiond.fixture_rate_history_sequence_seq TO app;
ALTER TABLE provisiond.operator_deficiencies DROP CONSTRAINT operator_deficiencies_absorbed_seconds_check;
ALTER TABLE provisiond.operator_deficiencies ADD CHECK(absorbed_seconds>=0 AND (cause='rate_outage' OR absorbed_seconds=0)),
 ADD COLUMN absorbed_from timestamptz,ADD COLUMN absorbed_until timestamptz;

-- LDG-38: a usage entry locates the period/increment it prices even when posted later.
ALTER TABLE provisiond.ledger_entries ADD COLUMN billing_period text,
 ADD COLUMN increment_end timestamptz;
-- LDG-72: an explicit audit runs after the offline migration, before serving.
-- It is never called by a meter posting. Imports/restores remain refused.
CREATE TABLE provisiond.meter_alerts (
 machine_id uuid PRIMARY KEY,reason text NOT NULL,observed_at timestamptz NOT NULL DEFAULT clock_timestamp());
GRANT SELECT,INSERT ON provisiond.meter_alerts TO app;
CREATE FUNCTION provisiond.usage_increment_end(key text) RETURNS timestamptz LANGUAGE plpgsql STABLE
SET search_path=pg_catalog AS $$ BEGIN
 RETURN substring(key FROM '^usage:machine:[^:]+:[^:]+:(.+)$')::timestamptz;
 EXCEPTION WHEN invalid_datetime_format OR datetime_field_overflow THEN RETURN NULL;
END $$;
CREATE FUNCTION provisiond.audit_machine_meters() RETURNS bigint LANGUAGE plpgsql
SET search_path=pg_catalog AS $$ DECLARE affected bigint; BEGIN
 WITH invalid AS (
  SELECT m.id FROM provisiond.machines m WHERE m.native_terms IS NULL
   OR NOT EXISTS(SELECT 1 FROM provisiond.meter_totals t WHERE t.subject_kind='machine' AND t.subject_id=m.id)
   OR EXISTS(SELECT 1 FROM provisiond.meter_totals t WHERE t.subject_kind='machine' AND t.subject_id=m.id AND (t.credit_num<0 OR t.credit_num>=t.credit_den))
   OR EXISTS(SELECT 1 FROM provisiond.ledger_entries l LEFT JOIN provisiond.meter_totals t
     ON t.subject_kind='machine' AND t.subject_id=l.machine_id AND t.billing_period=split_part(l.idempotency_key,':',4)
     WHERE l.machine_id=m.id AND l.kind='usage_debit' AND (t.high_water_mark IS NULL OR provisiond.usage_increment_end(l.idempotency_key) IS NULL OR t.high_water_mark<provisiond.usage_increment_end(l.idempotency_key)))
 ), quarantined AS (UPDATE provisiond.machines SET meter_quarantined=true WHERE id IN (SELECT id FROM invalid) RETURNING id)
 INSERT INTO provisiond.meter_alerts(machine_id,reason) SELECT id,'migration/import audit: missing evidence, range or high-water failure' FROM quarantined
 ON CONFLICT(machine_id) DO NOTHING;
 GET DIAGNOSTICS affected=ROW_COUNT; RETURN affected;
END $$;
SELECT provisiond.audit_machine_meters();
GRANT EXECUTE ON FUNCTION provisiond.audit_machine_meters() TO app;
-- Keep the exact credit fraction reduced rather than multiplying its denominator
-- on every piece. Neither value is a reconstruction of cumulative ledger totals.
CREATE FUNCTION provisiond.reduced_credit(n numeric,d numeric) RETURNS text[] LANGUAGE plpgsql IMMUTABLE
SET search_path=pg_catalog AS $$ DECLARE a numeric:=abs(n); b numeric:=d; r numeric; BEGIN
 WHILE b<>0 LOOP r=mod(a,b);a=b;b=r;END LOOP;
 RETURN ARRAY[(div(n,a))::text,(div(d,a))::text];
END $$;
GRANT EXECUTE ON FUNCTION provisiond.reduced_credit(numeric,numeric) TO app;
