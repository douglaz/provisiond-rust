-- STO-54/56: offline forward release; preserve unsupported placeholder history.
ALTER TABLE provisiond.recovery_incidents
 ADD COLUMN legacy boolean NOT NULL DEFAULT true,
 ADD COLUMN restore_instant timestamptz,
 ADD COLUMN steps_completed integer NOT NULL DEFAULT 0 CHECK (steps_completed BETWEEN 0 AND 3),
 ADD COLUMN grace_ends_at timestamptz,
 ADD COLUMN rederivation_seconds bigint CHECK (rederivation_seconds > 0),
 ADD COLUMN derivation_gap bigint CHECK (derivation_gap > 0),
 ADD COLUMN lost_window_starts_at timestamptz,
 ADD COLUMN loss_end_at timestamptz,
 ADD COLUMN history_evidence_ref text,
 ADD COLUMN history_evidence jsonb,
 ADD COLUMN supersedes_incident_id uuid REFERENCES provisiond.recovery_incidents,
 ADD CONSTRAINT recovery_established CHECK (legacy OR
   (restore_instant IS NOT NULL AND isfinite(restore_instant) AND
    history_evidence_ref IS NOT NULL AND history_evidence IS NOT NULL AND
    rederivation_seconds IS NOT NULL AND derivation_gap IS NOT NULL)),
 ADD CONSTRAINT recovery_grace_mark CHECK ((steps_completed = 3) = (grace_ends_at IS NOT NULL)),
 ADD CONSTRAINT recovery_loss_bounds CHECK (
   (lost_window_starts_at IS NULL OR isfinite(lost_window_starts_at)) AND
   (loss_end_at IS NULL OR isfinite(loss_end_at)) AND
   (lost_window_starts_at IS NULL OR loss_end_at IS NULL OR lost_window_starts_at <= loss_end_at));
ALTER TABLE provisiond.recovery_incidents ALTER COLUMN legacy SET DEFAULT false;
CREATE UNIQUE INDEX recovery_one_open ON provisiond.recovery_incidents ((true)) WHERE closed_at IS NULL;
CREATE TRIGGER recovery_retained BEFORE DELETE OR TRUNCATE ON provisiond.recovery_incidents
 FOR EACH STATEMENT EXECUTE FUNCTION provisiond.forbid_removal();

-- STO-54: metadata-only representation of deferred workers and waiting parents.
ALTER TABLE provisiond.operations DROP CONSTRAINT operations_kind_check;
ALTER TABLE provisiond.operations ADD CONSTRAINT operations_kind_check
 CHECK (kind IN ('create','refresh','power','delete','install','rescue_inventory','suspend_tenant'));
ALTER TABLE provisiond.operations ADD COLUMN IF NOT EXISTS restore_markers_untrusted boolean;
UPDATE provisiond.operations SET restore_markers_untrusted = kind IN ('install','rescue_inventory')
 WHERE restore_markers_untrusted IS NULL;
ALTER TABLE provisiond.operations ALTER COLUMN restore_markers_untrusted SET NOT NULL;
ALTER TABLE provisiond.operations ALTER COLUMN restore_markers_untrusted SET DEFAULT false;
CREATE FUNCTION provisiond.preserve_restore_provenance() RETURNS trigger LANGUAGE plpgsql
 SET search_path=pg_catalog AS $$ BEGIN
 IF OLD.restore_markers_untrusted AND NOT NEW.restore_markers_untrusted THEN
   RAISE EXCEPTION 'STO-54 restore marker provenance is irreversible';
 END IF;
 RETURN NEW;
 END $$;
CREATE TRIGGER restore_provenance BEFORE UPDATE ON provisiond.operations
 FOR EACH ROW EXECUTE FUNCTION provisiond.preserve_restore_provenance();
ALTER TABLE provisiond.operations ADD COLUMN rescue_address text, ADD COLUMN rescue_port integer;
-- WIR-9a: render from durable provenance, including already-settled errors.
-- No marker metadata is added to the closed request-summary enumeration.
ALTER VIEW provisiond.operation_views RENAME TO operation_views_before_recovery;
CREATE VIEW provisiond.operation_views AS
 SELECT v.id,v.tenant_id,v.created_at,
 CASE WHEN o.kind IN ('install','rescue_inventory') AND o.error IS NOT NULL THEN
 jsonb_set(v.body,'{error,details}',COALESCE(o.error->'details','{}'::jsonb) ||
 CASE WHEN o.kind='install' THEN jsonb_build_object('disk_effect',
 CASE WHEN o.restore_markers_untrusted THEN 'unknown'
 WHEN o.write_started_at IS NOT NULL THEN 'destructive_boundary_crossed' ELSE 'none' END)
 ELSE '{}'::jsonb END ||
 jsonb_build_object('rescue_exit',CASE WHEN o.restore_markers_untrusted THEN 'unknown'
 WHEN o.rescue_exited_cleanly THEN 'clean' WHEN o.rescue_exited_cleanly=false THEN 'unknown' ELSE 'none' END) ||
 CASE WHEN o.restore_markers_untrusted OR o.rescue_exited_cleanly=false THEN
 jsonb_build_object('rescue_address',COALESCE(o.rescue_address,o.error->'details'->>'rescue_address'),
 'rescue_port',COALESCE(to_jsonb(o.rescue_port),o.error->'details'->'rescue_port')) ELSE '{}'::jsonb END)
 ELSE v.body END AS body
 FROM provisiond.operation_views_before_recovery v JOIN provisiond.operations o USING(id);
GRANT SELECT ON provisiond.operation_views TO app;
CREATE TABLE provisiond.recovery_waiting_parents (
 incident_id uuid NOT NULL REFERENCES provisiond.recovery_incidents,
 -- STO-14/25: retain the historical identity/disposition after the settled
 -- operation retires. The operation is not the lifetime owner of this record.
 operation_id uuid NOT NULL,
 disposition text CHECK (disposition IN ('confirmed','cancelled')),
 PRIMARY KEY (incident_id,operation_id)
);
CREATE TABLE provisiond.recovery_account_passes (
 incident_id uuid NOT NULL REFERENCES provisiond.recovery_incidents,
 provider_account text NOT NULL REFERENCES provisiond.provider_accounts,
 first_completed_at timestamptz NOT NULL,
 second_completed_at timestamptz,
 PRIMARY KEY (incident_id,provider_account)
);

-- API-56/STO-54: a digest minted before the incident cannot authenticate afterward.
ALTER TABLE provisiond.tenants ADD COLUMN issued_credential_generation bigint;
UPDATE provisiond.tenants SET issued_credential_generation = credential_generation;
ALTER TABLE provisiond.tenants ALTER COLUMN issued_credential_generation SET NOT NULL;
ALTER TABLE provisiond.tenants ALTER COLUMN issued_credential_generation SET DEFAULT 0;
CREATE TABLE provisiond.credential_replacements (
 tenant_id text NOT NULL, key text NOT NULL, fingerprint text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
 PRIMARY KEY (tenant_id,key)
);
GRANT SELECT,INSERT,UPDATE ON provisiond.recovery_waiting_parents,provisiond.recovery_account_passes,
 provisiond.credential_replacements TO app;
GRANT SELECT,UPDATE ON SEQUENCE provisiond.deposit_derivation_index TO app;

-- STO-49, LDG-58/59: one retained history for controlled simulator rate
-- inputs, current reads, exit pricing and paused restore grace. These inputs are
-- already computed fixture verdicts, not a production source/window estimator.
ALTER TABLE provisiond.fixture_rate_history
 ADD COLUMN acceptance_order bigint,
 ADD COLUMN observed_at timestamptz,
 ADD COLUMN staleness_bound_seconds bigint NOT NULL DEFAULT 3600 CHECK (staleness_bound_seconds > 0),
 ADD COLUMN window_rate_num bigint,
 ADD COLUMN window_rate_den bigint;
UPDATE provisiond.fixture_rate_history SET acceptance_order=sequence,observed_at=accepted_at,
 window_rate_num=CASE WHEN available THEN (value->>'rate_num')::bigint END,
 window_rate_den=CASE WHEN available THEN (value->>'rate_den')::bigint END;
ALTER TABLE provisiond.fixture_rate_history
 ALTER COLUMN acceptance_order SET NOT NULL,
 ALTER COLUMN observed_at SET NOT NULL,
 ALTER COLUMN value SET DEFAULT '{}'::jsonb,
 ALTER COLUMN available SET DEFAULT false,
 ADD CHECK (acceptance_order > 0),
 ADD CHECK ((window_rate_num IS NULL) = (window_rate_den IS NULL)),
 ADD CHECK (window_rate_num > 0 AND window_rate_den > 0),
 ADD UNIQUE (currency,acceptance_order), ADD UNIQUE (currency,observed_at);
CREATE FUNCTION provisiond.stamp_fixture_rate() RETURNS trigger LANGUAGE plpgsql
 SET search_path=pg_catalog AS $$
 DECLARE previous_order bigint; previous_time timestamptz;
 BEGIN
 -- Transactional per-currency allocation; no clock rollback or equal timestamp
 -- can reorder verdicts. Only the isolated fixtures supply explicit stamps.
 PERFORM pg_advisory_xact_lock(1347572820,hashtext(NEW.currency));
 SELECT max(acceptance_order),max(accepted_at) INTO previous_order,previous_time
 FROM provisiond.fixture_rate_history WHERE currency=NEW.currency;
 NEW.acceptance_order=COALESCE(NEW.acceptance_order,COALESCE(previous_order,0)+1);
 NEW.accepted_at=greatest(NEW.accepted_at,previous_time);
 NEW.observed_at=COALESCE(NEW.observed_at,NEW.accepted_at);
 IF NEW.value <> '{}'::jsonb THEN
   NEW.window_rate_num=CASE WHEN NEW.available THEN (NEW.value->>'rate_num')::bigint END;
   NEW.window_rate_den=CASE WHEN NEW.available THEN (NEW.value->>'rate_den')::bigint END;
 END IF;
 RETURN NEW;
 END $$;
CREATE TRIGGER fixture_rate_stamped BEFORE INSERT ON provisiond.fixture_rate_history
 FOR EACH ROW EXECUTE FUNCTION provisiond.stamp_fixture_rate();
-- Simple updatable projection: isolated history probes use the same rows as
-- ordinary fixture input changes. There is no second independently fed history.
CREATE VIEW provisiond.rate_observations AS SELECT currency,acceptance_order,
 observed_at,accepted_at,staleness_bound_seconds,window_rate_num,window_rate_den
 FROM provisiond.fixture_rate_history;
GRANT SELECT,INSERT ON provisiond.rate_observations TO app;

CREATE FUNCTION provisiond.fixture_rate_at(c text, t timestamptz)
 RETURNS TABLE(value jsonb,available boolean) LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT v.value || CASE WHEN v.window_rate_num IS NOT NULL THEN
 jsonb_build_object('rate_num',v.window_rate_num,'rate_den',v.window_rate_den) ELSE '{}'::jsonb END,
 COALESCE(v.window_rate_num IS NOT NULL AND f.observed_at+f.staleness_bound_seconds*interval '1 second'>t,false)
 FROM (SELECT * FROM provisiond.fixture_rate_history WHERE currency=c AND accepted_at<=t
 ORDER BY acceptance_order DESC LIMIT 1) v
 CROSS JOIN LATERAL (SELECT observed_at,staleness_bound_seconds FROM provisiond.fixture_rate_history
 WHERE currency=c AND accepted_at<=t ORDER BY observed_at DESC,acceptance_order DESC LIMIT 1) f
 $$;
CREATE FUNCTION provisiond.fixture_rate_boundaries(c text) RETURNS TABLE(t timestamptz)
 LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 SELECT accepted_at FROM provisiond.fixture_rate_history WHERE currency=c UNION
 SELECT observed_at+staleness_bound_seconds*interval '1 second' FROM provisiond.fixture_rate_history WHERE currency=c
 $$;
CREATE VIEW provisiond.current_rates AS SELECT r.currency,COALESCE(h.value,r.value) AS value,
 COALESCE(h.available,false) AS available FROM provisiond.rates r
 LEFT JOIN LATERAL provisiond.fixture_rate_at(r.currency,statement_timestamp()) h ON true;
GRANT SELECT ON provisiond.current_rates TO app;
CREATE FUNCTION provisiond.recovery_rate_present(c text, lo timestamptz, hi timestamptz)
 RETURNS numeric LANGUAGE sql STABLE SET search_path=pg_catalog AS $$
 WITH boundaries AS (
 SELECT lo AS t UNION SELECT hi UNION
 SELECT accepted_at FROM provisiond.rate_observations WHERE currency=c AND accepted_at>lo AND accepted_at<hi UNION
 SELECT observed_at+staleness_bound_seconds*interval '1 second' FROM provisiond.rate_observations
 WHERE currency=c AND observed_at+staleness_bound_seconds*interval '1 second'>lo
 AND observed_at+staleness_bound_seconds*interval '1 second'<hi
 ), spans AS (SELECT t,lead(t) OVER (ORDER BY t) AS until FROM boundaries), qualified AS (
 SELECT s.*,v.window_rate_num,f.observed_at+f.staleness_bound_seconds*interval '1 second' AS fresh_until
 FROM spans s
 LEFT JOIN LATERAL (SELECT window_rate_num FROM provisiond.rate_observations WHERE currency=c AND accepted_at<=s.t ORDER BY acceptance_order DESC LIMIT 1) v ON true
 LEFT JOIN LATERAL (SELECT observed_at,staleness_bound_seconds FROM provisiond.rate_observations WHERE currency=c AND accepted_at<=s.t ORDER BY observed_at DESC,acceptance_order DESC LIMIT 1) f ON true
 ) SELECT COALESCE(sum(extract(epoch FROM until-t)) FILTER
 (WHERE until>t AND window_rate_num IS NOT NULL AND fresh_until>t),0) FROM qualified
 $$;
