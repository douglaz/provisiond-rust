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
