-- STO-12/13: schema/semantics 5, one transactional offline release.
-- Stop order (including the conservative declarations for historical 1-4):
-- stop all components; prevent restart; disable application login; drain all
-- application sessions; migrate; enable login; start engine; start api.
-- No CONCURRENTLY statements. This release tightens shared attachment ownership
-- and declares a contract step: rollback below semantics 5 is inadmissible.
-- Historical 1 already writes about_to_order_at and prohibits older claimers;
-- treat every historical contract conservatively: no rollback below its semantics.
ALTER TABLE provisiond.schema_migrations
 ADD COLUMN stop_order text,
 ADD COLUMN contract_step boolean,
 ADD COLUMN rollback_floor bigint;
UPDATE provisiond.schema_migrations
 SET stop_order='stop all components; prevent restart; disable application login; drain all application sessions; migrate; enable login; start engine; start api',
 contract_step=true, rollback_floor=semantics;
ALTER TABLE provisiond.schema_migrations
 ALTER COLUMN stop_order SET NOT NULL,
 ALTER COLUMN contract_step SET NOT NULL,
 ALTER COLUMN rollback_floor SET NOT NULL,
 ADD CHECK (rollback_floor > 0 AND rollback_floor <= semantics);
-- STO-13: application components may read declarations, never rewrite history.
REVOKE INSERT, UPDATE, DELETE ON provisiond.schema_migrations FROM app;
