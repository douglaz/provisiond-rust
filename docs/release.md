# Offline local release

STO-12/13, STO-7, STO-55, CNF-57 and ADR-0048 own this procedure. Use the release's
`deploy/systemd/provisiond-engine@.service` and `provisiond-api@.service` with systemd 260,
one engine and one API, one primary and its designated synchronous standby. This procedure
has not been run for schema 5. Never advance after a failed command or an uncertain migration.

1. **Prepare the inventory and private configuration.** In a shell with `psql`, `pg_ctl` and
   the release binaries available, assign these absolute paths and the exact owned unit names:

   ```sh
   export ENGINE_UNIT=provisiond-engine@local.service
   export API_UNIT=provisiond-api@local.service
   install -d -m 0700 "$HOME/.local/state/provisiond"
   export ENGINE_EVENTS="$HOME/.local/state/provisiond/local-engine-events.jsonl"
   export API_EVENTS="$HOME/.local/state/provisiond/local-api-events.jsonl"
   export APP_ROLE=app
   export PROVISIOND_APPLICATION_ROLES="$APP_ROLE"
   # Set NEW_RELEASE to the installed release directory containing bin/.
   # Set APP_ENV and MIGRATOR_ENV to private shell-compatible KEY=value files.
   # Set OPERATOR_DATABASE_URL to the bootstrap administrator connection.
   # Set PRIMARY_DATA, STANDBY_DATA, PRIMARY_LOG, STANDBY_LOG to owned paths.
   : "${NEW_RELEASE:?}" "${APP_ENV:?}" "${MIGRATOR_ENV:?}"
   : "${OPERATOR_DATABASE_URL:?}" "${PRIMARY_DATA:?}" "${STANDBY_DATA:?}"
   : "${PRIMARY_LOG:?}" "${STANDBY_LOG:?}"
   # Preserve the exact original postmaster options from the owned inventory.
   # Define empty strings only when every setting is already in postgresql.conf.
   : "${PRIMARY_SERVER_OPTIONS?}" "${STANDBY_SERVER_OPTIONS?}"
   ```

   OVR-18/19 and STO-55: high-volume events go to per-instance append files to avoid
   journald's per-user rate limit suppressing alarms and fatal/refusal diagnostics on stderr.
   The mode-0700 state directory must exist before either unit starts; for another instance,
   change both unit names and event filenames to match the templates' `%i` specifier.

   Template configuration files are `%h/.config/provisiond/local-engine.env` and
   `local-api.env`; the application connection uses `app`, the migrator connection uses
   `migrator`. Keep files mode 0600, outside the repository. Preserve all existing profile,
   standby identity, parameters, simulator and startup-wait settings. `APP_ENV` supplies the
   application store settings for schema checks. `MIGRATOR_ENV` supplies the same store
   settings with the migrator connection. Never print connection strings or enable shell tracing.
   `PROVISIOND_APPLICATION_ROLES` is mandatory for migrate: comma-separated, exact, distinct
   login identities for **all** components (including recovery), not a group role. The local
   bootstrap has only `app`. Other login aliases, jobs or role memberships capable of application
   writes must be included and stopped; the deployment owns the completeness of this inventory.
   Exclude the operator, migrator and replication identities. Freeze role/login administration
   until migration finishes. No new bootstrap grants are needed.
   `PRIMARY_SERVER_OPTIONS` and `STANDBY_SERVER_OPTIONS` preserve the original ports,
   listen addresses and any command-line configuration paths/settings. Plain pg_ctl start does
   not reuse postmaster.opts (only restart does); see the [PostgreSQL 18 command reference](https://www.postgresql.org/docs/18/app-pg-ctl.html).

2. **Stop all previous components and prevent restart.** Stop every inventory entry, including
   independently launched recovery actors. For the two local units:

   ```sh
   systemctl --user mask --runtime "$ENGINE_UNIT" "$API_UNIT"
   systemctl --user stop "$API_UNIT" "$ENGINE_UNIT"
   systemctl --user show "$ENGINE_UNIT" "$API_UNIT" -p ActiveState -p SubState -p MainPID
   ```

   Require inactive/failed units with MainPID=0 and no independently launched component.
   An exited process does not prove its PostgreSQL backend stopped. Do not reset the start
   counter or unmask units yet. A signal death is a crash; retain all in-flight uncertainty.

3. **Disable application login and drain old sessions.** As the bootstrap administrator:

   ```sh
   psql "$OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 -v app_role="$APP_ROLE" <<'SQL'
   SET synchronous_commit=on;
   ALTER ROLE :"app_role" NOLOGIN;
   SELECT rolname,rolcanlogin FROM pg_roles WHERE rolname=:'app_role';
   SELECT pid,usename,state,wait_event_type,wait_event FROM pg_stat_activity WHERE usename=:'app_role';
   SQL
   ```

   Never cancel or terminate a backend waiting in `SyncRep`. If the standby is paused/stopped,
   restore it in a second shell even if the command above itself waits for synchronous commit:

   ```sh
   pg_ctl -D "$STANDBY_DATA" status
   # Only if stopped:
   pg_ctl -D "$STANDBY_DATA" -l "$STANDBY_LOG" -o "$STANDBY_SERVER_OPTIONS" start -w -t 60
   # Only if replay was explicitly paused (as administrator on that standby):
   # psql "$STANDBY_OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 -c 'SELECT pg_wal_replay_resume()'
   ```

   For pg_ctl status, exit 0 means running and exit 3 means stopped; other statuses require
   inspection. For standby process suspension use only the recorded owned standby postmaster PID:
   `kill -CONT "$OWNED_STANDBY_PID"`. Do not guess a PID. Allow old waits to finish and
   disconnected application sessions to leave. If residual sessions remain or progress still
   requires shutdown, stop and start the **same** primary history:

   ```sh
   pg_ctl -D "$PRIMARY_DATA" stop -m fast -w -t 60
   pg_ctl -D "$PRIMARY_DATA" -l "$PRIMARY_LOG" -o "$PRIMARY_SERVER_OPTIONS" start -w -t 60
   ```

   Do not promote, replace data, lower synchronous commit, clear synchronous_standby_names or
   terminate/cancel individual SyncRep backends. Inspect before repeating a timed-out shutdown;
   it supplies no proof of completion. If SQL access is exhausted, follow the ordered
   [saturation procedure](recovery.md#saturation-recovery-on-unchanged-history) first.
   Recheck NOLOGIN after primary restart; repeat the ALTER if its earlier commit did not land.

4. **Require zero application backends before DDL.** Run this guard for the local login; repeat
   for every configured login if the inventory differs. Require exit 0:

   ```sh
   psql "$OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 -v app_role="$APP_ROLE" <<'SQL'
   SELECT COALESCE((SELECT NOT rolcanlogin FROM pg_roles WHERE rolname=:'app_role'),false)
     AND NOT EXISTS(SELECT 1 FROM pg_stat_activity WHERE usename=:'app_role') AS offline_ready \gset
   \if :offline_ready
   \echo Application login disabled and zero application backends
   \else
   \warn Offline prerequisites not satisfied; do not migrate
   DO $$ BEGIN RAISE EXCEPTION 'Offline prerequisites not satisfied; do not migrate'; END $$;
   \endif
   SQL
   ```

   The product independently checks `pg_roles.rolcanlogin` and `pg_stat_activity.usename/pid`
   before any DDL on every migration transaction. It refuses missing roles, LOGIN roles and
   any residual backend, across all databases. PostgreSQL 18 documents [pg_roles as publicly
   readable](https://www.postgresql.org/docs/18/view-pg-roles.html) and [session existence and
   user identity as visible to ordinary users](https://www.postgresql.org/docs/18/monitoring-stats.html#MONITORING-STATS-VIEWS).
   The migrator does not inspect privileged wait/query fields, change roles or terminate sessions.
   This visibility is documentation-verified; the unprivileged live check is not run yet.

5. **Run the new release's migration and require exit 0.** The subshell keeps application and
   migration identities separate:

   ```sh
   (
     set -a
     . "$MIGRATOR_ENV"
     set +a
     export PROVISIOND_APPLICATION_ROLES="$APP_ROLE"
     "$NEW_RELEASE/bin/migrate"
   )
   ```

   The runner accepts exact historical prefixes 0 through 4 and the declared 5/5 history.
   One transaction per version includes DDL and completion under MIGRATION_LOCK, distinct from
   STARTUP_LOCK. DDL lock_timeout is 500 ms with two retries (three attempts). A failed initial
   acquisition or reconnect exits immediately without connection retry. Diagnostics identify
   migrate, release 5/5, acquisition stage and the unchanged database reason plus SQLSTATE.
   Failed acquisition records nothing and retains prior completions. Unacknowledged completion
   is uncertain; inspect history and invoke migrate later, with components still stopped.
   Two migrators can serialize; neither starts components. Do not start components because a
   completion row is locally visible while its COMMIT is still waiting for the standby.

6. **Verify the completed release, re-enable login, then start engine and API in order.**

   ```sh
   psql "$OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 -c 'SELECT version,semantics,stop_order,contract_step,rollback_floor FROM provisiond.schema_migrations ORDER BY version'
   psql "$OPERATOR_DATABASE_URL" -X -v ON_ERROR_STOP=1 -v app_role="$APP_ROLE" <<'SQL'
   SET synchronous_commit=on;
   ALTER ROLE :"app_role" LOGIN;
   SQL
   (
     set -a
     . "$APP_ENV"
     set +a
     "$NEW_RELEASE/bin/engine" --check-schema &&
     "$NEW_RELEASE/bin/api" --check-schema &&
     "$NEW_RELEASE/bin/recovery" --check-schema
   )
   ```

   Require versions/semantics 1/1 through 5/5 and the exact declarations the new binaries
   accept, and exit 0 from every schema check. Then, in this order:

   ```sh
   ln -sfn "$NEW_RELEASE" "$HOME/.local/lib/provisiond/current"
   systemctl --user unmask --runtime "$ENGINE_UNIT" "$API_UNIT"
   for u in "$ENGINE_UNIT" "$API_UNIT"; do
     if systemctl --user is-failed --quiet "$u"; then
       systemctl --user reset-failed "$u"
     fi
   done
   # Both units must still be stopped when recording their event-file offsets.
   ENGINE_FROM=$(( $(stat -c %s "$ENGINE_EVENTS" 2>/dev/null || echo 0) + 1 ))
   API_FROM=$(( $(stat -c %s "$API_EVENTS" 2>/dev/null || echo 0) + 1 ))
   STARTED=no
   systemctl --user start "$ENGINE_UNIT" &&
   timeout 120 sh -c 'until tail -c +"$1" "$2" 2>/dev/null | grep -m1 -F "\"event\":\"restart_pass_complete\""; do sleep 1; done' _ "$ENGINE_FROM" "$ENGINE_EVENTS" &&
   systemctl --user is-active "$ENGINE_UNIT" &&
   # If an incident is open, require recovery_step_acknowledged step 2 in:
   # tail -c +"$ENGINE_FROM" "$ENGINE_EVENTS"
   systemctl --user start "$API_UNIT" &&
   timeout 120 sh -c 'until tail -c +"$1" "$2" 2>/dev/null | grep -m1 -F "\"event\":\"api_listening\""; do sleep 1; done' _ "$API_FROM" "$API_EVENTS" &&
   systemctl --user is-active "$API_UNIT" &&
   STARTED=yes
   # Alarms and fatal/refusal diagnostics remain in the journal.
   journalctl --user -u "$ENGINE_UNIT" -u "$API_UNIT" -n 200 --no-pager
   [ "$STARTED" = yes ] || { echo 'Startup not confirmed: do not resume customer traffic; inspect the journal and event files' >&2; false; }
   ```

   Event files append across restarts. Record offsets while both units are stopped; the
   bounded waits read only bytes appended after those offsets, including markers that would
   have scrolled out of a line-count tail. Require exit 0 from each start, wait and active check.
   A timeout (exit 124), other nonzero result or inactive unit is a failed step: stop here,
   inspect the journal and event files, and do not start the API or resume customer requests.
   After api_listening, inspect customer balance, payments, operations and machines.
   Reset only failed units because a cleanly stopped unit may have been collected and resetting
   an unloaded unit fails. A failed start is a failed release step. Inspect rather than repeatedly
   resetting limits.
   Ordinary release preserves spending tokens. Customers resend identical bytes with the same
   key after an uncertain reply; after 503 overloaded they obey retry_after_ms and Retry-After
   with bounded attempts. No receipt is created by a checkout refusal.

7. **Apply the rollback rule.** The runtime binaries accept schema range 5..=5, shared-row
   semantics 5, the declared contract steps and rollback floor 5; migrate additionally accepts
   historical input prefixes. Every historical declaration is conservatively marked contract
   with floor equal to its semantics. The initial about_to_order_at release prohibits earlier
   claimers; legacy running backfills mean potentially dispatched, never proof of no dispatch.
   Schema 5 tightens attachment ownership and disallows schema-4 binaries. Rollback means an
   older binary against the current schema only when it declares support and preserves every
   guard; never run a down migration. Past a contract step use STO-54 restore/incident recovery.
   All migrations are transactional and use no CONCURRENTLY statement: STO-12's invalid-index
   and nontransactional crash-resume path is not exercised by this release.
