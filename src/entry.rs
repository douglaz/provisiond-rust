//! STO-13: explicit local-prototype entry configuration and compatibility checks.
use crate::store::{Process, Settings, Store, Trace, migration::MigrationOptions};
use anyhow::{Context, Result, ensure};
use std::{env, sync::Arc, time::Duration};

/// No deployment defaults are selected by the prototype. This named profile uses
/// D-6's provisional local fixture settings and cannot contact external providers.
pub fn local_store(process: Arc<Process>, trace: Trace) -> Result<Store> {
    local_store_with_write_limit(process, trace, 1)
}
pub fn local_engine_store(process: Arc<Process>, trace: Trace) -> Result<Store> {
    let workers = env::var("PROVISIOND_WORKERS")
        .unwrap_or_else(|_| "3".into())
        .parse::<usize>()
        .context("invalid worker count")?;
    ensure!(workers > 0, "worker count must be positive");
    local_store_with_write_limit(process, trace, workers)
}
fn local_store_with_write_limit(
    process: Arc<Process>,
    trace: Trace,
    write_limit: usize,
) -> Result<Store> {
    ensure!(
        env::var("PROVISIOND_PROFILE").as_deref() == Ok("local-simulator"),
        "pre-conformance prototype requires PROVISIOND_PROFILE=local-simulator"
    );
    let connection = read_setting("PROVISIOND_DATABASE_URL")?
        .parse()
        .map_err(|_| anyhow::anyhow!("invalid database connection configuration"))?;
    Store::new(
        Settings {
            connection,
            designated_name: read_setting("PROVISIOND_STANDBY_NAME")?,
            designated_role: read_setting("PROVISIOND_STANDBY_ROLE")?,
            designated_address: read_setting("PROVISIOND_STANDBY_ADDRESS")?,
            statement_ms: 1000,
            lock_ms: 500,
            idle_transaction_ms: 1000,
            call_bound: Duration::from_secs(2),
            check_bound: Duration::from_secs(3),
            checkout_bound: Duration::from_millis(250),
            keepalive_idle: Duration::from_secs(2),
            keepalive_interval: Duration::from_secs(1),
            keepalive_count: 2,
        },
        process,
        1,
        write_limit,
        trace,
    )
}

// SEC-3 says "No credential — provider token, rescue password, private key, signed URL — may
// appear in a log line, an operation result, an error message, or a stored metadata blob."
// The database address setting can carry the database password, so a setting error names the
// setting and never repeats its value.
fn read_setting(name: &str) -> Result<String> {
    env::var(name).map_err(|error| match error {
        env::VarError::NotPresent => anyhow::anyhow!("missing {name}"),
        env::VarError::NotUnicode(_) => not_unicode(name),
    })
}

/// SEC-3: the fixed error for a setting that is not valid UTF-8 names the setting only.
pub(crate) fn not_unicode(name: &str) -> anyhow::Error {
    anyhow::anyhow!("{name} is not valid UTF-8 text")
}

/// SEC-3: `std::env::var` for a setting that holds an operator token, or that an operator
/// authority's `token_env` can name, since that may be any setting the API reads first.
/// `VarError::NotUnicode` carries the value, so it becomes the fixed error; an unset setting
/// keeps `VarError`'s own error, which carries no value.
pub(crate) fn var(name: &str) -> Result<String> {
    named(name, env::var(name))
}
fn named(name: &str, value: Result<String, env::VarError>) -> Result<String> {
    value.map_err(|error| match error {
        env::VarError::NotUnicode(_) => not_unicode(name),
        missing @ env::VarError::NotPresent => missing.into(),
    })
}

pub async fn migrate(store: &Store) -> Result<()> {
    store
        .migrate(MigrationOptions {
            lock_ms: 500,
            lock_retries: 2,
        })
        .await
}

/// STO-54/56: continue an unmarked pre-lock phase in a separate process. The
/// engine still makes no write before its own generation-only advance. A parent
/// pipe makes the recovery actor exit if this startup process dies mid-phase.
pub async fn continue_pre_lock(store: &Store, trace: Trace) -> Result<()> {
    // STO-13: the existing isolated startup-order control must bypass this
    // additional compatibility read too, so it still reaches the forbidden
    // real startup boundary. Ordinary builds cannot select the defect.
    if crate::store::test_defect("startup_order") {
        return Ok(());
    }
    store.compatible_schema_for("engine").await?;
    let incident = crate::recovery::current(store).await?;
    if incident.is_null() {
        return Ok(());
    }
    ensure!(
        incident["legacy"] == false,
        "open recovery incident unsupported: legacy establishment prerequisites missing"
    );
    if incident["steps_completed"]
        .as_i64()
        .context("recovery step marks")?
        >= 1
    {
        return Ok(());
    }
    let program = std::env::current_exe()?.with_file_name("recovery");
    let mut child = std::process::Command::new(program)
        .arg("prepare")
        .env("PROVISIOND_COMPONENTS_STOPPED", "yes")
        .env("PROVISIOND_RECOVERY_PARENT_PIPE", "yes")
        .stdin(std::process::Stdio::piped())
        .spawn()?;
    let pipe = child.stdin.take().context("recovery parent pipe")?;
    trace(
        serde_json::json!({"event":"pre_lock_actor_started","pid":child.id(),"incident":incident["id"]}),
    );
    let status = child.wait()?;
    drop(pipe);
    ensure!(
        status.success(),
        "pre-lock recovery actor failed; no startup lock or generation write"
    );
    Ok(())
}

/// OVR-18/19, STO-55: stderr is retained by the supervisor journal. Runtime
/// checkout failure streaks alarm at 2000 ms; fatal component exits alarm at once.
pub fn alarm(component: &str, message: &str, threshold_ms: u64) {
    eprintln!(
        "{}",
        serde_json::json!({"event":"alarm","component":component,
        "message":message,"threshold_ms":threshold_ms,"operator_action_required":true})
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env::VarError, ffi::OsString, os::unix::ffi::OsStringExt};

    /// SEC-3, pvr-r7ov: a token setting that is not valid UTF-8 is named, never repeated.
    #[test]
    fn invalid_utf8_token_setting_is_named_without_its_value() {
        let word = b"inventedsampleword";
        for value in [
            [b"\xff\xfe".as_slice(), word].concat(),
            [word.as_slice(), b"\xff\xfe"].concat(),
            [b"\xff".as_slice(), word, b"\xfe"].concat(),
        ] {
            let value = Err(VarError::NotUnicode(OsString::from_vec(value)));
            let error = named("TEST_OPERATOR_TOKEN", value).unwrap_err();
            for shown in [format!("{error:#}"), format!("{error:?}")] {
                assert!(
                    !shown.contains("inventedsampleword"),
                    "value exposed: {shown}"
                );
                assert!(!shown.contains('\u{fffd}') && !shown.contains("\\x"));
            }
            assert_eq!(
                format!("{error:#}"),
                "TEST_OPERATOR_TOKEN is not valid UTF-8 text"
            );
        }
        // Unchanged: a valid setting passes through and an unset one keeps its error.
        let token = named("TEST_OPERATOR_TOKEN", Ok("valid".into())).unwrap();
        assert_eq!(token, "valid");
        let error = named("TEST_OPERATOR_TOKEN", Err(VarError::NotPresent)).unwrap_err();
        assert_eq!(format!("{error:#}"), "environment variable not found");
    }
}
