//! STO-54/56: operator incident entry, persisted progress and possible-loss reports.
//! This actor never takes the engine startup lock or advances its generation.
use crate::store::{CommitOutcome, Store, Transaction};
use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};

pub(crate) const INCIDENT_LOCK: i64 = 0x5052_4f56_5245_434f;

pub struct Establish {
    pub id: String,
    pub predecessor: Option<String>,
    pub expected_instant: Option<String>,
    pub evidence: Value,
    pub rederivation_seconds: i64,
    pub derivation_gap: i64,
}

/// STO-54: unknown clock/prefix provenance is retained, never guessed from an LSN
/// or event timestamp. This profile accepts no finite certificates yet: its local
/// fixture has no independently certified source/event clock error bounds.
fn reporting_evidence(id: &str, evidence: &Value) -> Value {
    // Missing or invalid reporting inputs cannot add a recovery blockade. The
    // incident itself retains the unknown reasons under its own safe reference.
    let fallback = format!("unknown/{id}");
    let reference = evidence["reference"]
        .as_str()
        .filter(|reference| {
            !reference.is_empty()
                && reference.len() <= 256
                && reference
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_/.:".contains(&b))
        })
        .unwrap_or(&fallback);
    let lower = evidence["lower_unknown_reason"]
        .as_str()
        .filter(|s| !s.is_empty())
        .unwrap_or("no certified source clock error and recovered continuous-prefix inclusion proof supplied");
    let upper = evidence["upper_unknown_reason"]
        .as_str()
        .filter(|s| !s.is_empty())
        .unwrap_or("no certified event clock error and stopping/fencing certificate supplied");
    // STO-54: absent reporting input is an unknown, not an invalid certificate.
    let absent = evidence == &json!({"reporting_input":"absent"});
    let mut report = json!({"incident_id":id,"reference":reference,"lower_unknown_reason":lower,
        "upper_unknown_reason":upper,"finite_duration_established":false,
        "invalid_certificate_input":!absent && (!evidence["lower"].is_null() || !evidence["upper"].is_null() || evidence["incident_id"]!=id),
        "certificate_support":"not run: source/event clock and continuous-prefix certificates unavailable"});
    if let Some(input) = evidence.get("reporting_input") {
        report["reporting_input"] = input.clone();
    }
    report
}

pub(crate) async fn lock(tx: &mut Transaction) -> Result<()> {
    tx.rows("SELECT pg_advisory_xact_lock($1)", &[&INCIDENT_LOCK])
        .await?;
    Ok(())
}

pub async fn current(store: &Store) -> Result<Value> {
    store.value("SELECT COALESCE((SELECT to_jsonb(r) FROM provisiond.recovery_incidents r WHERE closed_at IS NULL),'null'::jsonb)::text", &[]).await
}

/// STO-56: report the retained incident, including after operator close.
pub async fn incident(store: &Store, id: &str) -> Result<Value> {
    store.value("SELECT COALESCE((SELECT to_jsonb(r) FROM provisiond.recovery_incidents r WHERE id=$1::text::uuid),'null'::jsonb)::text", &[&id]).await
}

/// STO-56: the retained event ID and predecessor identify one operator event.
/// Every successful call includes an acknowledged COMMIT, including no-op retry.
pub async fn establish(store: &Store, request: Establish) -> Result<Value> {
    uuid::Uuid::parse_str(&request.id)?;
    if let Some(id) = &request.predecessor {
        uuid::Uuid::parse_str(id)?;
    }
    ensure!(
        request.rederivation_seconds > 0
            && request.rederivation_seconds <= i32::MAX as i64
            && request.derivation_gap > 0
            && request.derivation_gap <= crate::wire::SAFE_INTEGER,
        "OVR-19 recovery interval and derivation gap must be positive and bounded"
    );
    let mut tx = store.begin().await?;
    lock(&mut tx).await?;
    if crate::store::test_defect("w7_lossless_restart") {
        ensure!(
            store.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
            "control no-op unacknowledged"
        );
        return Ok(Value::Null);
    }
    let existing = tx.rows("SELECT to_jsonb(r)::text FROM provisiond.recovery_incidents r WHERE id=$1::text::uuid FOR UPDATE", &[&request.id]).await?;
    let report = if let Some(row) = existing.first() {
        let retained: Value = serde_json::from_str(row.get::<_, &str>(0))?;
        ensure!(
            retained["legacy"] == false,
            "unsupported legacy incident lacks establishment prerequisites"
        );
        ensure!(
            retained["supersedes_incident_id"].as_str() == request.predecessor.as_deref(),
            "STO-56 predecessor mismatch; keep components stopped"
        );
        if let Some(instant) = &request.expected_instant {
            let same = tx.rows("SELECT restore_instant=$2::text::timestamptz FROM provisiond.recovery_incidents WHERE id=$1::text::uuid", &[&request.id, instant]).await?[0].get::<_, bool>(0);
            ensure!(
                same,
                "STO-56 restore instant mismatch; keep components stopped"
            );
        }
        // The evidence is deliberately not recomputed on a matching continuation.
        if crate::store::test_defect("w7_retry_reset") {
            tx.rows("UPDATE provisiond.recovery_incidents SET steps_completed=0,grace_ends_at=NULL WHERE id=$1::text::uuid RETURNING id", &[&request.id]).await?;
        }
        retained
    } else {
        ensure!(
            request.expected_instant.is_none(),
            "uncommitted event must use the new inserting transaction's instant"
        );
        let evidence = reporting_evidence(&request.id, &request.evidence);
        let predecessor = tx.rows("SELECT id::text FROM provisiond.recovery_incidents WHERE closed_at IS NULL FOR UPDATE", &[]).await?;
        let predecessor = predecessor.first().map(|r| r.get::<_, String>(0));
        ensure!(
            predecessor == request.predecessor,
            "STO-56 current predecessor mismatch; keep components stopped"
        );
        tx.rows("UPDATE provisiond.recovery_incidents SET closed_at=transaction_timestamp() WHERE closed_at IS NULL RETURNING id", &[]).await?;
        let rows = tx.rows("INSERT INTO provisiond.recovery_incidents(id,restore_instant,history_evidence_ref,history_evidence,supersedes_incident_id,rederivation_seconds,derivation_gap) VALUES($1::text::uuid,transaction_timestamp(),$2,$3::text::jsonb,$4::text::uuid,$5,$6) RETURNING to_jsonb(recovery_incidents)::text",
            &[&request.id,&evidence["reference"].as_str(),&evidence.to_string(),&request.predecessor,&request.rederivation_seconds,&request.derivation_gap]).await?;
        tx.rows("UPDATE provisiond.operations SET restore_markers_untrusted=true WHERE kind IN ('install','rescue_inventory') AND status IN ('queued','running','needs_reconciliation') RETURNING id", &[]).await?;
        if crate::store::test_defect("w7_supersession_inherits") && request.predecessor.is_some() {
            tx.rows("UPDATE provisiond.recovery_incidents r SET steps_completed=p.steps_completed,grace_ends_at=p.grace_ends_at FROM provisiond.recovery_incidents p WHERE r.id=$1::text::uuid AND p.id=r.supersedes_incident_id RETURNING r.id", &[&request.id]).await?;
        }
        if crate::store::test_defect("w7_finite_guess") {
            tx.rows("UPDATE provisiond.recovery_incidents SET lost_window_starts_at=restore_instant,loss_end_at=restore_instant,history_evidence=jsonb_set(history_evidence,'{finite_duration_established}','true') WHERE id=$1::text::uuid RETURNING id", &[&request.id]).await?;
        }
        serde_json::from_str::<Value>(rows[0].get::<_, &str>(0))?
    };
    ensure!(
        store.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
        "incident establishment unacknowledged; keep components stopped and retry the retained event ID"
    );
    Ok(report)
}

/// STO-54: the five actors use a live read; an open record cannot prolong the
/// wall-clock freeze. Unsupported legacy history stays unavailable.
pub async fn frozen(store: &Store) -> Result<bool> {
    let r = store.value("SELECT jsonb_build_object('legacy',EXISTS(SELECT 1 FROM provisiond.recovery_incidents WHERE closed_at IS NULL AND legacy),'frozen',EXISTS(SELECT 1 FROM provisiond.recovery_incidents WHERE closed_at IS NULL AND (grace_ends_at IS NULL OR grace_ends_at>statement_timestamp())))::text", &[]).await?;
    ensure!(r["legacy"] == false, "unsupported legacy recovery incident");
    Ok(r["frozen"] == true)
}

/// STO-54(1): a separate operator recovery actor re-establishes account status
/// before an engine can take its startup lock. No engine-generation write occurs.
pub async fn prepare(store: std::sync::Arc<Store>) -> Result<()> {
    let incident = current(&store).await?;
    ensure!(
        !incident.is_null() && incident["legacy"] == false,
        "missing established incident"
    );
    let component = store.component();
    component.trace(json!({"event":"recovery_loss_report","incident":incident}));
    if incident["steps_completed"].as_i64().context("step marks")? >= 1 {
        return Ok(());
    }
    let id = incident["id"].as_str().context("incident ID")?;
    let client = crate::provider::Client::configured()?;
    let accounts = store.value("SELECT COALESCE(jsonb_agg(jsonb_build_object('account',p.account,'descriptor',p.descriptor,'status',s.status) ORDER BY p.account),'[]'::jsonb)::text FROM provisiond.provider_accounts p JOIN provisiond.provider_account_status s ON s.provider_account=p.account", &[]).await?;
    let mut statuses = Vec::new();
    for account in accounts.as_array().context("configured accounts")? {
        let name = account["account"].as_str().context("account name")?;
        let old = account["status"].as_str().context("account status")?;
        let status = if matches!(old, "terminated" | "credentials_rejected") {
            old
        } else {
            match client
                .call(
                    &component,
                    &account["descriptor"],
                    "/offers",
                    &json!({"account":name,"recovery":true}),
                )
                .await
            {
                Ok(v) if v.is_array() => "healthy",
                Err(error) if error.0["kind"] == "authentication" => "credentials_rejected",
                _ => "account_unreachable",
            }
        };
        statuses.push((name.to_owned(), status.to_owned()));
    }
    let mut tx = component.begin().await?;
    lock(&mut tx).await?;
    let row = tx.rows("SELECT steps_completed FROM provisiond.recovery_incidents WHERE id=$1::text::uuid AND closed_at IS NULL FOR UPDATE", &[&id]).await?;
    ensure!(row.len() == 1, "incident changed during pre-lock recovery");
    if row[0].get::<_, i32>(0) == 0 {
        // LDG-72: before serving restored subjects, reuse the offline audit.
        // Consistent restored marks/entries can pass while history is missing.
        tx.rows("SELECT provisiond.audit_machine_meters()", &[])
            .await?;
        for (name, status) in statuses {
            tx.rows("UPDATE provisiond.provider_account_status SET status=$2,source='provider',observed_at=transaction_timestamp(),updated_at=transaction_timestamp() WHERE provider_account=$1 AND status NOT IN ('terminated','credentials_rejected') RETURNING provider_account", &[&name,&status]).await?;
        }
        tx.rows("UPDATE provisiond.recovery_incidents SET steps_completed=1 WHERE id=$1::text::uuid AND steps_completed=0 RETURNING id", &[&id]).await?;
    }
    ensure!(
        component.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
        "pre-lock recovery unacknowledged"
    );
    component.trace(json!({"event":"recovery_step_acknowledged","incident":id,"step":1,"actor":"operator-recovery"}));
    Ok(())
}

/// STO-56: close reads current disposition while a table lock orders every
/// concurrent machine attach/fence-clear before or after this transaction.
pub async fn close(store: &Store, id: &str) -> Result<()> {
    uuid::Uuid::parse_str(id)?;
    let mut tx = store.begin().await?;
    lock(&mut tx).await?;
    tx.rows("LOCK TABLE provisiond.machines IN SHARE MODE", &[])
        .await?;
    let rows = tx.rows("SELECT steps_completed=3 AND grace_ends_at<=transaction_timestamp(),legacy FROM provisiond.recovery_incidents WHERE id=$1::text::uuid AND closed_at IS NULL FOR UPDATE", &[&id]).await?;
    ensure!(
        rows.len() == 1 && !rows[0].get::<_, bool>(1),
        "missing supported open incident"
    );
    ensure!(
        rows[0].get::<_, bool>(0),
        "step 3 and wall-clock grace must complete before close"
    );
    let unmet = tx.rows("SELECT EXISTS(SELECT 1 FROM provisiond.recovery_waiting_parents WHERE incident_id=$1::text::uuid AND disposition IS NULL) OR EXISTS(SELECT 1 FROM provisiond.provider_accounts a WHERE NOT EXISTS(SELECT 1 FROM provisiond.recovery_account_passes p WHERE p.incident_id=$1::text::uuid AND p.provider_account=a.account AND second_completed_at IS NOT NULL)) OR EXISTS(SELECT 1 FROM provisiond.machines m CROSS JOIN provisiond.recovery_incidents r WHERE r.id=$1::text::uuid AND m.state<>'deleted' AND m.destroy_committed IS NULL AND provisiond.recovery_rate_present(m.native_terms->'offer'->>'currency',r.grace_ends_at-r.rederivation_seconds*interval '1 second',transaction_timestamp())<r.rederivation_seconds)", &[&id]).await?[0].get::<_,bool>(0);
    ensure!(
        !unmet,
        "STO-56 waiting parents, second account pass or per-currency grace remains unsatisfied"
    );
    tx.rows("UPDATE provisiond.recovery_incidents SET closed_at=transaction_timestamp() WHERE id=$1::text::uuid AND closed_at IS NULL RETURNING id", &[&id]).await?;
    ensure!(
        store.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
        "incident close unacknowledged; inspect retained record"
    );
    Ok(())
}

/// STO-54/56: a local database operator explicitly disposes a held parent. This
/// adds no suspension enqueuer or fan-out worker to the simulator profile.
pub async fn parent(
    store: &Store,
    incident: &str,
    operation: &str,
    disposition: &str,
) -> Result<()> {
    ensure!(
        matches!(disposition, "confirmed" | "cancelled"),
        "invalid parent disposition"
    );
    uuid::Uuid::parse_str(incident)?;
    uuid::Uuid::parse_str(operation)?;
    let mut tx = store.begin().await?;
    lock(&mut tx).await?;
    let rows=tx.rows("SELECT o.tenant_id FROM provisiond.recovery_waiting_parents p JOIN provisiond.operations o ON o.id=p.operation_id JOIN provisiond.recovery_incidents r ON r.id=p.incident_id WHERE r.id=$1::text::uuid AND r.closed_at IS NULL AND o.id=$2::text::uuid AND o.kind='suspend_tenant'", &[&incident,&operation]).await?;
    let tenant: String = rows
        .first()
        .context("no waiting parent in current incident")?
        .get(0);
    tx.serialize_tenants(&[&tenant]).await?;
    let changed=tx.rows("UPDATE provisiond.recovery_waiting_parents SET disposition=$3 WHERE incident_id=$1::text::uuid AND operation_id=$2::text::uuid AND (disposition IS NULL OR disposition=$3) RETURNING operation_id", &[&incident,&operation,&disposition]).await?;
    ensure!(
        changed.len() == 1,
        "waiting parent already received a different operator disposition"
    );
    if disposition == "cancelled" {
        tx.rows("UPDATE provisiond.operations SET status='failed',request=NULL,error=COALESCE(error,jsonb_build_object('kind','internal','message','recovery parent cancelled by operator','retryable',false,'details','{}'::jsonb)),revision=revision+1,updated_at=transaction_timestamp() WHERE id=$1::text::uuid AND status IN ('queued','running','needs_reconciliation') RETURNING id", &[&operation]).await?;
    }
    ensure!(
        store.commit(tx)?.wait().await? == CommitOutcome::Acknowledged,
        "parent decision unacknowledged"
    );
    Ok(())
}
