//! STO-7 sent-COMMIT ownership; OPS-49 uncertainty. One attempt, no retry.
use super::{Connection, Session, Store, Transaction, process::AbortOnPanic};
use anyhow::{Context, Result};
use serde_json::json;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::oneshot;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitOutcome {
    Acknowledged,
    /// OPS-49: ordinary failure before possible COMMIT dispatch; unlike an
    /// unresolved claim, the caller may repeat the whole transaction.
    NotDispatched {
        reason: String,
    },
    Rejected {
        reason: String,
    },
    Unresolved {
        reason: String,
    },
}
impl CommitOutcome {
    pub fn label(&self) -> &'static str {
        match self {
            Self::Acknowledged => "acknowledged",
            Self::NotDispatched { .. } | Self::Rejected { .. } => "rejected",
            Self::Unresolved { .. } => "unresolved",
        }
    }
}
/// Dropping this receiver cancels pending verification; a possibly sent COMMIT
/// remains process-owned until completion, transport death or process exit.
pub struct CommitReceipt {
    receiver: oneshot::Receiver<(CommitOutcome, std::time::Duration)>,
}
impl CommitReceipt {
    pub async fn wait(self) -> Result<CommitOutcome> {
        Ok(self.wait_accounted().await?.0)
    }
    pub(crate) async fn wait_accounted(self) -> Result<(CommitOutcome, std::time::Duration)> {
        self.receiver
            .await
            .context("process commit owner disappeared")
    }
}
#[derive(Clone, Copy)]
enum Mode {
    Normal,
    #[cfg(feature = "store-test-hooks")]
    ReleaseCheckout,
    #[cfg(feature = "store-test-hooks")]
    IgnoreWarning,
}
#[cfg(feature = "store-test-hooks")]
pub enum TestCommitDefect {
    ReleaseCheckout,
    IgnoreWarning,
}

impl Store {
    /// Synchronous registration/ownership transfer has no cancellation gap. Only
    /// the receipt is request-owned; the process observes completion regardless.
    pub fn commit(&self, transaction: Transaction) -> Result<CommitReceipt> {
        #[cfg(feature = "store-test-hooks")]
        if super::test_defect("w5_checkout") {
            return self.test_commit(transaction, TestCommitDefect::ReleaseCheckout);
        }
        self.commit_inner(transaction, Mode::Normal)
    }
    #[cfg(feature = "store-test-hooks")]
    pub fn test_commit(
        &self,
        transaction: Transaction,
        defect: TestCommitDefect,
    ) -> Result<CommitReceipt> {
        self.commit_inner(
            transaction,
            match defect {
                TestCommitDefect::ReleaseCheckout => Mode::ReleaseCheckout,
                TestCommitDefect::IgnoreWarning => Mode::IgnoreWarning,
            },
        )
    }
    fn commit_inner(&self, mut transaction: Transaction, mode: Mode) -> Result<CommitReceipt> {
        let (sender, receiver) = oneshot::channel();
        if transaction.failed {
            (self.trace)(
                json!({"event":"precommit_rejected","session":transaction.session_id(),"commit_not_dispatched":true}),
            );
            let _ = sender.send((
                CommitOutcome::NotDispatched {
                    reason: "failed/cancelled pre-COMMIT statement; COMMIT not dispatched".into(),
                },
                std::time::Duration::ZERO,
            ));
            return Ok(CommitReceipt { receiver });
        }
        let checkout = transaction
            .checkout
            .take()
            .context("transaction consumed")?;
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let owner = NEXT.fetch_add(1, Ordering::Relaxed);
        self.process.submit(own_commit(
            checkout,
            sender,
            owner,
            mode,
            transaction.permit.take(),
        ))?;
        Ok(CommitReceipt { receiver })
    }
}
async fn own_commit(
    checkout: Connection,
    mut sender: oneshot::Sender<(CommitOutcome, std::time::Duration)>,
    owner: u64,
    mode: Mode,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
) {
    let _permit = permit;
    let started = std::time::Instant::now();
    // This local precedes the guard, so panic aborts before it can release a slot.
    let mut checkout = Some(checkout);
    let _abort = AbortOnPanic;
    let session = checkout.as_ref().expect("checkout");
    let pid = session.pid;
    let trace = session.trace.clone();
    let client = session.client.clone();
    let protocol = session.protocol.clone();
    let notices = session.notices.clone();
    // STO-7: verify on the still-owned physical transaction before dispatch.
    // Ownership was registered synchronously, so dropping a receipt creates no gap.
    if !super::test_defect("commit_setting") {
        let verification = session.scalar("SHOW synchronous_commit /* precommit verify */");
        let verified = if super::test_defect("verify_cancel") {
            verification.await
        } else {
            tokio::select! {
                biased;
                _ = sender.closed() => {
                    session.close();
                    trace(json!({"event":"precommit_cancelled","session":pid,"commit_not_dispatched":true}));
                    return;
                }
                result = verification => result,
            }
        };
        if !matches!(verified.as_deref(), Ok("on")) {
            session.close();
            trace(json!({"event":"precommit_rejected","session":pid,"commit_not_dispatched":true}));
            // OPS-49: a caller can retry as soon as delivery wakes it. Release
            // this failed session/component before publishing that permission.
            checkout.take().expect("checkout").discard();
            drop(_permit);
            let _ = sender.send((
                CommitOutcome::NotDispatched {
                    reason: "synchronous_commit verification failed; COMMIT not dispatched".into(),
                },
                started.elapsed(),
            ));
            return;
        }
    }
    // Last pre-dispatch observation; no await separates this boundary from the
    // irrevocable owner below. Cancellation beyond it cannot release the checkout.
    if !super::test_defect("verify_cancel") && sender.is_closed() {
        session.close();
        trace(json!({"event":"precommit_cancelled","session":pid,"commit_not_dispatched":true}));
        return;
    }
    {
        let mut diagnostics = notices.lock().expect("notice lock");
        diagnostics.committing = true;
        diagnostics.commit_warning = false;
    }
    let verification_time = started.elapsed();
    let before = protocol.snapshot();
    trace(json!({"event":"owner_accepted","owner":owner,"session":pid}));
    trace(json!({"event":"commit_possible_dispatch","owner":owner,"session":pid}));
    // No timeout/cancel/shutdown select surrounds this actual attempt.
    let execution = client.simple_query("COMMIT /* store COMMIT */");
    tokio::pin!(execution);
    #[cfg(feature = "store-test-hooks")]
    let release = matches!(mode, Mode::ReleaseCheckout);
    #[cfg(not(feature = "store-test-hooks"))]
    let release = false;
    let mut unpooled: Option<Session> = None;
    let result = loop {
        tokio::select! {
            result=&mut execution=>break result,
            _=sender.closed(), if release && checkout.is_some()=> {
                // Deliberate disposable defect: keep the session live, but lose
                // the original checkout on request cancellation. Never default.
                unpooled=Some(checkout.take().expect("checkout").unpool());
                trace(json!({"event":"defect_checkout_released","session":pid,"owner":owner}));
            }
            request=ownership_request(owner)=> {
                // CNF-296: inspect the actual owner's checkout in both builds;
                // the sensitivity mutation's diagnostic is not this evidence.
                let pooled=matches!(checkout.as_ref(),Some(Connection::Pooled(_)));
                trace(json!({"event":"test_commit_checkout","session":pid,"owner":owner,"pooled_checkout_held":pooled,"transport_dead":protocol.snapshot().dead}));
                let _=std::fs::remove_file(request);
            }
        }
    };
    // ErrorResponse alone is insufficient: consume terminal response or death.
    let reply = protocol.complete(before.ready).await;
    let (diagnostics, sticky_warning) = {
        let retained = notices.lock().expect("notice lock");
        (retained.values.clone(), retained.commit_warning)
    };
    let warning = if super::test_defect("notice_sticky") {
        diagnostics.iter().any(|n| n["severity"] == "WARNING")
    } else {
        sticky_warning
    };
    #[cfg(feature = "store-test-hooks")]
    let ignore = matches!(mode, Mode::IgnoreWarning);
    #[cfg(not(feature = "store-test-hooks"))]
    let ignore = {
        let _ = mode;
        false
    };
    let outcome = if warning && !ignore {
        CommitOutcome::Unresolved {
            reason: "server warning during COMMIT (STO-7 replication warning is a lost reply)"
                .into(),
        }
    } else if result.is_ok()
        && reply.ready > before.ready
        && reply.status == b'I'
        && reply.commands == before.commands + 1
        && reply.command == "COMMIT"
    {
        CommitOutcome::Acknowledged
    } else if result.is_ok()
        && reply.ready > before.ready
        && reply.status == b'I'
        && reply.command == "ROLLBACK"
    {
        CommitOutcome::Rejected {
            reason: "server explicitly completed ROLLBACK".into(),
        }
    } else if let Err(error) = &result {
        let code = error.code().map(|c| c.code()).unwrap_or("");
        if reply.ready > before.ready
            && reply.status == b'I'
            && !warning
            && (code.starts_with("23") || matches!(code, "40001" | "40P01"))
        {
            CommitOutcome::Rejected {
                reason: format!("server rejected COMMIT with {code} and returned idle"),
            }
        } else {
            CommitOutcome::Unresolved {
                reason: format!("COMMIT error/transport death; no rollback inferred: {error}"),
            }
        }
    } else {
        CommitOutcome::Unresolved {
            reason: "incomplete/unexpected COMMIT command semantics".into(),
        }
    };
    trace(
        json!({"event":"commit_classified","owner":owner,"session":pid,"outcome":outcome.label(),"reason":format!("{outcome:?}"),"command_tag":reply.command,"ready_status":reply.status,"transport_dead":reply.dead,"diagnostics":diagnostics}),
    );
    if outcome == CommitOutcome::Acknowledged {
        drop(checkout.take());
    } else if let Some(object) = checkout.take() {
        object.discard();
    }
    drop(unpooled);
    drop(_permit);
    trace(json!({"event":"commit_owner_completed","owner":owner,"session":pid}));
    let _ = sender.send((outcome, verification_time));
}

// Concrete opt-in fixture observation, inert in ordinary builds. It never
// changes COMMIT ownership, sends a cancel packet or supplies a deadline.
async fn ownership_request(owner: u64) -> std::path::PathBuf {
    #[cfg(feature = "store-test-hooks")]
    if let Ok(root) = std::env::var("PROVISIOND_TEST_CONTROL") {
        let request = std::path::Path::new(&root).join("observe-commit-owner");
        loop {
            if std::fs::read_to_string(&request)
                .ok()
                .is_some_and(|value| value == owner.to_string())
            {
                return request;
            }
            tokio::time::sleep(std::time::Duration::from_millis(2)).await;
        }
    }
    let _ = owner;
    std::future::pending().await
}
