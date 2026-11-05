//! STO-7: connection drivers have an explicitly retained process lifetime.
use super::Trace;
use futures_util::{StreamExt, stream::FuturesUnordered};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::Duration,
};
use tokio::sync::mpsc;

type Work = Pin<Box<dyn Future<Output = ()> + Send>>;
enum Job {
    Work(Work),
    Stop,
}

/// A process must retain this owner and explicitly drain it, or exit the process.
/// Dropping it with a live supervisor aborts rather than silently abandoning work.
pub struct Process {
    sender: mpsc::UnboundedSender<Job>,
    thread: Option<thread::JoinHandle<()>>,
    done: Arc<AtomicBool>,
    pub(crate) accepting: Arc<AtomicBool>,
}

/// Abort before unwinding can discard any other supervised owner/checkout.
pub(super) struct AbortOnPanic;
impl Drop for AbortOnPanic {
    fn drop(&mut self) {
        if thread::panicking() {
            std::process::abort();
        }
    }
}

impl Process {
    pub fn new(trace: Trace) -> Arc<Self> {
        let (sender, mut receiver) = mpsc::unbounded_channel();
        let done = Arc::new(AtomicBool::new(false));
        let finished = done.clone();
        let handle = thread::Builder::new()
            .name("store-process".into())
            .spawn(move || {
                let _abort = AbortOnPanic;
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("store runtime");
                runtime.block_on(async move {
                    let mut tasks = FuturesUnordered::<Work>::new();
                    let _abort = AbortOnPanic;
                    let mut stopping = false;
                    loop {
                        if stopping && tasks.is_empty() {
                            break;
                        }
                        tokio::select! {
                            job = receiver.recv(), if !stopping => match job {
                                Some(Job::Work(work)) => tasks.push(work),
                                Some(Job::Stop) | None => { stopping = true; receiver.close(); }
                            },
                            Some(()) = tasks.next(), if !tasks.is_empty() => {}
                        }
                    }
                    trace(serde_json::json!({"event":"supervisor_drained"}));
                });
                finished.store(true, Ordering::Release);
            })
            .expect("store supervisor thread");
        Arc::new(Self {
            sender,
            thread: Some(handle),
            done,
            accepting: Arc::new(AtomicBool::new(true)),
        })
    }

    pub(crate) fn submit(
        &self,
        work: impl Future<Output = ()> + Send + 'static,
    ) -> anyhow::Result<()> {
        anyhow::ensure!(self.accepting.load(Ordering::Acquire), "store is stopping");
        self.sender
            .send(Job::Work(Box::pin(work)))
            .map_err(|_| anyhow::anyhow!("store supervisor unavailable"))
    }

    /// OPS-21 lifecycle fixture support. Drop stores first. Expiry does not abort
    /// tasks or release ownership: the process owner must then exit the process.
    pub async fn shutdown(&mut self, bound: Duration) -> anyhow::Result<()> {
        self.accepting.store(false, Ordering::Release);
        let _ = self.sender.send(Job::Stop);
        tokio::time::timeout(bound, async {
            while !self.done.load(Ordering::Acquire) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .map_err(|_| {
            anyhow::anyhow!("unresolved store work at process stop bound; process must exit")
        })?;
        if self.thread.take().expect("shutdown once").join().is_err() {
            std::process::abort();
        }
        Ok(())
    }
}

impl Drop for Process {
    fn drop(&mut self) {
        if self.thread.is_some() {
            std::process::abort();
        }
    }
}
