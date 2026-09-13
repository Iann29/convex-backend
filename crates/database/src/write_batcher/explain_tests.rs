use async_trait::async_trait;
use common::{
    errors::database_timeout_error,
    execution_context::{
        ExecutionContext,
        RequestContext,
    },
    explain::{
        ExplainRecorder,
        StartCapture,
    },
    persistence::{
        IndexRetentionProgress,
        IndexRetentionRequest,
        PersistenceGlobalKey,
        PersistenceReader,
    },
    types::{
        FunctionCaller,
        Timestamp,
    },
    RequestId,
};
use runtime::prod::ProdRuntime;
use serde_json::{
    json,
    Value,
};
use value::{
    InternalDocumentId,
    TabletId,
};

use super::*;

/// Only write is exercised. The response channel controls when persistence
/// confirms a write, so an early ACK cannot hide behind scheduler timing.
struct ControlledPersistence {
    writes: mpsc::UnboundedSender<(Vec<DocumentLogEntry>, oneshot::Sender<anyhow::Result<()>>)>,
}

#[async_trait]
impl Persistence for ControlledPersistence {
    async fn write<'a>(
        &self,
        documents: &'a [DocumentLogEntry],
        indexes: &'a [PersistenceIndexEntry],
        conflict_strategy: ConflictStrategy,
    ) -> anyhow::Result<()> {
        assert!(indexes.is_empty());
        assert!(matches!(conflict_strategy, ConflictStrategy::Error));
        let (send, receive) = oneshot::channel();
        self.writes.send((documents.to_vec(), send)).unwrap();
        receive.await?
    }

    fn is_fresh(&self) -> bool {
        panic!("not used by batcher")
    }

    fn reader(&self) -> Arc<dyn PersistenceReader> {
        panic!("not used by batcher")
    }

    async fn write_persistence_global(
        &self,
        _: PersistenceGlobalKey,
        _: Value,
    ) -> anyhow::Result<()> {
        panic!("not used by batcher")
    }

    async fn has_index_entries(&self) -> anyhow::Result<bool> {
        panic!("not used by batcher")
    }

    async fn reclaim_index_history(
        &self,
        _: IndexRetentionRequest<'_>,
    ) -> anyhow::Result<IndexRetentionProgress> {
        panic!("not used by batcher")
    }

    async fn delete(&self, _: Vec<(Timestamp, InternalDocumentId)>) -> anyhow::Result<usize> {
        panic!("not used by batcher")
    }

    async fn delete_tablet_documents(&self, _: TabletId, _: usize) -> anyhow::Result<usize> {
        panic!("not used by batcher")
    }
}

fn context() -> ExecutionContext {
    ExecutionContext::new(
        RequestContext::new_for_system_request(RequestId::new()),
        &FunctionCaller::HttpEndpoint,
    )
}

fn document(id: InternalDocumentId) -> DocumentLogEntry {
    DocumentLogEntry {
        ts: Timestamp::MIN,
        id,
        value: None,
        prev_ts: None,
    }
}

#[test]
fn shared_batch_retry_preserves_identity_and_waits_for_confirmed_write() -> anyhow::Result<()> {
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let runtime = ProdRuntime::new(&tokio);
    // Drive the current-thread scheduler itself; Handle::block_on does not
    // poll its spawned tasks or timers.
    tokio.block_on(async move {
        tokio::time::timeout(Duration::from_secs(10), async move {
            let recorder = ExplainRecorder::default();
            let capture = recorder.start(StartCapture::default())?;
            let first_context = context();
            let second_context = context();
            let first = recorder
                .begin(&first_context, "a:write", "", "mutation", None)
                .unwrap();
            let second = recorder
                .begin(&second_context, "b:write", "", "mutation", None)
                .unwrap();
            let (writes, mut calls) = mpsc::unbounded_channel();
            let (batcher, _worker) = WriteBatcher::start(
                runtime,
                Arc::new(ControlledPersistence { writes }),
                WriteBatcherConfig {
                    max_documents: 2,
                    max_bytes: u64::MAX,
                    max_delay: Duration::from_secs(1),
                    max_concurrent_writes: 1,
                    batch_write_threshold: 1,
                },
            );
            let documents = [
                document(InternalDocumentId::MIN),
                document(InternalDocumentId::MAX),
            ];
            let write_first =
                batcher.write(vec![documents[0].clone()], vec![], 1, Some(first.handle()));
            let write_second =
                batcher.write(vec![documents[1].clone()], vec![], 1, Some(second.handle()));
            pin_mut!(write_first, write_second);
            assert!(write_first.as_mut().now_or_never().is_none());
            assert!(write_second.as_mut().now_or_never().is_none());
            let (attempt_zero, reject) = calls.recv().await.unwrap();
            assert_eq!(attempt_zero, documents);
            assert!(write_first.as_mut().now_or_never().is_none());
            assert!(write_second.as_mut().now_or_never().is_none());
            reject.send(Err(database_timeout_error("fixture"))).unwrap();
            let (attempt_one, confirm) = calls.recv().await.unwrap();
            assert_eq!(
                attempt_one, attempt_zero,
                "Retry changed the physical write payload"
            );
            assert!(
                write_first.as_mut().now_or_never().is_none(),
                "First commit acknowledged before persistence confirmation"
            );
            assert!(
                write_second.as_mut().now_or_never().is_none(),
                "Second commit acknowledged before persistence confirmation"
            );
            let before = recorder.poll(&capture.session_id, 0, None, false)?;
            assert_eq!(
                before
                    .events
                    .iter()
                    .filter(|e| matches!(e.event, ExplainEvent::PersistenceBatch { .. }))
                    .count(),
                6
            );
            confirm.send(Ok(())).unwrap();
            futures::try_join!(write_first, write_second)?;
            assert!(
                calls.try_recv().is_err(),
                "Unexpected third persistence attempt"
            );
            let snapshot = recorder.poll(&capture.session_id, 0, None, false)?;
            let physical: Vec<_> = snapshot
                .events
                .iter()
                .filter(|e| matches!(e.event, ExplainEvent::PersistenceBatch { .. }))
                .collect();
            assert_eq!(
                physical.len(),
                8,
                "Two members need start/finish for each of two attempts"
            );
            let batch_id = serde_json::to_value(&physical[0].event)?["batchId"].clone();
            for attempt in [0, 1] {
                let events: Vec<_> = physical
                    .iter()
                    .filter(|e| {
                        matches!(
                        e.event,
                        ExplainEvent::PersistenceBatch { attempt: a, .. } if a == attempt
                        )
                    })
                    .collect();
                assert_eq!(events.len(), 4);
                let left: Vec<_> = events
                    .iter()
                    .filter(|e| e.execution_id == first_context.execution_id.to_string())
                    .map(|e| serde_json::to_value(&e.event).unwrap())
                    .collect();
                let right: Vec<_> = events
                    .iter()
                    .filter(|e| e.execution_id == second_context.execution_id.to_string())
                    .map(|e| serde_json::to_value(&e.event).unwrap())
                    .collect();
                assert_eq!(
                    left, right,
                    "Members must share exact physical identity, start and duration"
                );
                assert_eq!(left.len(), 2);
                assert_eq!(left[0]["batchId"], batch_id);
                assert_eq!(left[1]["batchId"], batch_id);
                assert_eq!(left[0]["startOffsetMs"], left[1]["startOffsetMs"]);
                assert_eq!(left[0]["durationMs"], Value::Null);
                assert_eq!(left[0]["outcome"], Value::Null);
                assert!(left[1]["durationMs"].as_f64().unwrap() >= 0.0);
                assert_eq!(
                    left[1]["outcome"],
                    json!(if attempt == 0 { "error" } else { "success" })
                );
                assert_eq!(left[1]["willRetry"], json!(attempt == 0));
                for event in left {
                    assert_eq!(event["commitCount"], 2);
                    assert_eq!(event["documentRows"], 2);
                    assert_eq!(event["shared"], true);
                }
            }
            assert_eq!(snapshot.coverage.dropped_events, 0);
            Ok::<_, anyhow::Error>(())
        })
        .await?
    })
}

#[test]
fn terminal_write_failure_reaches_every_member_without_retry() -> anyhow::Result<()> {
    let tokio = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    let runtime = ProdRuntime::new(&tokio);
    tokio.block_on(async move {
        tokio::time::timeout(Duration::from_secs(10), async move {
            let recorder = ExplainRecorder::default();
            let capture = recorder.start(StartCapture::default())?;
            let guard = recorder
                .begin(&context(), "a:write", "", "mutation", None)
                .unwrap();
            let (writes, mut calls) = mpsc::unbounded_channel();
            let (ack_one, mut receive_one) = oneshot::channel();
            let (ack_two, mut receive_two) = oneshot::channel();
            let batch = Batch {
                documents: vec![],
                indexes: vec![],
                size_bytes: 0,
                acks: vec![ack_one, ack_two],
                explain: vec![(
                    guard.handle(),
                    guard.handle().phase(Phase::PersistenceQueue),
                )],
            };
            let future = write_batch(runtime, Arc::new(ControlledPersistence { writes }), batch);
            pin_mut!(future);
            assert!(future.as_mut().now_or_never().is_none());
            let (_, fail) = calls.recv().await.unwrap();
            assert!(receive_one.try_recv().is_err());
            assert!(receive_two.try_recv().is_err());
            fail.send(Err(anyhow::anyhow!("fixture terminal write error")))
                .unwrap();
            future.await;
            for receive in [receive_one, receive_two] {
                assert!(receive
                    .await?
                    .unwrap_err()
                    .to_string()
                    .contains("fixture terminal write error"));
            }
            assert!(calls.try_recv().is_err());
            let snapshot = recorder.poll(&capture.session_id, 0, None, false)?;
            let physical: Vec<_> = snapshot
                .events
                .iter()
                .filter(|e| matches!(e.event, ExplainEvent::PersistenceBatch { .. }))
                .collect();
            assert_eq!(physical.len(), 2);
            let finish = serde_json::to_value(&physical[1].event)?;
            assert_eq!(finish["outcome"], "error");
            assert_eq!(finish["willRetry"], false);
            assert_eq!(finish["attempt"], 0);
            assert_eq!(
                finish["commitCount"], 2,
                "Uncaptured member still belongs to the physical batch"
            );
            Ok::<_, anyhow::Error>(())
        })
        .await?
    })
}
