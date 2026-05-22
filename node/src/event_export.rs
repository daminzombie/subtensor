use crate::client::FullClient;
use codec::{Decode, Encode};
use futures::{FutureExt, StreamExt};
use node_subtensor_runtime::{RuntimeCall, opaque::Block};
use sc_client_api::client::BlockchainEvents;
use sc_service::{TaskManager, error::Error as ServiceError};
use sc_transaction_pool::TransactionPoolHandle;
use sc_transaction_pool_api::{
    InPoolTransaction, PoolLifecycleEvent, PoolMaintenanceEvent, PoolTransactionEvent,
    TransactionPool,
};
use serde::Serialize;
use serde_json::{Map, Value, json};
use sp_consensus::block_validation::{BlockAnnounceValidator, Validation};
use sp_runtime::traits::{BlakeTwo256, Block as BlockT, Hash as HashT, Header as HeaderT};
use std::error::Error;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{
    Arc, OnceLock,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::mpsc;

const LOG_TARGET: &str = "event-export";
const DB_BATCH_SIZE: usize = 512;
const DB_FLUSH_INTERVAL: Duration = Duration::from_millis(500);
static VIEW_SNAPSHOT_SEQ: AtomicU64 = AtomicU64::new(1);

#[derive(Clone, Debug)]
pub struct EventExportConfig {
    pub db_path: PathBuf,
    pub log_xt_data: bool,
    pub channel_capacity: usize,
}

#[derive(Clone)]
pub struct EventSink {
    sender: mpsc::Sender<DbEvent>,
    dropped: Arc<AtomicU64>,
}

impl EventSink {
    fn new(sender: mpsc::Sender<DbEvent>) -> Self {
        Self {
            sender,
            dropped: Arc::new(AtomicU64::new(0)),
        }
    }

    fn enqueue(&self, event: DbEvent) {
        if self.sender.try_send(event).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[derive(Clone, Default)]
pub struct SharedEventSink {
    inner: Arc<OnceLock<EventSink>>,
}

impl SharedEventSink {
    pub fn new() -> Self {
        Self::default()
    }

    fn set(&self, sink: EventSink) {
        let _ = self.inner.set(sink);
    }

    fn enqueue(&self, event: DbEvent) {
        if let Some(sink) = self.inner.get() {
            sink.enqueue(event);
        }
    }
}

#[derive(Debug)]
enum DbEvent {
    Extrinsic(ExtrinsicRow),
    ExtrinsicEvent(ExtrinsicEventRow),
    PoolView(PoolViewRow),
    PoolViewMember(PoolViewMemberRow),
    ChainEvent(ChainEventRow),
    Timeline(TimelineEventRow),
    WriterStats(WriterStatsRow),
}

#[derive(Debug)]
struct ExtrinsicRow {
    event_time_ms: i64,
    tx_hash: String,
    first_seen_source: String,
    encoded: Option<Vec<u8>>,
    encoded_len: u64,
    classification: String,
    details_json: String,
    last_status: String,
}

#[derive(Debug)]
struct ExtrinsicEventRow {
    event_time_ms: i64,
    tx_hash: String,
    event_kind: String,
    slot: Option<u64>,
    block_number: Option<u64>,
    parent_hash: Option<String>,
    view_id: Option<String>,
    insertion_id: Option<u64>,
    details_json: String,
}

#[derive(Debug)]
struct PoolViewRow {
    event_time_ms: i64,
    view_id: String,
    slot: Option<u64>,
    parent_hash: String,
    parent_number: u64,
    best_hash: String,
    ready_count: u64,
    future_count: u64,
    queue_depth: u64,
    reason: String,
    trigger_tx_hash: Option<String>,
}

#[derive(Debug)]
struct PoolViewMemberRow {
    event_time_ms: i64,
    view_id: String,
    section: String,
    ordinal: u64,
    insertion_id: Option<u64>,
    tx_hash: String,
    priority: Option<String>,
    requires_json: String,
    provides_json: String,
    encoded_len: u64,
}

#[derive(Debug)]
struct ChainEventRow {
    event_time_ms: i64,
    event_kind: String,
    block_hash: String,
    block_number: u64,
    is_new_best: bool,
    origin: String,
    details_json: String,
}

#[derive(Debug)]
struct TimelineEventRow {
    event_time_ms: i64,
    slot: Option<u64>,
    parent_hash: Option<String>,
    event_kind: String,
    details_json: String,
}

#[derive(Debug)]
struct WriterStatsRow {
    event_time_ms: i64,
    queued: u64,
    written: u64,
    dropped: u64,
    flush_duration_ms: u64,
    batch_size: u64,
    last_error: Option<String>,
}

pub fn spawn_event_export(
    task_manager: &TaskManager,
    config: EventExportConfig,
    client: Arc<FullClient>,
    transaction_pool: Arc<TransactionPoolHandle<Block, FullClient>>,
    shared_sink: Option<SharedEventSink>,
) -> Result<(), ServiceError> {
    let channel_capacity = config.channel_capacity.max(1);
    let (sender, receiver) = mpsc::channel(channel_capacity);
    let sink = EventSink::new(sender);
    if let Some(shared_sink) = shared_sink {
        shared_sink.set(sink.clone());
    }

    spawn_db_writer(task_manager, config.db_path.clone(), receiver, sink.clone());
    spawn_chain_event_recorder(
        task_manager,
        client.clone(),
        transaction_pool.clone(),
        sink.clone(),
        config.log_xt_data,
    );
    spawn_txpool_event_recorder(
        task_manager,
        client.clone(),
        transaction_pool.clone(),
        sink.clone(),
        config.log_xt_data,
    );
    spawn_txpool_lifecycle_event_recorder(
        task_manager,
        client.clone(),
        transaction_pool.clone(),
        sink.clone(),
        config.log_xt_data,
    );

    log::info!(
        target: LOG_TARGET,
        "Event export enabled. Writing diagnostics to {:?}",
        config.db_path
    );

    Ok(())
}

pub fn block_announce_validator_builder(
    sink: SharedEventSink,
) -> Box<dyn FnOnce(Arc<FullClient>) -> Box<dyn BlockAnnounceValidator<Block> + Send> + Send> {
    Box::new(move |_| Box::new(RecordingBlockAnnounceValidator { sink }))
}

struct RecordingBlockAnnounceValidator {
    sink: SharedEventSink,
}

impl BlockAnnounceValidator<Block> for RecordingBlockAnnounceValidator {
    fn validate(
        &mut self,
        header: &<Block as BlockT>::Header,
        data: &[u8],
    ) -> Pin<Box<dyn futures::Future<Output = Result<Validation, Box<dyn Error + Send>>> + Send>>
    {
        let sink = self.sink.clone();
        let block_hash = header.hash();
        let block_hash_string = hash_string(&block_hash);
        let block_number = (*header.number()).into();
        let data_len = data.len();

        async move {
            let validation = Validation::Success { is_new_best: false };
            let validation_result = match validation {
                Validation::Success { is_new_best } => {
                    json!({ "result": "success", "is_new_best": is_new_best })
                }
                Validation::Failure { disconnect } => json!({
                    "result": "failure",
                    "disconnect": disconnect
                }),
            };

            sink.enqueue(DbEvent::ChainEvent(ChainEventRow {
                event_time_ms: now_ms(),
                event_kind: "block_announce".to_string(),
                block_hash: block_hash_string.clone(),
                block_number,
                is_new_best: false,
                origin: "network_block_announce_validator".to_string(),
                details_json: json!({
                    "data_len": data_len,
                    "validation": validation_result,
                })
                .to_string(),
            }));
            sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                event_time_ms: now_ms(),
                slot: None,
                parent_hash: Some(block_hash_string.clone()),
                event_kind: "block_announce_received".to_string(),
                details_json: json!({
                    "block_hash": block_hash_string,
                    "block_number": block_number,
                    "data_len": data_len,
                    "validation": validation_result,
                })
                .to_string(),
            }));

            Ok(validation)
        }
        .boxed()
    }
}

async fn capture_pool_view(
    transaction_pool: &Arc<TransactionPoolHandle<Block, FullClient>>,
    parent_hash: &<Block as BlockT>::Hash,
    parent_number: u64,
    slot: Option<u64>,
    reason: &str,
    trigger_tx_hash: Option<String>,
    sink: &EventSink,
    log_xt_data: bool,
) -> String {
    let event_time_ms = now_ms();
    let snapshot_seq = VIEW_SNAPSHOT_SEQ.fetch_add(1, Ordering::Relaxed);
    let view_id = format!(
        "{}-{}-{snapshot_seq}-{reason}",
        slot.unwrap_or(parent_number),
        event_time_ms
    );
    let status = transaction_pool.status();
    let mut ready_count = 0u64;
    let mut future_count = 0u64;

    let ready = transaction_pool
        .ready_at_with_timeout(*parent_hash, Duration::from_millis(25))
        .await;
    for (ordinal, tx) in ready.enumerate() {
        let tx_hash = hash_string(tx.hash());
        let data = tx.data();
        let insertion_id = tx.insertion_id();
        let record = classify_extrinsic(data, log_xt_data, "pool_ready");
        sink.enqueue(DbEvent::Extrinsic(record.with_hash(tx_hash.clone())));
        sink.enqueue(DbEvent::PoolViewMember(PoolViewMemberRow {
            event_time_ms,
            view_id: view_id.clone(),
            section: "ready".to_string(),
            ordinal: ordinal as u64,
            insertion_id,
            tx_hash: tx_hash.clone(),
            priority: Some(tx.priority().to_string()),
            requires_json: hex_tags(tx.requires()),
            provides_json: hex_tags(tx.provides()),
            encoded_len: data.encode().len() as u64,
        }));
        sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
            event_time_ms,
            tx_hash,
            event_kind: "ready".to_string(),
            slot,
            block_number: Some(parent_number.saturating_add(1)),
            parent_hash: Some(hash_string(parent_hash)),
            view_id: Some(view_id.clone()),
            insertion_id,
            details_json: json!({
                "reason": reason,
                "section": "ready",
                "ordinal": ordinal,
                "insertion_id": insertion_id,
                "view_id": view_id,
                "parent_block_number": parent_number,
                "build_block_number": parent_number.saturating_add(1),
                "view_block_number": parent_number.saturating_add(1),
            })
            .to_string(),
        }));
        ready_count = ready_count.saturating_add(1);
    }

    for (ordinal, tx) in transaction_pool.futures().into_iter().enumerate() {
        let tx_hash = hash_string(tx.hash());
        let data = tx.data();
        let insertion_id = tx.insertion_id();
        let record = classify_extrinsic(data, log_xt_data, "pool_future");
        sink.enqueue(DbEvent::Extrinsic(record.with_hash(tx_hash.clone())));
        sink.enqueue(DbEvent::PoolViewMember(PoolViewMemberRow {
            event_time_ms,
            view_id: view_id.clone(),
            section: "future".to_string(),
            ordinal: ordinal as u64,
            insertion_id,
            tx_hash: tx_hash.clone(),
            priority: Some(tx.priority().to_string()),
            requires_json: hex_tags(tx.requires()),
            provides_json: hex_tags(tx.provides()),
            encoded_len: data.encode().len() as u64,
        }));
        sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
            event_time_ms,
            tx_hash,
            event_kind: "future".to_string(),
            slot,
            block_number: Some(parent_number.saturating_add(1)),
            parent_hash: Some(hash_string(parent_hash)),
            view_id: Some(view_id.clone()),
            insertion_id,
            details_json: json!({
                "reason": reason,
                "section": "future",
                "ordinal": ordinal,
                "insertion_id": insertion_id,
                "view_id": view_id,
                "parent_block_number": parent_number,
                "build_block_number": parent_number.saturating_add(1),
                "view_block_number": parent_number.saturating_add(1),
            })
            .to_string(),
        }));
        future_count = future_count.saturating_add(1);
    }

    sink.enqueue(DbEvent::PoolView(PoolViewRow {
        event_time_ms,
        view_id: view_id.clone(),
        slot,
        parent_hash: hash_string(parent_hash),
        parent_number,
        best_hash: hash_string(parent_hash),
        ready_count,
        future_count,
        queue_depth: status.ready.saturating_add(status.future) as u64,
        reason: reason.to_string(),
        trigger_tx_hash: trigger_tx_hash.clone(),
    }));
    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
        event_time_ms,
        slot,
        parent_hash: Some(hash_string(parent_hash)),
        event_kind: "view_created".to_string(),
        details_json: json!({
            "view_id": view_id,
            "parent_block_number": parent_number,
            "build_block_number": parent_number.saturating_add(1),
            "view_block_number": parent_number.saturating_add(1),
            "parent_number": parent_number,
            "parent_hash": hash_string(parent_hash),
            "ready_count": ready_count,
            "future_count": future_count,
            "status_ready": status.ready,
            "status_future": status.future,
            "reason": reason,
            "trigger_tx_hash": trigger_tx_hash,
        })
        .to_string(),
    }));

    view_id
}

fn spawn_chain_event_recorder(
    task_manager: &TaskManager,
    client: Arc<FullClient>,
    transaction_pool: Arc<TransactionPoolHandle<Block, FullClient>>,
    sink: EventSink,
    log_xt_data: bool,
) {
    task_manager.spawn_handle().spawn(
        "event-export-chain-events",
        Some("event-export"),
        async move {
            let mut stream = client.import_notification_stream();
            while let Some(notification) = stream.next().await {
                let event_time_ms = now_ms();
                let block_number = (*notification.header.number()).into();
                let block_hash = hash_string(&notification.hash);
                sink.enqueue(DbEvent::ChainEvent(ChainEventRow {
                    event_time_ms,
                    event_kind: "block_import".to_string(),
                    block_hash: block_hash.clone(),
                    block_number,
                    is_new_best: notification.is_new_best,
                    origin: format!("{:?}", notification.origin),
                    details_json: json!({
                        "has_tree_route": notification.tree_route.is_some(),
                    })
                    .to_string(),
                }));

                if notification.is_new_best {
                    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                        event_time_ms,
                        slot: None,
                        parent_hash: Some(block_hash.clone()),
                        event_kind: "new_best_block_import".to_string(),
                        details_json: json!({
                            "block_hash": block_hash,
                            "block_number": block_number,
                        })
                        .to_string(),
                    }));
                }

                let _ = capture_pool_view(
                    &transaction_pool,
                    &notification.hash,
                    block_number,
                    None,
                    "new_best_block_import",
                    None,
                    &sink,
                    log_xt_data,
                )
                .await;
            }
        },
    );
}

fn spawn_txpool_event_recorder(
    task_manager: &TaskManager,
    client: Arc<FullClient>,
    transaction_pool: Arc<TransactionPoolHandle<Block, FullClient>>,
    sink: EventSink,
    log_xt_data: bool,
) {
    task_manager.spawn_handle().spawn(
        "event-export-txpool-events",
        Some("event-export"),
        async move {
            let mut stream = transaction_pool.import_notification_stream();
            while let Some(tx_hash) = stream.next().await {
                let tx_hash_string = hash_string(&tx_hash);
                sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
                    event_time_ms: now_ms(),
                    tx_hash: tx_hash_string.clone(),
                    event_kind: "imported_to_pool".to_string(),
                    slot: None,
                    block_number: None,
                    parent_hash: None,
                    view_id: None,
                    insertion_id: None,
                    details_json: "{}".to_string(),
                }));
                sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                    event_time_ms: now_ms(),
                    slot: None,
                    parent_hash: None,
                    event_kind: "txpool_import".to_string(),
                    details_json: json!({ "tx_hash": tx_hash_string.clone() }).to_string(),
                }));

                let chain_info = client.chain_info();
                let parent_number = chain_info.best_number.into();
                let _ = capture_pool_view(
                    &transaction_pool,
                    &chain_info.best_hash,
                    parent_number,
                    None,
                    "txpool_import",
                    Some(tx_hash_string),
                    &sink,
                    log_xt_data,
                )
                .await;
            }
        },
    );
}

fn spawn_txpool_lifecycle_event_recorder(
    task_manager: &TaskManager,
    client: Arc<FullClient>,
    transaction_pool: Arc<TransactionPoolHandle<Block, FullClient>>,
    sink: EventSink,
    log_xt_data: bool,
) {
    task_manager.spawn_handle().spawn(
        "event-export-txpool-lifecycle-events",
        Some("event-export"),
        async move {
            sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                event_time_ms: now_ms(),
                slot: None,
                parent_hash: None,
                event_kind: "txpool_lifecycle_stream_subscribed".to_string(),
                details_json: "{}".to_string(),
            }));
            let mut stream = transaction_pool.pool_lifecycle_event_stream();
            while let Some(event) = stream.next().await {
                match event {
                    PoolLifecycleEvent::Transaction { hash, event } => {
                        let tx_hash = hash_string(&hash);
                        let event_kind = txpool_transaction_event_kind(&event);
                        sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
                            event_time_ms: now_ms(),
                            tx_hash: tx_hash.clone(),
                            event_kind: event_kind.to_string(),
                            slot: None,
                            block_number: None,
                            parent_hash: None,
                            view_id: None,
                            insertion_id: None,
                            details_json: serde_json::to_string(&event)
                                .unwrap_or_else(|_| "{}".to_string()),
                        }));
                        sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                            event_time_ms: now_ms(),
                            slot: None,
                            parent_hash: None,
                            event_kind: event_kind.to_string(),
                            details_json: json!({ "tx_hash": tx_hash.clone(), "event": event.clone() })
                                .to_string(),
                        }));

                        if matches!(event, PoolTransactionEvent::Ready) {
                            let chain_info = client.chain_info();
                            let parent_number = chain_info.best_number.into();
                            let _ = capture_pool_view(
                                &transaction_pool,
                                &chain_info.best_hash,
                                parent_number,
                                None,
                                "txpool_ready",
                                Some(tx_hash.clone()),
                                &sink,
                                log_xt_data,
                            )
                            .await;
                        }
                    }
                    PoolLifecycleEvent::Maintenance { event } => {
                        let (event_kind, block_hash, is_finalized) =
                            txpool_maintenance_event_parts(&event);
                        let block_hash_string = hash_string(&block_hash);
                        sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                            event_time_ms: now_ms(),
                            slot: None,
                            parent_hash: Some(block_hash_string.clone()),
                            event_kind: event_kind.to_string(),
                            details_json: json!({
                                "block_hash": block_hash_string,
                                "is_finalized": is_finalized,
                                "event": event,
                            })
                            .to_string(),
                        }));
                    }
                }
            }
            sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                event_time_ms: now_ms(),
                slot: None,
                parent_hash: None,
                event_kind: "txpool_lifecycle_stream_ended".to_string(),
                details_json: json!({
                    "meaning": "The transaction pool implementation returned an empty/default lifecycle stream. Maintenance lifecycle events are unavailable from this pool instance.",
                })
                .to_string(),
            }));
        },
    );
}

fn txpool_transaction_event_kind(
    event: &PoolTransactionEvent<<Block as BlockT>::Hash, <Block as BlockT>::Hash>,
) -> &'static str {
    match event {
        PoolTransactionEvent::ImportedReady => "txpool_imported_ready",
        PoolTransactionEvent::ImportedFuture => "txpool_imported_future",
        PoolTransactionEvent::Ready => "txpool_ready",
        PoolTransactionEvent::Future => "txpool_future",
        PoolTransactionEvent::Invalid => "txpool_invalid",
        PoolTransactionEvent::Dropped => "txpool_dropped",
        PoolTransactionEvent::LimitEnforced => "txpool_limit_enforced",
        PoolTransactionEvent::Usurped { .. } => "txpool_usurped",
        PoolTransactionEvent::Broadcasted { .. } => "txpool_broadcasted",
        PoolTransactionEvent::Pruned { .. } => "txpool_pruned",
        PoolTransactionEvent::Retracted { .. } => "txpool_retracted",
        PoolTransactionEvent::FinalityTimeout { .. } => "txpool_finality_timeout",
        PoolTransactionEvent::Finalized { .. } => "txpool_finalized",
    }
}

fn txpool_maintenance_event_parts(
    event: &PoolMaintenanceEvent<<Block as BlockT>::Hash>,
) -> (&'static str, <Block as BlockT>::Hash, bool) {
    match event {
        PoolMaintenanceEvent::Started {
            block_hash,
            is_finalized,
        } => ("pool_maintenance_started", *block_hash, *is_finalized),
        PoolMaintenanceEvent::Finished {
            block_hash,
            is_finalized,
        } => ("pool_maintenance_finished", *block_hash, *is_finalized),
        PoolMaintenanceEvent::Skipped {
            block_hash,
            is_finalized,
        } => ("pool_maintenance_skipped", *block_hash, *is_finalized),
    }
}

fn spawn_db_writer(
    task_manager: &TaskManager,
    db_path: PathBuf,
    mut receiver: mpsc::Receiver<DbEvent>,
    sink: EventSink,
) {
    task_manager
        .spawn_handle()
        .spawn("event-export-db-writer", Some("event-export"), async move {
            if let Err(error) = run_db_writer(db_path, &mut receiver, sink).await {
                log::error!(target: LOG_TARGET, "event export DB writer stopped: {error}");
            }
        });
}

async fn run_db_writer(
    db_path: PathBuf,
    receiver: &mut mpsc::Receiver<DbEvent>,
    sink: EventSink,
) -> Result<(), String> {
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("create event export db directory failed: {error}"))?;
    }

    let db_url = format!("sqlite://{}", db_path.display());
    let options = db_url
        .parse::<sqlx::sqlite::SqliteConnectOptions>()
        .map_err(|error| format!("invalid sqlite URL {db_url}: {error}"))?
        .create_if_missing(true)
        .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
        .synchronous(sqlx::sqlite::SqliteSynchronous::Normal)
        .busy_timeout(Duration::from_millis(250));
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .map_err(|error| format!("open event export db failed: {error}"))?;
    migrate(&pool).await?;

    let mut queued = 0u64;
    let mut written = 0u64;
    let mut interval = tokio::time::interval(DB_FLUSH_INTERVAL);
    let mut batch = Vec::with_capacity(DB_BATCH_SIZE);

    loop {
        tokio::select! {
            Some(event) = receiver.recv() => {
                batch.push(event);
                queued = queued.saturating_add(1);
                while batch.len() < DB_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(event) => {
                            batch.push(event);
                            queued = queued.saturating_add(1);
                        }
                        Err(_) => break,
                    }
                }
            }
            _ = interval.tick() => {}
            else => break,
        }

        if batch.is_empty() {
            continue;
        }

        let started = std::time::Instant::now();
        let batch_size = batch.len() as u64;
        match flush_batch(&pool, &mut batch).await {
            Ok(()) => {
                written = written.saturating_add(batch_size);
                let flush_duration_ms = started.elapsed().as_millis() as u64;
                if batch_size > 1 || flush_duration_ms > 100 {
                    sink.enqueue(DbEvent::WriterStats(WriterStatsRow {
                        event_time_ms: now_ms(),
                        queued,
                        written,
                        dropped: sink.dropped(),
                        flush_duration_ms,
                        batch_size,
                        last_error: None,
                    }));
                }
            }
            Err(error) => {
                let message = error.to_string();
                log::warn!(target: LOG_TARGET, "failed to flush event export batch: {message}");
                batch.clear();
                sink.enqueue(DbEvent::WriterStats(WriterStatsRow {
                    event_time_ms: now_ms(),
                    queued,
                    written,
                    dropped: sink.dropped(),
                    flush_duration_ms: started.elapsed().as_millis() as u64,
                    batch_size,
                    last_error: Some(message),
                }));
            }
        }
    }

    Ok(())
}

async fn migrate(pool: &sqlx::SqlitePool) -> Result<(), String> {
    for statement in [
        "CREATE TABLE IF NOT EXISTS events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_ms INTEGER NOT NULL,
            source TEXT NOT NULL,
            event_kind TEXT NOT NULL,
            slot INTEGER,
            block_number INTEGER,
            block_hash TEXT,
            parent_hash TEXT,
            tx_hash TEXT,
            view_id TEXT,
            insertion_id INTEGER,
            classification TEXT,
            status TEXT,
            details_json TEXT NOT NULL
        )",
        "CREATE INDEX IF NOT EXISTS idx_events_time ON events (event_time_ms, seq)",
        "CREATE INDEX IF NOT EXISTS idx_events_kind_time ON events (event_kind, event_time_ms)",
        "CREATE INDEX IF NOT EXISTS idx_events_tx_time ON events (tx_hash, event_time_ms, seq)",
        "CREATE INDEX IF NOT EXISTS idx_events_block ON events (block_number, block_hash)",
        "CREATE INDEX IF NOT EXISTS idx_events_view_time ON events (view_id, event_time_ms)",
    ] {
        sqlx::query(statement)
            .execute(pool)
            .await
            .map_err(|error| format!("migration failed: {error}; statement: {statement}"))?;
    }
    Ok(())
}

async fn flush_batch(pool: &sqlx::SqlitePool, batch: &mut Vec<DbEvent>) -> Result<(), sqlx::Error> {
    let mut tx = pool.begin().await?;
    for event in batch.drain(..) {
        insert_event(&mut tx, event).await?;
    }
    tx.commit().await
}

async fn insert_event(
    tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
    event: DbEvent,
) -> Result<(), sqlx::Error> {
    let row = NormalizedEvent::from(event);
    sqlx::query(
        "INSERT INTO events (
            event_time_ms, source, event_kind, slot, block_number, block_hash, parent_hash,
            tx_hash, view_id, insertion_id, classification, status, details_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )
    .bind(row.event_time_ms)
    .bind(row.source)
    .bind(row.event_kind)
    .bind(row.slot.map(|v| v as i64))
    .bind(row.block_number.map(|v| v as i64))
    .bind(row.block_hash)
    .bind(row.parent_hash)
    .bind(row.tx_hash)
    .bind(row.view_id)
    .bind(row.insertion_id.map(|v| v as i64))
    .bind(row.classification)
    .bind(row.status)
    .bind(row.details_json)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

struct NormalizedEvent {
    event_time_ms: i64,
    source: &'static str,
    event_kind: String,
    slot: Option<u64>,
    block_number: Option<u64>,
    block_hash: Option<String>,
    parent_hash: Option<String>,
    tx_hash: Option<String>,
    view_id: Option<String>,
    insertion_id: Option<u64>,
    classification: Option<String>,
    status: Option<String>,
    details_json: String,
}

impl From<DbEvent> for NormalizedEvent {
    fn from(event: DbEvent) -> Self {
        match event {
            DbEvent::Extrinsic(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "tx",
                event_kind: "extrinsic_observed".to_string(),
                slot: None,
                block_number: None,
                block_hash: None,
                parent_hash: None,
                tx_hash: Some(row.tx_hash),
                view_id: None,
                insertion_id: None,
                classification: Some(row.classification),
                status: Some(row.last_status),
                details_json: merge_details(
                    row.details_json,
                    json!({
                        "first_seen_source": row.first_seen_source,
                        "encoded_len": row.encoded_len,
                        "encoded_logged": row.encoded.is_some(),
                    }),
                ),
            },
            DbEvent::ExtrinsicEvent(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "tx",
                event_kind: row.event_kind,
                slot: row.slot,
                block_number: row.block_number,
                block_hash: None,
                parent_hash: row.parent_hash,
                tx_hash: Some(row.tx_hash),
                view_id: row.view_id,
                insertion_id: row.insertion_id,
                classification: None,
                status: None,
                details_json: row.details_json,
            },
            DbEvent::PoolView(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "pool",
                event_kind: "pool_view_created".to_string(),
                slot: row.slot,
                block_number: Some(row.parent_number.saturating_add(1)),
                block_hash: Some(row.best_hash.clone()),
                parent_hash: Some(row.parent_hash.clone()),
                tx_hash: None,
                view_id: Some(row.view_id.clone()),
                insertion_id: None,
                classification: None,
                status: Some(row.reason.clone()),
                details_json: json!({
                    "view_id": row.view_id,
                    "parent_hash": row.parent_hash,
                    "parent_block_number": row.parent_number,
                    "build_block_number": row.parent_number.saturating_add(1),
                    "view_block_number": row.parent_number.saturating_add(1),
                    "parent_number": row.parent_number,
                    "best_hash": row.best_hash,
                    "ready_count": row.ready_count,
                    "future_count": row.future_count,
                    "queue_depth": row.queue_depth,
                    "reason": row.reason,
                    "trigger_tx_hash": row.trigger_tx_hash,
                })
                .to_string(),
            },
            DbEvent::PoolViewMember(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "pool",
                event_kind: "pool_view_member".to_string(),
                slot: None,
                block_number: None,
                block_hash: None,
                parent_hash: None,
                tx_hash: Some(row.tx_hash),
                view_id: Some(row.view_id),
                insertion_id: row.insertion_id,
                classification: None,
                status: Some(row.section.clone()),
                details_json: json!({
                    "section": row.section,
                    "ordinal": row.ordinal,
                    "insertion_id": row.insertion_id,
                    "priority": row.priority,
                    "requires": row.requires_json,
                    "provides": row.provides_json,
                    "encoded_len": row.encoded_len,
                })
                .to_string(),
            },
            DbEvent::ChainEvent(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "chain",
                event_kind: row.event_kind,
                slot: None,
                block_number: Some(row.block_number),
                block_hash: Some(row.block_hash),
                parent_hash: None,
                tx_hash: None,
                view_id: None,
                insertion_id: None,
                classification: None,
                status: Some(row.origin.clone()),
                details_json: merge_details(
                    row.details_json,
                    json!({
                        "is_new_best": row.is_new_best,
                        "origin": row.origin,
                    }),
                ),
            },
            DbEvent::Timeline(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "timeline",
                event_kind: row.event_kind,
                slot: row.slot,
                block_number: None,
                block_hash: row.parent_hash.clone(),
                parent_hash: row.parent_hash,
                tx_hash: None,
                view_id: None,
                insertion_id: None,
                classification: None,
                status: None,
                details_json: row.details_json,
            },
            DbEvent::WriterStats(row) => Self {
                event_time_ms: row.event_time_ms,
                source: "writer",
                event_kind: "writer_stats".to_string(),
                slot: None,
                block_number: None,
                block_hash: None,
                parent_hash: None,
                tx_hash: None,
                view_id: None,
                insertion_id: None,
                classification: None,
                status: row.last_error.as_ref().map(|_| "error".to_string()),
                details_json: json!({
                    "queued": row.queued,
                    "written": row.written,
                    "dropped": row.dropped,
                    "flush_duration_ms": row.flush_duration_ms,
                    "batch_size": row.batch_size,
                    "last_error": row.last_error,
                })
                .to_string(),
            },
        }
    }
}

fn merge_details(details_json: String, extra: Value) -> String {
    let mut details = match serde_json::from_str::<Value>(&details_json) {
        Ok(Value::Object(map)) => map,
        Ok(value) => {
            let mut map = Map::new();
            map.insert("details".to_string(), value);
            map
        }
        Err(_) => {
            let mut map = Map::new();
            map.insert("details".to_string(), Value::String(details_json));
            map
        }
    };

    if let Value::Object(extra) = extra {
        for (key, value) in extra {
            details.insert(key, value);
        }
    }

    Value::Object(details).to_string()
}

#[derive(Debug)]
struct ClassifiedExtrinsic {
    encoded: Option<Vec<u8>>,
    encoded_len: u64,
    classification: String,
    details_json: String,
    last_status: String,
    first_seen_source: String,
}

impl ClassifiedExtrinsic {
    fn with_hash(self, tx_hash: String) -> ExtrinsicRow {
        ExtrinsicRow {
            event_time_ms: now_ms(),
            tx_hash,
            first_seen_source: self.first_seen_source,
            encoded: self.encoded,
            encoded_len: self.encoded_len,
            classification: self.classification,
            details_json: self.details_json,
            last_status: self.last_status,
        }
    }
}

fn classify_extrinsic(
    xt: &<Block as BlockT>::Extrinsic,
    include_data: bool,
    source: &str,
) -> ClassifiedExtrinsic {
    let encoded = xt.encode();
    let encoded_len = encoded.len() as u64;
    let mut classification = "substrate".to_string();
    let mut details = json!({
        "encoded_len": encoded_len,
        "encoded_blake2_256": hash_string(&BlakeTwo256::hash(&encoded)),
    });

    if let Ok(decoded) = node_subtensor_runtime::UncheckedExtrinsic::decode(&mut &encoded[..]) {
        if let RuntimeCall::Ethereum(pallet_ethereum::Call::transact { transaction }) =
            decoded.0.function
        {
            let data = pallet_ethereum::TransactionData::from(&transaction);
            classification = "evm".to_string();
            details = json!({
                "ethereum_tx_hash": format!("{:#x}", transaction.hash()),
                "from": recover_evm_sender(&transaction),
                "to": match data.action {
                    pallet_ethereum::TransactionAction::Call(to) => Some(format!("{:#x}", to)),
                    pallet_ethereum::TransactionAction::Create => None,
                },
                "action": format!("{:?}", data.action),
                "nonce": data.nonce.to_string(),
                "gas_limit": data.gas_limit.to_string(),
                "gas_price": data.gas_price.map(|v| v.to_string()),
                "max_fee_per_gas": data.max_fee_per_gas.map(|v| v.to_string()),
                "max_priority_fee_per_gas": data.max_priority_fee_per_gas.map(|v| v.to_string()),
                "value": data.value.to_string(),
                "chain_id": data.chain_id,
                "input_len": data.input.len(),
                "input_blake2_256": hash_string(&BlakeTwo256::hash(&data.input)),
            });
        }
    }

    ClassifiedExtrinsic {
        encoded: include_data.then_some(encoded),
        encoded_len,
        classification,
        details_json: details.to_string(),
        last_status: source.to_string(),
        first_seen_source: source.to_string(),
    }
}

fn recover_evm_sender(transaction: &pallet_ethereum::Transaction) -> Option<String> {
    let mut sig = [0u8; 65];
    let mut msg = [0u8; 32];
    match transaction {
        pallet_ethereum::Transaction::Legacy(tx) => {
            sig[0..32].copy_from_slice(&tx.signature.r()[..]);
            sig[32..64].copy_from_slice(&tx.signature.s()[..]);
            sig[64] = tx.signature.standard_v();
            msg.copy_from_slice(&ethereum::LegacyTransactionMessage::from(tx.clone()).hash()[..]);
        }
        pallet_ethereum::Transaction::EIP2930(tx) => {
            sig[0..32].copy_from_slice(&tx.signature.r()[..]);
            sig[32..64].copy_from_slice(&tx.signature.s()[..]);
            sig[64] = tx.signature.odd_y_parity() as u8;
            msg.copy_from_slice(&ethereum::EIP2930TransactionMessage::from(tx.clone()).hash()[..]);
        }
        pallet_ethereum::Transaction::EIP1559(tx) => {
            sig[0..32].copy_from_slice(&tx.signature.r()[..]);
            sig[32..64].copy_from_slice(&tx.signature.s()[..]);
            sig[64] = tx.signature.odd_y_parity() as u8;
            msg.copy_from_slice(&ethereum::EIP1559TransactionMessage::from(tx.clone()).hash()[..]);
        }
        pallet_ethereum::Transaction::EIP7702(tx) => {
            sig[0..32].copy_from_slice(&tx.signature.r()[..]);
            sig[32..64].copy_from_slice(&tx.signature.s()[..]);
            sig[64] = tx.signature.odd_y_parity() as u8;
            msg.copy_from_slice(&ethereum::EIP7702TransactionMessage::from(tx.clone()).hash()[..]);
        }
    }

    let pubkey = sp_io::crypto::secp256k1_ecdsa_recover(&sig, &msg).ok()?;
    let hash = sp_io::hashing::keccak_256(&pubkey);
    Some(format!("0x{}", hex::encode(&hash[12..])))
}

fn hex_tags<T: AsRef<[u8]> + Serialize>(tags: &[T]) -> String {
    let tags = tags
        .iter()
        .map(|tag| format!("0x{}", hex::encode(tag.as_ref())))
        .collect::<Vec<_>>();
    serde_json::to_string(&tags).unwrap_or_else(|_| "[]".to_string())
}

fn hash_string<T: std::fmt::LowerHex>(hash: &T) -> String {
    format!("{hash:#x}")
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}
