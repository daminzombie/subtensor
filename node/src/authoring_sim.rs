use crate::client::FullClient;
use crate::consensus::ConsensusMechanism;
use crate::service::FullSelectChain;
use codec::{Decode, Encode};
use futures::StreamExt;
use node_subtensor_runtime::{RuntimeCall, opaque::Block};
use sc_client_api::HeaderBackend;
use sc_client_api::client::BlockchainEvents;
use sc_consensus_slots::InherentDataProviderExt;
use sc_service::{TaskManager, error::Error as ServiceError};
use sc_telemetry::TelemetryHandle;
use sc_transaction_pool::TransactionPoolHandle;
use sc_transaction_pool_api::{InPoolTransaction, TransactionPool};
use serde::Serialize;
use serde_json::json;
use sp_consensus::{Environment, Proposer, SelectChain};
use sp_consensus_slots::SlotDuration;
use sp_inherents::InherentDataProvider;
use sp_runtime::traits::{BlakeTwo256, Block as BlockT, Hash as HashT, Header as HeaderT};
use stc_shield::MemoryShieldKeystore;
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{
    Arc,
    atomic::{AtomicU64, Ordering},
};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use substrate_prometheus_endpoint::Registry;
use tokio::sync::mpsc;

const LOG_TARGET: &str = "authoring-sim";
const DB_BATCH_SIZE: usize = 512;
const DB_FLUSH_INTERVAL: Duration = Duration::from_millis(500);

#[derive(Clone, Debug)]
pub struct AuthoringSimulationConfig {
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

#[derive(Debug)]
enum DbEvent {
    SimBlock(SimBlockRow),
    Extrinsic(ExtrinsicRow),
    ExtrinsicEvent(ExtrinsicEventRow),
    PoolView(PoolViewRow),
    PoolViewMember(PoolViewMemberRow),
    ChainEvent(ChainEventRow),
    Timeline(TimelineEventRow),
    WriterStats(WriterStatsRow),
}

#[derive(Debug)]
struct SimBlockRow {
    event_time_ms: i64,
    slot: u64,
    parent_hash: String,
    parent_number: u64,
    block_hash: Option<String>,
    block_number: Option<u64>,
    best_hash_start: String,
    best_hash_end: Option<String>,
    duration_ms: u64,
    result: String,
    error: Option<String>,
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
}

#[derive(Debug)]
struct PoolViewMemberRow {
    view_id: String,
    section: String,
    ordinal: u64,
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

pub fn spawn_authoring_simulation<CM>(
    task_manager: &TaskManager,
    config: AuthoringSimulationConfig,
    client: Arc<FullClient>,
    transaction_pool: Arc<TransactionPoolHandle<Block, FullClient>>,
    select_chain: FullSelectChain,
    slot_duration: SlotDuration,
    prometheus_registry: Option<&Registry>,
    telemetry: Option<TelemetryHandle>,
) -> Result<(), ServiceError>
where
    CM: ConsensusMechanism + Send + 'static,
    CM::InherentDataProviders: Send,
{
    let channel_capacity = config.channel_capacity.max(1);
    let (sender, receiver) = mpsc::channel(channel_capacity);
    let sink = EventSink::new(sender);

    spawn_db_writer(task_manager, config.db_path.clone(), receiver, sink.clone());
    spawn_chain_event_recorder(task_manager, client.clone(), sink.clone());
    spawn_txpool_event_recorder(task_manager, transaction_pool.clone(), sink.clone());

    let sim_sink = sink.clone();
    let log_xt_data = config.log_xt_data;
    let shield_keystore = Arc::new(MemoryShieldKeystore::new());
    let mut proposer_factory = sc_basic_authorship::ProposerFactory::new(
        task_manager.spawn_handle(),
        client.clone(),
        transaction_pool.clone(),
        prometheus_registry,
        telemetry,
        shield_keystore.clone(),
    );

    task_manager
        .spawn_handle()
        .spawn("authoring-sim", Some("authoring-sim"), async move {
            log::info!(
                target: LOG_TARGET,
                "Authoring simulation enabled. Writing diagnostics to {:?}",
                config.db_path
            );
            let mut ticker = tokio::time::interval(slot_duration.as_duration());
            loop {
                ticker.tick().await;
                let started = std::time::Instant::now();
                if let Err(error) = simulate_slot::<CM, _>(
                    &client,
                    &transaction_pool,
                    &select_chain,
                    &mut proposer_factory,
                    slot_duration,
                    shield_keystore.clone(),
                    &sim_sink,
                    log_xt_data,
                )
                .await
                {
                    sim_sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                        event_time_ms: now_ms(),
                        slot: None,
                        parent_hash: None,
                        event_kind: "simulation_error".to_string(),
                        details_json: json!({
                            "error": error,
                            "elapsed_ms": started.elapsed().as_millis() as u64,
                        })
                        .to_string(),
                    }));
                }

                let dropped = sim_sink.dropped();
                if dropped > 0 {
                    sim_sink.enqueue(DbEvent::WriterStats(WriterStatsRow {
                        event_time_ms: now_ms(),
                        queued: 0,
                        written: 0,
                        dropped,
                        flush_duration_ms: 0,
                        batch_size: 0,
                        last_error: None,
                    }));
                }
            }
        });

    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn simulate_slot<CM, PF>(
    client: &Arc<FullClient>,
    transaction_pool: &Arc<TransactionPoolHandle<Block, FullClient>>,
    select_chain: &FullSelectChain,
    proposer_factory: &mut PF,
    slot_duration: SlotDuration,
    shield_keystore: stp_shield::ShieldKeystorePtr,
    sink: &EventSink,
    log_xt_data: bool,
) -> Result<(), String>
where
    CM: ConsensusMechanism,
    CM::InherentDataProviders: Send,
    PF: Environment<Block>,
    PF::Error: std::fmt::Display,
    <PF::Proposer as Proposer<Block>>::Error: std::fmt::Display,
{
    let parent_header = select_chain
        .best_chain()
        .await
        .map_err(|error| format!("select best chain failed: {error}"))?;
    let parent_hash = parent_header.hash();
    let parent_hash_string = hash_string(&parent_hash);
    let parent_number = (*parent_header.number()).into();

    let providers = CM::create_inherent_data_providers(slot_duration, shield_keystore)
        .map_err(|error| format!("create inherent providers failed: {error}"))?;
    let slot = providers.slot();
    let slot_number = *slot;

    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
        event_time_ms: now_ms(),
        slot: Some(slot_number),
        parent_hash: Some(parent_hash_string.clone()),
        event_kind: "claim_simulated".to_string(),
        details_json: json!({ "claim_result": "simulated_some" }).to_string(),
    }));

    let view_id = capture_pool_view(
        transaction_pool,
        &parent_hash,
        parent_number,
        slot_number,
        "pre_build",
        sink,
        log_xt_data,
    )
    .await;

    let inherent_data = providers
        .create_inherent_data()
        .await
        .map_err(|error| format!("create inherent data failed: {error}"))?;
    let digest = CM::simulation_pre_digest(slot);

    let ready_hashes = ready_hashes_for_view(transaction_pool, parent_hash).await;
    for tx_hash in &ready_hashes {
        sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
            event_time_ms: now_ms(),
            tx_hash: tx_hash.clone(),
            event_kind: "xt_attempted".to_string(),
            slot: Some(slot_number),
            block_number: Some(parent_number.saturating_add(1)),
            parent_hash: Some(parent_hash_string.clone()),
            view_id: Some(view_id.clone()),
            details_json: "{}".to_string(),
        }));
    }

    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
        event_time_ms: now_ms(),
        slot: Some(slot_number),
        parent_hash: Some(parent_hash_string.clone()),
        event_kind: "block_build_started".to_string(),
        details_json: json!({ "view_id": view_id }).to_string(),
    }));

    let started = std::time::Instant::now();
    let proposer = proposer_factory
        .init(&parent_header)
        .await
        .map_err(|error| format!("proposer init failed: {error}"))?;
    let proposal = proposer
        .propose(
            inherent_data,
            digest,
            slot_duration.as_duration().mul_f32(2.0 / 3.0),
            None,
        )
        .await
        .map_err(|error| format!("proposal failed: {error}"))?;

    let block = proposal.block;
    let block_hash = block.hash();
    let block_hash_string = hash_string(&block_hash);
    let block_number = (*block.header().number()).into();
    let included_hashes: HashSet<String> = block
        .extrinsics()
        .iter()
        .map(|xt| hash_string(&BlakeTwo256::hash_of(xt)))
        .collect();

    for (ordinal, xt) in block.extrinsics().iter().enumerate() {
        let tx_hash = hash_string(&BlakeTwo256::hash_of(xt));
        let record = classify_extrinsic(xt, log_xt_data, "sim_block");
        sink.enqueue(DbEvent::Extrinsic(record.with_hash(tx_hash.clone())));
        sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
            event_time_ms: now_ms(),
            tx_hash,
            event_kind: "included_in_sim_block".to_string(),
            slot: Some(slot_number),
            block_number: Some(block_number),
            parent_hash: Some(parent_hash_string.clone()),
            view_id: Some(view_id.clone()),
            details_json: json!({ "ordinal": ordinal }).to_string(),
        }));
    }

    for tx_hash in ready_hashes {
        if !included_hashes.contains(&tx_hash) {
            sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
                event_time_ms: now_ms(),
                tx_hash,
                event_kind: "not_included_in_sim_block".to_string(),
                slot: Some(slot_number),
                block_number: Some(block_number),
                parent_hash: Some(parent_hash_string.clone()),
                view_id: Some(view_id.clone()),
                details_json: json!({ "reason": "not_returned_by_proposer" }).to_string(),
            }));
        }
    }

    let duration_ms = started.elapsed().as_millis() as u64;
    sink.enqueue(DbEvent::SimBlock(SimBlockRow {
        event_time_ms: now_ms(),
        slot: slot_number,
        parent_hash: parent_hash_string.clone(),
        parent_number,
        block_hash: Some(block_hash_string.clone()),
        block_number: Some(block_number),
        best_hash_start: parent_hash_string.clone(),
        best_hash_end: Some(hash_string(&client.info().best_hash)),
        duration_ms,
        result: "built_and_dropped".to_string(),
        error: None,
    }));
    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
        event_time_ms: now_ms(),
        slot: Some(slot_number),
        parent_hash: Some(parent_hash_string),
        event_kind: "proposal_dropped".to_string(),
        details_json: json!({
            "block_hash": block_hash_string,
            "block_number": block_number,
            "duration_ms": duration_ms,
        })
        .to_string(),
    }));

    Ok(())
}

async fn ready_hashes_for_view(
    transaction_pool: &Arc<TransactionPoolHandle<Block, FullClient>>,
    parent_hash: <Block as BlockT>::Hash,
) -> Vec<String> {
    transaction_pool
        .ready_at_with_timeout(parent_hash, Duration::from_millis(25))
        .await
        .map(|tx| hash_string(tx.hash()))
        .collect()
}

async fn capture_pool_view(
    transaction_pool: &Arc<TransactionPoolHandle<Block, FullClient>>,
    parent_hash: &<Block as BlockT>::Hash,
    parent_number: u64,
    slot: u64,
    reason: &str,
    sink: &EventSink,
    log_xt_data: bool,
) -> String {
    let event_time_ms = now_ms();
    let view_id = format!("{slot}-{}-{reason}", event_time_ms);
    let status = transaction_pool.status();
    let mut ready_count = 0u64;
    let mut future_count = 0u64;

    let ready = transaction_pool
        .ready_at_with_timeout(*parent_hash, Duration::from_millis(25))
        .await;
    for (ordinal, tx) in ready.enumerate() {
        let tx_hash = hash_string(tx.hash());
        let data = tx.data();
        let record = classify_extrinsic(data, log_xt_data, "pool_ready");
        sink.enqueue(DbEvent::Extrinsic(record.with_hash(tx_hash.clone())));
        sink.enqueue(DbEvent::PoolViewMember(PoolViewMemberRow {
            view_id: view_id.clone(),
            section: "ready".to_string(),
            ordinal: ordinal as u64,
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
            slot: Some(slot),
            block_number: Some(parent_number.saturating_add(1)),
            parent_hash: Some(hash_string(parent_hash)),
            view_id: Some(view_id.clone()),
            details_json: json!({ "reason": reason, "ordinal": ordinal }).to_string(),
        }));
        ready_count = ready_count.saturating_add(1);
    }

    for (ordinal, tx) in transaction_pool.futures().into_iter().enumerate() {
        let tx_hash = hash_string(tx.hash());
        let data = tx.data();
        let record = classify_extrinsic(data, log_xt_data, "pool_future");
        sink.enqueue(DbEvent::Extrinsic(record.with_hash(tx_hash.clone())));
        sink.enqueue(DbEvent::PoolViewMember(PoolViewMemberRow {
            view_id: view_id.clone(),
            section: "future".to_string(),
            ordinal: ordinal as u64,
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
            slot: Some(slot),
            block_number: Some(parent_number.saturating_add(1)),
            parent_hash: Some(hash_string(parent_hash)),
            view_id: Some(view_id.clone()),
            details_json: json!({ "reason": reason, "ordinal": ordinal }).to_string(),
        }));
        future_count = future_count.saturating_add(1);
    }

    sink.enqueue(DbEvent::PoolView(PoolViewRow {
        event_time_ms,
        view_id: view_id.clone(),
        slot: Some(slot),
        parent_hash: hash_string(parent_hash),
        parent_number,
        best_hash: hash_string(parent_hash),
        ready_count,
        future_count,
        queue_depth: status.ready.saturating_add(status.future) as u64,
        reason: reason.to_string(),
    }));
    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
        event_time_ms,
        slot: Some(slot),
        parent_hash: Some(hash_string(parent_hash)),
        event_kind: "view_created".to_string(),
        details_json: json!({
            "view_id": view_id,
            "ready_count": ready_count,
            "future_count": future_count,
            "status_ready": status.ready,
            "status_future": status.future,
            "reason": reason,
        })
        .to_string(),
    }));

    view_id
}

fn spawn_chain_event_recorder(
    task_manager: &TaskManager,
    client: Arc<FullClient>,
    sink: EventSink,
) {
    task_manager.spawn_handle().spawn(
        "authoring-sim-chain-events",
        Some("authoring-sim"),
        async move {
            let mut stream = client.import_notification_stream();
            while let Some(notification) = stream.next().await {
                sink.enqueue(DbEvent::ChainEvent(ChainEventRow {
                    event_time_ms: now_ms(),
                    event_kind: "block_import".to_string(),
                    block_hash: hash_string(&notification.hash),
                    block_number: (*notification.header.number()).into(),
                    is_new_best: notification.is_new_best,
                    origin: format!("{:?}", notification.origin),
                    details_json: json!({
                        "has_tree_route": notification.tree_route.is_some(),
                    })
                    .to_string(),
                }));

                if notification.is_new_best {
                    sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                        event_time_ms: now_ms(),
                        slot: None,
                        parent_hash: Some(hash_string(&notification.hash)),
                        event_kind: "new_best_block_import".to_string(),
                        details_json: json!({
                            "block_number": u64::from(*notification.header.number()),
                        })
                        .to_string(),
                    }));
                }
            }
        },
    );
}

fn spawn_txpool_event_recorder(
    task_manager: &TaskManager,
    transaction_pool: Arc<TransactionPoolHandle<Block, FullClient>>,
    sink: EventSink,
) {
    task_manager.spawn_handle().spawn(
        "authoring-sim-txpool-events",
        Some("authoring-sim"),
        async move {
            let mut stream = transaction_pool.import_notification_stream();
            while let Some(tx_hash) = stream.next().await {
                sink.enqueue(DbEvent::ExtrinsicEvent(ExtrinsicEventRow {
                    event_time_ms: now_ms(),
                    tx_hash: hash_string(&tx_hash),
                    event_kind: "imported_to_pool".to_string(),
                    slot: None,
                    block_number: None,
                    parent_hash: None,
                    view_id: None,
                    details_json: "{}".to_string(),
                }));
                sink.enqueue(DbEvent::Timeline(TimelineEventRow {
                    event_time_ms: now_ms(),
                    slot: None,
                    parent_hash: None,
                    event_kind: "txpool_import".to_string(),
                    details_json: json!({ "tx_hash": hash_string(&tx_hash) }).to_string(),
                }));
            }
        },
    );
}

fn spawn_db_writer(
    task_manager: &TaskManager,
    db_path: PathBuf,
    mut receiver: mpsc::Receiver<DbEvent>,
    sink: EventSink,
) {
    task_manager.spawn_handle().spawn(
        "authoring-sim-db-writer",
        Some("authoring-sim"),
        async move {
            if let Err(error) = run_db_writer(db_path, &mut receiver, sink).await {
                log::error!(target: LOG_TARGET, "authoring simulation DB writer stopped: {error}");
            }
        },
    );
}

async fn run_db_writer(
    db_path: PathBuf,
    receiver: &mut mpsc::Receiver<DbEvent>,
    sink: EventSink,
) -> Result<(), String> {
    if let Some(parent) = db_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(|error| format!("create simulation db directory failed: {error}"))?;
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
        .map_err(|error| format!("open simulation db failed: {error}"))?;
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
                log::warn!(target: LOG_TARGET, "failed to flush authoring simulation batch: {message}");
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
        "CREATE TABLE IF NOT EXISTS sim_blocks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_ms INTEGER NOT NULL,
            slot INTEGER NOT NULL,
            parent_hash TEXT NOT NULL,
            parent_number INTEGER NOT NULL,
            block_hash TEXT,
            block_number INTEGER,
            best_hash_start TEXT NOT NULL,
            best_hash_end TEXT,
            duration_ms INTEGER NOT NULL,
            result TEXT NOT NULL,
            error TEXT
        )",
        "CREATE TABLE IF NOT EXISTS extrinsics (
            tx_hash TEXT PRIMARY KEY,
            first_seen_time_ms INTEGER NOT NULL,
            first_seen_source TEXT NOT NULL,
            encoded BLOB,
            encoded_len INTEGER NOT NULL,
            classification TEXT NOT NULL,
            details_json TEXT NOT NULL,
            last_status TEXT NOT NULL,
            updated_time_ms INTEGER NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS extrinsic_events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_ms INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            event_kind TEXT NOT NULL,
            slot INTEGER,
            block_number INTEGER,
            parent_hash TEXT,
            view_id TEXT,
            details_json TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS pool_views (
            view_id TEXT PRIMARY KEY,
            event_time_ms INTEGER NOT NULL,
            slot INTEGER,
            parent_hash TEXT NOT NULL,
            parent_number INTEGER NOT NULL,
            best_hash TEXT NOT NULL,
            ready_count INTEGER NOT NULL,
            future_count INTEGER NOT NULL,
            queue_depth INTEGER NOT NULL,
            reason TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS pool_view_members (
            view_id TEXT NOT NULL,
            section TEXT NOT NULL,
            ordinal INTEGER NOT NULL,
            tx_hash TEXT NOT NULL,
            priority TEXT,
            requires_json TEXT NOT NULL,
            provides_json TEXT NOT NULL,
            encoded_len INTEGER NOT NULL,
            PRIMARY KEY (view_id, section, ordinal)
        )",
        "CREATE TABLE IF NOT EXISTS chain_events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_ms INTEGER NOT NULL,
            event_kind TEXT NOT NULL,
            block_hash TEXT NOT NULL,
            block_number INTEGER NOT NULL,
            is_new_best INTEGER NOT NULL,
            origin TEXT NOT NULL,
            details_json TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS sim_timeline_events (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_ms INTEGER NOT NULL,
            slot INTEGER,
            parent_hash TEXT,
            event_kind TEXT NOT NULL,
            details_json TEXT NOT NULL
        )",
        "CREATE TABLE IF NOT EXISTS writer_stats (
            seq INTEGER PRIMARY KEY AUTOINCREMENT,
            event_time_ms INTEGER NOT NULL,
            queued INTEGER NOT NULL,
            written INTEGER NOT NULL,
            dropped INTEGER NOT NULL,
            flush_duration_ms INTEGER NOT NULL,
            batch_size INTEGER NOT NULL,
            last_error TEXT
        )",
        "CREATE INDEX IF NOT EXISTS idx_extrinsic_events_hash_time ON extrinsic_events (tx_hash, event_time_ms, seq)",
        "CREATE INDEX IF NOT EXISTS idx_extrinsic_events_slot ON extrinsic_events (slot, event_time_ms)",
        "CREATE INDEX IF NOT EXISTS idx_pool_view_members_hash ON pool_view_members (tx_hash)",
        "CREATE INDEX IF NOT EXISTS idx_pool_views_slot ON pool_views (slot, event_time_ms)",
        "CREATE INDEX IF NOT EXISTS idx_chain_events_block ON chain_events (block_number, block_hash)",
        "CREATE INDEX IF NOT EXISTS idx_timeline_kind_time ON sim_timeline_events (event_kind, event_time_ms)",
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
    match event {
        DbEvent::SimBlock(row) => {
            sqlx::query(
                "INSERT INTO sim_blocks (
                    event_time_ms, slot, parent_hash, parent_number, block_hash, block_number,
                    best_hash_start, best_hash_end, duration_ms, result, error
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.event_time_ms)
            .bind(row.slot as i64)
            .bind(row.parent_hash)
            .bind(row.parent_number as i64)
            .bind(row.block_hash)
            .bind(row.block_number.map(|n| n as i64))
            .bind(row.best_hash_start)
            .bind(row.best_hash_end)
            .bind(row.duration_ms as i64)
            .bind(row.result)
            .bind(row.error)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::Extrinsic(row) => {
            sqlx::query(
                "INSERT INTO extrinsics (
                    tx_hash, first_seen_time_ms, first_seen_source, encoded, encoded_len,
                    classification, details_json, last_status, updated_time_ms
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(tx_hash) DO UPDATE SET
                    encoded = COALESCE(excluded.encoded, extrinsics.encoded),
                    encoded_len = excluded.encoded_len,
                    classification = excluded.classification,
                    details_json = excluded.details_json,
                    last_status = excluded.last_status,
                    updated_time_ms = excluded.updated_time_ms",
            )
            .bind(row.tx_hash)
            .bind(row.event_time_ms)
            .bind(row.first_seen_source)
            .bind(row.encoded)
            .bind(row.encoded_len as i64)
            .bind(row.classification)
            .bind(row.details_json)
            .bind(row.last_status)
            .bind(row.event_time_ms)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::ExtrinsicEvent(row) => {
            sqlx::query(
                "INSERT INTO extrinsic_events (
                    event_time_ms, tx_hash, event_kind, slot, block_number, parent_hash, view_id,
                    details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.event_time_ms)
            .bind(row.tx_hash)
            .bind(row.event_kind)
            .bind(row.slot.map(|v| v as i64))
            .bind(row.block_number.map(|v| v as i64))
            .bind(row.parent_hash)
            .bind(row.view_id)
            .bind(row.details_json)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::PoolView(row) => {
            sqlx::query(
                "INSERT OR REPLACE INTO pool_views (
                    view_id, event_time_ms, slot, parent_hash, parent_number, best_hash,
                    ready_count, future_count, queue_depth, reason
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.view_id)
            .bind(row.event_time_ms)
            .bind(row.slot.map(|v| v as i64))
            .bind(row.parent_hash)
            .bind(row.parent_number as i64)
            .bind(row.best_hash)
            .bind(row.ready_count as i64)
            .bind(row.future_count as i64)
            .bind(row.queue_depth as i64)
            .bind(row.reason)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::PoolViewMember(row) => {
            sqlx::query(
                "INSERT OR REPLACE INTO pool_view_members (
                    view_id, section, ordinal, tx_hash, priority, requires_json, provides_json,
                    encoded_len
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.view_id)
            .bind(row.section)
            .bind(row.ordinal as i64)
            .bind(row.tx_hash)
            .bind(row.priority)
            .bind(row.requires_json)
            .bind(row.provides_json)
            .bind(row.encoded_len as i64)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::ChainEvent(row) => {
            sqlx::query(
                "INSERT INTO chain_events (
                    event_time_ms, event_kind, block_hash, block_number, is_new_best, origin,
                    details_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.event_time_ms)
            .bind(row.event_kind)
            .bind(row.block_hash)
            .bind(row.block_number as i64)
            .bind(row.is_new_best)
            .bind(row.origin)
            .bind(row.details_json)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::Timeline(row) => {
            sqlx::query(
                "INSERT INTO sim_timeline_events (
                    event_time_ms, slot, parent_hash, event_kind, details_json
                ) VALUES (?, ?, ?, ?, ?)",
            )
            .bind(row.event_time_ms)
            .bind(row.slot.map(|v| v as i64))
            .bind(row.parent_hash)
            .bind(row.event_kind)
            .bind(row.details_json)
            .execute(&mut **tx)
            .await?;
        }
        DbEvent::WriterStats(row) => {
            sqlx::query(
                "INSERT INTO writer_stats (
                    event_time_ms, queued, written, dropped, flush_duration_ms, batch_size,
                    last_error
                ) VALUES (?, ?, ?, ?, ?, ?, ?)",
            )
            .bind(row.event_time_ms)
            .bind(row.queued as i64)
            .bind(row.written as i64)
            .bind(row.dropped as i64)
            .bind(row.flush_duration_ms as i64)
            .bind(row.batch_size as i64)
            .bind(row.last_error)
            .execute(&mut **tx)
            .await?;
        }
    }
    Ok(())
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
