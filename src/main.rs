use mysql::{params, prelude::Queryable, OptsBuilder, Pool, PooledConn, TxOpts};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::env;
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::net::{IpAddr, TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

const DEFAULT_MAX_FRAME_BYTES: usize = 32768;
const DEFAULT_MAX_INBOUND_EVENTS: usize = 512;
const DEFAULT_DEDUPE_EVENTS: usize = 65536;
const DEFAULT_DEAD_LETTER_PATH: &str = "statistics_dead_letters.log";
const DEFAULT_PENDING_JOURNAL_PATH: &str = "statistics_pending_journal.log";
const DEFAULT_PENDING_JOURNAL_COMPACT_BYTES: u64 = 16 * 1024 * 1024;
const PENDING_JOURNAL_COMPACT_CHECK_EVERY: u64 = 1024;
const SCHEMA_VERSION: u32 = 1;

#[derive(Clone)]
struct Config {
    bind: String,
    db_driver: String,
    db_host: String,
    db_port: u16,
    db_name: String,
    db_user: String,
    db_pass: String,
    flush_interval: Duration,
    max_batch_rows: usize,
    queue_limit: usize,
    require_localhost: bool,
    max_frame_bytes: usize,
    max_inbound_events: usize,
    dedupe_events: usize,
    pending_journal_compact_bytes: u64,
    debug: bool,
    auth_token: String,
}

impl Config {
    fn from_env() -> Self {
        Self {
            bind: env::var("PLUGIN_STATS_BIND").unwrap_or_else(|_| "127.0.0.1:28019".to_string()),
            db_driver: env::var("PLUGIN_STATS_DB_DRIVER").unwrap_or_else(|_| "mysql".to_string()),
            db_host: env::var("PLUGIN_STATS_DB_HOST").unwrap_or_else(|_| "127.0.0.1".to_string()),
            db_port: env_u16("PLUGIN_STATS_DB_PORT", 3306),
            db_name: env::var("PLUGIN_STATS_DB_NAME").unwrap_or_else(|_| "sourcemod".to_string()),
            db_user: env::var("PLUGIN_STATS_DB_USER").unwrap_or_else(|_| "root".to_string()),
            db_pass: env::var("PLUGIN_STATS_DB_PASS").unwrap_or_default(),
            flush_interval: Duration::from_millis(
                env_u64("PLUGIN_STATS_FLUSH_INTERVAL_MS", 500).max(1),
            ),
            max_batch_rows: env_usize("PLUGIN_STATS_MAX_BATCH_ROWS", 500).max(1),
            queue_limit: env_usize("PLUGIN_STATS_QUEUE_LIMIT", 50_000).clamp(1, 1_000_000),
            require_localhost: env_bool("PLUGIN_STATS_REQUIRE_LOCALHOST", true),
            max_frame_bytes: env_usize("PLUGIN_STATS_MAX_FRAME_BYTES", DEFAULT_MAX_FRAME_BYTES)
                .clamp(1024, 1024 * 1024),
            max_inbound_events: env_usize(
                "PLUGIN_STATS_MAX_INBOUND_EVENTS",
                DEFAULT_MAX_INBOUND_EVENTS,
            )
            .clamp(1, 4096),
            dedupe_events: env_usize("PLUGIN_STATS_DEDUPE_EVENTS", DEFAULT_DEDUPE_EVENTS)
                .min(1_000_000),
            pending_journal_compact_bytes: env_u64(
                "PLUGIN_STATS_PENDING_JOURNAL_COMPACT_BYTES",
                DEFAULT_PENDING_JOURNAL_COMPACT_BYTES,
            ),
            debug: env_bool("PLUGIN_STATS_DEBUG", false),
            auth_token: env::var("PLUGIN_STATS_AUTH_TOKEN").unwrap_or_default(),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
struct StatsEvent {
    #[serde(default)]
    table: String,
    #[serde(default = "default_record_type")]
    record_type: String,
    #[serde(default)]
    server_id: String,
    #[serde(default)]
    server_name: String,
    #[serde(default)]
    source_plugin: String,
    #[serde(default)]
    event_id: Option<String>,
    #[serde(default)]
    occurred_at: i64,
    #[serde(default)]
    host_port: i32,
    #[serde(default)]
    map_session_id: String,
    #[serde(default)]
    map_name: String,
    #[serde(default)]
    gamemode: String,
    #[serde(default)]
    event_name: String,
    #[serde(default)]
    message: String,
    #[serde(default)]
    weekday: i32,
    #[serde(default)]
    hour_of_day: i32,
    #[serde(default)]
    server_tick: i64,
    #[serde(default)]
    tick_interval_seconds: f64,
    #[serde(default)]
    expected_tickrate: f64,
    #[serde(default)]
    observed_tickrate: f64,
    #[serde(default)]
    session_started_at: i64,
    #[serde(default)]
    session_ended_at: i64,
    #[serde(default)]
    session_sample_count: i32,
    #[serde(default)]
    session_average_tickrate: f64,
    #[serde(default)]
    session_minimum_tickrate: f64,
    #[serde(default)]
    end_reason: String,
}

#[derive(Clone, Debug)]
struct QueuedEvent {
    source_plugin: String,
    event: StatsEvent,
    event_id: Option<String>,
    batch_id: Option<u64>,
    enqueued_at_ms: u64,
    completion: Option<Arc<BatchCompletion>>,
}

#[derive(Default)]
struct QueueState {
    events: VecDeque<QueuedEvent>,
    dropped: u64,
}

type SharedQueue = Arc<(Mutex<QueueState>, Condvar)>;

#[derive(Default)]
struct ServiceStats {
    accepted_events: AtomicU64,
    executed_events: AtomicU64,
    db_errors: AtomicU64,
    parse_errors: AtomicU64,
    dropped_events: AtomicU64,
    journal_pending_startup: AtomicU64,
    journal_replayed_startup: AtomicU64,
    journal_done_records_startup: AtomicU64,
    journal_bad_lines_startup: AtomicU64,
    journal_compactions: AtomicU64,
    journal_events_since_compaction_check: AtomicU64,
}

#[derive(Debug)]
struct BatchCompletion {
    state: Mutex<BatchCompletionState>,
    notify: Condvar,
}

#[derive(Debug)]
struct BatchCompletionState {
    remaining: usize,
    executed: usize,
    db_errors: usize,
}

#[derive(Clone, Copy)]
struct BatchCompletionSnapshot {
    executed: usize,
    db_errors: usize,
}

impl BatchCompletion {
    fn new(remaining: usize) -> Self {
        Self {
            state: Mutex::new(BatchCompletionState {
                remaining,
                executed: 0,
                db_errors: 0,
            }),
            notify: Condvar::new(),
        }
    }

    fn finish(&self, success: bool) {
        let mut state = self.state.lock().expect("batch completion mutex poisoned");
        state.remaining = state.remaining.saturating_sub(1);
        if success {
            state.executed += 1;
        } else {
            state.db_errors += 1;
        }
        if state.remaining == 0 {
            self.notify.notify_all();
        }
    }

    fn wait(&self) -> BatchCompletionSnapshot {
        let mut state = self
            .state
            .lock()
            .expect("batch completion wait mutex poisoned");
        while state.remaining > 0 {
            state = self
                .notify
                .wait(state)
                .expect("batch completion wait failed");
        }
        BatchCompletionSnapshot {
            executed: state.executed,
            db_errors: state.db_errors,
        }
    }
}

struct EnqueueResult {
    accepted: usize,
    completion: Option<Arc<BatchCompletion>>,
}

struct EnqueueError {
    message: &'static str,
    queued: usize,
    incoming: usize,
    limit: usize,
}

struct DedupeCache {
    state: Mutex<DedupeState>,
    max_entries: usize,
}

struct DedupeState {
    seen: HashSet<String>,
    order: VecDeque<String>,
}

impl DedupeCache {
    fn new(max_entries: usize) -> Self {
        Self {
            state: Mutex::new(DedupeState {
                seen: HashSet::new(),
                order: VecDeque::new(),
            }),
            max_entries,
        }
    }

    fn contains(&self, event_id: &str) -> bool {
        !event_id.is_empty()
            && self
                .state
                .lock()
                .expect("dedupe mutex poisoned")
                .seen
                .contains(event_id)
    }

    fn remember(&self, event_id: &str) {
        if event_id.is_empty() || self.max_entries == 0 {
            return;
        }

        let mut state = self.state.lock().expect("dedupe mutex poisoned");
        if !state.seen.insert(event_id.to_string()) {
            return;
        }
        state.order.push_back(event_id.to_string());
        while state.order.len() > self.max_entries {
            if let Some(oldest) = state.order.pop_front() {
                state.seen.remove(&oldest);
            }
        }
    }

    fn len(&self) -> usize {
        self.state.lock().expect("dedupe mutex poisoned").seen.len()
    }

    fn capacity(&self) -> usize {
        self.max_entries
    }
}

struct DeadLetterWriter {
    file: Option<Mutex<File>>,
}

#[derive(Serialize)]
struct DeadLetterEntry<'a> {
    ts_ms: u64,
    reason: &'a str,
    detail: Option<&'a str>,
    source_plugin: Option<&'a str>,
    batch_id: Option<u64>,
    event_id: Option<&'a str>,
    event: Option<&'a StatsEvent>,
}

impl DeadLetterWriter {
    fn from_env() -> Self {
        let path = env::var("PLUGIN_STATS_DEAD_LETTER_PATH")
            .unwrap_or_else(|_| DEFAULT_DEAD_LETTER_PATH.to_string());
        if path.trim().is_empty() {
            println!("[statistics] dead-letter journal disabled");
            return Self { file: None };
        }

        match OpenOptions::new().create(true).append(true).open(&path) {
            Ok(file) => {
                println!("[statistics] dead-letter journal path={path}");
                Self {
                    file: Some(Mutex::new(file)),
                }
            }
            Err(err) => {
                eprintln!("[statistics] failed to open dead-letter journal {path}: {err}");
                Self { file: None }
            }
        }
    }

    fn record(
        &self,
        reason: &str,
        detail: Option<&str>,
        source_plugin: Option<&str>,
        batch_id: Option<u64>,
        event_id: Option<&str>,
        event: Option<&StatsEvent>,
    ) {
        let Some(file) = &self.file else {
            return;
        };
        let entry = DeadLetterEntry {
            ts_ms: now_ms_u64(),
            reason,
            detail,
            source_plugin,
            batch_id,
            event_id,
            event,
        };

        let mut file = file.lock().expect("dead-letter mutex poisoned");
        if let Err(err) = serde_json::to_writer(&mut *file, &entry) {
            eprintln!("[statistics] failed to encode dead-letter entry: {err}");
            return;
        }
        if let Err(err) = file.write_all(b"\n").and_then(|_| file.flush()) {
            eprintln!("[statistics] failed to write dead-letter entry: {err}");
        }
    }
}

struct PendingJournal {
    file: Option<Mutex<File>>,
    path: Option<String>,
}

#[derive(Debug, Clone)]
struct JournalPendingEvent {
    event_id: String,
    source_plugin: String,
    event: StatsEvent,
    batch_id: Option<u64>,
    ts_ms: u64,
}

#[derive(Debug, Default)]
struct JournalReplayState {
    pending: Vec<JournalPendingEvent>,
    recent_done: Vec<String>,
    done_records: usize,
    bad_lines: usize,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum PendingJournalRecord {
    Pending {
        event_id: String,
        #[serde(default, alias = "table")]
        source_plugin: String,
        event: StatsEvent,
        batch_id: Option<u64>,
        ts_ms: u64,
    },
    Done {
        event_id: String,
        ts_ms: u64,
    },
}

impl PendingJournal {
    fn from_env() -> Result<Self, String> {
        let path = env::var("PLUGIN_STATS_PENDING_JOURNAL_PATH")
            .unwrap_or_else(|_| DEFAULT_PENDING_JOURNAL_PATH.to_string());
        if path.trim().is_empty() {
            println!("[statistics] pending journal disabled");
            return Ok(Self {
                file: None,
                path: None,
            });
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)
            .map_err(|err| format!("failed to open pending journal {path}: {err}"))?;
        println!("[statistics] pending journal path={path}");
        Ok(Self {
            file: Some(Mutex::new(file)),
            path: Some(path),
        })
    }

    fn append_pending(
        &self,
        event_id: &str,
        source_plugin: &str,
        event: &StatsEvent,
        batch_id: Option<u64>,
    ) -> Result<(), String> {
        self.append_record(&PendingJournalRecord::Pending {
            event_id: event_id.to_string(),
            source_plugin: source_plugin.to_string(),
            event: event.clone(),
            batch_id,
            ts_ms: now_ms_u64(),
        })
    }

    fn append_done(&self, event_id: &str) -> Result<(), String> {
        self.append_record(&PendingJournalRecord::Done {
            event_id: event_id.to_string(),
            ts_ms: now_ms_u64(),
        })
    }

    fn append_record(&self, record: &PendingJournalRecord) -> Result<(), String> {
        let Some(file) = &self.file else {
            return Ok(());
        };

        let mut file = file.lock().expect("pending journal mutex poisoned");
        serde_json::to_writer(&mut *file, record).map_err(|err| err.to_string())?;
        file.write_all(b"\n").map_err(|err| err.to_string())?;
        file.flush().map_err(|err| err.to_string())?;
        Ok(())
    }

    fn load_replay_state(&self, dedupe: &DedupeCache) -> JournalReplayState {
        let Some(path) = &self.path else {
            return JournalReplayState::default();
        };

        let file = match File::open(path) {
            Ok(file) => file,
            Err(err) => {
                eprintln!("[statistics] failed to read pending journal {path}: {err}");
                return JournalReplayState::default();
            }
        };

        let mut pending = HashMap::<String, JournalPendingEvent>::new();
        let mut recent_done = VecDeque::<String>::new();
        let mut done_records = 0usize;
        let mut bad_lines = 0usize;
        let done_capacity = dedupe.capacity();

        for line in BufReader::new(file).lines() {
            let Ok(line) = line else {
                bad_lines += 1;
                continue;
            };
            if line.trim().is_empty() {
                continue;
            }

            match serde_json::from_str::<PendingJournalRecord>(&line) {
                Ok(PendingJournalRecord::Pending {
                    event_id,
                    source_plugin,
                    mut event,
                    batch_id,
                    ts_ms,
                }) => {
                    event.event_id = Some(event_id.clone());
                    pending.insert(
                        event_id.clone(),
                        JournalPendingEvent {
                            event_id,
                            source_plugin,
                            event,
                            batch_id,
                            ts_ms,
                        },
                    );
                }
                Ok(PendingJournalRecord::Done { event_id, .. }) => {
                    pending.remove(&event_id);
                    dedupe.remember(&event_id);
                    if done_capacity > 0 {
                        recent_done.push_back(event_id);
                        while recent_done.len() > done_capacity {
                            recent_done.pop_front();
                        }
                    }
                    done_records += 1;
                }
                Err(err) => {
                    bad_lines += 1;
                    eprintln!("[statistics] bad pending journal line: {err} | {line}");
                }
            }
        }

        let mut pending = pending.into_values().collect::<Vec<_>>();
        pending.sort_by(|a, b| {
            a.ts_ms
                .cmp(&b.ts_ms)
                .then_with(|| a.event_id.cmp(&b.event_id))
        });
        JournalReplayState {
            pending,
            recent_done: recent_done.into_iter().collect(),
            done_records,
            bad_lines,
        }
    }

    fn compact_from_state(&self, state: &JournalReplayState) -> Result<(), String> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let Some(file_mutex) = &self.file else {
            return Ok(());
        };

        let mut file = file_mutex.lock().expect("pending journal mutex poisoned");
        Self::compact_locked(&mut file, path, state)
    }

    fn compact_if_needed(&self, dedupe: &DedupeCache, minimum_bytes: u64) -> Result<bool, String> {
        if minimum_bytes == 0 {
            return Ok(false);
        }

        let Some(path) = &self.path else {
            return Ok(false);
        };
        let Some(file_mutex) = &self.file else {
            return Ok(false);
        };

        let mut file = file_mutex.lock().expect("pending journal mutex poisoned");
        file.sync_all().map_err(|err| err.to_string())?;
        if file.metadata().map_err(|err| err.to_string())?.len() < minimum_bytes {
            return Ok(false);
        }

        // Holding the append lock makes the replay state and replacement file one atomic view.
        let state = self.load_replay_state(dedupe);
        Self::compact_locked(&mut file, path, &state)?;
        Ok(true)
    }

    fn compact_locked(
        file: &mut File,
        path: &str,
        state: &JournalReplayState,
    ) -> Result<(), String> {
        file.sync_all().map_err(|err| err.to_string())?;

        let tmp_path = format!("{path}.compact.{}", now_ms_u64());
        let result = (|| -> Result<(), String> {
            let mut tmp = File::create(&tmp_path).map_err(|err| err.to_string())?;
            for pending in &state.pending {
                serde_json::to_writer(
                    &mut tmp,
                    &PendingJournalRecord::Pending {
                        event_id: pending.event_id.clone(),
                        source_plugin: pending.source_plugin.clone(),
                        event: pending.event.clone(),
                        batch_id: pending.batch_id,
                        ts_ms: pending.ts_ms,
                    },
                )
                .map_err(|err| err.to_string())?;
                tmp.write_all(b"\n").map_err(|err| err.to_string())?;
            }
            for event_id in &state.recent_done {
                serde_json::to_writer(
                    &mut tmp,
                    &PendingJournalRecord::Done {
                        event_id: event_id.clone(),
                        ts_ms: now_ms_u64(),
                    },
                )
                .map_err(|err| err.to_string())?;
                tmp.write_all(b"\n").map_err(|err| err.to_string())?;
            }
            tmp.sync_all().map_err(|err| err.to_string())?;
            fs::rename(&tmp_path, path).map_err(|err| err.to_string())?;
            *file = OpenOptions::new()
                .create(true)
                .append(true)
                .read(true)
                .open(path)
                .map_err(|err| err.to_string())?;
            Ok(())
        })();

        if result.is_err() {
            let _ = fs::remove_file(&tmp_path);
        }
        result
    }
}

#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum IncomingMessage {
    Hello {
        service: Option<String>,
        proto: Option<u32>,
        server_id: Option<String>,
        server_name: Option<String>,
        auth: Option<String>,
        ts: Option<u64>,
    },
    StatsBatch {
        batch_id: Option<u64>,
        table: Option<String>,
        sent_at: Option<u64>,
        events: Vec<StatsEvent>,
    },
    SqlBatch {
        batch_id: Option<u64>,
    },
    Health,
}

#[derive(Serialize)]
struct HelloResponse<'a> {
    r#type: &'a str,
    service: &'a str,
    proto: u32,
    server_id: &'a str,
    ts: u64,
    server_time: u64,
}

#[derive(Serialize)]
struct AckResponse<'a> {
    r#type: &'a str,
    batch_id: Option<u64>,
    accepted: usize,
    executed: usize,
    db_errors: usize,
    queue_depth: usize,
    sent_at: u64,
    ts: u64,
}

#[derive(Serialize)]
struct ErrorResponse<'a> {
    r#type: &'a str,
    batch_id: Option<u64>,
    message: &'a str,
    ts: u64,
}

#[derive(Serialize)]
struct HealthResponse<'a> {
    r#type: &'a str,
    queue_depth: usize,
    queue_dropped: u64,
    dedupe_events: usize,
    accepted_events: u64,
    executed_events: u64,
    db_errors: u64,
    parse_errors: u64,
    dropped_events: u64,
    journal_pending_startup: u64,
    journal_replayed_startup: u64,
    journal_done_records_startup: u64,
    journal_bad_lines_startup: u64,
    journal_compactions: u64,
    ts: u64,
}

fn main() {
    let config = Config::from_env();
    if !config.db_driver.eq_ignore_ascii_case("mysql") {
        eprintln!(
            "unsupported PLUGIN_STATS_DB_DRIVER={} (only mysql is currently supported)",
            config.db_driver
        );
        std::process::exit(2);
    }

    let pool = match create_pool(&config) {
        Ok(pool) => pool,
        Err(err) => {
            eprintln!("failed to create MySQL pool: {err}");
            std::process::exit(1);
        }
    };
    if let Err(err) = ensure_schema(&pool) {
        eprintln!("failed to initialize statistics schema: {err}");
        std::process::exit(1);
    }

    let queue: SharedQueue = Arc::new((Mutex::new(QueueState::default()), Condvar::new()));
    let stats = Arc::new(ServiceStats::default());
    let dedupe = Arc::new(DedupeCache::new(config.dedupe_events));
    let dead_letters = Arc::new(DeadLetterWriter::from_env());
    let pending_journal = match PendingJournal::from_env() {
        Ok(journal) => Arc::new(journal),
        Err(err) => {
            eprintln!("failed to initialize pending journal: {err}");
            std::process::exit(1);
        }
    };

    if let Err(err) = replay_pending_journal(
        &queue,
        &config,
        &dedupe,
        &pending_journal,
        &dead_letters,
        &stats,
    ) {
        eprintln!("pending journal replay failed: {err}");
        std::process::exit(1);
    }

    spawn_writer(
        config.clone(),
        pool,
        queue.clone(),
        dedupe.clone(),
        pending_journal.clone(),
        stats.clone(),
    );

    let listener = TcpListener::bind(&config.bind).unwrap_or_else(|err| {
        eprintln!("failed to bind {}: {err}", config.bind);
        std::process::exit(1);
    });

    println!(
        "plugin-statisticsd listening on {} with queue_limit={} max_batch_rows={} flush_interval_ms={} journal_compact_bytes={} max_frame_bytes={} max_inbound_events={} dedupe_events={} auth_required={} debug={}",
        config.bind,
        config.queue_limit,
        config.max_batch_rows,
        config.flush_interval.as_millis(),
        config.pending_journal_compact_bytes,
        config.max_frame_bytes,
        config.max_inbound_events,
        config.dedupe_events,
        protocol_auth_required(&config) as u8,
        config.debug as u8,
    );

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                let cfg = config.clone();
                let q = queue.clone();
                let d = dedupe.clone();
                let pj = pending_journal.clone();
                let s = stats.clone();
                thread::spawn(move || {
                    if let Err(err) = handle_client(stream, cfg, q, d, pj, s) {
                        if !matches!(
                            err.kind(),
                            std::io::ErrorKind::BrokenPipe
                                | std::io::ErrorKind::ConnectionAborted
                                | std::io::ErrorKind::ConnectionReset
                                | std::io::ErrorKind::UnexpectedEof
                        ) {
                            eprintln!("client handler error: {err}");
                        }
                    }
                });
            }
            Err(err) => eprintln!("accept failed: {err}"),
        }
    }
}

fn create_pool(config: &Config) -> mysql::Result<Pool> {
    let builder = OptsBuilder::new()
        .ip_or_hostname(Some(config.db_host.clone()))
        .tcp_port(config.db_port)
        .db_name(Some(config.db_name.clone()))
        .user(Some(config.db_user.clone()))
        .pass(Some(config.db_pass.clone()));

    Pool::new(builder)
}

fn ensure_schema(pool: &Pool) -> mysql::Result<()> {
    let mut conn = pool.get_conn()?;
    conn.query_drop(
        "CREATE TABLE IF NOT EXISTS plugin_statistics_schema_migrations (
            version INT UNSIGNED NOT NULL PRIMARY KEY,
            applied_at BIGINT UNSIGNED NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci",
    )?;

    let migrations = [(
        SCHEMA_VERSION,
        include_str!("../migrations/001_initial.sql"),
    )];
    for (version, sql) in migrations {
        let applied = conn.exec_first::<u32, _, _>(
            "SELECT version FROM plugin_statistics_schema_migrations WHERE version = :version",
            params! { "version" => version },
        )?;
        if applied.is_some() {
            continue;
        }

        for statement in sql.split(';').map(str::trim).filter(|sql| !sql.is_empty()) {
            conn.query_drop(statement)?;
        }
        conn.exec_drop(
            "INSERT INTO plugin_statistics_schema_migrations (version, applied_at)
             VALUES (:version, :applied_at)",
            params! {
                "version" => version,
                "applied_at" => now_unix_i64(),
            },
        )?;
    }
    Ok(())
}

fn handle_client(
    mut stream: TcpStream,
    config: Config,
    queue: SharedQueue,
    dedupe: Arc<DedupeCache>,
    pending_journal: Arc<PendingJournal>,
    stats: Arc<ServiceStats>,
) -> std::io::Result<()> {
    let peer = stream.peer_addr()?;

    if config.require_localhost && !is_loopback(peer.ip()) {
        let _ = send_json_line(
            &mut stream,
            &ErrorResponse {
                r#type: "error",
                batch_id: None,
                message: "only loopback clients are accepted",
                ts: now_unix_u64(),
            },
        );
        eprintln!("rejected non-loopback client {peer}");
        return Ok(());
    }

    let reader_stream = stream.try_clone()?;
    let mut reader = BufReader::new(reader_stream);
    let mut authenticated = false;
    let mut connection_server_id = String::new();
    let mut connection_server_name = String::new();
    let mut frame = Vec::with_capacity(4096);

    loop {
        match read_protocol_frame(&mut reader, config.max_frame_bytes, &mut frame)? {
            FrameRead::Eof => return Ok(()),
            FrameRead::TooLong => {
                stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!(
                    "frame too large from {peer}: max_frame_bytes={}",
                    config.max_frame_bytes
                );
                send_json_line(
                    &mut stream,
                    &ErrorResponse {
                        r#type: "error",
                        batch_id: None,
                        message: "frame too large",
                        ts: now_unix_u64(),
                    },
                )?;
                continue;
            }
            FrameRead::Line => {
                if frame.is_empty() {
                    continue;
                }
            }
        }

        let line = match std::str::from_utf8(&frame) {
            Ok(line) => line,
            Err(_) => {
                stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                send_json_line(
                    &mut stream,
                    &ErrorResponse {
                        r#type: "error",
                        batch_id: None,
                        message: "invalid utf8",
                        ts: now_unix_u64(),
                    },
                )?;
                continue;
            }
        };

        let message = match serde_json::from_str::<IncomingMessage>(line) {
            Ok(message) => message,
            Err(err) => {
                stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                eprintln!("invalid json from {peer}: {err}");
                send_json_line(
                    &mut stream,
                    &ErrorResponse {
                        r#type: "error",
                        batch_id: None,
                        message: "invalid json message",
                        ts: now_unix_u64(),
                    },
                )?;
                continue;
            }
        };

        match message {
            IncomingMessage::Hello {
                service,
                proto,
                server_id,
                server_name,
                auth,
                ts,
            } => {
                if !protocol_auth_matches(&config, auth.as_deref()) {
                    stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    eprintln!("unauthorized hello from {peer}");
                    send_json_line(
                        &mut stream,
                        &ErrorResponse {
                            r#type: "error",
                            batch_id: None,
                            message: "unauthorized",
                            ts: now_unix_u64(),
                        },
                    )?;
                    return Ok(());
                }

                authenticated = true;
                connection_server_id =
                    normalize_identifier(server_id.as_deref().unwrap_or("unknown"), 128, "unknown");
                connection_server_name = limit_chars(server_name.as_deref().unwrap_or(""), 255);
                send_json_line(
                    &mut stream,
                    &HelloResponse {
                        r#type: "hello_ack",
                        service: service.as_deref().unwrap_or("unknown"),
                        proto: proto.unwrap_or(1),
                        server_id: &connection_server_id,
                        ts: ts.unwrap_or_else(now_unix_u64),
                        server_time: now_unix_u64(),
                    },
                )?;
            }
            IncomingMessage::StatsBatch {
                batch_id,
                table,
                sent_at,
                events,
            } => {
                if !authenticated {
                    stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    send_json_line(
                        &mut stream,
                        &ErrorResponse {
                            r#type: "error",
                            batch_id,
                            message: "hello required",
                            ts: now_unix_u64(),
                        },
                    )?;
                    continue;
                }

                if events.len() > config.max_inbound_events {
                    stats.parse_errors.fetch_add(1, Ordering::Relaxed);
                    send_json_line(
                        &mut stream,
                        &ErrorResponse {
                            r#type: "error",
                            batch_id,
                            message: "too many events",
                            ts: now_unix_u64(),
                        },
                    )?;
                    continue;
                }

                let total = events.len();
                let enqueue = match enqueue_events(
                    &queue,
                    &config,
                    batch_id,
                    table,
                    events,
                    &connection_server_id,
                    &connection_server_name,
                    &dedupe,
                    &pending_journal,
                    &stats,
                ) {
                    Ok(enqueue) => enqueue,
                    Err(err) => {
                        eprintln!(
                            "rejected stats batch {:?} from {peer}: {} (queued={}, incoming={}, limit={})",
                            batch_id, err.message, err.queued, err.incoming, err.limit
                        );
                        send_json_line(
                            &mut stream,
                            &ErrorResponse {
                                r#type: "error",
                                batch_id,
                                message: err.message,
                                ts: now_unix_u64(),
                            },
                        )?;
                        continue;
                    }
                };

                let completion = enqueue
                    .completion
                    .as_ref()
                    .map(|completion| completion.wait())
                    .unwrap_or(BatchCompletionSnapshot {
                        executed: 0,
                        db_errors: 0,
                    });
                if config.debug {
                    println!(
                        "batch {:?}: accepted {}/{} events, executed={}, db_errors={} queue_depth={} sent_at={}",
                        batch_id,
                        enqueue.accepted,
                        total,
                        completion.executed,
                        completion.db_errors,
                        queue_depth(&queue),
                        sent_at.unwrap_or(0)
                    );
                }
                send_json_line(
                    &mut stream,
                    &AckResponse {
                        r#type: "ack",
                        batch_id,
                        accepted: enqueue.accepted,
                        executed: completion.executed,
                        db_errors: completion.db_errors,
                        queue_depth: queue_depth(&queue),
                        sent_at: sent_at.unwrap_or(0),
                        ts: now_unix_u64(),
                    },
                )?;
            }
            IncomingMessage::SqlBatch { batch_id } => {
                send_json_line(
                    &mut stream,
                    &ErrorResponse {
                        r#type: "error",
                        batch_id,
                        message:
                            "raw sql_batch is intentionally unsupported; send stats_batch events",
                        ts: now_unix_u64(),
                    },
                )?;
            }
            IncomingMessage::Health => {
                if !authenticated {
                    send_json_line(
                        &mut stream,
                        &ErrorResponse {
                            r#type: "error",
                            batch_id: None,
                            message: "hello required",
                            ts: now_unix_u64(),
                        },
                    )?;
                    continue;
                }

                let (depth, dropped) = queue_depth_and_dropped(&queue);
                send_json_line(
                    &mut stream,
                    &HealthResponse {
                        r#type: "health",
                        queue_depth: depth,
                        queue_dropped: dropped,
                        dedupe_events: dedupe.len(),
                        accepted_events: stats.accepted_events.load(Ordering::Relaxed),
                        executed_events: stats.executed_events.load(Ordering::Relaxed),
                        db_errors: stats.db_errors.load(Ordering::Relaxed),
                        parse_errors: stats.parse_errors.load(Ordering::Relaxed),
                        dropped_events: stats.dropped_events.load(Ordering::Relaxed),
                        journal_pending_startup: stats
                            .journal_pending_startup
                            .load(Ordering::Relaxed),
                        journal_replayed_startup: stats
                            .journal_replayed_startup
                            .load(Ordering::Relaxed),
                        journal_done_records_startup: stats
                            .journal_done_records_startup
                            .load(Ordering::Relaxed),
                        journal_bad_lines_startup: stats
                            .journal_bad_lines_startup
                            .load(Ordering::Relaxed),
                        journal_compactions: stats.journal_compactions.load(Ordering::Relaxed),
                        ts: now_unix_u64(),
                    },
                )?;
            }
        }
    }
}

fn enqueue_events(
    queue: &SharedQueue,
    config: &Config,
    batch_id: Option<u64>,
    batch_table: Option<String>,
    events: Vec<StatsEvent>,
    connection_server_id: &str,
    connection_server_name: &str,
    dedupe: &DedupeCache,
    pending_journal: &PendingJournal,
    stats: &ServiceStats,
) -> Result<EnqueueResult, EnqueueError> {
    let mut accepted_events = Vec::new();

    for mut event in events {
        let legacy_table = if event.table.is_empty() {
            batch_table.clone().unwrap_or_default()
        } else {
            std::mem::take(&mut event.table)
        };
        event.server_id = normalize_identifier(
            if event.server_id.trim().is_empty() {
                connection_server_id
            } else {
                &event.server_id
            },
            128,
            "unknown",
        );
        if event.server_name.trim().is_empty() {
            event.server_name = connection_server_name.to_string();
        }
        event.source_plugin = normalize_identifier(
            if event.source_plugin.trim().is_empty() {
                legacy_source_name(&legacy_table)
            } else {
                &event.source_plugin
            },
            64,
            "unknown",
        );

        normalize_event(&mut event);
        let event_id = sanitize_event_id(event.event_id.as_deref()).or_else(|| {
            Some(format!(
                "rust-{}-{}-{}",
                event.host_port,
                now_ms_u64(),
                accepted_events.len()
            ))
        });
        event.event_id = event_id.clone();
        if let Some(event_id) = event_id.as_deref() {
            if dedupe.contains(event_id) {
                stats.dropped_events.fetch_add(1, Ordering::Relaxed);
                continue;
            }
        }

        accepted_events.push(QueuedEvent {
            source_plugin: event.source_plugin.clone(),
            event,
            event_id,
            batch_id,
            enqueued_at_ms: now_ms_u64(),
            completion: None,
        });
    }

    let accepted = accepted_events.len();
    let (lock, cvar) = &**queue;
    let mut state = lock.lock().expect("queue mutex poisoned");
    let queued = state.events.len();
    if accepted > 0 && queued.saturating_add(accepted) > config.queue_limit {
        stats
            .dropped_events
            .fetch_add(accepted as u64, Ordering::Relaxed);
        return Err(EnqueueError {
            message: "queue full",
            queued,
            incoming: accepted,
            limit: config.queue_limit,
        });
    }

    for item in &accepted_events {
        if let Some(event_id) = item.event_id.as_deref() {
            if let Err(err) = pending_journal.append_pending(
                event_id,
                &item.source_plugin,
                &item.event,
                item.batch_id,
            ) {
                stats
                    .dropped_events
                    .fetch_add(accepted as u64, Ordering::Relaxed);
                eprintln!(
                    "failed to append pending journal event_id={} batch_id={:?}: {}",
                    event_id, item.batch_id, err
                );
                return Err(EnqueueError {
                    message: "journal write failed",
                    queued,
                    incoming: accepted,
                    limit: config.queue_limit,
                });
            }
        }
    }

    let completion = (accepted > 0).then(|| Arc::new(BatchCompletion::new(accepted)));
    for mut item in accepted_events {
        item.completion = completion.clone();
        state.events.push_back(item);
    }
    if accepted > 0 {
        stats
            .accepted_events
            .fetch_add(accepted as u64, Ordering::Relaxed);
        cvar.notify_one();
    }

    Ok(EnqueueResult {
        accepted,
        completion,
    })
}

fn spawn_writer(
    config: Config,
    pool: Pool,
    queue: SharedQueue,
    dedupe: Arc<DedupeCache>,
    pending_journal: Arc<PendingJournal>,
    stats: Arc<ServiceStats>,
) {
    thread::spawn(move || {
        let mut cache = WriterCache::default();
        loop {
            let batch = take_batch(&queue, config.flush_interval, config.max_batch_rows);
            if batch.is_empty() {
                continue;
            }

            let (rows, failed) = write_batch(
                &pool,
                &mut cache,
                batch,
                &dedupe,
                &pending_journal,
                &stats,
                config.pending_journal_compact_bytes,
            );
            if rows > 0 && config.debug {
                println!("wrote {rows} statistics row(s)");
            }
            if !failed.is_empty() {
                requeue_front(&queue, &config, failed);
                thread::sleep(Duration::from_secs(2));
            }
        }
    });
}

fn take_batch(queue: &SharedQueue, interval: Duration, max_rows: usize) -> Vec<QueuedEvent> {
    let (lock, cvar) = &**queue;
    let mut state = lock.lock().expect("queue mutex poisoned");

    if state.events.is_empty() {
        let (new_state, _timeout) = cvar
            .wait_timeout(state, interval)
            .expect("queue mutex poisoned");
        state = new_state;
    }

    let count = state.events.len().min(max_rows.max(1));
    state.events.drain(..count).collect()
}

fn requeue_front(queue: &SharedQueue, config: &Config, mut batch: Vec<QueuedEvent>) {
    let (lock, cvar) = &**queue;
    let mut state = lock.lock().expect("queue mutex poisoned");

    while state.events.len() + batch.len() > config.queue_limit {
        state.events.pop_back();
        state.dropped += 1;
    }

    while let Some(event) = batch.pop() {
        state.events.push_front(event);
    }

    cvar.notify_one();
}

fn write_batch(
    pool: &Pool,
    cache: &mut WriterCache,
    batch: Vec<QueuedEvent>,
    dedupe: &DedupeCache,
    pending_journal: &PendingJournal,
    stats: &ServiceStats,
    pending_journal_compact_bytes: u64,
) -> (usize, Vec<QueuedEvent>) {
    let mut conn = match pool.get_conn() {
        Ok(conn) => conn,
        Err(err) => {
            let failed = batch;
            stats
                .db_errors
                .fetch_add(failed.len() as u64, Ordering::Relaxed);
            eprintln!("statistics DB connection failed: {err}");
            return (0, failed);
        }
    };

    if let Err(err) = write_records(&mut conn, cache, &batch) {
        cache.clear();
        stats
            .db_errors
            .fetch_add(batch.len() as u64, Ordering::Relaxed);
        let oldest_age = batch
            .iter()
            .map(|item| now_ms_u64().saturating_sub(item.enqueued_at_ms))
            .max()
            .unwrap_or(0);
        eprintln!(
            "statistics write failed rows={} oldest_age_ms={}: {}",
            batch.len(),
            oldest_age,
            err
        );
        return (0, batch);
    }

    let total = batch.len();
    finish_written_events(
        batch,
        dedupe,
        pending_journal,
        stats,
        pending_journal_compact_bytes,
    );
    (total, Vec::new())
}

fn finish_written_events(
    items: Vec<QueuedEvent>,
    dedupe: &DedupeCache,
    pending_journal: &PendingJournal,
    stats: &ServiceStats,
    pending_journal_compact_bytes: u64,
) {
    let mut journal_rows = 0u64;
    for item in items {
        if let Some(event_id) = item.event_id.as_deref() {
            if let Err(err) = pending_journal.append_done(event_id) {
                eprintln!("failed to mark journal done event_id={event_id}: {err}");
            }
            dedupe.remember(event_id);
            journal_rows += 1;
        }
        stats.executed_events.fetch_add(1, Ordering::Relaxed);
        if let Some(completion) = &item.completion {
            completion.finish(true);
        }
    }

    if journal_rows == 0 {
        return;
    }

    let completed = stats
        .journal_events_since_compaction_check
        .fetch_add(journal_rows, Ordering::Relaxed)
        + journal_rows;
    if completed < PENDING_JOURNAL_COMPACT_CHECK_EVERY {
        return;
    }

    stats
        .journal_events_since_compaction_check
        .store(0, Ordering::Relaxed);
    match pending_journal.compact_if_needed(dedupe, pending_journal_compact_bytes) {
        Ok(true) => {
            stats.journal_compactions.fetch_add(1, Ordering::Relaxed);
            println!("[statistics] compacted pending journal");
        }
        Ok(false) => {}
        Err(err) => eprintln!("[statistics] pending journal compaction failed: {err}"),
    }
}

#[derive(Default)]
struct WriterCache {
    server_ids: HashMap<String, u64>,
    session_ids: HashMap<(u64, String), u64>,
}

impl WriterCache {
    fn clear(&mut self) {
        self.server_ids.clear();
        self.session_ids.clear();
    }
}

struct EventRow<'a> {
    session_id: u64,
    event: &'a StatsEvent,
}

struct TickRow<'a> {
    session_id: u64,
    event: &'a StatsEvent,
}

fn write_records(
    conn: &mut PooledConn,
    cache: &mut WriterCache,
    items: &[QueuedEvent],
) -> mysql::Result<()> {
    let mut transaction = conn.start_transaction(TxOpts::default())?;
    let mut event_rows = Vec::new();
    let mut tick_rows = Vec::new();
    let mut server_activity = HashMap::<u64, (i64, i32, &str)>::new();

    for item in items {
        let event = &item.event;
        let server_id = ensure_server(&mut transaction, cache, event)?;
        let activity = server_activity.entry(server_id).or_insert((
            event.occurred_at,
            event.host_port,
            event.server_name.as_str(),
        ));
        if event.occurred_at >= activity.0 {
            *activity = (
                event.occurred_at,
                event.host_port,
                event.server_name.as_str(),
            );
        }
        let session_id = ensure_session(&mut transaction, cache, server_id, event)?;

        match event.record_type.as_str() {
            "session_start" => start_session(&mut transaction, server_id, session_id, event)?,
            "session_end" => end_session(&mut transaction, session_id, event, false)?,
            "tick_sample" => tick_rows.push(TickRow { session_id, event }),
            _ => event_rows.push(EventRow { session_id, event }),
        }
    }

    for (server_id, (last_seen_at, host_port, hostname)) in server_activity {
        transaction.exec_drop(
            "UPDATE plugin_statistics_servers SET hostname = :hostname, host_port = :host_port, \
             last_seen_at = GREATEST(last_seen_at, :last_seen_at) WHERE id = :server_id",
            params! {
                "server_id" => server_id,
                "hostname" => hostname,
                "host_port" => host_port,
                "last_seen_at" => last_seen_at,
            },
        )?;
    }

    insert_event_rows(&mut transaction, &event_rows)?;
    insert_tick_rows(&mut transaction, &tick_rows)?;
    transaction.commit()
}

fn ensure_server<Q: Queryable>(
    conn: &mut Q,
    cache: &mut WriterCache,
    event: &StatsEvent,
) -> mysql::Result<u64> {
    if let Some(id) = cache.server_ids.get(&event.server_id) {
        return Ok(*id);
    }

    conn.exec_drop(
        "INSERT INTO plugin_statistics_servers \
         (server_key, hostname, host_port, created_at, last_seen_at) \
         VALUES (:server_key, :hostname, :host_port, :now, :now) \
         ON DUPLICATE KEY UPDATE \
           id = LAST_INSERT_ID(id), hostname = VALUES(hostname), host_port = VALUES(host_port), \
           last_seen_at = GREATEST(last_seen_at, VALUES(last_seen_at))",
        params! {
            "server_key" => event.server_id.as_str(),
            "hostname" => event.server_name.as_str(),
            "host_port" => event.host_port,
            "now" => event.occurred_at,
        },
    )?;
    let id = conn
        .query_first::<u64, _>("SELECT LAST_INSERT_ID()")?
        .unwrap_or_default();
    cache.server_ids.insert(event.server_id.clone(), id);
    Ok(id)
}

fn ensure_session<Q: Queryable>(
    conn: &mut Q,
    cache: &mut WriterCache,
    server_id: u64,
    event: &StatsEvent,
) -> mysql::Result<u64> {
    let cache_key = (server_id, event.map_session_id.clone());
    if let Some(id) = cache.session_ids.get(&cache_key) {
        return Ok(*id);
    }

    let started_at = if event.session_started_at > 0 {
        event.session_started_at
    } else {
        event.occurred_at
    };
    conn.exec_drop(
        "INSERT INTO plugin_statistics_map_sessions \
         (session_key, server_id, host_port, map_name, gamemode, started_at, \
          start_tick, tick_interval_seconds, expected_tickrate, created_at, updated_at) \
         VALUES \
         (:session_key, :server_id, :host_port, :map_name, :gamemode, :started_at, \
          :start_tick, :tick_interval, :expected_tickrate, :now, :now) \
         ON DUPLICATE KEY UPDATE \
           id = LAST_INSERT_ID(id), map_name = VALUES(map_name), \
           gamemode = VALUES(gamemode), updated_at = VALUES(updated_at)",
        params! {
            "session_key" => event.map_session_id.as_str(),
            "server_id" => server_id,
            "host_port" => event.host_port,
            "map_name" => event.map_name.as_str(),
            "gamemode" => event.gamemode.as_str(),
            "started_at" => started_at,
            "start_tick" => event.server_tick,
            "tick_interval" => event.tick_interval_seconds,
            "expected_tickrate" => event.expected_tickrate,
            "now" => event.occurred_at,
        },
    )?;
    let id = conn
        .query_first::<u64, _>("SELECT LAST_INSERT_ID()")?
        .unwrap_or_default();
    cache.session_ids.insert(cache_key, id);
    Ok(id)
}

fn start_session<Q: Queryable>(
    conn: &mut Q,
    server_id: u64,
    session_id: u64,
    event: &StatsEvent,
) -> mysql::Result<()> {
    let stale_sessions = conn.exec_map(
        "SELECT id, started_at FROM plugin_statistics_map_sessions \
         WHERE server_id = :server_id AND ended_at IS NULL AND id <> :session_id",
        params! {
            "server_id" => server_id,
            "session_id" => session_id,
        },
        |(id, started_at): (u64, i64)| (id, started_at),
    )?;
    for (stale_id, stale_started_at) in stale_sessions {
        finalize_stale_session(conn, stale_id, stale_started_at, event.occurred_at)?;
    }

    let started_at = if event.session_started_at > 0 {
        event.session_started_at
    } else {
        event.occurred_at
    };
    conn.exec_drop(
        "UPDATE plugin_statistics_map_sessions SET \
           map_name = :map_name, gamemode = :gamemode, started_at = :started_at, \
           start_tick = :start_tick, tick_interval_seconds = :tick_interval, \
           expected_tickrate = :expected_tickrate, updated_at = :now \
         WHERE id = :session_id",
        params! {
            "session_id" => session_id,
            "map_name" => event.map_name.as_str(),
            "gamemode" => event.gamemode.as_str(),
            "started_at" => started_at,
            "start_tick" => event.server_tick,
            "tick_interval" => event.tick_interval_seconds,
            "expected_tickrate" => event.expected_tickrate,
            "now" => event.occurred_at,
        },
    )
}

fn finalize_stale_session<Q: Queryable>(
    conn: &mut Q,
    session_id: u64,
    started_at: i64,
    next_started_at: i64,
) -> mysql::Result<()> {
    let last_event = conn
        .exec_first::<Option<i64>, _, _>(
            "SELECT MAX(occurred_at) FROM plugin_statistics_events WHERE session_id = :session_id",
            params! { "session_id" => session_id },
        )?
        .flatten();
    let last_tick = conn
        .exec_first::<Option<i64>, _, _>(
            "SELECT MAX(sampled_at) FROM plugin_statistics_tick_samples WHERE session_id = :session_id",
            params! { "session_id" => session_id },
        )?
        .flatten();
    let ended_at = last_event
        .into_iter()
        .chain(last_tick)
        .max()
        .unwrap_or(started_at)
        .clamp(started_at, next_started_at.max(started_at));
    let aggregate = conn
        .exec_first::<(Option<f64>, Option<f64>, u64), _, _>(
            "SELECT AVG(observed_tickrate), MIN(observed_tickrate), COUNT(*) \
             FROM plugin_statistics_tick_samples WHERE session_id = :session_id",
            params! { "session_id" => session_id },
        )?
        .unwrap_or((None, None, 0));

    conn.exec_drop(
        "UPDATE plugin_statistics_map_sessions SET \
           ended_at = :ended_at, duration_seconds = GREATEST(0, :ended_at - started_at), \
           end_reason = 'superseded', end_inferred = 1, \
           average_observed_tickrate = :average_tickrate, \
           minimum_observed_tickrate = :minimum_tickrate, \
           tick_sample_count = :sample_count, updated_at = :ended_at \
         WHERE id = :session_id",
        params! {
            "session_id" => session_id,
            "ended_at" => ended_at,
            "average_tickrate" => aggregate.0,
            "minimum_tickrate" => aggregate.1,
            "sample_count" => aggregate.2,
        },
    )
}

fn end_session<Q: Queryable>(
    conn: &mut Q,
    session_id: u64,
    event: &StatsEvent,
    inferred: bool,
) -> mysql::Result<()> {
    let ended_at = if event.session_ended_at > 0 {
        event.session_ended_at
    } else {
        event.occurred_at
    };
    conn.exec_drop(
        "UPDATE plugin_statistics_map_sessions SET \
           ended_at = :ended_at, duration_seconds = GREATEST(0, :ended_at - started_at), \
           end_reason = :end_reason, end_inferred = :end_inferred, end_tick = :end_tick, \
           average_observed_tickrate = :average_tickrate, \
           minimum_observed_tickrate = :minimum_tickrate, \
           tick_sample_count = :sample_count, updated_at = :ended_at \
         WHERE id = :session_id",
        params! {
            "session_id" => session_id,
            "ended_at" => ended_at,
            "end_reason" => event.end_reason.as_str(),
            "end_inferred" => inferred,
            "end_tick" => event.server_tick,
            "average_tickrate" => event.session_average_tickrate,
            "minimum_tickrate" => event.session_minimum_tickrate,
            "sample_count" => event.session_sample_count,
        },
    )
}

fn insert_event_rows<Q: Queryable>(conn: &mut Q, rows: &[EventRow<'_>]) -> mysql::Result<()> {
    let created_at = now_unix_i64();
    conn.exec_batch(
        "INSERT INTO plugin_statistics_events \
         (event_id, session_id, source_plugin, event_name, message, occurred_at, \
          server_tick, tick_interval_seconds, expected_tickrate, observed_tickrate, created_at) \
         VALUES \
         (:event_id, :session_id, :source_plugin, :event_name, :message, :occurred_at, \
          :server_tick, :tick_interval, :expected_tickrate, :observed_tickrate, :created_at) \
         ON DUPLICATE KEY UPDATE event_id = VALUES(event_id)",
        rows.iter().map(|row| {
            params! {
                "event_id" => row.event.event_id.as_deref().unwrap_or_default(),
                "session_id" => row.session_id,
                "source_plugin" => row.event.source_plugin.as_str(),
                "event_name" => row.event.event_name.as_str(),
                "message" => row.event.message.as_str(),
                "occurred_at" => row.event.occurred_at,
                "server_tick" => row.event.server_tick,
                "tick_interval" => row.event.tick_interval_seconds,
                "expected_tickrate" => row.event.expected_tickrate,
                "observed_tickrate" => row.event.observed_tickrate,
                "created_at" => created_at,
            }
        }),
    )
}

fn insert_tick_rows<Q: Queryable>(conn: &mut Q, rows: &[TickRow<'_>]) -> mysql::Result<()> {
    let created_at = now_unix_i64();
    conn.exec_batch(
        "INSERT INTO plugin_statistics_tick_samples \
         (event_id, session_id, sampled_at, server_tick, tick_interval_seconds, \
          expected_tickrate, observed_tickrate, created_at) \
         VALUES \
         (:event_id, :session_id, :sampled_at, :server_tick, :tick_interval, \
          :expected_tickrate, :observed_tickrate, :created_at) \
         ON DUPLICATE KEY UPDATE event_id = VALUES(event_id)",
        rows.iter().map(|row| {
            params! {
                "event_id" => row.event.event_id.as_deref().unwrap_or_default(),
                "session_id" => row.session_id,
                "sampled_at" => row.event.occurred_at,
                "server_tick" => row.event.server_tick,
                "tick_interval" => row.event.tick_interval_seconds,
                "expected_tickrate" => row.event.expected_tickrate,
                "observed_tickrate" => row.event.observed_tickrate,
                "created_at" => created_at,
            }
        }),
    )
}

fn replay_pending_journal(
    queue: &SharedQueue,
    config: &Config,
    dedupe: &DedupeCache,
    pending_journal: &PendingJournal,
    dead_letters: &DeadLetterWriter,
    stats: &ServiceStats,
) -> Result<(), String> {
    let replay = pending_journal.load_replay_state(dedupe);
    pending_journal.compact_from_state(&replay)?;
    stats.journal_compactions.fetch_add(1, Ordering::Relaxed);
    stats
        .journal_pending_startup
        .store(replay.pending.len() as u64, Ordering::Relaxed);
    stats
        .journal_done_records_startup
        .store(replay.done_records as u64, Ordering::Relaxed);
    stats
        .journal_bad_lines_startup
        .store(replay.bad_lines as u64, Ordering::Relaxed);

    let total = replay.pending.len();
    if total == 0 {
        println!(
            "pending journal replay: pending=0 recent_done={} done_records={} bad_lines={}",
            replay.recent_done.len(),
            replay.done_records,
            replay.bad_lines
        );
        return Ok(());
    }

    let (lock, cvar) = &**queue;
    let mut state = lock.lock().expect("queue mutex poisoned");
    let mut replayed = 0usize;
    for mut pending in replay.pending {
        if dedupe.contains(&pending.event_id) {
            continue;
        }
        if pending.event.server_id.is_empty() {
            pending.event.server_id = format!("legacy:{}", pending.event.host_port);
        }
        if pending.event.source_plugin.is_empty() {
            pending.event.source_plugin = pending.source_plugin.clone();
        }
        normalize_event(&mut pending.event);
        if state.events.len() >= config.queue_limit {
            dead_letters.record(
                "journal_replay_queue_full",
                Some("queue full during startup replay"),
                Some(&pending.source_plugin),
                pending.batch_id,
                Some(&pending.event_id),
                Some(&pending.event),
            );
            stats.dropped_events.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        state.events.push_back(QueuedEvent {
            source_plugin: pending.source_plugin,
            event: pending.event,
            event_id: Some(pending.event_id),
            batch_id: pending.batch_id,
            enqueued_at_ms: pending.ts_ms,
            completion: None,
        });
        replayed += 1;
    }
    if replayed > 0 {
        cvar.notify_one();
    }
    stats
        .journal_replayed_startup
        .store(replayed as u64, Ordering::Relaxed);
    println!(
        "pending journal replay: replayed={}/{} recent_done={} done_records={} bad_lines={}",
        replayed,
        total,
        replay.recent_done.len(),
        replay.done_records,
        replay.bad_lines
    );
    Ok(())
}

fn normalize_event(event: &mut StatsEvent) {
    if event.occurred_at <= 0 {
        event.occurred_at = now_unix_i64();
    }

    event.table.clear();
    event.record_type = match event.record_type.as_str() {
        "event" | "session_start" | "session_end" | "tick_sample" => event.record_type.clone(),
        _ => default_record_type(),
    };
    event.server_id = limit_chars(&event.server_id, 128);
    event.server_name = limit_chars(&event.server_name, 255);
    event.source_plugin = limit_chars(&event.source_plugin, 64);
    event.map_session_id = limit_chars(&event.map_session_id, 128);
    if event.map_session_id.is_empty() {
        event.map_session_id = format!(
            "{}-{}",
            event.host_port,
            event.session_started_at.max(event.occurred_at)
        );
    }
    event.map_name = limit_chars(&event.map_name, 128);
    event.gamemode = limit_chars(&event.gamemode, 64);
    event.event_name = limit_chars(&event.event_name, 64);
    if event.event_name.is_empty() {
        event.event_name = "event".to_string();
    }
    event.message = limit_chars(&event.message, 512);
    event.end_reason = limit_chars(&event.end_reason, 32);
    event.server_tick = event.server_tick.max(0);
    event.tick_interval_seconds = finite_nonnegative(event.tick_interval_seconds);
    event.expected_tickrate = finite_nonnegative(event.expected_tickrate);
    event.observed_tickrate = finite_nonnegative(event.observed_tickrate);
    event.session_average_tickrate = finite_nonnegative(event.session_average_tickrate);
    event.session_minimum_tickrate = finite_nonnegative(event.session_minimum_tickrate);

    if event.weekday < 0 || event.weekday > 6 {
        event.weekday = 0;
    }

    if event.hour_of_day < 0 || event.hour_of_day > 23 {
        event.hour_of_day = 0;
    }
}

fn sanitize_event_id(value: Option<&str>) -> Option<String> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    let filtered = value
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | ':' | '.'))
        .take(128)
        .collect::<String>();
    (!filtered.is_empty()).then_some(filtered)
}

fn default_record_type() -> String {
    "event".to_string()
}

fn finite_nonnegative(value: f64) -> f64 {
    if value.is_finite() && value >= 0.0 {
        value
    } else {
        0.0
    }
}

fn legacy_source_name(table: &str) -> &str {
    table.strip_suffix("_statistics_events").unwrap_or(table)
}

fn normalize_identifier(value: &str, max_chars: usize, fallback: &str) -> String {
    let normalized = value
        .trim()
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-' | '.' | ':') {
                ch
            } else {
                '_'
            }
        })
        .take(max_chars)
        .collect::<String>();
    if normalized.is_empty() {
        fallback.to_string()
    } else {
        normalized
    }
}

fn queue_depth(queue: &SharedQueue) -> usize {
    queue.0.lock().expect("queue mutex poisoned").events.len()
}

fn queue_depth_and_dropped(queue: &SharedQueue) -> (usize, u64) {
    let state = queue.0.lock().expect("queue mutex poisoned");
    (state.events.len(), state.dropped)
}

fn send_json_line<T: Serialize>(stream: &mut TcpStream, value: &T) -> std::io::Result<()> {
    serde_json::to_writer(&mut *stream, value).map_err(std::io::Error::other)?;
    stream.write_all(b"\n")?;
    stream.flush()
}

#[derive(Debug, PartialEq, Eq)]
enum FrameRead {
    Line,
    Eof,
    TooLong,
}

fn read_protocol_frame<R: BufRead>(
    reader: &mut R,
    max_frame_bytes: usize,
    out: &mut Vec<u8>,
) -> std::io::Result<FrameRead> {
    out.clear();

    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            if out.is_empty() {
                return Ok(FrameRead::Eof);
            }
            trim_frame_cr(out);
            return Ok(FrameRead::Line);
        }

        let newline_pos = available.iter().position(|&byte| byte == b'\n');
        let take_len = newline_pos.unwrap_or(available.len());

        if out.len().saturating_add(take_len) > max_frame_bytes {
            let consume_len = newline_pos.map(|pos| pos + 1).unwrap_or(available.len());
            reader.consume(consume_len);
            if newline_pos.is_none() {
                drain_protocol_frame(reader)?;
            }
            out.clear();
            return Ok(FrameRead::TooLong);
        }

        out.extend_from_slice(&available[..take_len]);
        let consume_len = newline_pos.map(|pos| pos + 1).unwrap_or(available.len());
        reader.consume(consume_len);

        if newline_pos.is_some() {
            trim_frame_cr(out);
            return Ok(FrameRead::Line);
        }
    }
}

fn drain_protocol_frame<R: BufRead>(reader: &mut R) -> std::io::Result<()> {
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            return Ok(());
        }

        if let Some(newline_pos) = available.iter().position(|&byte| byte == b'\n') {
            reader.consume(newline_pos + 1);
            return Ok(());
        }

        let consume_len = available.len();
        reader.consume(consume_len);
    }
}

fn trim_frame_cr(frame: &mut Vec<u8>) {
    if frame.last().copied() == Some(b'\r') {
        frame.pop();
    }
}

fn protocol_auth_required(config: &Config) -> bool {
    !config.auth_token.trim().is_empty()
}

fn protocol_auth_matches(config: &Config, provided: Option<&str>) -> bool {
    if !protocol_auth_required(config) {
        return true;
    }
    provided == Some(config.auth_token.as_str())
}

fn is_loopback(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => ip.is_loopback(),
        IpAddr::V6(ip) => ip.is_loopback(),
    }
}

fn limit_chars(value: &str, max_chars: usize) -> String {
    value.chars().take(max_chars).collect()
}

fn now_unix_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn now_unix_i64() -> i64 {
    now_unix_u64() as i64
}

fn now_ms_u64() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

fn env_u16(key: &str, default: u16) -> u16 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(key: &str, default: u64) -> u64 {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_usize(key: &str, default: usize) -> usize {
    env::var(key)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_bool(key: &str, default: bool) -> bool {
    env::var(key)
        .ok()
        .map(|value| matches!(value.as_str(), "1" | "true" | "TRUE" | "yes" | "YES"))
        .unwrap_or(default)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_protocol_frame_with_crlf() {
        let input = std::io::Cursor::new(b"{\"type\":\"health\"}\r\n".to_vec());
        let mut reader = std::io::BufReader::new(input);
        let mut out = Vec::new();

        assert_eq!(
            read_protocol_frame(&mut reader, 64, &mut out).unwrap(),
            FrameRead::Line
        );
        assert_eq!(std::str::from_utf8(&out).unwrap(), "{\"type\":\"health\"}");
    }

    #[test]
    fn rejects_and_drains_oversized_protocol_frame() {
        let input = std::io::Cursor::new(b"abcdef\nok\n".to_vec());
        let mut reader = std::io::BufReader::new(input);
        let mut out = Vec::new();

        assert_eq!(
            read_protocol_frame(&mut reader, 4, &mut out).unwrap(),
            FrameRead::TooLong
        );
        assert!(out.is_empty());
        assert_eq!(
            read_protocol_frame(&mut reader, 4, &mut out).unwrap(),
            FrameRead::Line
        );
        assert_eq!(std::str::from_utf8(&out).unwrap(), "ok");
    }

    #[test]
    fn sanitizes_event_id() {
        assert_eq!(
            sanitize_event_id(Some("abc:123-zz")),
            Some("abc:123-zz".to_string())
        );
        assert_eq!(sanitize_event_id(Some("***")), None);
        assert_eq!(sanitize_event_id(None), None);
    }

    #[test]
    fn optional_protocol_auth_matches_only_when_configured() {
        let mut open_cfg = Config::from_env();
        open_cfg.auth_token.clear();
        assert!(protocol_auth_matches(&open_cfg, None));

        open_cfg.auth_token = "secret".to_string();
        assert!(!protocol_auth_matches(&open_cfg, None));
        assert!(!protocol_auth_matches(&open_cfg, Some("wrong")));
        assert!(protocol_auth_matches(&open_cfg, Some("secret")));
    }

    #[test]
    fn deserializes_tickrate_stamped_event() {
        let event: StatsEvent = serde_json::from_str(
            r#"{
                "record_type":"event",
                "event_id":"27015-1",
                "occurred_at":1786400000,
                "host_port":27015,
                "map_session_id":"1786400000_ABCD",
                "map_name":"cp_badlands",
                "source_plugin":"example",
                "event_name":"round_end",
                "server_tick":123456,
                "tick_interval_seconds":0.015,
                "expected_tickrate":66.667,
                "observed_tickrate":66.2
            }"#,
        )
        .unwrap();

        assert_eq!(event.source_plugin, "example");
        assert_eq!(event.server_tick, 123456);
        assert_eq!(event.expected_tickrate, 66.667);
        assert_eq!(event.observed_tickrate, 66.2);
    }

    #[test]
    fn normalizes_legacy_event_without_runtime_fields() {
        let mut event = StatsEvent {
            occurred_at: 100,
            host_port: 27015,
            map_name: "koth_viaduct".to_string(),
            event_name: "class_snapshot".to_string(),
            ..StatsEvent::default()
        };

        normalize_event(&mut event);

        assert_eq!(event.record_type, "event");
        assert_eq!(event.map_session_id, "27015-100");
        assert_eq!(event.expected_tickrate, 0.0);
    }

    #[test]
    fn migration_requires_tickrate_on_every_event() {
        let migration = include_str!("../migrations/001_initial.sql");
        assert!(migration.contains("plugin_statistics_map_sessions"));
        assert!(migration.contains("server_tick BIGINT NOT NULL"));
        assert!(migration.contains("expected_tickrate DECIMAL(9,3) NOT NULL"));
        assert!(migration.contains("observed_tickrate DECIMAL(9,3) NOT NULL"));
    }
}
