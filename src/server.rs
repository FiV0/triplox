//! TCP server for the Triplox wire protocol.
//!
//! Accepts TCP connections, manages DB snapshot handles with a shared
//! reference-counted cache, and dispatches Query/Execute operations
//! to the underlying Node.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

use crate::log::TxLog;
use crate::node::{Basis, Database, Node, QueryNode, SubmitNode, TransactionResult, DB};
use crate::parse::parse_query;
use crate::protocol::*;
use crate::query::QueryResult;

// ---------------------------------------------------------------------------
// DB Cache
// ---------------------------------------------------------------------------

/// A shared, reference-counted cache of DB snapshots.
/// Keyed by tx_id (the point-in-time identifier for the snapshot).
/// DBs are read-only and safe to share across connections.
struct DbCacheEntry {
    db: Arc<DB>,
    refcount: usize,
}

struct DbCache {
    entries: RwLock<HashMap<i64, DbCacheEntry>>,
    max_open: usize,
}

impl DbCache {
    fn new(max_open: usize) -> Self {
        DbCache {
            entries: RwLock::new(HashMap::new()),
            max_open,
        }
    }

    /// Get or create a DB snapshot for the given tx_id.
    /// The `create` future is only evaluated on cache miss.
    // TODO(triplox-d2q): the write lock is held across create().await (disk I/O),
    // blocking all other acquire/release calls across every connection.
    async fn acquire<F, Fut>(&self, tx_id: i64, create: F) -> Result<Arc<DB>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DB>>,
    {
        // Fast path: cache hit under read lock
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(&tx_id) {
                entry.refcount += 1;
                return Ok(entry.db.clone());
            }
            if entries.len() >= self.max_open {
                bail!("Too many open DB snapshots (max {})", self.max_open);
            }
            // Drop the lock before the potentially expensive create()
        }

        let db = create().await?;
        let arc_db = Arc::new(db);

        // Re-acquire the lock to insert. Another caller may have raced us
        // for the same tx_id — if so, use their entry and drop ours.
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&tx_id) {
            entry.refcount += 1;
            return Ok(entry.db.clone());
        }
        if entries.len() >= self.max_open {
            bail!("Too many open DB snapshots (max {})", self.max_open);
        }
        entries.insert(
            tx_id,
            DbCacheEntry {
                db: arc_db.clone(),
                refcount: 1,
            },
        );
        Ok(arc_db)
    }

    /// Release a reference to a DB snapshot.
    /// Evicts the entry when refcount reaches 0.
    async fn release(&self, tx_id: i64) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&tx_id) {
            entry.refcount -= 1;
            if entry.refcount == 0 {
                entries.remove(&tx_id);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Per-connection state
// ---------------------------------------------------------------------------

/// Tracks the DB handles open on a single connection.
struct ConnectionState {
    /// Maps client-facing db_id -> tx_id (for cache lookup)
    handles: HashMap<u32, i64>,
    /// Maps client-facing db_id -> Arc<DB> (for quick access during queries)
    dbs: HashMap<u32, Arc<DB>>,
    next_db_id: u32,
}

impl ConnectionState {
    fn new() -> Self {
        ConnectionState {
            handles: HashMap::new(),
            dbs: HashMap::new(),
            next_db_id: 1,
        }
    }

    fn allocate_handle(&mut self, tx_id: i64, db: Arc<DB>) -> Result<u32> {
        let db_id = self.next_db_id;
        self.next_db_id = self.next_db_id.checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("DB handle ID space exhausted"))?;
        self.handles.insert(db_id, tx_id);
        self.dbs.insert(db_id, db);
        Ok(db_id)
    }

    fn get_db(&self, db_id: u32) -> Option<&Arc<DB>> {
        self.dbs.get(&db_id)
    }

    fn remove_handle(&mut self, db_id: u32) -> Option<i64> {
        self.dbs.remove(&db_id);
        self.handles.remove(&db_id)
    }

    fn all_tx_ids(&self) -> Vec<i64> {
        self.handles.values().copied().collect()
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

pub struct Server<L: TxLog> {
    node: Arc<Node<L>>,
    db_cache: Arc<DbCache>,
}

impl<L: TxLog + 'static> Server<L> {
    pub fn new(node: Arc<Node<L>>, max_open_dbs: usize) -> Self {
        Server {
            node,
            db_cache: Arc::new(DbCache::new(max_open_dbs)),
        }
    }

    /// Start listening on the given address.
    ///
    /// Runs until the `token` is cancelled, then stops accepting new connections.
    /// Each connection handler receives a child token so it can also observe
    /// the shutdown and clean up promptly.
    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    /// Start serving on a pre-bound listener.
    pub async fn listen_on(
        &self,
        listener: TcpListener,
        token: CancellationToken,
    ) -> Result<()> {
        info!(
            "Triplox server listening on {}",
            listener.local_addr()?
        );

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("Shutdown signal received, stopping listener");
                    return Ok(());
                }
                result = listener.accept() => {
                    let (stream, peer_addr) = result?;
                    stream.set_nodelay(true)?;
                    info!("New connection from {}", peer_addr);

                    let node = self.node.clone();
                    let db_cache = self.db_cache.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(stream, node, db_cache).await {
                            warn!("Connection from {} closed with error: {}", peer_addr, e);
                        } else {
                            info!("Connection from {} closed cleanly", peer_addr);
                        }
                    });
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Connection handler
// ---------------------------------------------------------------------------

async fn handle_connection<L: TxLog + 'static>(
    stream: TcpStream,
    node: Arc<Node<L>>,
    db_cache: Arc<DbCache>,
) -> Result<()> {
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader);
    let mut writer = BufWriter::new(writer);

    // Phase 1: Startup handshake
    let startup = read_frontend_message(&mut reader, true, DEFAULT_MAX_MESSAGE_SIZE).await?;
    match startup {
        FrontendMessage::Startup {
            version_major,
            version_minor,
            ..
        } => {
            if version_major != PROTOCOL_VERSION_MAJOR {
                write_backend_message(
                    &mut writer,
                    &BackendMessage::ErrorResponse {
                        severity: SEVERITY_FATAL,
                        code: ErrorCode::ProtocolVersionMismatch.as_u16(),
                        message: format!(
                            "Unsupported protocol version {}.{}",
                            version_major, version_minor
                        ),
                        detail: None,
                        hint: Some(format!(
                            "Server supports {}.{}",
                            PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR
                        )),
                    },
                )
                .await?;
                writer.flush().await?;
                return Ok(());
            }

            write_backend_message(
                &mut writer,
                &BackendMessage::AuthenticationOk {
                    server_version: format!(
                        "triplox {}.{}.0",
                        PROTOCOL_VERSION_MAJOR, PROTOCOL_VERSION_MINOR
                    ),
                },
            )
            .await?;
            ready_for_query_flush(&mut writer).await?;
        }
        _ => {
            bail!("Expected Startup message, got {:?}", startup);
        }
    }

    // Phase 2: Main message loop
    let mut conn_state = ConnectionState::new();

    loop {
        let msg = match read_frontend_message(&mut reader, false, DEFAULT_MAX_MESSAGE_SIZE).await {
            Ok(msg) => msg,
            Err(e) => {
                // Connection dropped or read error — clean up
                cleanup_connection(&conn_state, &db_cache).await;
                return Err(e);
            }
        };

        match msg {
            FrontendMessage::Terminate => {
                cleanup_connection(&conn_state, &db_cache).await;
                return Ok(());
            }

            FrontendMessage::OpenDb { basis_tx_id, basis_system_time, basis_seq_num } => {
                match handle_open_db(&node, &db_cache, &mut conn_state, basis_tx_id, basis_system_time, basis_seq_num).await {
                    Ok((db_id, tx_id)) => {
                        write_backend_message(
                            &mut writer,
                            &BackendMessage::DbOpened { db_id, tx_id },
                        )
                        .await?;
                    }
                    Err(e) => {
                        write_error_response(&mut writer, SEVERITY_ERROR, e).await?;
                    }
                }
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::CloseDb { db_id } => {
                if let Some(tx_id) = conn_state.remove_handle(db_id) {
                    db_cache.release(tx_id).await;
                    write_backend_message(
                        &mut writer,
                        &BackendMessage::DbClosed { db_id },
                    )
                    .await?;
                } else {
                    write_backend_message(
                        &mut writer,
                        &BackendMessage::ErrorResponse {
                            severity: SEVERITY_ERROR,
                            code: ErrorCode::InvalidDbHandle.as_u16(),
                            message: format!("Invalid DB handle: {}", db_id),
                            detail: None,
                            hint: None,
                        },
                    )
                    .await?;
                }
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::Query {
                query_string,
                db_id,
            } => {
                match handle_query(&conn_state, &query_string, db_id).await {
                    Ok((result, find_vars)) => {
                        // Send RowDescription
                        let columns: Vec<ColumnDescription> = find_vars
                            .iter()
                            .map(|name| ColumnDescription {
                                name: name.clone(),
                                data_type: TAG_UNKNOWN,
                            })
                            .collect();
                        write_backend_message(
                            &mut writer,
                            &BackendMessage::RowDescription { columns },
                        )
                        .await?;

                        // Send DataRows
                        let row_count = result.len() as u64;
                        for row in result {
                            write_backend_message(
                                &mut writer,
                                &BackendMessage::DataRow { values: row },
                            )
                            .await?;
                        }

                        // Send CommandComplete
                        write_backend_message(
                            &mut writer,
                            &BackendMessage::CommandComplete {
                                tag: "SELECT".to_string(),
                                row_count,
                            },
                        )
                        .await?;
                    }
                    Err(e) => {
                        write_error_response(&mut writer, SEVERITY_ERROR, e).await?;
                    }
                }
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::Execute {
                ops,
                await_indexing,
            } => {
                match handle_execute(&node, ops, await_indexing).await {
                    Ok(tx_result_msg) => {
                        write_backend_message(&mut writer, &tx_result_msg).await?;
                    }
                    Err(e) => {
                        write_error_response(&mut writer, SEVERITY_ERROR, e).await?;
                    }
                }
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::Subscribe { .. } => {
                // Subscription is deferred (triplox-b00)
                write_backend_message(
                    &mut writer,
                    &BackendMessage::ErrorResponse {
                        severity: SEVERITY_ERROR,
                        code: ErrorCode::SubscriptionError.as_u16(),
                        message: "Subscriptions are not yet implemented".to_string(),
                        detail: None,
                        hint: None,
                    },
                )
                .await?;
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::Unsubscribe => {
                // Not in subscribed state — ignore or error
                write_backend_message(
                    &mut writer,
                    &BackendMessage::ErrorResponse {
                        severity: SEVERITY_ERROR,
                        code: ErrorCode::SubscriptionError.as_u16(),
                        message: "No active subscription to cancel".to_string(),
                        detail: None,
                        hint: None,
                    },
                )
                .await?;
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::BasisForTx { tx_id, system_time } => {
                match handle_basis_for_tx(&node, tx_id, system_time).await {
                    Ok(basis_msg) => {
                        write_backend_message(&mut writer, &basis_msg).await?;
                    }
                    Err(e) => {
                        write_error_response(&mut writer, SEVERITY_ERROR, e).await?;
                    }
                }
                ready_for_query_flush(&mut writer).await?;
            }

            FrontendMessage::Startup { .. } => {
                write_backend_message(
                    &mut writer,
                    &BackendMessage::ErrorResponse {
                        severity: SEVERITY_FATAL,
                        code: ErrorCode::InvalidStartup.as_u16(),
                        message: "Unexpected Startup message on established connection".to_string(),
                        detail: None,
                        hint: None,
                    },
                )
                .await?;
                writer.flush().await?;
                cleanup_connection(&conn_state, &db_cache).await;
                return Ok(());
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handler helpers
// ---------------------------------------------------------------------------

async fn handle_open_db<L: TxLog + 'static>(
    node: &Arc<Node<L>>,
    db_cache: &DbCache,
    conn_state: &mut ConnectionState,
    basis_tx_id: Option<i64>,
    basis_system_time: Option<i64>,
    basis_seq_num: Option<u64>,
) -> Result<(u32, i64)> {
    let (arc_db, tx_id) = match (basis_tx_id, basis_system_time, basis_seq_num) {
        (None, None, None) => {
            // Latest snapshot — not cacheable because each call may see a different
            // point-in-time. We bypass the cache entirely and use a unique negative
            // sentinel as the tx_id for ConnectionState bookkeeping. release() is a
            // no-op for tx_ids not present in the cache.
            let tx_id = -(conn_state.next_db_id as i64); // unique negative sentinel
            let db = node.db().await?;
            let arc_db = Arc::new(db);
            (arc_db, tx_id)
        }
        (Some(tid), Some(st), Some(seq)) => {
            let system_time = crate::protocol::micros_to_datetime(st)?;
            let tx_key = crate::transaction::TxKey {
                tx_id: tid,
                system_time,
            };
            let basis = Basis { tx_key, seq_num: seq };
            let tx_id = basis.tx_key.tx_id;
            let node = node.clone();
            let arc_db = db_cache.acquire(tx_id, || async move {
                node.db_with_basis(basis).await
            }).await?;
            (arc_db, tx_id)
        }
        _ => bail!("OpenDb requires all three basis fields or none"),
    };

    let db_id = conn_state.allocate_handle(tx_id, arc_db)?;
    Ok((db_id, tx_id))
}

async fn handle_query(
    conn_state: &ConnectionState,
    query_string: &str,
    db_id: u32,
) -> Result<(QueryResult, Vec<String>)> {
    let db = conn_state
        .get_db(db_id)
        .ok_or_else(|| anyhow::anyhow!("Invalid DB handle: {}", db_id))?;

    let parsed = parse_query(query_string)?;

    // Extract find variable names for RowDescription
    let find_vars: Vec<String> = match &parsed.find {
        crate::datalog::FindSpec::FindRel(elements) => elements
            .iter()
            .map(|e| match e {
                crate::datalog::FindElement::Variable(v) => v.clone(),
                crate::datalog::FindElement::PullExpr(_) => "?_pull".to_string(),
                crate::datalog::FindElement::Aggregate(f, v) => format!("({} {})", f, v),
            })
            .collect(),
    };

    let result = db.query(&parsed).await?;
    Ok((result, find_vars))
}

async fn handle_basis_for_tx<L: TxLog + 'static>(
    node: &Arc<Node<L>>,
    tx_id: i64,
    system_time: i64,
) -> Result<BackendMessage> {
    let tx_key = crate::transaction::TxKey {
        tx_id,
        system_time: crate::protocol::micros_to_datetime(system_time)?,
    };
    let basis = node.basis_for_tx(tx_key).await?;
    Ok(BackendMessage::BasisResult {
        tx_id: basis.tx_key.tx_id,
        system_time: basis.tx_key.system_time.timestamp_micros(),
        seq_num: basis.seq_num,
    })
}

async fn handle_execute<L: TxLog + 'static>(
    node: &Arc<Node<L>>,
    ops: Vec<crate::ops::TxOp>,
    await_indexing: bool,
) -> Result<BackendMessage> {
    if await_indexing {
        let result = node.execute_tx(ops).await?;
        match result {
            TransactionResult::TxCommited(basis) => Ok(BackendMessage::TxResult {
                status: 0,
                tx_id: basis.tx_key.tx_id,
                system_time: basis.tx_key.system_time.timestamp_micros(),
                seq_num: basis.seq_num,
                error_message: None,
            }),
            TransactionResult::TxAborted(tx_key, err) => Ok(BackendMessage::TxResult {
                status: 1,
                tx_id: tx_key.tx_id,
                system_time: tx_key.system_time.timestamp_micros(),
                seq_num: 0,
                error_message: Some(err.to_string()),
            }),
        }
    } else {
        let tx_key = node.submit_tx(ops).await?;
        Ok(BackendMessage::TxKey {
            tx_id: tx_key.tx_id,
            system_time: tx_key.system_time.timestamp_micros(),
        })
    }
}

/// Send ReadyForQuery(Idle) and flush the writer.
async fn ready_for_query_flush<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
) -> Result<()> {
    write_backend_message(
        writer,
        &BackendMessage::ReadyForQuery {
            status: STATUS_IDLE,
        },
    )
    .await?;
    writer.flush().await?;
    Ok(())
}

async fn cleanup_connection(conn_state: &ConnectionState, db_cache: &DbCache) {
    for tx_id in conn_state.all_tx_ids() {
        db_cache.release(tx_id).await;
    }
}

// TODO(triplox-c36): thread ErrorCode through instead of always using InternalError
async fn write_error_response<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    severity: u8,
    err: anyhow::Error,
) -> Result<()> {
    let code = ErrorCode::InternalError.as_u16();
    let message = err.to_string();
    write_backend_message(
        writer,
        &BackendMessage::ErrorResponse {
            severity,
            code,
            message,
            detail: None,
            hint: None,
        },
    )
    .await?;
    Ok(())
}
