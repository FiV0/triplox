//! HTTP/2 server for Triplox.
//!
//! Replaces the custom TCP wire protocol with HTTP/2 transport while keeping
//! the same binary payload encoding. Each operation maps to an HTTP endpoint:
//!
//! - `POST /db/open`           — Open a DB snapshot
//! - `DELETE /db/{db_id}`      — Release a DB snapshot
//! - `POST /db/{db_id}/query`  — Execute a Datalog query
//! - `POST /tx/submit`         — Submit a fire-and-forget transaction
//! - `POST /tx/execute`        — Execute a transaction and wait for indexing

use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use axum::body::Bytes;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::Router;
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use tower::Service;

use crate::log::TxLog;
use crate::node::{Database, IntoQuery, Node, QueryNode, SubmitNode, TransactionResult, DB};
use crate::protocol::*;
use crate::server::DbCache;

// ---------------------------------------------------------------------------
// Handle Store
// ---------------------------------------------------------------------------

struct HandleEntry {
    conn_id: u64,
    tx_id: i64,
    db: Arc<DB>,
    last_used: Instant,
}

struct HandleStore {
    handles: RwLock<HashMap<u32, HandleEntry>>,
    next_id: AtomicU32,
}

impl HandleStore {
    fn new() -> Self {
        HandleStore {
            handles: RwLock::new(HashMap::new()),
            next_id: AtomicU32::new(1),
        }
    }

    fn alloc_id(&self) -> u32 {
        self.next_id.fetch_add(1, Ordering::Relaxed)
    }

    async fn insert(&self, db_id: u32, conn_id: u64, tx_id: i64, db: Arc<DB>) {
        let mut handles = self.handles.write().await;
        handles.insert(
            db_id,
            HandleEntry {
                conn_id,
                tx_id,
                db,
                last_used: Instant::now(),
            },
        );
    }

    async fn get_db(&self, db_id: u32) -> Option<Arc<DB>> {
        let mut handles = self.handles.write().await;
        if let Some(entry) = handles.get_mut(&db_id) {
            entry.last_used = Instant::now();
            Some(entry.db.clone())
        } else {
            None
        }
    }

    async fn remove(&self, db_id: u32) -> Option<i64> {
        let mut handles = self.handles.write().await;
        handles.remove(&db_id).map(|e| e.tx_id)
    }

    /// Remove all handles belonging to a connection. Returns their tx_ids.
    async fn remove_by_conn(&self, conn_id: u64) -> Vec<i64> {
        let mut handles = self.handles.write().await;
        let to_remove: Vec<u32> = handles
            .iter()
            .filter(|(_, e)| e.conn_id == conn_id)
            .map(|(id, _)| *id)
            .collect();
        let mut tx_ids = Vec::new();
        for id in to_remove {
            if let Some(entry) = handles.remove(&id) {
                tx_ids.push(entry.tx_id);
            }
        }
        tx_ids
    }

    /// Remove handles idle for longer than `ttl`. Returns their tx_ids.
    async fn reap_expired(&self, ttl: Duration) -> Vec<i64> {
        let mut handles = self.handles.write().await;
        let now = Instant::now();
        let to_remove: Vec<u32> = handles
            .iter()
            .filter(|(_, e)| now.duration_since(e.last_used) > ttl)
            .map(|(id, _)| *id)
            .collect();
        let mut tx_ids = Vec::new();
        for id in to_remove {
            if let Some(entry) = handles.remove(&id) {
                tx_ids.push(entry.tx_id);
            }
        }
        tx_ids
    }
}

// ---------------------------------------------------------------------------
// Connection ID middleware
// ---------------------------------------------------------------------------

/// Extension inserted into each request to identify its HTTP/2 connection.
#[derive(Clone, Copy)]
struct ConnId(u64);

static NEXT_CONN_ID: AtomicU64 = AtomicU64::new(1);

// ---------------------------------------------------------------------------
// App State
// ---------------------------------------------------------------------------

struct AppState<L: TxLog> {
    node: Arc<Node<L>>,
    db_cache: Arc<DbCache>,
    handle_store: Arc<HandleStore>,
}

// ---------------------------------------------------------------------------
// Error response helper
// ---------------------------------------------------------------------------

const CONTENT_TYPE: &str = "application/x-triplox";

fn error_response(status: StatusCode, severity: u8, code: ErrorCode, message: &str) -> Response {
    let body = encode_error_body(severity, code.as_u16(), message, &None, &None);
    (status, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)], body).into_response()
}

fn ok_response(body: Vec<u8>) -> Response {
    (StatusCode::OK, [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)], body).into_response()
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn open_db<L: TxLog + 'static>(
    State(state): State<Arc<AppState<L>>>,
    axum::Extension(conn_id): axum::Extension<ConnId>,
    body: Bytes,
) -> Response {
    let (tx_id, system_time) = match decode_open_db_request(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                SEVERITY_ERROR,
                ErrorCode::InvalidStartup,
                &format!("Invalid open_db request: {}", e),
            );
        }
    };

    let result = match (tx_id, system_time) {
        (None, None) => {
            let db = match state.node.db().await {
                Ok(db) => db,
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        SEVERITY_ERROR,
                        ErrorCode::InternalError,
                        &e.to_string(),
                    );
                }
            };
            let tx_id = db.tx_key().tx_id;
            match state
                .db_cache
                .acquire(tx_id, || async move { Ok(db) })
                .await
            {
                Ok(arc_db) => (arc_db, tx_id),
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        SEVERITY_ERROR,
                        ErrorCode::TooManyOpenDbs,
                        &e.to_string(),
                    );
                }
            }
        }
        (Some(tid), Some(st)) => {
            let system_time_dt = match micros_to_datetime(st) {
                Ok(dt) => dt,
                Err(e) => {
                    return error_response(
                        StatusCode::BAD_REQUEST,
                        SEVERITY_ERROR,
                        ErrorCode::InvalidDbHandle,
                        &e.to_string(),
                    );
                }
            };
            let tx_key = crate::transaction::TxKey {
                tx_id: tid,
                system_time: system_time_dt,
            };
            let node = state.node.clone();
            match state
                .db_cache
                .acquire(tid, || async move { node.db_as_of(tx_key).await })
                .await
            {
                Ok(arc_db) => (arc_db, tid),
                Err(e) => {
                    return error_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        SEVERITY_ERROR,
                        ErrorCode::InternalError,
                        &e.to_string(),
                    );
                }
            }
        }
        _ => {
            return error_response(
                StatusCode::BAD_REQUEST,
                SEVERITY_ERROR,
                ErrorCode::InvalidStartup,
                "OpenDb requires both tx_id and system_time, or neither",
            );
        }
    };

    let (arc_db, tx_id) = result;
    let db_id = state.handle_store.alloc_id();
    state
        .handle_store
        .insert(db_id, conn_id.0, tx_id, arc_db)
        .await;

    ok_response(encode_db_opened_response(db_id, tx_id))
}

async fn close_db<L: TxLog + 'static>(
    State(state): State<Arc<AppState<L>>>,
    Path(db_id): Path<u32>,
) -> Response {
    match state.handle_store.remove(db_id).await {
        Some(tx_id) => {
            state.db_cache.release(tx_id).await;
            ok_response(encode_db_closed_response(db_id))
        }
        None => error_response(
            StatusCode::NOT_FOUND,
            SEVERITY_ERROR,
            ErrorCode::InvalidDbHandle,
            &format!("Invalid DB handle: {}", db_id),
        ),
    }
}

async fn query<L: TxLog + 'static>(
    State(state): State<Arc<AppState<L>>>,
    Path(db_id): Path<u32>,
    body: Bytes,
) -> Response {
    let db = match state.handle_store.get_db(db_id).await {
        Some(db) => db,
        None => {
            return error_response(
                StatusCode::NOT_FOUND,
                SEVERITY_ERROR,
                ErrorCode::InvalidDbHandle,
                &format!("Invalid DB handle: {}", db_id),
            );
        }
    };

    let (query_string, args) = match decode_query_request(&body) {
        Ok(v) => v,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                SEVERITY_ERROR,
                ErrorCode::ParseError,
                &format!("Invalid query request: {}", e),
            );
        }
    };

    let parsed = match query_string.into_query() {
        Ok(p) => p,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                SEVERITY_ERROR,
                ErrorCode::ParseError,
                &e.to_string(),
            );
        }
    };

    // Extract find variable names for RowDescription
    let find_vars: Vec<String> = match &parsed.find_spec {
        edn::query::FindSpec::FindRel(elements) => elements.iter().map(|e| e.to_string()).collect(),
        _ => vec![],
    };

    let result = match db.query_with_args(&parsed, &args).await {
        Ok(r) => r,
        Err(e) => {
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                SEVERITY_ERROR,
                ErrorCode::QueryError,
                &e.to_string(),
            );
        }
    };

    let columns: Vec<ColumnDescription> = find_vars
        .iter()
        .map(|name| ColumnDescription {
            name: name.clone(),
            data_type: TAG_UNKNOWN,
        })
        .collect();

    ok_response(encode_query_response(&columns, &result))
}

async fn submit_tx<L: TxLog + 'static>(
    State(state): State<Arc<AppState<L>>>,
    body: Bytes,
) -> Response {
    let ops = match decode_execute_request(&body) {
        Ok(ops) => ops,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                SEVERITY_ERROR,
                ErrorCode::TxError,
                &format!("Invalid execute request: {}", e),
            );
        }
    };

    match state.node.submit_tx(ops).await {
        Ok(tx_key) => ok_response(encode_tx_key_response(
            tx_key.tx_id,
            tx_key.system_time.timestamp_micros(),
        )),
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            SEVERITY_ERROR,
            ErrorCode::TxError,
            &e.to_string(),
        ),
    }
}

async fn execute_tx<L: TxLog + 'static>(
    State(state): State<Arc<AppState<L>>>,
    body: Bytes,
) -> Response {
    let ops = match decode_execute_request(&body) {
        Ok(ops) => ops,
        Err(e) => {
            return error_response(
                StatusCode::BAD_REQUEST,
                SEVERITY_ERROR,
                ErrorCode::TxError,
                &format!("Invalid execute request: {}", e),
            );
        }
    };

    match state.node.execute_tx(ops).await {
        Ok(result) => match result {
            TransactionResult::TxCommited(tx_key) => ok_response(encode_tx_result_response(
                0,
                tx_key.tx_id,
                tx_key.system_time.timestamp_micros(),
                &None,
            )),
            TransactionResult::TxAborted(tx_key, err) => ok_response(encode_tx_result_response(
                1,
                tx_key.tx_id,
                tx_key.system_time.timestamp_micros(),
                &Some(err.to_string()),
            )),
        },
        Err(e) => error_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            SEVERITY_ERROR,
            ErrorCode::TxError,
            &e.to_string(),
        ),
    }
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

fn build_router<L: TxLog + 'static>(state: Arc<AppState<L>>) -> Router {
    Router::new()
        .route("/db/open", post(open_db::<L>))
        .route("/db/{db_id}", delete(close_db::<L>))
        .route("/db/{db_id}/query", post(query::<L>))
        .route("/tx/submit", post(submit_tx::<L>))
        .route("/tx/execute", post(execute_tx::<L>))
        .with_state(state)
}

pub struct HttpServer<L: TxLog> {
    node: Arc<Node<L>>,
    db_cache: Arc<DbCache>,
    handle_store: Arc<HandleStore>,
}

impl<L: TxLog + 'static> HttpServer<L> {
    pub fn new(node: Arc<Node<L>>, max_open_dbs: usize) -> Self {
        HttpServer {
            node,
            db_cache: Arc::new(DbCache::new(max_open_dbs)),
            handle_store: Arc::new(HandleStore::new()),
        }
    }

    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    pub async fn listen_on(&self, listener: TcpListener, token: CancellationToken) -> Result<()> {
        info!("Triplox HTTP server listening on {}", listener.local_addr()?);

        let handle_store = self.handle_store.clone();
        let db_cache = self.db_cache.clone();

        // TTL reaper task
        let reaper_handle_store = self.handle_store.clone();
        let reaper_db_cache = self.db_cache.clone();
        let reaper_token = token.clone();
        tokio::spawn(async move {
            let ttl = Duration::from_secs(300); // 5 minutes
            let mut interval = tokio::time::interval(Duration::from_secs(30));
            loop {
                tokio::select! {
                    _ = reaper_token.cancelled() => break,
                    _ = interval.tick() => {
                        let expired = reaper_handle_store.reap_expired(ttl).await;
                        for tx_id in expired {
                            reaper_db_cache.release(tx_id).await;
                        }
                    }
                }
            }
        });

        // Accept loop with per-connection tracking
        let app_state = Arc::new(AppState {
            node: self.node.clone(),
            db_cache: self.db_cache.clone(),
            handle_store: self.handle_store.clone(),
        });

        // Use hyper directly for per-connection lifecycle control

        let mut join_set = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("Shutdown signal received, stopping HTTP server");
                    break;
                }
                result = listener.accept() => {
                    let (stream, peer_addr) = result?;
                    stream.set_nodelay(true)?;
                    let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
                    info!("New HTTP connection from {} (conn_id={})", peer_addr, conn_id);

                    let handle_store = handle_store.clone();
                    let db_cache = db_cache.clone();

                    // Build a per-connection router with ConnId extension
                    let router = build_router(app_state.clone())
                        .layer(axum::Extension(ConnId(conn_id)));

                    let conn_token = token.child_token();
                    join_set.spawn(async move {
                        let io = TokioIo::new(stream);
                        let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                            let mut router = router.clone();
                            async move {
                                router.call(req).await
                            }
                        });

                        let builder = Builder::new(TokioExecutor::new());
                        let conn = builder.serve_connection(io, service);
                        tokio::pin!(conn);

                        tokio::select! {
                            result = &mut conn => {
                                if let Err(e) = result {
                                    warn!("HTTP connection {} error: {}", conn_id, e);
                                }
                            }
                            _ = conn_token.cancelled() => {
                                info!("Shutdown: closing HTTP connection {}", conn_id);
                                conn.as_mut().graceful_shutdown();
                                let _ = conn.await;
                            }
                        }

                        // Connection-drop cleanup: release all handles for this connection
                        let tx_ids = handle_store.remove_by_conn(conn_id).await;
                        for tx_id in tx_ids {
                            db_cache.release(tx_id).await;
                        }
                        info!("HTTP connection {} closed", conn_id);
                    });
                }
            }
        }

        // Drain remaining connections
        let remaining = join_set.len();
        if remaining > 0 {
            info!("Waiting for {} HTTP connection(s) to drain...", remaining);
            match tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(result) = join_set.join_next().await {
                    if let Err(e) = result {
                        warn!("Connection task error during drain: {}", e);
                    }
                }
            })
            .await
            {
                Ok(()) => info!("All HTTP connections drained"),
                Err(_) => warn!("Drain timeout (30s) exceeded"),
            }
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Dev Server (per-connection in-memory nodes)
// ---------------------------------------------------------------------------

pub struct DevHttpServer {
    max_open_dbs: usize,
}

impl DevHttpServer {
    pub fn new(max_open_dbs: usize) -> Self {
        DevHttpServer { max_open_dbs }
    }

    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    pub async fn listen_on(&self, listener: TcpListener, token: CancellationToken) -> Result<()> {
        info!(
            "Triplox Dev HTTP server listening on {}",
            listener.local_addr()?
        );


        let max_open_dbs = self.max_open_dbs;
        let mut join_set = tokio::task::JoinSet::new();

        loop {
            tokio::select! {
                _ = token.cancelled() => {
                    info!("Shutdown signal received, stopping Dev HTTP server");
                    break;
                }
                result = listener.accept() => {
                    let (stream, peer_addr) = result?;
                    stream.set_nodelay(true)?;
                    let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
                    info!("New dev HTTP connection from {} (conn_id={})", peer_addr, conn_id);

                    let conn_token = token.child_token();
                    join_set.spawn(async move {
                        let node = Arc::new(Node::memory_node().await);
                        let db_cache = Arc::new(DbCache::new(max_open_dbs));
                        let handle_store = Arc::new(HandleStore::new());

                        let app_state = Arc::new(AppState {
                            node: node.clone(),
                            db_cache: db_cache.clone(),
                            handle_store: handle_store.clone(),
                        });

                        let router = build_router(app_state)
                            .layer(axum::Extension(ConnId(conn_id)));

                        let io = TokioIo::new(stream);
                        let service = hyper::service::service_fn(move |req: hyper::Request<hyper::body::Incoming>| {
                            let mut router = router.clone();
                            async move {
                                router.call(req).await
                            }
                        });

                        let builder = Builder::new(TokioExecutor::new());
                        let conn = builder.serve_connection(io, service);
                        tokio::pin!(conn);

                        tokio::select! {
                            result = &mut conn => {
                                if let Err(e) = result {
                                    warn!("Dev HTTP connection {} error: {}", conn_id, e);
                                }
                            }
                            _ = conn_token.cancelled() => {
                                conn.as_mut().graceful_shutdown();
                                let _ = conn.await;
                            }
                        }

                        // Cleanup
                        let tx_ids = handle_store.remove_by_conn(conn_id).await;
                        for tx_id in tx_ids {
                            db_cache.release(tx_id).await;
                        }

                        let node = Arc::try_unwrap(node).unwrap_or_else(|_| {
                            panic!("dev node Arc should have refcount 1 after connection close")
                        });
                        node.close().await;
                        info!("Dev HTTP connection {} closed", conn_id);
                    });
                }
            }
        }

        // Drain
        let remaining = join_set.len();
        if remaining > 0 {
            info!("Waiting for {} dev connection(s) to drain...", remaining);
            match tokio::time::timeout(Duration::from_secs(30), async {
                while let Some(result) = join_set.join_next().await {
                    if let Err(e) = result {
                        warn!("Dev connection task error during drain: {}", e);
                    }
                }
            })
            .await
            {
                Ok(()) => info!("All dev connections drained"),
                Err(_) => warn!("Drain timeout (30s) exceeded"),
            }
        }

        Ok(())
    }
}
