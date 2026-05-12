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
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{bail, Error, Result};
use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use axum::Router;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::error::TriploxError;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;

use crate::log::TxLog;
use crate::node::{Database, IntoQuery, Node, QueryNode, SubmitNode, TransactionResult, DB};
use triplox_client::msgpack_codec::{
    decode_execute_request, decode_open_db_request, decode_query_request,
    encode_db_closed_response, encode_db_opened_response, encode_error_body, encode_query_response,
    encode_tx_key_response, encode_tx_result_response, DbClosedResponse, DbOpenedResponse,
    ErrorResponseBody, QueryResponse, TxKeyResponse, TxResultResponse,
};
use triplox_client::protocol::{
    ColumnDescription, ErrorCode, DEFAULT_MAX_MESSAGE_SIZE, SEVERITY_ERROR, TAG_UNKNOWN,
};
use triplox_client::transaction::{TxBasis, TxKey};

// ---------------------------------------------------------------------------
// DB Cache
// ---------------------------------------------------------------------------

const MAX_OPEN_DBS: usize = 1024;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
struct DbCacheKey {
    tx_id: i64,
    system_time: crate::clock::Instant,
    tx_eid: i64,
}

impl From<TxBasis> for DbCacheKey {
    fn from(basis: TxBasis) -> Self {
        DbCacheKey {
            tx_id: basis.tx_key.tx_id,
            system_time: basis.tx_key.system_time,
            tx_eid: basis.tx_eid,
        }
    }
}

/// A shared, reference-counted cache of DB snapshots.
/// Keyed by the full TxBasis so stale basis values cannot alias a valid snapshot.
/// DBs are read-only and safe to share across connections.
struct DbCacheEntry {
    db: Arc<DB>,
    refcount: usize,
}

pub(crate) struct DbCache {
    entries: RwLock<HashMap<DbCacheKey, DbCacheEntry>>,
}

impl DbCache {
    pub(crate) fn new() -> Self {
        DbCache {
            entries: RwLock::new(HashMap::new()),
        }
    }

    /// Get or create a DB snapshot for the given TxKey.
    /// The `create` future is only evaluated on cache miss.
    async fn acquire<F, Fut>(&self, key: DbCacheKey, create: F) -> Result<Arc<DB>>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DB>>,
    {
        // Fast path: cache hit (uses write lock because we mutate refcount)
        {
            let mut entries = self.entries.write().await;
            if let Some(entry) = entries.get_mut(&key) {
                entry.refcount += 1;
                return Ok(entry.db.clone());
            }
            if entries.len() >= MAX_OPEN_DBS {
                bail!("Too many open DB snapshots (max {})", MAX_OPEN_DBS);
            }
        }

        let db = create().await?;
        let arc_db = Arc::new(db);

        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&key) {
            entry.refcount += 1;
            return Ok(entry.db.clone());
        }
        if entries.len() >= MAX_OPEN_DBS {
            bail!("Too many open DB snapshots (max {})", MAX_OPEN_DBS);
        }
        entries.insert(
            key,
            DbCacheEntry {
                db: arc_db.clone(),
                refcount: 1,
            },
        );
        Ok(arc_db)
    }

    /// Release a reference to a DB snapshot.
    /// Evicts the entry when refcount reaches 0.
    async fn release(&self, key: DbCacheKey) {
        let mut entries = self.entries.write().await;
        if let Some(entry) = entries.get_mut(&key) {
            entry.refcount -= 1;
            if entry.refcount == 0 {
                entries.remove(&key);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Handle Store
// ---------------------------------------------------------------------------

struct HandleEntry {
    conn_id: u64,
    cache_key: DbCacheKey,
    db: Arc<DB>,
    last_used: Instant,
}

struct HandleStore {
    handles: RwLock<HashMap<u32, HandleEntry>>,
    next_id: AtomicU32,
    db_cache: Arc<DbCache>,
}

impl HandleStore {
    fn new(db_cache: Arc<DbCache>) -> Self {
        HandleStore {
            handles: RwLock::new(HashMap::new()),
            next_id: AtomicU32::new(1),
            db_cache,
        }
    }

    /// Acquire a DB snapshot via the cache and allocate a handle for it.
    async fn open<F, Fut>(&self, conn_id: u64, cache_key: DbCacheKey, create: F) -> Result<u32>
    where
        F: FnOnce() -> Fut,
        Fut: std::future::Future<Output = Result<DB>>,
    {
        let db = self.db_cache.acquire(cache_key, create).await?;
        let db_id = self.next_id.fetch_add(1, Ordering::Relaxed);
        let mut handles = self.handles.write().await;
        handles.insert(
            db_id,
            HandleEntry {
                conn_id,
                cache_key,
                db,
                last_used: Instant::now(),
            },
        );
        Ok(db_id)
    }

    // TODO(P1): Validate conn_id in get_db/remove so global db_ids cannot cross connection boundaries.
    async fn get_db(&self, db_id: u32) -> Option<Arc<DB>> {
        let mut handles = self.handles.write().await;
        if let Some(entry) = handles.get_mut(&db_id) {
            entry.last_used = Instant::now();
            Some(entry.db.clone())
        } else {
            None
        }
    }

    /// Remove a handle and release its DbCache refcount. Returns true if the
    /// handle existed.
    async fn remove(&self, db_id: u32) -> bool {
        let cache_key = {
            let mut handles = self.handles.write().await;
            match handles.remove(&db_id) {
                Some(entry) => entry.cache_key,
                None => return false,
            }
        };
        self.db_cache.release(cache_key).await;
        true
    }

    /// Remove all handles belonging to a connection, releasing their DbCache
    /// refcounts.
    async fn remove_by_conn(&self, conn_id: u64) {
        let cache_keys: Vec<DbCacheKey> = {
            let mut handles = self.handles.write().await;
            let to_remove: Vec<u32> = handles
                .iter()
                .filter(|(_, e)| e.conn_id == conn_id)
                .map(|(id, _)| *id)
                .collect();
            to_remove
                .into_iter()
                .filter_map(|id| handles.remove(&id).map(|e| e.cache_key))
                .collect()
        };
        for cache_key in cache_keys {
            self.db_cache.release(cache_key).await;
        }
    }

    /// Remove handles idle for longer than `ttl`, releasing their DbCache
    /// refcounts.
    async fn reap_expired(&self, ttl: Duration) {
        let cache_keys: Vec<DbCacheKey> = {
            let mut handles = self.handles.write().await;
            let now = Instant::now();
            let to_remove: Vec<u32> = handles
                .iter()
                .filter(|(_, e)| now.duration_since(e.last_used) > ttl)
                .map(|(id, _)| *id)
                .collect();
            to_remove
                .into_iter()
                .filter_map(|id| handles.remove(&id).map(|e| e.cache_key))
                .collect()
        };
        for cache_key in cache_keys {
            self.db_cache.release(cache_key).await;
        }
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
// Response helpers and error type
// ---------------------------------------------------------------------------

const CONTENT_TYPE: &str = "application/vnd.triplox+msgpack";

fn ok_response(body: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)],
        body,
    )
        .into_response()
}

struct ApiError {
    status: StatusCode,
    code: ErrorCode,
    message: String,
}

impl ApiError {
    fn new(status: StatusCode, code: ErrorCode, message: impl Into<String>) -> Self {
        ApiError {
            status,
            code,
            message: message.into(),
        }
    }

    fn bad_request(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(StatusCode::BAD_REQUEST, code, message)
    }

    fn not_found(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(StatusCode::NOT_FOUND, code, message)
    }

    fn internal(code: ErrorCode, message: impl Into<String>) -> Self {
        Self::new(StatusCode::INTERNAL_SERVER_ERROR, code, message)
    }
}

fn open_db_error(e: Error) -> ApiError {
    let message = e.to_string();
    match e.downcast_ref::<TriploxError>() {
        Some(TriploxError::TxIndexingTimeout { .. }) => {
            ApiError::new(StatusCode::CONFLICT, ErrorCode::TxNotIndexed, message)
        }
        _ => ApiError::internal(ErrorCode::TooManyOpenDbs, message),
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = encode_error_body(&ErrorResponseBody {
            severity: SEVERITY_ERROR,
            code: self.code.as_u16(),
            message: self.message,
            detail: None,
            hint: None,
        })
        .expect("ErrorResponseBody encoding is infallible for fixed inputs");
        (
            self.status,
            [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)],
            body,
        )
            .into_response()
    }
}

// ---------------------------------------------------------------------------
// Route handlers
// ---------------------------------------------------------------------------

async fn open_db<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    axum::Extension(conn_id): axum::Extension<ConnId>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let open_request = decode_open_db_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::InvalidStartup,
            format!("Invalid open_db request: {}", e),
        )
    })?;

    let (db_id, tx_id) = match (
        open_request.tx_id,
        open_request.system_time,
        open_request.tx_eid,
    ) {
        (None, None, None) => {
            let db = state
                .node
                .db()
                .await
                .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
            let tx_id = db.tx_key().tx_id;
            let cache_key = (*db.tx_basis()).into();
            let db_id = state
                .handle_store
                .open(conn_id.0, cache_key, || async move { Ok(db) })
                .await
                .map_err(|e| ApiError::internal(ErrorCode::TooManyOpenDbs, e.to_string()))?;
            (db_id, tx_id)
        }
        (Some(tid), Some(system_time), Some(tx_eid)) => {
            let basis = TxBasis {
                tx_key: TxKey {
                    tx_id: tid,
                    system_time,
                },
                tx_eid,
            };
            let node = state.node.clone();
            let db_id = state
                .handle_store
                .open(conn_id.0, basis.into(), || async move {
                    node.db_as_of(basis).await
                })
                .await
                .map_err(open_db_error)?;
            (db_id, tid)
        }
        _ => {
            return Err(ApiError::bad_request(
                ErrorCode::InvalidStartup,
                "OpenDb requires tx_id, system_time, and tx_eid, or none of them",
            ))
        }
    };

    let body = encode_db_opened_response(&DbOpenedResponse { db_id, tx_id })
        .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
    Ok(ok_response(body))
}

async fn close_db<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    Path(db_id): Path<u32>,
) -> Result<Response, ApiError> {
    if state.handle_store.remove(db_id).await {
        let body = encode_db_closed_response(&DbClosedResponse { db_id })
            .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
        Ok(ok_response(body))
    } else {
        Err(ApiError::not_found(
            ErrorCode::InvalidDbHandle,
            format!("Invalid DB handle: {}", db_id),
        ))
    }
}

async fn query<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    Path(db_id): Path<u32>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let db = state.handle_store.get_db(db_id).await.ok_or_else(|| {
        ApiError::not_found(
            ErrorCode::InvalidDbHandle,
            format!("Invalid DB handle: {}", db_id),
        )
    })?;

    let query_request = decode_query_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::ParseError,
            format!("Invalid query request: {}", e),
        )
    })?;

    let parsed = query_request
        .query
        .into_query()
        .map_err(|e| ApiError::bad_request(ErrorCode::ParseError, e.to_string()))?;

    // Extract find variable names for RowDescription
    let find_vars: Vec<String> = match &parsed.find_spec {
        edn::query::FindSpec::FindRel(elements) => elements.iter().map(|e| e.to_string()).collect(),
        _ => vec![],
    };

    let result = db
        .query_with_args(&parsed, &query_request.args)
        .await
        .map_err(|e| ApiError::internal(ErrorCode::QueryError, e.to_string()))?;

    let columns: Vec<ColumnDescription> = find_vars
        .into_iter()
        .map(|name| ColumnDescription {
            name,
            data_type: TAG_UNKNOWN,
            members: None,
        })
        .collect();

    let body = encode_query_response(&QueryResponse {
        columns,
        rows: result,
    })
    .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
    Ok(ok_response(body))
}

async fn submit_tx<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request = decode_execute_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::TxError,
            format!("Invalid execute request: {}", e),
        )
    })?;

    let tx_key = state
        .node
        .submit_tx(request.ops)
        .await
        .map_err(|e| ApiError::internal(ErrorCode::TxError, e.to_string()))?;

    let body = encode_tx_key_response(&TxKeyResponse {
        tx_id: tx_key.tx_id,
        system_time: tx_key.system_time,
    })
    .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
    Ok(ok_response(body))
}

async fn execute_tx<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request = decode_execute_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::TxError,
            format!("Invalid execute request: {}", e),
        )
    })?;

    let result = state
        .node
        .execute_tx(request.ops)
        .await
        .map_err(|e| ApiError::internal(ErrorCode::TxError, e.to_string()))?;

    let resp = match result {
        TransactionResult::TxCommited(basis) => TxResultResponse {
            status: 0,
            tx_id: basis.tx_key.tx_id,
            system_time: basis.tx_key.system_time,
            tx_eid: basis.tx_eid,
            error_message: None,
        },
        TransactionResult::TxAborted(basis, err) => TxResultResponse {
            status: 1,
            tx_id: basis.tx_key.tx_id,
            system_time: basis.tx_key.system_time,
            tx_eid: basis.tx_eid,
            error_message: Some(err.to_string()),
        },
    };
    let body = encode_tx_result_response(&resp)
        .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
    Ok(ok_response(body))
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

fn build_router<L: TxLog + 'static>(state: Arc<Server<L>>) -> Router {
    Router::new()
        .route("/db/open", post(open_db::<L>))
        .route("/db/{db_id}", delete(close_db::<L>))
        .route("/db/{db_id}/query", post(query::<L>))
        .route("/tx/submit", post(submit_tx::<L>))
        .route("/tx/execute", post(execute_tx::<L>))
        .layer(DefaultBodyLimit::max(DEFAULT_MAX_MESSAGE_SIZE as usize))
        .with_state(state)
}

pub struct Server<L: TxLog> {
    node: Arc<Node<L>>,
    handle_store: Arc<HandleStore>,
}

impl<L: TxLog + 'static> Server<L> {
    pub fn new(node: Arc<Node<L>>) -> Self {
        let db_cache = Arc::new(DbCache::new());
        let handle_store = Arc::new(HandleStore::new(db_cache));
        Server { node, handle_store }
    }

    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    pub async fn listen_on(&self, listener: TcpListener, token: CancellationToken) -> Result<()> {
        let reaper_handle_store = self.handle_store.clone();
        let reaper_token = token.clone();
        tokio::spawn(async move {
            let ttl = Duration::from_secs(86400); // 24 hours
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                tokio::select! {
                    _ = reaper_token.cancelled() => break,
                    _ = interval.tick() => reaper_handle_store.reap_expired(ttl).await,
                }
            }
        });

        let app_state = Arc::new(Server {
            node: self.node.clone(),
            handle_store: self.handle_store.clone(),
        });
        let handle_store = self.handle_store.clone();

        accept_loop(
            listener,
            token,
            "HTTP",
            move |stream, _peer, conn_id, conn_token, join_set| {
                let router =
                    build_router(app_state.clone()).layer(axum::Extension(ConnId(conn_id)));
                let handle_store = handle_store.clone();
                join_set.spawn(async move {
                    serve_connection(stream, router, conn_id, conn_token, "HTTP").await;
                    handle_store.remove_by_conn(conn_id).await;
                    info!("HTTP connection {} closed", conn_id);
                });
            },
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Dev Server (per-connection in-memory nodes)
// ---------------------------------------------------------------------------

pub struct DevServer;

impl Default for DevServer {
    fn default() -> Self {
        Self::new()
    }
}

impl DevServer {
    pub fn new() -> Self {
        DevServer
    }

    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    pub async fn listen_on(&self, listener: TcpListener, token: CancellationToken) -> Result<()> {
        accept_loop(
            listener,
            token,
            "Dev HTTP",
            move |stream, _peer, conn_id, conn_token, join_set| {
                join_set.spawn(async move {
                    let node = Arc::new(Node::memory_node().await);
                    let db_cache = Arc::new(DbCache::new());
                    let handle_store = Arc::new(HandleStore::new(db_cache));

                    let app_state = Arc::new(Server {
                        node: node.clone(),
                        handle_store: handle_store.clone(),
                    });
                    let router = build_router(app_state).layer(axum::Extension(ConnId(conn_id)));

                    serve_connection(stream, router, conn_id, conn_token, "Dev HTTP").await;

                    handle_store.remove_by_conn(conn_id).await;
                    let node = Arc::try_unwrap(node).unwrap_or_else(|_| {
                        panic!("dev node Arc should have refcount 1 after connection close")
                    });
                    node.close().await;
                    info!("Dev HTTP connection {} closed", conn_id);
                });
            },
        )
        .await
    }
}

// ---------------------------------------------------------------------------
// Accept loop / per-connection serving (shared)
// ---------------------------------------------------------------------------

async fn accept_loop<F>(
    listener: TcpListener,
    token: CancellationToken,
    name: &'static str,
    mut spawn_handler: F,
) -> Result<()>
where
    F: FnMut(TcpStream, SocketAddr, u64, CancellationToken, &mut JoinSet<()>),
{
    info!(
        "Triplox {} server listening on {}",
        name,
        listener.local_addr()?
    );
    let mut join_set = JoinSet::new();

    loop {
        tokio::select! {
            _ = token.cancelled() => {
                info!("Shutdown signal received, stopping {} server", name);
                break;
            }
            result = listener.accept() => {
                let (stream, peer_addr) = result?;
                stream.set_nodelay(true)?;
                let conn_id = NEXT_CONN_ID.fetch_add(1, Ordering::Relaxed);
                info!("New {} connection from {} (conn_id={})", name, peer_addr, conn_id);
                spawn_handler(stream, peer_addr, conn_id, token.child_token(), &mut join_set);
            }
        }
    }

    drain_connections(&mut join_set, name).await;
    Ok(())
}

async fn drain_connections(join_set: &mut JoinSet<()>, name: &str) {
    let remaining = join_set.len();
    if remaining == 0 {
        return;
    }
    info!(
        "Waiting for {} {} connection(s) to drain...",
        remaining, name
    );
    match tokio::time::timeout(Duration::from_secs(30), async {
        while let Some(result) = join_set.join_next().await {
            if let Err(e) = result {
                warn!("{} connection task error during drain: {}", name, e);
            }
        }
    })
    .await
    {
        Ok(()) => info!("All {} connections drained", name),
        Err(_) => warn!("Drain timeout (30s) exceeded"),
    }
}

async fn serve_connection(
    stream: TcpStream,
    router: Router,
    conn_id: u64,
    conn_token: CancellationToken,
    name: &'static str,
) {
    let io = TokioIo::new(stream);
    let service = TowerToHyperService::new(router);
    let builder = Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection(io, service);
    tokio::pin!(conn);

    tokio::select! {
        result = &mut conn => {
            if let Err(e) = result {
                warn!("{} connection {} error: {}", name, conn_id, e);
            }
        }
        _ = conn_token.cancelled() => {
            info!("Shutdown: closing {} connection {}", name, conn_id);
            conn.as_mut().graceful_shutdown();
            let _ = conn.await;
        }
    }
}
