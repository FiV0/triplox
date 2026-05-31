//! HTTP/2 server for Triplox.
//!
//! Replaces the custom TCP wire protocol with HTTP/2 transport while keeping
//! the same binary payload encoding. Each operation maps to an HTTP endpoint:
//!
//! - `POST /db/open`           — Open a DB read basis
//! - `POST /db/query`          — Execute a Datalog query
//! - `POST /tx/submit`         — Submit a fire-and-forget transaction
//! - `POST /tx/execute`        — Execute a transaction and wait for indexing

use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Error, Result};
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, State};
use axum::http::header::{CONNECTION, UPGRADE};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures::future::BoxFuture;
use hyper::body::Incoming;
use hyper::service::Service;
use hyper::Request;
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;

use crate::error::TriploxError;
use crate::log::TxLog;
use crate::node::{Database, IntoQuery, Node, QueryNode, SubmitNode, TransactionResult};
use triplox_client::msgpack_codec::{
    decode_execute_request, decode_open_db_request, decode_query_request,
    encode_db_opened_response, encode_error_body, encode_query_response, encode_tx_key_response,
    encode_tx_result_response, DbOpenedResponse, ErrorResponseBody, QueryResponse, TxKeyResponse,
    TxResultResponse,
};
use triplox_client::protocol::{
    ColumnDescription, ErrorCode, DEFAULT_MAX_MESSAGE_SIZE, SEVERITY_ERROR, TAG_UNKNOWN,
};
use triplox_client::transaction::{TxBasis, TxKey};

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
        _ => ApiError::internal(ErrorCode::InternalError, message),
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
    body: Bytes,
) -> Result<Response, ApiError> {
    let open_request = decode_open_db_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::InvalidStartup,
            format!("Invalid open_db request: {}", e),
        )
    })?;

    let basis = match (
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
            *db.tx_basis()
        }
        (Some(tid), Some(system_time), Some(tx_eid)) => {
            let basis = TxBasis {
                tx_key: TxKey {
                    tx_id: tid,
                    system_time,
                },
                tx_eid,
            };
            state.node.db_as_of(basis).await.map_err(open_db_error)?;
            basis
        }
        _ => {
            return Err(ApiError::bad_request(
                ErrorCode::InvalidStartup,
                "OpenDb requires tx_id, system_time, and tx_eid, or none of them",
            ))
        }
    };

    let body = encode_db_opened_response(&DbOpenedResponse {
        tx_id: basis.tx_key.tx_id,
        system_time: basis.tx_key.system_time,
        tx_eid: basis.tx_eid,
    })
    .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;
    Ok(ok_response(body))
}

async fn query<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let query_request = decode_query_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::ParseError,
            format!("Invalid query request: {}", e),
        )
    })?;
    let db = state
        .node
        .db_as_of(query_request.db)
        .await
        .map_err(open_db_error)?;

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
        .route("/db/query", post(query::<L>))
        .route("/tx/submit", post(submit_tx::<L>))
        .route("/tx/execute", post(execute_tx::<L>))
        .layer(DefaultBodyLimit::max(DEFAULT_MAX_MESSAGE_SIZE as usize))
        .with_state(state)
}

pub struct Server<L: TxLog> {
    node: Arc<Node<L>>,
}

impl<L: TxLog + 'static> Server<L> {
    pub fn new(node: Arc<Node<L>>) -> Self {
        Server { node }
    }

    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    pub async fn listen_on(&self, listener: TcpListener, token: CancellationToken) -> Result<()> {
        let app_state = Arc::new(Server {
            node: self.node.clone(),
        });

        accept_loop(
            listener,
            token,
            "HTTP",
            move |stream, _peer, conn_id, conn_token, join_set| {
                let router = build_router(app_state.clone());
                join_set.spawn(async move {
                    serve_connection(stream, router, conn_id, conn_token, "HTTP").await;
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

                    let app_state = Arc::new(Server { node: node.clone() });
                    let router = build_router(app_state);

                    serve_connection(stream, router, conn_id, conn_token, "Dev HTTP").await;

                    let node = Arc::try_unwrap(node).unwrap_or_else(|_| {
                        panic!("dev node Arc should have refcount 1 after connection close")
                    });
                    if let Err(err) = node.close().await {
                        warn!(
                            "Dev HTTP connection {} failed to close node: {:#}",
                            conn_id, err
                        );
                    }
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
    let service = h2c_upgrade_service(router, conn_id, name);
    let builder = Builder::new(TokioExecutor::new());
    let conn = builder.serve_connection_with_upgrades(io, service);
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

fn h2c_upgrade_service(router: Router, conn_id: u64, name: &'static str) -> H2cUpgradeService {
    H2cUpgradeService {
        router,
        conn_id,
        name,
    }
}

#[derive(Clone)]
struct H2cUpgradeService {
    router: Router,
    conn_id: u64,
    name: &'static str,
}

impl Service<Request<Incoming>> for H2cUpgradeService {
    type Response = Response;
    type Error = Infallible;
    type Future = BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn call(&self, req: Request<Incoming>) -> Self::Future {
        let router = self.router.clone();
        let conn_id = self.conn_id;
        let name = self.name;
        Box::pin(async move {
            if is_h2c_upgrade_request(req.headers()) {
                let on_upgrade = hyper::upgrade::on(req);
                tokio::spawn(async move {
                    match on_upgrade.await {
                        Ok(upgraded) => {
                            let service = TowerToHyperService::new(router);
                            let builder = Builder::new(TokioExecutor::new()).http2_only();
                            if let Err(err) = builder.serve_connection(upgraded, service).await {
                                warn!(
                                    "{} h2c upgraded connection {} error: {}",
                                    name, conn_id, err
                                );
                            }
                        }
                        Err(err) => warn!(
                            "{} h2c upgrade for connection {} failed: {}",
                            name, conn_id, err
                        ),
                    }
                });

                let response = Response::builder()
                    .status(StatusCode::SWITCHING_PROTOCOLS)
                    .header(CONNECTION, "Upgrade")
                    .header(UPGRADE, "h2c")
                    .body(Body::empty())
                    .expect("h2c upgrade response is valid");
                Ok(response)
            } else {
                TowerToHyperService::new(router).call(req).await
            }
        })
    }
}

fn is_h2c_upgrade_request(headers: &HeaderMap) -> bool {
    header_contains_token(headers, CONNECTION, "upgrade")
        && header_contains_token(headers, UPGRADE, "h2c")
        && headers.contains_key("http2-settings")
}

fn header_contains_token(
    headers: &HeaderMap,
    name: axum::http::header::HeaderName,
    token: &str,
) -> bool {
    headers
        .get_all(name)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|value| value.trim().eq_ignore_ascii_case(token))
}
