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
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::post;
use axum::Router;
use futures::StreamExt;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder;
use hyper_util::service::TowerToHyperService;

use crate::error::TriploxError;
use crate::incremental::IncrementalQueryDelta;
use crate::log::TxLog;
use crate::node::{Database, IntoQuery, Node, QueryNode, SubmitNode, TransactionResult};
use triplox_client::msgpack_codec::{
    decode_execute_request, decode_open_db_request, decode_query_request, decode_subscribe_request,
    encode_db_opened_response, encode_error_body, encode_query_response, encode_subscription_frame,
    encode_tx_key_response, encode_tx_result_response, DbOpenedResponse, ErrorResponseBody,
    QueryResponse, SubscriptionFrame, TxKeyResponse, TxResultResponse,
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

#[derive(Debug)]
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

async fn subscribe<L: TxLog + 'static>(
    State(state): State<Arc<Server<L>>>,
    body: Bytes,
) -> Result<Response, ApiError> {
    let request = decode_subscribe_request(&body).map_err(|e| {
        ApiError::bad_request(
            ErrorCode::ParseError,
            format!("Invalid subscribe request: {}", e),
        )
    })?;

    // subscriptions start at the latest indexed basis only (for now).
    if request.db.is_some() {
        return Err(ApiError::bad_request(
            ErrorCode::InvalidQuery,
            "Subscriptions must start at the latest indexed basis; `db` must be nil",
        ));
    }

    let parsed = request
        .query
        .into_query()
        .map_err(|e| ApiError::bad_request(ErrorCode::ParseError, e.to_string()))?;

    let columns: Vec<ColumnDescription> = match &parsed.find_spec {
        edn::query::FindSpec::FindRel(elements) => elements
            .iter()
            .map(|e| ColumnDescription {
                name: e.to_string(),
                data_type: TAG_UNKNOWN,
                members: None,
            })
            .collect(),
        _ => vec![],
    };

    let subscription = state
        .node
        .register_incremental_query(parsed)
        .await
        .map_err(|e| ApiError::internal(ErrorCode::QueryError, e.to_string()))?;

    let open_frame = encode_subscription_frame(&SubscriptionFrame::Open {
        basis: subscription.basis,
        columns,
    })
    .map_err(|e| ApiError::internal(ErrorCode::InternalError, e.to_string()))?;

    Ok((
        StatusCode::OK,
        [(axum::http::header::CONTENT_TYPE, CONTENT_TYPE)],
        subscription_body(open_frame, subscription.deltas, state.shutdown.clone()),
    )
        .into_response())
}

/// Stream state for subscription deltas after the leading `open` frame.
enum SubscribeBody {
    Streaming {
        deltas: mpsc::Receiver<IncrementalQueryDelta>,
        shutdown: CancellationToken,
    },
    Closed,
}

/// Build the subscription response body: the `open` frame, then one `delta` frame
/// per received delta (write-one/recv-one, so HTTP/2 flow control backpressures the
/// engine instead of buffering unboundedly). The receiver lives in the stream, so a
/// dropped response drops it and the engine tears the query down.
fn subscription_body(
    open_frame: Vec<u8>,
    deltas: mpsc::Receiver<IncrementalQueryDelta>,
    shutdown: CancellationToken,
) -> Body {
    let open =
        futures::stream::once(async move { Ok::<Bytes, Infallible>(Bytes::from(open_frame)) });
    let deltas = futures::stream::unfold(
        SubscribeBody::Streaming { deltas, shutdown },
        |state| async move {
            match state {
                SubscribeBody::Streaming {
                    mut deltas,
                    shutdown,
                } => {
                    tokio::select! {
                        _ = shutdown.cancelled() => None,
                        delta = deltas.recv() => match delta {
                            Some(delta) => {
                                let frame = SubscriptionFrame::Delta {
                                    basis: delta.basis,
                                    rows: delta
                                        .rows
                                        .into_iter()
                                        .map(|(values, weight)| (values, weight as i64))
                                        .collect(),
                                };
                                match encode_subscription_frame(&frame) {
                                    Ok(bytes) => Some((
                                        Ok(Bytes::from(bytes)),
                                        SubscribeBody::Streaming { deltas, shutdown },
                                    )),
                                    Err(err) => Some((
                                        Ok(Bytes::from(internal_error_frame(&err))),
                                        SubscribeBody::Closed,
                                    )),
                                }
                            }
                            None => None,
                        }
                    }
                }
                SubscribeBody::Closed => None,
            }
        },
    );
    Body::from_stream(open.chain(deltas))
}

/// Encode a terminal `error` frame for a failure that occurs mid-stream.
fn internal_error_frame(err: &Error) -> Vec<u8> {
    let frame = SubscriptionFrame::Error(ErrorResponseBody {
        severity: SEVERITY_ERROR,
        code: ErrorCode::InternalError.as_u16(),
        message: err.to_string(),
        detail: None,
        hint: None,
    });
    encode_subscription_frame(&frame).unwrap_or_default()
}

// ---------------------------------------------------------------------------
// Server
// ---------------------------------------------------------------------------

fn build_router<L: TxLog + 'static>(state: Arc<Server<L>>) -> Router {
    Router::new()
        .route("/db/open", post(open_db::<L>))
        .route("/db/query", post(query::<L>))
        .route("/db/subscribe", post(subscribe::<L>))
        .route("/tx/submit", post(submit_tx::<L>))
        .route("/tx/execute", post(execute_tx::<L>))
        .layer(DefaultBodyLimit::max(DEFAULT_MAX_MESSAGE_SIZE as usize))
        .with_state(state)
}

pub struct Server<L: TxLog> {
    node: Arc<Node<L>>,
    shutdown: CancellationToken,
}

impl<L: TxLog + 'static> Server<L> {
    pub fn new(node: Arc<Node<L>>) -> Self {
        Server {
            node,
            shutdown: CancellationToken::new(),
        }
    }

    pub async fn listen(&self, addr: &str, token: CancellationToken) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        self.listen_on(listener, token).await
    }

    pub async fn listen_on(&self, listener: TcpListener, token: CancellationToken) -> Result<()> {
        let app_state = Arc::new(Server {
            node: self.node.clone(),
            shutdown: token.clone(),
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

                    let app_state = Arc::new(Server {
                        node: node.clone(),
                        shutdown: conn_token.clone(),
                    });
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

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt;
    use triplox_client::msgpack_codec::{
        decode_subscription_frame, encode_subscribe_request, SubscribeRequest,
    };

    fn subscribe_body(query: &str, db: Option<TxBasis>) -> Bytes {
        Bytes::from(
            encode_subscribe_request(&SubscribeRequest {
                db,
                query: query.to_string(),
                args: vec![],
            })
            .expect("encode subscribe request"),
        )
    }

    #[tokio::test]
    async fn subscribe_rejects_non_nil_db() {
        let server = Arc::new(Server::new(Arc::new(Node::memory_node().await)));
        let basis = TxBasis {
            tx_key: TxKey {
                tx_id: 0,
                system_time: chrono::Utc::now(),
            },
            tx_eid: 0,
        };
        let err = subscribe(
            State(server),
            subscribe_body("[:find ?n :where [?e :name ?n]]", Some(basis)),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, ErrorCode::InvalidQuery);
    }

    #[tokio::test]
    async fn subscribe_rejects_parse_error() {
        let server = Arc::new(Server::new(Arc::new(Node::memory_node().await)));
        let err = subscribe(State(server), subscribe_body("not a query (", None))
            .await
            .unwrap_err();
        assert_eq!(err.status, StatusCode::BAD_REQUEST);
        assert_eq!(err.code, ErrorCode::ParseError);
    }

    #[tokio::test]
    async fn subscribe_registration_error_matches_query_error_mapping() {
        let server = Arc::new(Server::new(Arc::new(Node::memory_node().await)));
        // `:in` parses as a one-shot query but is unsupported by the incremental engine.
        let err = subscribe(
            State(server),
            subscribe_body("[:find ?n :in ?x :where [?e :name ?n]]", None),
        )
        .await
        .unwrap_err();
        assert_eq!(err.status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_eq!(err.code, ErrorCode::QueryError);
    }

    #[tokio::test]
    async fn subscribe_streams_open_frame_first() {
        let node = Arc::new(Node::memory_node().await);
        node.execute_tx(crate::schema::test_schema_tx())
            .await
            .expect("schema tx");
        let expected_basis = *node.db().await.unwrap().tx_basis();
        let server = Arc::new(Server::new(node));

        let resp = subscribe(
            State(server),
            subscribe_body("[:find ?name :where [?e :name ?name]]", None),
        )
        .await
        .expect("subscribe should return a stream");

        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers()
                .get(axum::http::header::CONTENT_TYPE)
                .unwrap(),
            CONTENT_TYPE
        );

        let mut stream = resp.into_body().into_data_stream();
        let chunk = stream
            .next()
            .await
            .expect("open frame chunk")
            .expect("chunk should be ok");
        match decode_subscription_frame(&chunk).expect("decode open frame") {
            SubscriptionFrame::Open { basis, columns } => {
                assert_eq!(basis, expected_basis);
                assert_eq!(columns.len(), 1);
            }
            other => panic!("expected open frame, got {other:?}"),
        }
    }
}
