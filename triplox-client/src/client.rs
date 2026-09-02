//! HTTP/2 client library for connecting to a Triplox server.
//!
//! `ClientNode` mirrors the `Node` API and `ClientDb` mirrors the `DB` API,
//! both operating over HTTP/2 via the binary protocol encoding.

use anyhow::{bail, Error, Result};
use reqwest::Client;

use crate::msgpack_codec::{
    decode_error_body, decode_query_response, decode_tx_key, decode_tx_result_response,
    encode_execute_request, encode_open_db_request, encode_query_request, encode_subscribe_request,
    ExecuteRequest, OpenDbRequest, QueryRequest, SubscribeRequest,
};
use crate::node::{collect_tx_ops, Database, IntoQuery, IntoTxOp, QueryNode, SubmitNode};
use crate::ops::QueryArg;
use crate::query::QueryResult;
use crate::subscription::Subscription;
use crate::transaction::{TransactionResult, TxKey};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CONTENT_TYPE: &str = "application/vnd.triplox+msgpack";

/// Check an HTTP response for errors. If the status is not success,
/// attempt to decode a binary ErrorResponse from the body.
async fn check_response(resp: reqwest::Response) -> Result<bytes::Bytes> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp.bytes().await?)
    } else {
        let body = resp.bytes().await?;
        if let Ok(error) = decode_error_body(&body) {
            let mut msg = format!("Server error (code {}): {}", error.code, error.message);
            if let Some(d) = error.detail {
                msg.push_str(&format!(" — {}", d));
            }
            bail!("{}", msg);
        }
        bail!("HTTP error {}: {}", status, String::from_utf8_lossy(&body));
    }
}

// ---------------------------------------------------------------------------
// ClientNode
// ---------------------------------------------------------------------------

pub struct ClientNode {
    client: Client,
    base_url: String,
}

impl ClientNode {
    /// Connect to a Triplox HTTP server.
    ///
    /// `url` should be the base URL, e.g. `http://127.0.0.1:5490`.
    pub async fn connect(url: &str) -> Result<Self> {
        let client = Client::builder().http2_prior_knowledge().build()?;
        Ok(ClientNode {
            client,
            base_url: url.trim_end_matches('/').to_string(),
        })
    }

    /// Register an incremental query and stream its result deltas.
    ///
    /// Subscribes at the latest indexed basis. The returned [`Subscription`] is a
    /// `Stream` of [`Delta`](crate::Delta)s. A non-empty initial result is emitted
    /// first at the registration basis; dropping the subscription unsubscribes.
    pub async fn subscribe(&self, query: impl IntoQuery) -> Result<Subscription> {
        let parsed = query.into_query()?;
        let body = encode_subscribe_request(&SubscribeRequest {
            tx_key: None,
            query: parsed.to_string(),
            args: vec![],
        })?;
        let resp = self
            .client
            .post(format!("{}/db/subscribe", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let data = resp.bytes().await?;
            if let Ok(error) = decode_error_body(&data) {
                bail!("Server error (code {}): {}", error.code, error.message);
            }
            bail!("HTTP error {}: {}", status, String::from_utf8_lossy(&data));
        }

        Subscription::connect(resp).await
    }

    async fn open_db(&self, tx_key: Option<TxKey>) -> Result<ClientDb> {
        let (tx_id, system_time) = match tx_key {
            None => (None, None),
            Some(tx_key) => (Some(tx_key.tx_id), Some(tx_key.system_time)),
        };

        let body = encode_open_db_request(&OpenDbRequest { tx_id, system_time })?;
        let resp = self
            .client
            .post(format!("{}/db/open", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let tx_key = decode_tx_key(&data)?;

        Ok(ClientDb {
            tx_key,
            client: self.client.clone(),
            base_url: self.base_url.clone(),
        })
    }
}

impl SubmitNode for ClientNode {
    async fn submit_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TxKey, Error> {
        let ops = collect_tx_ops(ops)?;
        let body = encode_execute_request(&ExecuteRequest { ops })?;
        let resp = self
            .client
            .post(format!("{}/tx/submit", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let tx_key = decode_tx_key(&data)?;
        Ok(tx_key)
    }

    async fn execute_tx<O: IntoTxOp>(&self, ops: Vec<O>) -> Result<TransactionResult, Error> {
        let ops = collect_tx_ops(ops)?;
        let body = encode_execute_request(&ExecuteRequest { ops })?;
        let resp = self
            .client
            .post(format!("{}/tx/execute", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let tx_result = decode_tx_result_response(&data)?;
        let tx_key = TxKey {
            tx_id: tx_result.tx_id,
            system_time: tx_result.system_time,
        };

        if tx_result.status == 0 {
            Ok(TransactionResult::TxCommitted(tx_key))
        } else {
            let err_msg = tx_result
                .error_message
                .unwrap_or_else(|| "transaction aborted".to_string());
            Ok(TransactionResult::TxAborted(
                tx_key,
                anyhow::anyhow!("{}", err_msg).into(),
            ))
        }
    }
}

impl QueryNode for ClientNode {
    type DB = ClientDb;

    async fn db(&self) -> Result<ClientDb, Error> {
        self.open_db(None).await
    }

    async fn db_as_of(&self, tx_key: TxKey) -> Result<ClientDb, Error> {
        self.open_db(Some(tx_key)).await
    }
}

// ---------------------------------------------------------------------------
// ClientDb
// ---------------------------------------------------------------------------

/// A remote DB read basis. Mirrors the `DB` API.
pub struct ClientDb {
    tx_key: TxKey,
    client: Client,
    base_url: String,
}

impl ClientDb {
    /// The transaction key this DB value is pinned to.
    pub fn tx_key(&self) -> TxKey {
        self.tx_key
    }
}

impl Database for ClientDb {
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error> {
        self.query_with_args(query, &[]).await
    }

    async fn query_with_args(
        &self,
        query: impl IntoQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error> {
        let parsed = query.into_query()?;
        let body = encode_query_request(&QueryRequest {
            tx_key: self.tx_key,
            query: parsed.to_string(),
            args: args.to_vec(),
        })?;
        let resp = self
            .client
            .post(format!("{}/db/query", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let query_response = decode_query_response(&data)?;
        Ok(query_response.rows)
    }
}
