//! HTTP/2 client library for connecting to a Triplox server.
//!
//! `ClientNode` mirrors the `Node` API and `ClientDb` mirrors the `DB` API,
//! both operating over HTTP/2 via the binary protocol encoding.

use anyhow::{bail, Error, Result};
use reqwest::Client;

use crate::node::{Database, IntoQuery, QueryNode, SubmitNode, TransactionResult, TxKey};
use crate::ops::{DataType, QueryArg, TxOp};
use crate::protocol::*;
use crate::query::QueryResult;
use edn::query::ParsedQuery;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const CONTENT_TYPE: &str = "application/x-triplox";

/// Check an HTTP response for errors. If the status is not success,
/// attempt to decode a binary ErrorResponse from the body.
async fn check_response(resp: reqwest::Response) -> Result<bytes::Bytes> {
    let status = resp.status();
    if status.is_success() {
        Ok(resp.bytes().await?)
    } else {
        let body = resp.bytes().await?;
        if let Ok((_, code, message, detail, _)) = decode_error_body(&body) {
            let mut msg = format!("Server error (code {}): {}", code, message);
            if let Some(d) = detail {
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
        let client = Client::builder()
            .http2_prior_knowledge()
            .build()?;
        Ok(ClientNode {
            client,
            base_url: url.trim_end_matches('/').to_string(),
        })
    }

    async fn open_db(&self, tx_key: Option<TxKey>) -> Result<ClientDb> {
        let (tx_id, system_time) = match &tx_key {
            None => (None, None),
            Some(tk) => (Some(tk.tx_id), Some(tk.system_time.timestamp_micros())),
        };

        let body = encode_open_db_request(tx_id, system_time);
        let resp = self
            .client
            .post(format!("{}/db/open", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let (db_id, tx_id) = decode_db_opened_response(&data)?;

        Ok(ClientDb {
            db_id,
            tx_id,
            client: self.client.clone(),
            base_url: self.base_url.clone(),
        })
    }

    /// Graceful close. With HTTP, handles are cleaned up on connection drop.
    pub async fn close(&self) -> Result<()> {
        Ok(())
    }
}

impl SubmitNode for ClientNode {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> Result<TxKey, Error> {
        let body = encode_execute_request(&ops);
        let resp = self
            .client
            .post(format!("{}/tx/submit", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let (tx_id, system_time) = decode_tx_key_response(&data)?;
        let dt = micros_to_datetime(system_time)?;
        Ok(TxKey {
            tx_id,
            system_time: dt,
        })
    }

    async fn execute_tx(&self, ops: Vec<TxOp>) -> Result<TransactionResult, Error> {
        let body = encode_execute_request(&ops);
        let resp = self
            .client
            .post(format!("{}/tx/execute", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let (status, tx_id, system_time, error_message) = decode_tx_result_response(&data)?;
        let dt = micros_to_datetime(system_time)?;
        let tx_key = TxKey {
            tx_id,
            system_time: dt,
        };

        if status == 0 {
            Ok(TransactionResult::TxCommited(tx_key))
        } else {
            let err_msg = error_message.unwrap_or_else(|| "transaction aborted".to_string());
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

/// A remote DB snapshot handle. Mirrors the `DB` API.
///
/// Callers should call [`.close()`](ClientDb::close) when done to release
/// the server-side handle. If not closed explicitly, the server will clean
/// up via TTL expiration or connection-drop detection.
pub struct ClientDb {
    db_id: u32,
    tx_id: i64,
    client: Client,
    base_url: String,
}

impl ClientDb {
    /// The tx_id this snapshot is pinned to.
    pub fn tx_id(&self) -> i64 {
        self.tx_id
    }

    /// Release this DB handle on the server.
    pub async fn close(self) -> Result<()> {
        let resp = self
            .client
            .delete(format!("{}/db/{}", self.base_url, self.db_id))
            .send()
            .await?;
        check_response(resp).await?;
        Ok(())
    }
}

impl Database for ClientDb {
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error> {
        let parsed = query.into_query()?;
        self.query_with_args(&parsed, &[]).await
    }

    async fn query_with_args(
        &self,
        query: &ParsedQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error> {
        let body = encode_query_request(&query.to_string(), args);
        let resp = self
            .client
            .post(format!("{}/db/{}/query", self.base_url, self.db_id))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let (_columns, rows) = decode_query_response(&data)?;
        Ok(rows)
    }
}
