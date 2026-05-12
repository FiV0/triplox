//! HTTP/2 client library for connecting to a Triplox server.
//!
//! `ClientNode` mirrors the `Node` API and `ClientDb` mirrors the `DB` API,
//! both operating over HTTP/2 via the binary protocol encoding.

use anyhow::{bail, Error, Result};
use reqwest::Client;

use crate::msgpack_codec::{
    decode_db_opened_response, decode_error_body, decode_query_response, decode_tx_key_response,
    decode_tx_result_response, encode_execute_request, encode_open_db_request,
    encode_query_request, ExecuteRequest, OpenDbRequest, QueryRequest,
};
use crate::node::{collect_tx_ops, Database, IntoQuery, IntoTxOp, QueryNode, SubmitNode};
use crate::ops::QueryArg;
use crate::query::QueryResult;
use crate::transaction::{TransactionResult, TxBasis, TxKey};
use edn::query::ParsedQuery;

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

    async fn open_db(&self, basis: Option<TxBasis>) -> Result<ClientDb> {
        let (tx_id, system_time, tx_eid) = match basis {
            None => (None, None, None),
            Some(basis) => (
                Some(basis.tx_key.tx_id),
                Some(basis.tx_key.system_time),
                Some(basis.tx_eid),
            ),
        };

        let body = encode_open_db_request(&OpenDbRequest {
            tx_id,
            system_time,
            tx_eid,
        })?;
        let resp = self
            .client
            .post(format!("{}/db/open", self.base_url))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let opened = decode_db_opened_response(&data)?;

        Ok(ClientDb {
            db_id: opened.db_id,
            tx_eid: opened.tx_eid,
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
        let tx_key = decode_tx_key_response(&data)?;
        Ok(TxKey {
            tx_id: tx_key.tx_id,
            system_time: tx_key.system_time,
        })
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
        let basis = TxBasis {
            tx_key: TxKey {
                tx_id: tx_result.tx_id,
                system_time: tx_result.system_time,
            },
            tx_eid: tx_result.tx_eid,
        };

        if tx_result.status == 0 {
            Ok(TransactionResult::TxCommited(basis))
        } else {
            let err_msg = tx_result
                .error_message
                .unwrap_or_else(|| "transaction aborted".to_string());
            Ok(TransactionResult::TxAborted(
                basis,
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

    async fn db_as_of(&self, basis: TxBasis) -> Result<ClientDb, Error> {
        self.open_db(Some(basis)).await
    }
}

// ---------------------------------------------------------------------------
// ClientDb
// ---------------------------------------------------------------------------

/// A remote DB read handle. Mirrors the `DB` API.
///
/// Callers should call [`.close()`](ClientDb::close) when done to release
/// the server-side handle. If not closed explicitly, the server will clean
/// up via TTL expiration or connection-drop detection.
pub struct ClientDb {
    db_id: u32,
    tx_eid: i64,
    client: Client,
    base_url: String,
}

impl ClientDb {
    /// The transaction entity id this handle is pinned to.
    pub fn tx_eid(&self) -> i64 {
        self.tx_eid
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
        let body = encode_query_request(&QueryRequest {
            query: query.to_string(),
            args: args.to_vec(),
        })?;
        let resp = self
            .client
            .post(format!("{}/db/{}/query", self.base_url, self.db_id))
            .header("Content-Type", CONTENT_TYPE)
            .body(body)
            .send()
            .await?;

        let data = check_response(resp).await?;
        let query_response = decode_query_response(&data)?;
        Ok(query_response.rows)
    }
}
