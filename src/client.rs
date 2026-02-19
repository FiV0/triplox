//! Rust client library for connecting to a Triplox server.
//!
//! `ClientNode` mirrors the `Node` API and `ClientDB` mirrors the `DB` API,
//! both operating over TCP via the wire protocol.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use chrono::TimeZone;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::node::{Basis, Database, QueryNode, SubmitNode, TransactionResult, TxKey};
use crate::ops::TxOp;
use crate::protocol::*;
use crate::query::QueryResult;

// ---------------------------------------------------------------------------
// ClientNode
// ---------------------------------------------------------------------------

/// Shared connection state protected by a mutex (serial protocol — one op at a time).
struct ConnectionInner {
    reader: BufReader<tokio::net::tcp::OwnedReadHalf>,
    writer: BufWriter<tokio::net::tcp::OwnedWriteHalf>,
}

pub struct ClientNode {
    conn: Arc<Mutex<ConnectionInner>>,
}

impl ClientNode {
    /// Connect to a Triplox server and perform the startup handshake.
    pub async fn connect(addr: &str) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        let (reader, writer) = stream.into_split();
        let mut reader = BufReader::new(reader);
        let mut writer = BufWriter::new(writer);

        // Send Startup
        write_frontend_message(
            &mut writer,
            &FrontendMessage::Startup {
                version_major: PROTOCOL_VERSION_MAJOR,
                version_minor: PROTOCOL_VERSION_MINOR,
                params: BTreeMap::new(),
            },
        )
        .await?;
        writer.flush().await?;

        // Expect AuthenticationOk
        let msg = read_backend_message(&mut reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::AuthenticationOk { .. } => {}
            BackendMessage::ErrorResponse { message, .. } => {
                bail!("Server rejected connection: {}", message);
            }
            other => bail!("Unexpected response to Startup: {:?}", other),
        }

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { status: STATUS_IDLE } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(ClientNode {
            conn: Arc::new(Mutex::new(ConnectionInner { reader, writer })),
        })
    }

    /// Open a DB snapshot (latest).
    pub async fn db(&self) -> Result<ClientDB> {
        self.open_db(None).await
    }

    /// Open a DB snapshot at a specific basis.
    pub async fn db_with_basis(&self, basis: &Basis) -> Result<ClientDB> {
        self.open_db(Some(basis.tx_key.tx_id)).await
    }

    async fn open_db(&self, basis_tx_id: Option<i64>) -> Result<ClientDB> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::OpenDb { basis_tx_id },
        )
        .await?;
        conn.writer.flush().await?;

        // Expect DbOpened
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        let (db_id, tx_id) = match msg {
            BackendMessage::DbOpened { db_id, tx_id } => (db_id, tx_id),
            BackendMessage::ErrorResponse { message, .. } => {
                // Consume the ReadyForQuery that follows
                let _ =
                    read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await;
                bail!("Failed to open DB: {}", message);
            }
            other => bail!("Expected DbOpened, got {:?}", other),
        };

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(ClientDB {
            db_id,
            tx_id,
            conn: self.conn.clone(),
        })
    }

    /// Submit a transaction without waiting for indexing.
    pub async fn submit_tx(&self, ops: Vec<TxOp>) -> Result<TxKey> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::Execute {
                ops,
                await_indexing: false,
            },
        )
        .await?;
        conn.writer.flush().await?;

        let tx_result = read_tx_result(&mut conn.reader).await?;

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(tx_result.0)
    }

    /// Submit a transaction and wait for it to be indexed.
    pub async fn execute_tx(&self, ops: Vec<TxOp>) -> Result<TransactionResult> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::Execute {
                ops,
                await_indexing: true,
            },
        )
        .await?;
        conn.writer.flush().await?;

        let (tx_key, status, error_message) = read_tx_result(&mut conn.reader).await?;

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        if status == 0 {
            // We don't have the seq_num from the wire — use 0 as placeholder.
            // The server-side Basis has the real seq_num, but for the client
            // the TxKey is the meaningful identifier.
            Ok(TransactionResult::TxCommited(Basis {
                tx_key,
                seq_num: 0,
            }))
        } else {
            let err_msg = error_message.unwrap_or_else(|| "transaction aborted".to_string());
            Ok(TransactionResult::TxAborted(
                tx_key,
                anyhow::anyhow!("{}", err_msg).into(),
            ))
        }
    }

    /// Gracefully close the connection.
    pub async fn close(&self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(&mut conn.writer, &FrontendMessage::Terminate).await?;
        conn.writer.flush().await?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ClientDB
// ---------------------------------------------------------------------------

/// A remote DB snapshot handle. Mirrors the `DB` API.
pub struct ClientDB {
    db_id: u32,
    tx_id: i64,
    conn: Arc<Mutex<ConnectionInner>>,
}

impl ClientDB {
    /// The tx_id this snapshot is pinned to.
    pub fn tx_id(&self) -> i64 {
        self.tx_id
    }

    /// Execute a query using a raw EDN string.
    pub async fn query_edn(&self, edn: &str) -> Result<QueryResult> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::Query {
                query_string: edn.to_string(),
                db_id: self.db_id,
            },
        )
        .await?;
        conn.writer.flush().await?;

        // Read response: could be RowDescription + DataRow* + CommandComplete, or ErrorResponse
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;

        match msg {
            BackendMessage::RowDescription { .. } => {
                // Collect DataRows until CommandComplete
                let mut rows = Vec::new();
                loop {
                    let msg = read_backend_message(
                        &mut conn.reader,
                        false,
                        DEFAULT_MAX_MESSAGE_SIZE,
                    )
                    .await?;
                    match msg {
                        BackendMessage::DataRow { values } => {
                            rows.push(values);
                        }
                        BackendMessage::CommandComplete { .. } => break,
                        other => bail!("Unexpected message during query: {:?}", other),
                    }
                }

                // Expect ReadyForQuery
                let msg = read_backend_message(
                    &mut conn.reader,
                    false,
                    DEFAULT_MAX_MESSAGE_SIZE,
                )
                .await?;
                match msg {
                    BackendMessage::ReadyForQuery { .. } => {}
                    other => bail!("Expected ReadyForQuery, got {:?}", other),
                }

                Ok(rows)
            }
            BackendMessage::ErrorResponse { message, .. } => {
                // Expect ReadyForQuery after error
                let _ = read_backend_message(
                    &mut conn.reader,
                    false,
                    DEFAULT_MAX_MESSAGE_SIZE,
                )
                .await;
                bail!("Query error: {}", message);
            }
            other => bail!("Expected RowDescription or ErrorResponse, got {:?}", other),
        }
    }

    // TODO: fn query(&self, query: &Query) -> Result<QueryResult>
    // Requires Query -> EDN string serialization, which the edn crate
    // doesn't support yet. Will be implemented when Query::to_edn() is available.

    /// Release this DB handle on the server.
    pub async fn close(self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::CloseDb {
                db_id: self.db_id,
            },
        )
        .await?;
        conn.writer.flush().await?;

        // Expect DbClosed
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::DbClosed { .. } => {}
            BackendMessage::ErrorResponse { message, .. } => {
                let _ =
                    read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await;
                bail!("Failed to close DB: {}", message);
            }
            other => bail!("Expected DbClosed, got {:?}", other),
        }

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Read a TxResult message, handling ErrorResponse.
/// Returns (TxKey, status, optional error_message).
async fn read_tx_result(
    reader: &mut BufReader<tokio::net::tcp::OwnedReadHalf>,
) -> Result<(TxKey, u8, Option<String>)> {
    let msg = read_backend_message(reader, false, DEFAULT_MAX_MESSAGE_SIZE).await?;
    match msg {
        BackendMessage::TxResult {
            status,
            tx_id,
            system_time,
            error_message,
        } => {
            let secs = system_time / 1_000_000;
            let remainder_micros = (system_time % 1_000_000).unsigned_abs() as u32;
            let nanos = remainder_micros * 1000;
            let dt = chrono::Utc
                .timestamp_opt(secs, nanos)
                .single()
                .unwrap_or_else(chrono::Utc::now);
            let tx_key = TxKey {
                tx_id,
                system_time: dt,
            };
            Ok((tx_key, status, error_message))
        }
        BackendMessage::ErrorResponse { message, .. } => {
            bail!("Transaction error: {}", message);
        }
        other => bail!("Expected TxResult, got {:?}", other),
    }
}
