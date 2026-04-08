//! Rust client library for connecting to a Triplox server.
//!
//! `ClientNode` mirrors the `Node` API and `ClientDb` mirrors the `DB` API,
//! both operating over TCP via the wire protocol.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::{bail, Error, Result};
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::node::{Database, IntoQuery, QueryNode, SubmitNode, TransactionResult, TxKey};
use crate::ops::{DataType, TxOp};
use crate::protocol::*;
use crate::query::QueryResult;
use edn::query::ParsedQuery;

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
        stream.set_nodelay(true)?;
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
        let msg = read_backend_message(&mut reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::AuthenticationOk { .. } => {}
            BackendMessage::ErrorResponse { message, .. } => {
                bail!("Server rejected connection: {}", message);
            }
            other => bail!("Unexpected response to Startup: {:?}", other),
        }

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery {
                status: STATUS_IDLE,
            } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(ClientNode {
            conn: Arc::new(Mutex::new(ConnectionInner { reader, writer })),
        })
    }

    async fn open_db(&self, tx_key: Option<TxKey>) -> Result<ClientDb> {
        let mut conn = self.conn.lock().await;
        let (tx_id, system_time) = match &tx_key {
            None => (None, None),
            Some(tk) => (Some(tk.tx_id), Some(tk.system_time.timestamp_micros())),
        };
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::OpenDb { tx_id, system_time },
        )
        .await?;
        conn.writer.flush().await?;

        // Expect DbOpened
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        let (db_id, tx_id) = match msg {
            BackendMessage::DbOpened { db_id, tx_id } => (db_id, tx_id),
            BackendMessage::ErrorResponse { message, .. } => {
                read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
                bail!("Failed to open DB: {}", message);
            }
            other => bail!("Expected DbOpened, got {:?}", other),
        };

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(ClientDb {
            db_id,
            tx_id,
            conn: self.conn.clone(),
        })
    }

    /// Gracefully close the connection.
    pub async fn close(&self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(&mut conn.writer, &FrontendMessage::Terminate).await?;
        conn.writer.flush().await?;
        Ok(())
    }
}

impl SubmitNode for ClientNode {
    async fn submit_tx(&self, ops: Vec<TxOp>) -> Result<TxKey, Error> {
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

        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        let tx_key = match msg {
            BackendMessage::TxKey { tx_id, system_time } => {
                let dt = crate::protocol::micros_to_datetime(system_time)?;
                TxKey {
                    tx_id,
                    system_time: dt,
                }
            }
            BackendMessage::ErrorResponse { message, .. } => {
                // TODO: fatal errors may not be followed by ReadyForQuery
                read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
                bail!("Transaction error: {}", message);
            }
            other => bail!("Expected TxKey, got {:?}", other),
        };

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(tx_key)
    }

    async fn execute_tx(&self, ops: Vec<TxOp>) -> Result<TransactionResult, Error> {
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

        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        let result = match msg {
            BackendMessage::TxResult {
                status,
                tx_id,
                system_time,
                error_message,
            } => {
                let dt = crate::protocol::micros_to_datetime(system_time)?;
                let tx_key = TxKey {
                    tx_id,
                    system_time: dt,
                };
                if status == 0 {
                    TransactionResult::TxCommited(tx_key)
                } else {
                    let err_msg =
                        error_message.unwrap_or_else(|| "transaction aborted".to_string());
                    TransactionResult::TxAborted(tx_key, anyhow::anyhow!("{}", err_msg).into())
                }
            }
            BackendMessage::ErrorResponse { message, .. } => {
                // TODO: fatal errors may not be followed by ReadyForQuery
                read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
                bail!("Transaction error: {}", message);
            }
            other => bail!("Expected TxResult, got {:?}", other),
        };

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(result)
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
/// Callers must call [`.close()`](ClientDb::close) when done. Dropping without
/// closing leaks the server-side handle until the connection is closed.
pub struct ClientDb {
    db_id: u32,
    tx_id: i64,
    conn: Arc<Mutex<ConnectionInner>>,
}

impl ClientDb {
    /// The tx_id this snapshot is pinned to.
    pub fn tx_id(&self) -> i64 {
        self.tx_id
    }

    /// Release this DB handle on the server.
    pub async fn close(self) -> Result<()> {
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::CloseDb { db_id: self.db_id },
        )
        .await?;
        conn.writer.flush().await?;

        // Expect DbClosed
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::DbClosed { .. } => {}
            BackendMessage::ErrorResponse { message, .. } => {
                read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
                bail!("Failed to close DB: {}", message);
            }
            other => bail!("Expected DbClosed, got {:?}", other),
        }

        // Expect ReadyForQuery
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::ReadyForQuery { .. } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(())
    }
}

impl Database for ClientDb {
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error> {
        let query = query.into_query()?;
        let mut conn = self.conn.lock().await;
        write_frontend_message(
            &mut conn.writer,
            &FrontendMessage::Query {
                query_string: query.to_string(),
                db_id: self.db_id,
            },
        )
        .await?;
        conn.writer.flush().await?;

        // Read response: could be RowDescription + DataRow* + ReadyForQuery, or ErrorResponse
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;

        match msg {
            BackendMessage::RowDescription { .. } => {
                // Collect DataRows until ReadyForQuery
                let mut rows = Vec::new();
                loop {
                    let msg =
                        read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
                    match msg {
                        BackendMessage::DataRow { values } => {
                            rows.push(values);
                        }
                        BackendMessage::ReadyForQuery { .. } => break,
                        other => bail!("Unexpected message during query: {:?}", other),
                    }
                }
                Ok(rows)
            }
            BackendMessage::ErrorResponse { message, .. } => {
                read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
                bail!("Query error: {}", message);
            }
            other => bail!("Expected RowDescription or ErrorResponse, got {:?}", other),
        }
    }
}
