//! Rust client library for connecting to a Triplox server.
//!
//! `ClientNode` mirrors the `Node` API and `ClientDB` mirrors the `DB` API,
//! both operating over TCP via the wire protocol.

use std::collections::BTreeMap;
use std::fmt::Write;
use std::sync::Arc;

use anyhow::{bail, Error, Result};
use chrono::TimeZone;
use tokio::io::{AsyncWriteExt, BufReader, BufWriter};
use tokio::net::TcpStream;
use tokio::sync::Mutex;

use crate::datalog::{FindElement, FindSpec, OrBranch, PatternElement, Query, WhereClause};
use crate::node::{Basis, Database, QueryNode, SubmitNode, TransactionResult, TxKey};
use crate::ops::{DataType, TxOp};
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
            BackendMessage::ReadyForQuery { status: STATUS_IDLE } => {}
            other => bail!("Expected ReadyForQuery, got {:?}", other),
        }

        Ok(ClientNode {
            conn: Arc::new(Mutex::new(ConnectionInner { reader, writer })),
        })
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
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        let (db_id, tx_id) = match msg {
            BackendMessage::DbOpened { db_id, tx_id } => (db_id, tx_id),
            BackendMessage::ErrorResponse { message, .. } => {
                // Consume the ReadyForQuery that follows
                let _ =
                    read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await;
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

        Ok(ClientDB {
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
                TxKey { tx_id, system_time: dt }
            }
            BackendMessage::ErrorResponse { message, .. } => {
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
                seq_num,
                error_message,
            } => {
                let dt = crate::protocol::micros_to_datetime(system_time)?;
                let tx_key = TxKey { tx_id, system_time: dt };
                if status == 0 {
                    TransactionResult::TxCommited(Basis { tx_key, seq_num })
                } else {
                    let err_msg =
                        error_message.unwrap_or_else(|| "transaction aborted".to_string());
                    TransactionResult::TxAborted(tx_key, anyhow::anyhow!("{}", err_msg).into())
                }
            }
            BackendMessage::ErrorResponse { message, .. } => {
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
    type DB = ClientDB;

    async fn db(&self) -> Result<ClientDB, Error> {
        self.open_db(None).await
    }

    async fn db_with_basis(&self, basis: Basis) -> Result<ClientDB, Error> {
        self.open_db(Some(basis.tx_key.tx_id)).await
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
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;

        match msg {
            BackendMessage::RowDescription { .. } => {
                // Collect DataRows until CommandComplete
                let mut rows = Vec::new();
                loop {
                    let msg = read_backend_message(
                        &mut conn.reader,
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
                    DEFAULT_MAX_MESSAGE_SIZE,
                )
                .await;
                bail!("Query error: {}", message);
            }
            other => bail!("Expected RowDescription or ErrorResponse, got {:?}", other),
        }
    }

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
        let msg = read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await?;
        match msg {
            BackendMessage::DbClosed { .. } => {}
            BackendMessage::ErrorResponse { message, .. } => {
                let _ =
                    read_backend_message(&mut conn.reader, DEFAULT_MAX_MESSAGE_SIZE).await;
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

impl Database for ClientDB {
    async fn query(&self, query: &Query) -> Result<QueryResult, Error> {
        let edn = query_to_edn(query)?;
        self.query_edn(&edn).await
    }
}

// ---------------------------------------------------------------------------
// Query → EDN serialization
// TODO(triplox-1cp): Move this to the EDN crate as a proper serializer.
// ---------------------------------------------------------------------------

fn query_to_edn(query: &Query) -> Result<String> {
    let mut s = String::new();
    s.push_str("{:find [");
    match &query.find {
        FindSpec::FindRel(elems) => {
            for (i, elem) in elems.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                find_element_to_edn(elem, &mut s)?;
            }
        }
    }
    s.push_str("] :where [");
    for (i, clause) in query.where_clauses.iter().enumerate() {
        if i > 0 {
            s.push(' ');
        }
        where_clause_to_edn(clause, &mut s)?;
    }
    s.push_str("]}");
    Ok(s)
}

fn find_element_to_edn(elem: &FindElement, s: &mut String) -> Result<()> {
    match elem {
        FindElement::Variable(v) => s.push_str(v),
        FindElement::Aggregate(func, arg) => {
            s.push('(');
            s.push_str(func);
            s.push(' ');
            s.push_str(arg);
            s.push(')');
        }
        FindElement::PullExpr(_) => bail!("PullExpr not supported in EDN serialization"),
    }
    Ok(())
}

fn where_clause_to_edn(clause: &WhereClause, s: &mut String) -> Result<()> {
    match clause {
        WhereClause::Triple(tp) => {
            s.push('[');
            pattern_element_to_edn(&tp.entity, s)?;
            s.push(' ');
            pattern_element_to_edn(&tp.attribute, s)?;
            s.push(' ');
            pattern_element_to_edn(&tp.value, s)?;
            s.push(']');
        }
        WhereClause::Or(branches) => {
            s.push_str("(or ");
            for (i, branch) in branches.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                or_branch_to_edn(branch, s)?;
            }
            s.push(')');
        }
        WhereClause::Not(clauses) => {
            s.push_str("(not ");
            for (i, c) in clauses.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                where_clause_to_edn(c, s)?;
            }
            s.push(')');
        }
        WhereClause::OrJoin(vars, branches) => {
            s.push_str("(or-join [");
            for (i, v) in vars.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str(v);
            }
            s.push_str("] ");
            for (i, branch) in branches.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                or_branch_to_edn(branch, s)?;
            }
            s.push(')');
        }
        WhereClause::NotJoin(vars, clauses) => {
            s.push_str("(not-join [");
            for (i, v) in vars.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                s.push_str(v);
            }
            s.push_str("] ");
            for (i, c) in clauses.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                where_clause_to_edn(c, s)?;
            }
            s.push(')');
        }
        WhereClause::Predicate(_) => {
            bail!("Predicate clauses cannot be serialized to EDN yet")
        }
        WhereClause::FnExpr(_) => {
            bail!("FnExpr clauses cannot be serialized to EDN yet")
        }
    }
    Ok(())
}

fn or_branch_to_edn(branch: &OrBranch, s: &mut String) -> Result<()> {
    match branch {
        OrBranch::Clause(c) => where_clause_to_edn(c, s),
        OrBranch::And(clauses) => {
            s.push_str("(and ");
            for (i, c) in clauses.iter().enumerate() {
                if i > 0 {
                    s.push(' ');
                }
                where_clause_to_edn(c, s)?;
            }
            s.push(')');
            Ok(())
        }
    }
}

fn pattern_element_to_edn(elem: &PatternElement, s: &mut String) -> Result<()> {
    match elem {
        PatternElement::Variable(v) => s.push_str(v),
        PatternElement::Wildcard => s.push('_'),
        PatternElement::Constant(dt) => datatype_to_edn(dt, s)?,
    }
    Ok(())
}

fn datatype_to_edn(dt: &DataType, s: &mut String) -> Result<()> {
    match dt {
        DataType::Nil => s.push_str("nil"),
        DataType::Long(v) => write!(s, "{}", v)?,
        DataType::BigInt(v) => write!(s, "{}", v)?,
        DataType::Boolean(b) => write!(s, "{}", b)?,
        DataType::Double(f) => {
            if f.fract() == 0.0 {
                write!(s, "{:.1}", f)?;
            } else {
                write!(s, "{}", f)?;
            }
        }
        DataType::Float(f) => {
            if f.fract() == 0.0 {
                write!(s, "{:.1}", f)?;
            } else {
                write!(s, "{}", f)?;
            }
        }
        DataType::String(v) => {
            s.push('"');
            for c in v.chars() {
                match c {
                    '"' => s.push_str("\\\""),
                    '\\' => s.push_str("\\\\"),
                    '\n' => s.push_str("\\n"),
                    '\r' => s.push_str("\\r"),
                    '\t' => s.push_str("\\t"),
                    c => s.push(c),
                }
            }
            s.push('"');
        }
        DataType::Keyword(kw) => {
            s.push(':');
            if let Some(ns) = kw.namespace() {
                s.push_str(ns);
                s.push('/');
            }
            s.push_str(kw.name());
        }
        DataType::Instant(dt) => {
            write!(s, "#inst \"{}\"", dt.format("%Y-%m-%dT%H:%M:%S%.3fZ"))?;
        }
        DataType::Uuid(u) => {
            write!(s, "#uuid \"{}\"", u)?;
        }
        DataType::Bytes(_) => bail!("Bytes cannot be represented in EDN"),
        DataType::Tuple(_) | DataType::Vector(_) | DataType::Map(_) => {
            bail!("Composite types cannot be represented in EDN query constants")
        }
    }
    Ok(())
}

