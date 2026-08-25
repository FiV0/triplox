use std::sync::Arc;

use anyhow::Error;
use edn::query::ParsedQuery;
use tokio::runtime::Handle;

use crate::indexer::latest_tx_key_from_sdb;
use crate::ops::{Entid, QueryArg};
use crate::partition::tx_eid_from_tx_id;
use crate::query::{execute_query, QueryResult};
use crate::schema::IdentMap;
use triplox_client::node::{Database, IntoQuery};
use triplox_client::transaction::TxKey;

pub struct DB<D = slatedb::Db, M = slatedb::Db>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    sdb: Arc<D>,
    ident_map: Arc<IdentMap>,
    handle: Handle,
    tx_key: TxKey,
    range_stats: Arc<slatedb_estimates::RangeStats<M>>,
}

impl<D, M> Clone for DB<D, M>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            sdb: Arc::clone(&self.sdb),
            ident_map: Arc::clone(&self.ident_map),
            handle: self.handle.clone(),
            tx_key: self.tx_key,
            range_stats: Arc::clone(&self.range_stats),
        }
    }
}

#[allow(unused)]
impl<D, M> DB<D, M>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    pub fn new(
        sdb: Arc<D>,
        ident_map: IdentMap,
        handle: Handle,
        tx_key: TxKey,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Self {
        Self {
            sdb,
            ident_map: Arc::new(ident_map),
            handle,
            tx_key,
            range_stats,
        }
    }

    /// Construct a DB from a Db by scanning EAV for TX_PARTITION entities to find the latest TxKey.
    pub async fn from_latest_sdb(
        sdb: Arc<D>,
        ident_map: IdentMap,
        handle: Handle,
        range_stats: Arc<slatedb_estimates::RangeStats<M>>,
    ) -> Result<Self, Error> {
        let tx_key = latest_tx_key_from_sdb(sdb.as_ref()).await?;
        Ok(Self {
            sdb,
            ident_map: Arc::new(ident_map),
            handle,
            tx_key,
            range_stats,
        })
    }

    pub fn tx_key(&self) -> &TxKey {
        &self.tx_key
    }

    pub(crate) fn sdb(&self) -> &D {
        self.sdb.as_ref()
    }

    pub(crate) fn ident_map(&self) -> &IdentMap {
        self.ident_map.as_ref()
    }

    pub(crate) fn handle(&self) -> &Handle {
        &self.handle
    }

    pub(crate) fn as_of(&self) -> i64 {
        tx_eid_from_tx_id(self.tx_key.tx_id)
    }

    pub(crate) fn range_stats(&self) -> &Arc<slatedb_estimates::RangeStats<M>> {
        &self.range_stats
    }

    pub fn entity(&self, _eid: Entid) {
        todo!()
    }
}

impl<D, M> Database for DB<D, M>
where
    D: slatedb::DbReadOps + Send + Sync + 'static,
    M: slatedb::DbMetadataOps + Send + Sync + 'static,
{
    async fn query(&self, query: impl IntoQuery) -> Result<QueryResult, Error> {
        let parsed = query.into_query()?;
        self.query_with_args(&parsed, &[]).await
    }

    /// Execute a query against this database basis.
    /// Runs the sync join algorithm in a blocking task to avoid blocking the async runtime.
    async fn query_with_args(
        &self,
        query: &ParsedQuery,
        args: &[QueryArg],
    ) -> Result<QueryResult, Error> {
        let db = Arc::new(self.clone());
        let query = query.clone();
        let args = args.to_vec();

        tokio::task::spawn_blocking(move || execute_query(&query, &args, db))
            .await
            .map_err(|e| anyhow::anyhow!("Query task failed: {}", e))?
    }
}
