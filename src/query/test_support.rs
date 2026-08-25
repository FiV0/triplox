use std::sync::Arc;

use tokio::runtime::Handle;
use triplox_client::transaction::TxKey;

use crate::clock::st_from_unix_epoch;
use crate::db::DB;
use crate::schema::IdentMap;
use crate::slate::SlateComponents;

pub(crate) fn db_at_tx_id(
    components: &SlateComponents,
    handle: &Handle,
    ident_map: IdentMap,
    tx_id: i64,
) -> Arc<DB> {
    Arc::new(DB::new(
        Arc::clone(&components.db),
        ident_map,
        handle.clone(),
        TxKey {
            tx_id,
            system_time: st_from_unix_epoch(0),
        },
        Arc::clone(&components.range_stats),
    ))
}
