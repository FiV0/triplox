use edn::symbols::Keyword;

pub use triplox_client::ops::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DatomOp {
    Assert,
    Retract,
}

/// A normalized fact: (entity, attribute, value, op).
/// The attribute is an unresolved keyword ident.
/// The tx_eid is not stored here — it's passed separately to write_index_entries.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Datom {
    pub entity: i64,
    pub attribute: Keyword,
    pub value: DataType,
    pub op: DatomOp,
}
