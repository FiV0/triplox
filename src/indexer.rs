use std::sync::Arc;
use slatedb::db::Db;
use anyhow::{Result, Error};

use crate::ops::{TxOp, Document, Triple};
use crate::codec;
use crate::transaction::TxKey;

struct Indexer {
    slatedb: Arc<Db>,
}

struct TxIndexPermutations<'a> {
    eav: Vec<&'a [u8]>,
    ave: Vec<&'a [u8]>,
    aev: Vec<&'a [u8]>,
}

fn assert_valid_attribute(attribute: &str) -> Result<(), Error> {
    if attribute.starts_with("db/") {
        return Err(anyhow::anyhow!("Attribute '{}' cannot start with db/", attribute));
    }
    Ok(())
}

fn assert_attributes(attributes: &[&str]) -> Result<(), Error> {
    for attribute in attributes {
        assert_valid_attribute(attribute)?;
    }
    Ok(())
}

impl Indexer {
    pub fn new(slatedb: Arc<Db>) -> Self {
        Indexer { slatedb }
    }

    fn op_to_keys(&self, tx_key: TxKey, tx_op: TxOp) -> Result<TxIndexPermutations, Error> {
        match tx_op {
            TxOp::Put(Document(doc)) => {
                let entity_id = match doc.get("db/id") {
                    Some(DataType::Long(uuid)) => uuid,
                    Some(_) => return Err(anyhow::anyhow!("Document db/id must be a long")),
                    None => return Err(anyhow::anyhow!("Document must have a db/id")),
                };
                let other_keys = doc.keys().filter(|k| k != "db/id").collect::<Vec<_>>();
                assert_attributes(&other_keys)?;
            },
            TxOp::Add(Triple { entity: entity_id, attribute, value }) => {

                let entity_id = bincode::serialize(&entity_id)?;
                let attribute = bincode::serialize(&attribute)?;
                let value = bincode::serialize(&value)?;

                let eav: Vec<u8> = [&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]].concat();
                let ave: Vec<u8> = [&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]].concat();
                let aev: Vec<u8> = [&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]].concat();

                Ok(TxIndexPermutations { 
                    eav: vec![&eav], 
                    ave: vec![&ave], 
                    aev: vec![&aev] 
                })
            },
            TxOp::Retract(Triple { entity: entity_id, attribute, value }) => {
                let entity_id = bincode::serialize(&entity_id)?;
                let attribute = bincode::serialize(&attribute)?;
                let value = bincode::serialize(&value)?;

                let eav: Vec<u8> = [&[codec::EAV], &entity_id, &attribute, &value, &[codec::DELETE]].concat();
                let ave: Vec<u8> = [&[codec::AVE], &attribute, &value, &entity_id, &[codec::DELETE]].concat();
                let aev: Vec<u8> = [&[codec::AEV], &attribute, &entity_id, &value, &[codec::DELETE]].concat();

                Ok(TxIndexPermutations { 
                    eav: vec![&eav], 
                    ave: vec![&ave], 
                    aev: vec![&aev] 
                })  
            },
            TxOp::Delete(entity) => todo!(),
            TxOp::Erase(entity) => todo!(),
        }
    }


    pub fn transact_tx(&mut self, tx_key: TxKey, tx_ops: Vec<TxOp>) -> Result<TxKey, Error> {
        todo!()
    }
}
