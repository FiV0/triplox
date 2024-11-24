use std::sync::Arc;
use slatedb::db::Db;
use anyhow::{Error, Ok, Result};

use crate::ops::{TxOp, Document, Triple};
use crate::codec;
use crate::transaction::TxKey;
use crate::ops::DataType;

pub struct Indexer {
    slatedb: Arc<Db>,
}

struct TxIndexKeys {
    eav: Vec<Vec<u8>>,
    ave: Vec<Vec<u8>>,
    aev: Vec<Vec<u8>>,
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

    fn concat_index(parts: &[&[u8]]) -> Vec<u8> {
        let mut result = Vec::new();
        for part in parts {
            result.extend(*part);
        }
        result
    }

    fn op_to_index_keys(&self, _tx_key: TxKey, tx_op: TxOp) -> Result<TxIndexKeys, Error> {
        match tx_op {
            TxOp::Put(Document(doc)) => {
                let entity_id = match doc.get("db/id") {
                    Some(DataType::Long(uuid)) => uuid,
                    Some(_) => return Err(anyhow::anyhow!("Document db/id must be a long")),
                    None => return Err(anyhow::anyhow!("Document must have a db/id")),
                };
                let attribute_and_values = doc.iter().filter(|(k, _)| *k != "db/id").collect::<Vec<_>>();
                assert_attributes(&attribute_and_values.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>())?;

                let entity_id = bincode::serialize(&entity_id)?;
                let attribute_and_values = attribute_and_values
                    .iter()
                    .map(|(k, v)| -> Result<(Vec<u8>, Vec<u8>)> {
                        Ok((bincode::serialize(k)?, bincode::serialize(v)?))
                    }).collect::<Result<Vec<(Vec<u8>, Vec<u8>)>>>()?;

                let mut eav : Vec<Vec<u8>> = Vec::new();
                let mut ave : Vec<Vec<u8>> = Vec::new();
                let mut aev : Vec<Vec<u8>> = Vec::new();

                for (attribute, value) in attribute_and_values {
                    eav.push(Self::concat_index(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]));
                    ave.push(Self::concat_index(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]));
                    aev.push(Self::concat_index(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]));
                }

                Ok(TxIndexKeys { 
                    eav: eav, 
                    ave: ave, 
                    aev: aev 
                })

            },
            TxOp::Add(Triple { entity: entity_id, attribute, value }) => {
                let entity_id = bincode::serialize(&entity_id)?;
                let attribute = bincode::serialize(&attribute)?;
                let value = bincode::serialize(&value)?;

                let eav = Self::concat_index(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]);
                let ave = Self::concat_index(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]);
                let aev = Self::concat_index(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]);

                Ok(TxIndexKeys { 
                    eav: vec![eav], 
                    ave: vec![ave], 
                    aev: vec![aev] 
                })
            },
            TxOp::Retract(Triple { entity: entity_id, attribute, value }) => {
                let entity_id = bincode::serialize(&entity_id)?;
                let attribute = bincode::serialize(&attribute)?;
                let value = bincode::serialize(&value)?;

                let eav = Self::concat_index(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::RETRACT]]);
                let ave = Self::concat_index(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::RETRACT]]);
                let aev = Self::concat_index(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::RETRACT]]);

                Ok(TxIndexKeys { 
                    eav: vec![eav], 
                    ave: vec![ave], 
                    aev: vec![aev] 
                })  
            },
            TxOp::Delete(_entity) => todo!(),
            TxOp::Erase(_entity) => todo!(),
        }
    }


    pub fn transact_tx(&mut self, tx_key: TxKey, tx_ops: Vec<TxOp>) -> Result<TxKey, Error> {
        let index_keys = tx_ops.iter().map(|op| self.op_to_index_keys(tx_key, op)).collect::<Result<Vec<TxIndexKeys>>>()?;


        Ok(tx_key)
    }
}
