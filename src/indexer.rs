#![allow(dead_code, unused)]

use std::collections::HashMap;
use std::sync::Arc;
use slatedb::Db;
use slatedb::WriteBatch;
use anyhow::{Error, Ok, Result};
use bincode;

use crate::ops::{Attribute, Document, Triple, TxOp};
use crate::codec;
use crate::transaction::TxKey;
use crate::ops::DataType;
use crate::slate::DEFAULT_WRITE_OPTIONS;
use crate::slate::{get_and_create_attribute_id, in_memory_slate, read_attribute_map};
use crate::util::concat_bytes;
use crate::clock::Instant;

pub struct Indexer {
    slatedb: Arc<Db>,
    attribute_to_id: HashMap<String, u64>
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
        let attribute_to_id = HashMap::new();
        Indexer { slatedb, attribute_to_id }
    }

    async fn op_to_index_keys(&self, _tx_key: TxKey, tx_op: &TxOp) -> Result<TxIndexKeys, Error> {
        let attribute_map = read_attribute_map(self.slatedb.clone()).await;
        match tx_op {
            TxOp::Put(Document(doc)) => {
                let entity_id = match doc.get("db/id") {
                    Some(DataType::Long(id)) => id,
                    Some(_) => return Err(anyhow::anyhow!("Document db/id must be a long")),
                    None => return Err(anyhow::anyhow!("Document must have a db/id")),
                };
                let attribute_and_values = doc.iter().filter(|(k, _)| *k != "db/id").collect::<Vec<_>>();
                assert_attributes(&attribute_and_values.iter().map(|(k, _)| k.as_str()).collect::<Vec<_>>())?;

                let entity_id = bincode::serialize(&entity_id)?;
                let attribute_and_values = attribute_and_values
                    .iter()
                    .map(|(k, v)| -> Result<(Vec<u8>, Vec<u8>)> {
                        let attribute_id = get_and_create_attribute_id(self.slatedb.clone(), k, &attribute_map);
                        Ok((bincode::serialize(&attribute_id)?, bincode::serialize(v)?))
                    }).collect::<Result<Vec<(Vec<u8>, Vec<u8>)>>>()?;

                let mut eav : Vec<Vec<u8>> = Vec::new();
                let mut ave : Vec<Vec<u8>> = Vec::new();
                let mut aev : Vec<Vec<u8>> = Vec::new();

                for (attribute, value) in attribute_and_values {
                    let value_len = bincode::serialize(&(value.len() as u64)).unwrap();
                    eav.push(concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value_len, &value, &[codec::ADD]]));
                    ave.push(concat_bytes(&[&[codec::AVE], &attribute, &value_len, &value, &entity_id, &[codec::ADD]]));
                    aev.push(concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value_len, &value, &[codec::ADD]]));
                }

                Ok(TxIndexKeys { 
                    eav: eav, 
                    ave: ave, 
                    aev: aev 
                })

            },
            TxOp::Add(Triple { entity: entity_id, attribute, value }) => {
                let Attribute(attr)= attribute;
                let entity_id = bincode::serialize(&entity_id)?;
                let attribute_id = get_and_create_attribute_id(self.slatedb.clone(), attr, &attribute_map);
                let attribute = bincode::serialize(&attribute_id)?;
                let value = bincode::serialize(&value)?;

                let eav = concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::ADD]]);
                let ave = concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::ADD]]);
                let aev = concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::ADD]]);

                Ok(TxIndexKeys { 
                    eav: vec![eav], 
                    ave: vec![ave], 
                    aev: vec![aev] 
                })
            },
            TxOp::Retract(Triple { entity: entity_id, attribute, value }) => {
                let Attribute(attr)= attribute;
                let entity_id = bincode::serialize(&entity_id)?;
                let attribute_id = get_and_create_attribute_id(self.slatedb.clone(), attr, &attribute_map);
                let attribute = bincode::serialize(&attribute_id)?;
                let value = bincode::serialize(&value)?;

                let eav = concat_bytes(&[&[codec::EAV], &entity_id, &attribute, &value, &[codec::RETRACT]]);
                let ave = concat_bytes(&[&[codec::AVE], &attribute, &value, &entity_id, &[codec::RETRACT]]);
                let aev = concat_bytes(&[&[codec::AEV], &attribute, &entity_id, &value, &[codec::RETRACT]]);

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
        let mut write_batch = WriteBatch::new();

        for (i, index_keys) in index_keys.iter().enumerate() {
            write_batch.put(index_keys.eav[i].as_slice(), &[]);
            write_batch.put(index_keys.ave[i].as_slice(), &[]);
            write_batch.put(index_keys.aev[i].as_slice(), &[]);
        }

        self.slatedb.write_with_options(write_batch, &DEFAULT_WRITE_OPTIONS);

        Ok(tx_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_indexer() {
        let indexer = Indexer::new(Arc::new(in_memory_slate().await));
        let tx_key = TxKey { tx_id: 0, system_time: Instant::now() };
        let doc = Document(HashMap::new().insert("db/id".to_string(), DataType::Long(1)).insert("name".to_string(), DataType::String("alan".to_string())));
        let tx_ops = vec![TxOp::Put(doc)];
        indexer.transact_tx(tx_key, tx_ops).await.unwrap();
    }
}
