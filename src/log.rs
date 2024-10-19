use serde::{Deserialize, Serialize};
use crate::transaction::TxKey;

#[derive(Serialize, Deserialize, Debug)]
struct Record {
    tx_key: TxKey,
    record: Vec<u8>,
}

#[allow(unused)]
trait Subscriber {

}