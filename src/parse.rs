use anyhow::{anyhow, Result};

use edn::parse::parse_query as edn_parse_query;
use edn::query::ParsedQuery;

pub fn parse_query(input: &str) -> Result<ParsedQuery> {
    edn_parse_query(input).map_err(|e| anyhow!("EDN parse error: {}", e))
}
