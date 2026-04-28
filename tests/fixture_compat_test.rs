//! Cross-language byte fixtures for the msgpack wire codec.
//!
//! These tests pin the exact byte-level encoding produced by the Rust
//! codec. The same fixtures are decoded by the JVM client (see
//! `triplox-jvm/.../FixtureCompatTest.java`) — if both sides emit the same
//! bytes for the same canonical input and read back the same logical
//! values, the encodings are interchangeable.
//!
//! The fixtures live in `tests/fixtures/` and are committed to source
//! control. To regenerate after an intentional encoding change, set
//! `TRIPLOX_REGEN_FIXTURES=1` and rerun this test.

use std::collections::BTreeMap;
use std::fs;
use std::path::PathBuf;

use chrono::{TimeZone, Utc};
use edn::kw;
use uuid::Uuid;

use triplox::msgpack_codec::{
    data_type_from_value, encode_error_body, encode_query_response, write_data_type,
};
use triplox::ops::DataType;
use triplox::protocol::{ColumnDescription, TAG_LONG, TAG_STRING};

fn fixtures_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures")
}

fn check_or_regen(path: &str, bytes: &[u8]) {
    let full = fixtures_dir().join(path);
    if std::env::var_os("TRIPLOX_REGEN_FIXTURES").is_some() {
        fs::create_dir_all(full.parent().unwrap()).unwrap();
        fs::write(&full, bytes).unwrap();
        return;
    }
    let expected = fs::read(&full)
        .unwrap_or_else(|_| panic!("fixture missing: {}; rerun with TRIPLOX_REGEN_FIXTURES=1", full.display()));
    assert_eq!(
        bytes, &expected[..],
        "encoded bytes do not match fixture {}; rerun with TRIPLOX_REGEN_FIXTURES=1 if intentional",
        path
    );
}

/// A single map-of-DataType value covering every variant. Stable ordering
/// comes from the BTreeMap's lexicographic key sort.
fn data_type_zoo() -> DataType {
    let mut inner = BTreeMap::new();
    inner.insert("nested_long".to_string(), DataType::Long(1));
    let mut m = BTreeMap::new();
    m.insert("bigint".into(), DataType::BigInt(170141183460469231731687303715884105727)); // i128::MAX
    m.insert("boolean".into(), DataType::Boolean(true));
    m.insert("bytes".into(), DataType::Bytes(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    m.insert("double".into(), DataType::Double(std::f64::consts::PI));
    m.insert("float".into(), DataType::Float(1.5_f32));
    m.insert(
        "instant".into(),
        DataType::Instant(Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap()),
    );
    m.insert("keyword".into(), DataType::Keyword(kw!(:person/name)));
    m.insert("long".into(), DataType::Long(42));
    m.insert("map".into(), DataType::Map(inner));
    m.insert("string".into(), DataType::String("hello 世界".into()));
    m.insert(
        "uuid".into(),
        DataType::Uuid(Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").unwrap()),
    );
    m.insert(
        "vector".into(),
        DataType::Vector(vec![DataType::Long(1), DataType::Long(2), DataType::Long(3)]),
    );
    DataType::Map(m)
}

#[test]
fn data_type_zoo_fixture_matches() {
    let dt = data_type_zoo();
    let mut buf = Vec::new();
    write_data_type(&mut buf, &dt).unwrap();
    check_or_regen("data_type_zoo.mpk", &buf);

    // Decode the fixture and confirm logical equality.
    let bytes = fs::read(fixtures_dir().join("data_type_zoo.mpk")).unwrap();
    let mut cursor: &[u8] = &bytes;
    let value = rmpv::decode::read_value(&mut cursor).unwrap();
    let decoded = data_type_from_value(value).unwrap();
    assert_eq!(decoded, dt);
}

#[test]
fn query_response_fixture_matches() {
    let columns = vec![
        ColumnDescription {
            name: "?e".into(),
            data_type: TAG_LONG,
            members: None,
        },
        ColumnDescription {
            name: "?name".into(),
            data_type: TAG_STRING,
            members: None,
        },
    ];
    let rows = vec![
        vec![DataType::Long(1), DataType::String("alice".into())],
        vec![DataType::Long(2), DataType::String("bob".into())],
    ];
    let buf = encode_query_response(&columns, &rows);
    check_or_regen("query_response.mpk", &buf);
}

#[test]
fn error_response_fixture_matches() {
    let buf = encode_error_body(
        b'E',
        2001,
        "parse error: unexpected token",
        &Some("near position 17".into()),
        &None,
    );
    check_or_regen("error_response.mpk", &buf);
}
