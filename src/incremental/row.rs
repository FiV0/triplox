//! Rows in incremental circuits: one `FlatVariant` array whose BINARY elements each hold one
//! encoded value. Cloning a row bumps a refcount, so DBSP batch and trace clones share row bytes.

use std::ops::Range;

use feldera_sqllib::flat_variant::{build_document_infallible, Container, TAG_BINARY};
use smallvec::SmallVec;

use crate::incremental::EncodedRow;

// Builds a row from encoded values in one allocation.
pub(crate) fn row_from_values<'a>(values: impl IntoIterator<Item = &'a [u8]>) -> EncodedRow {
    build_document_infallible(|writer| {
        let elements: SmallVec<[Range<usize>; 8]> = values
            .into_iter()
            .map(|value| writer.scalar(TAG_BINARY, value))
            .collect();
        writer.array(&elements)
    })
}

/// O(1) access to the encoded values of one row through its end-offset table.
pub(crate) struct RowValues<'a> {
    bytes: &'a [u8],
    container: Container<'a>,
}

impl<'a> RowValues<'a> {
    pub(crate) fn new(row: &'a EncodedRow) -> Self {
        let bytes = row.as_bytes();
        Self {
            bytes,
            container: Container::new(bytes),
        }
    }

    pub(crate) fn len(&self) -> usize {
        self.container.count
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub(crate) fn get(&self, position: usize) -> &'a [u8] {
        let element = self.container.element(position);
        debug_assert_eq!(self.bytes[element.start], TAG_BINARY);
        &self.bytes[element.start + 1..element.end]
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &'a [u8]> + '_ {
        (0..self.len()).map(|position| self.get(position))
    }
}

// The encoded value at one row position.
pub(crate) fn row_value(row: &EncodedRow, position: usize) -> &[u8] {
    RowValues::new(row).get(position)
}

// The row's encoded values as owned vectors, e.g. for test expectations.
pub(crate) fn row_to_vecs(row: &EncodedRow) -> Vec<Vec<u8>> {
    RowValues::new(row).iter().map(<[u8]>::to_vec).collect()
}

// Selects row values at the requested positions.
pub(crate) fn select_values(row: &EncodedRow, positions: &[usize]) -> EncodedRow {
    let values = RowValues::new(row);
    row_from_values(positions.iter().map(|position| values.get(*position)))
}

// Appends one encoded value to a row.
pub(crate) fn append_value(row: &EncodedRow, value: &[u8]) -> EncodedRow {
    let values = RowValues::new(row);
    row_from_values(values.iter().chain(std::iter::once(value)))
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::hash::{DefaultHasher, Hash, Hasher};

    use dbsp::dynamic::OrdRepr;
    use edn::Keyword;
    use feldera_sqllib::flat_variant::ArchivedFlatVariant;
    use uuid::Uuid;

    use super::*;
    use crate::codec::Encode;
    use crate::ops::DataType;

    fn sample_values() -> Vec<Vec<u8>> {
        vec![
            DataType::Long(-1).encode(),
            DataType::Long(0).encode(),
            DataType::Long(42).encode(),
            DataType::BigInt(i128::MAX).encode(),
            DataType::Boolean(true).encode(),
            DataType::Double(1.5).encode(),
            DataType::Float(-0.25).encode(),
            DataType::Instant(chrono::DateTime::from_timestamp(1_700_000_000, 0).unwrap()).encode(),
            DataType::String(String::new()).encode(),
            DataType::String("a".to_string()).encode(),
            DataType::String("a\u{0}b\u{1}".to_string()).encode(),
            DataType::String("ab".to_string()).encode(),
            DataType::Bytes(vec![0, 1, 2, 255]).encode(),
            DataType::Keyword(Keyword::namespaced("item", "open")).encode(),
            DataType::Uuid(Uuid::from_u128(7)).encode(),
            DataType::Vector(vec![DataType::Long(1), DataType::String("x".to_string())]).encode(),
            DataType::Map(BTreeMap::from([("k".to_string(), DataType::Long(1))])).encode(),
        ]
    }

    // Rows of length zero to three over the sample values, as plain vectors.
    fn sample_rows() -> Vec<Vec<Vec<u8>>> {
        let values = sample_values();
        let mut rows = vec![vec![]];
        for (i, a) in values.iter().enumerate() {
            rows.push(vec![a.clone()]);
            for b in values.iter().skip(i % 3).step_by(3) {
                rows.push(vec![a.clone(), b.clone()]);
                rows.push(vec![a.clone(), b.clone(), values[i % values.len()].clone()]);
            }
        }
        rows
    }

    fn build(values: &[Vec<u8>]) -> EncodedRow {
        row_from_values(values.iter().map(Vec::as_slice))
    }

    fn hash_of(row: &EncodedRow) -> u64 {
        let mut hasher = DefaultHasher::new();
        row.hash(&mut hasher);
        hasher.finish()
    }

    #[test]
    fn rows_round_trip_their_values() {
        for values in sample_rows() {
            let row = build(&values);
            assert_eq!(row_to_vecs(&row), values);
            let row_values = RowValues::new(&row);
            assert_eq!(row_values.len(), values.len());
            for (position, value) in values.iter().enumerate() {
                assert_eq!(row_value(&row, position), value.as_slice());
            }
        }
    }

    #[test]
    fn rows_order_like_vectors_of_values() {
        let rows = sample_rows();
        for a in &rows {
            for b in &rows {
                let (flat_a, flat_b) = (build(a), build(b));
                assert_eq!(flat_a.cmp(&flat_b), a.cmp(b), "{a:?} vs {b:?}");
                assert_eq!(flat_a == flat_b, a == b);
                if a == b {
                    assert_eq!(hash_of(&flat_a), hash_of(&flat_b));
                }
            }
        }
    }

    #[test]
    fn select_and_append_build_new_rows() {
        let values = sample_values();
        let row = build(&values[..4]);
        assert_eq!(
            row_to_vecs(&select_values(&row, &[2, 0])),
            vec![values[2].clone(), values[0].clone()]
        );
        assert!(RowValues::new(&select_values(&row, &[])).is_empty());
        assert_eq!(row_to_vecs(&append_value(&row, &values[5])), {
            let mut expected = values[..4].to_vec();
            expected.push(values[5].clone());
            expected
        });
    }

    #[test]
    fn archived_rows_compare_like_live_rows() {
        let rows = sample_rows();
        let archived = rows
            .iter()
            .map(|values| rkyv::to_bytes::<_, 256>(&build(values)).unwrap())
            .collect::<Vec<_>>();
        for (i, a) in rows.iter().enumerate() {
            let archived_a = unsafe { rkyv::archived_root::<EncodedRow>(&archived[i]) };
            let decoded: EncodedRow =
                rkyv::Deserialize::deserialize(archived_a, &mut rkyv::Infallible).unwrap();
            assert_eq!(row_to_vecs(&decoded), *a);
            for b in &rows {
                let live_b = build(b);
                let archived_cmp: &ArchivedFlatVariant = archived_a;
                assert_eq!(archived_cmp.ord_cmp(&live_b), a.cmp(b), "{a:?} vs {b:?}");
            }
        }
    }
}
