/// Entity ID bit layout (from PARTITIONS.md):
///
/// ```text
///  63  62   61                    42  41                              0
/// +----+----+---------------------+---+-------------------------------+
/// | S  | 0  | partition (20 bits) |          counter (42 bits)        |
/// +----+----+---------------------+---+-------------------------------+
/// ```

pub const COUNTER_BITS: u32 = 42;
pub const COUNTER_MASK: i64 = (1i64 << 42) - 1;
pub const PARTITION_MASK: i64 = ((1i64 << 20) - 1) << 42;

pub const DB_PARTITION: u32 = 0;
pub const TX_PARTITION: u32 = 1;
pub const USER_PARTITION: u32 = 2;

/// Construct an entity ID from a partition number and counter value.
///
/// For partition 0, the result equals the counter (small, readable IDs).
pub fn make_entity_id(partition: u32, counter: i64) -> i64 {
    assert!(partition < (1 << 20), "partition must fit in 20 bits");
    assert!(counter >= 0 && counter <= COUNTER_MASK, "counter must fit in 42 bits");
    ((partition as i64) << COUNTER_BITS) | counter
}

/// Extract the partition number (bits 61–42) from an entity ID.
pub fn extract_partition(eid: i64) -> u32 {
    ((eid & PARTITION_MASK) >> COUNTER_BITS) as u32
}

/// Extract the counter value (bits 41–0) from an entity ID.
pub fn extract_counter(eid: i64) -> i64 {
    eid & COUNTER_MASK
}

/// Returns true if the entity ID is a tempid (sign bit set, i.e. negative).
pub fn is_tempid(eid: i64) -> bool {
    eid < 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_zero_preserves_counter() {
        assert_eq!(make_entity_id(DB_PARTITION, 0), 0);
        assert_eq!(make_entity_id(DB_PARTITION, 1), 1);
        assert_eq!(make_entity_id(DB_PARTITION, 31), 31);
        assert_eq!(make_entity_id(DB_PARTITION, 100), 100);
    }

    #[test]
    fn test_bootstrap_ids_unchanged() {
        // Schema attribute entity IDs (sequential from 0)
        assert_eq!(make_entity_id(DB_PARTITION, 0), crate::schema::DB_IDENT);
        assert_eq!(make_entity_id(DB_PARTITION, 1), crate::schema::DB_VALUE_TYPE);
        assert_eq!(make_entity_id(DB_PARTITION, 2), crate::schema::DB_CARDINALITY);
        // Cardinality enum entities
        assert_eq!(make_entity_id(DB_PARTITION, 17), crate::schema::DB_CARDINALITY_ONE);
        assert_eq!(make_entity_id(DB_PARTITION, 18), crate::schema::DB_CARDINALITY_MANY);
    }

    #[test]
    fn test_round_trip_db_partition() {
        let eid = make_entity_id(DB_PARTITION, 42);
        assert_eq!(extract_partition(eid), DB_PARTITION);
        assert_eq!(extract_counter(eid), 42);
    }

    #[test]
    fn test_round_trip_tx_partition() {
        let eid = make_entity_id(TX_PARTITION, 100);
        assert_eq!(extract_partition(eid), TX_PARTITION);
        assert_eq!(extract_counter(eid), 100);
        assert_eq!(eid, (1i64 << 42) | 100);
    }

    #[test]
    fn test_round_trip_user_partition() {
        let eid = make_entity_id(USER_PARTITION, 500);
        assert_eq!(extract_partition(eid), USER_PARTITION);
        assert_eq!(extract_counter(eid), 500);
        assert_eq!(eid, (2i64 << 42) | 500);
    }

    #[test]
    fn test_is_tempid() {
        assert!(!is_tempid(0));
        assert!(!is_tempid(1));
        assert!(!is_tempid(make_entity_id(USER_PARTITION, 100)));
        assert!(is_tempid(-1));
        assert!(is_tempid(-100));
        assert!(is_tempid(i64::MIN));
    }

    #[test]
    #[should_panic(expected = "partition must fit in 20 bits")]
    fn test_partition_overflow() {
        make_entity_id(1 << 20, 0);
    }

    #[test]
    #[should_panic(expected = "counter must fit in 42 bits")]
    fn test_counter_overflow() {
        make_entity_id(0, COUNTER_MASK + 1);
    }

    #[test]
    #[should_panic(expected = "counter must fit in 42 bits")]
    fn test_counter_negative() {
        make_entity_id(0, -1);
    }

    #[test]
    fn test_partitions_non_overlapping() {
        let db_max = make_entity_id(DB_PARTITION, COUNTER_MASK);
        let tx_min = make_entity_id(TX_PARTITION, 0);
        let tx_max = make_entity_id(TX_PARTITION, COUNTER_MASK);
        let user_min = make_entity_id(USER_PARTITION, 0);
        assert!(db_max < tx_min);
        assert!(tx_max < user_min);
    }
}
