// TODO: remove unused warnings
#![allow(unused)]
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Timelike, Utc};

pub type Instant = DateTime<Utc>;

pub trait SystemTimeSource: Send + Sync + 'static {
    fn now(&mut self) -> Instant;
}

pub struct SystemClock;

impl SystemTimeSource for SystemClock {
    fn now(&mut self) -> Instant {
        // Truncate to microsecond precision — all persistent encodings
        // (encode_timestamp, encode_instant) use microseconds, so sub-µs
        // nanoseconds would be silently dropped on round-trip.
        let now = Utc::now();
        now.with_nanosecond((now.timestamp_subsec_nanos() / 1_000) * 1_000)
            .expect("truncated nanoseconds should always be valid")
    }
}

pub struct MockClock {
    sys_times: Vec<Instant>,
}

impl MockClock {
    pub fn new(sys_times: Vec<Instant>) -> Self {
        Self { sys_times }
    }
}

impl SystemTimeSource for MockClock {
    fn now(&mut self) -> Instant {
        if self.sys_times.is_empty() {
            panic!("No more instants in the mock clock");
        }
        let res = self.sys_times[0];
        self.sys_times.remove(0);
        res
    }
}

pub struct FnMockClock {
    f: Box<dyn FnMut() -> Instant + Send + Sync>,
}

impl FnMockClock {
    pub fn new(f: Box<dyn FnMut() -> Instant + Send + Sync>) -> Self {
        FnMockClock { f }
    }
}

impl SystemTimeSource for FnMockClock {
    fn now(&mut self) -> Instant {
        (self.f)() // Call the stored closure
    }
}

#[allow(unused, deprecated)]
pub fn st_from_unix_epoch(micros: u64) -> Instant {
    let seconds = micros / 1_000_000;
    let nanoseconds = (micros % 1_000_000) * 1000;
    Utc.timestamp(seconds as i64, nanoseconds as u32)
}

#[allow(unused)]
pub fn st_micros_since_unix_epoch(inst: Instant) -> u128 {
    inst.timestamp_micros() as u128
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_clock() {
        let base_instant = st_from_unix_epoch(0);
        let mut clock = MockClock {
            sys_times: vec![
                base_instant,
                base_instant + std::time::Duration::from_secs(1),
                base_instant + std::time::Duration::from_secs(2),
            ],
        };

        assert_eq!(clock.now(), base_instant);
        assert_eq!(clock.now(), base_instant + std::time::Duration::from_secs(1));
        assert_eq!(clock.now(), base_instant + std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_fn_mock_clock() {
        let base_instant = st_from_unix_epoch(0);
        let mut sys_times = vec![
            base_instant,
            base_instant + std::time::Duration::from_secs(1),
            base_instant + std::time::Duration::from_secs(2),
        ];

        let mut clock = FnMockClock::new(Box::new(move || {
            if sys_times.is_empty() {
                panic!("No more instants in the mock clock");
            }
            let res = sys_times[0];
            sys_times.remove(0);
            res
        }));

        assert_eq!(clock.now(), base_instant);
        assert_eq!(clock.now(), base_instant + std::time::Duration::from_secs(1));
        assert_eq!(clock.now(), base_instant + std::time::Duration::from_secs(2));
    }
}
