use std::time::{Instant, SystemTime};

pub trait SystemTimeSource {
    fn now(& mut self) -> SystemTime;
}

pub struct SystemClock;

impl SystemTimeSource for SystemClock {
    fn now(&mut self) -> SystemTime {
        SystemTime::now()
    }
}

pub struct MockClock {
    sys_times : Vec<SystemTime>
}

impl SystemTimeSource for MockClock{
    fn now(&mut self) -> SystemTime {
        if self.sys_times.len() == 0 {
            panic!("No more instants in the mock clock");
        }
        let res = self.sys_times[0];
        self.sys_times.remove(0);
        res
    }
}

pub struct FnMockClock {
    f: Box<dyn FnMut() -> SystemTime>,
}

impl FnMockClock {
    pub fn new(f: Box<dyn FnMut() -> SystemTime>) -> Self {
        FnMockClock { f }
    }

}

impl SystemTimeSource for FnMockClock {
    fn now(&mut self) -> SystemTime {
        (self.f)() // Call the stored closure
    }
}

pub fn st_from_unix_epoch(micros: u64) -> SystemTime {
    SystemTime::UNIX_EPOCH + std::time::Duration::from_micros(micros)
}

pub fn st_since_unix_epoch(st: SystemTime) -> u128 {
    st.duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mock_clock() {
        let base_instant = st_from_unix_epoch(0);
        let mut clock = MockClock {
            sys_times: vec![base_instant,
                            base_instant + std::time::Duration::from_secs(1),
                            base_instant + std::time::Duration::from_secs(2)]
        };

        assert_eq!(clock.now(), base_instant);
        assert_eq!(clock.now(), base_instant + std::time::Duration::from_secs(1));
        assert_eq!(clock.now(), base_instant + std::time::Duration::from_secs(2));
    }

    #[test]
    fn test_fn_mock_clock() {
        let base_instant = st_from_unix_epoch(0);
        let mut sys_times = vec![base_instant,
                                 base_instant + std::time::Duration::from_secs(1),
                                 base_instant + std::time::Duration::from_secs(2)];

        let mut clock = FnMockClock::new(Box::new(move || {
            if sys_times.len() == 0 {
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