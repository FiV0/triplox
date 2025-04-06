// use log::Record;
// use crate::log::Subscriber;
// use crate::memory_log::MemoryLog;


// struct MockSubscriber {
//     close_hook: Option<Box<dyn FnOnce() + Send>>,
//     records: Vec<Record>
// }

// impl Subscriber for MockSubscriber {
//     fn new() -> Self {
//         Self {
//             close_hook: None,
//             records: vec![],
//         }
//     }

//     fn on_subscribe(&self, close_hook: Box<dyn FnOnce() + Send>) {
//         self.close_hook = Some(close_hook);
//     }

//     fn accept(&self, record: Record) {
//         self.records.push(record);
//     }
// }

// impl Drop for MockSubscriber {
//     fn drop(&mut self) {
//         self.close_hook.take().unwrap()();
//     }
// }

// #[test]
// fn test_memory_log() {
//     let subscriber = MockSubscriber::new();
//     let log = MemoryLog::new(MockClock::new(vec![st_from_unix_epoch(0), st_from_unix_epoch(100), st_from_unix_epoch(200)]));
//     log.subscribe(subscriber);
//     log.append_tx(vec![1, 2, 3]);
//     log.append_tx(vec![4, 5, 6]);
//     log.append_tx(vec![7, 8, 9]);

//     thread::sleep(Duration::from_millis(100));

//     drop(subscriber);

//     assert_eq!(subscriber.records.len(), 3);
//     assert_eq!(subscriber.records[0], Record { tx_key: TxKey { tx_id: 0, tx_time: st_from_unix_epoch(0) }, record: vec![1, 2, 3] });
//     assert_eq!(subscriber.records[1], Record { tx_key: TxKey { tx_id: 1, tx_time: st_from_unix_epoch(100) }, record: vec![4, 5, 6] });
//     assert_eq!(subscriber.records[2], Record { tx_key: TxKey { tx_id: 2, tx_time: st_from_unix_epoch(200) }, record: vec![7, 8, 9] });
// }
