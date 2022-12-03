use std::{
    collections::{HashMap, VecDeque},
    time::SystemTime,
};

use crate::{
    storage::util::Key,
    tx::{Op, TransactionId},
};

pub struct Lock(String);
pub type OpQueue = VecDeque<(u32, Op)>;

pub enum Protocol {
    Lock,
    Validation,
    Timestamp,
}

pub struct LockManager {
    locks: HashMap<Key, TransactionId>,
    queue: VecDeque<(TransactionId, Key)>,
    granted: Vec<TransactionId>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            queue: VecDeque::new(),
            granted: Vec::new(),
        }
    }

    pub fn request(&mut self, id: TransactionId, key: &str) -> bool {
        let key = key.to_owned();

        if self.locks.contains_key(&key) {
            println!(
                "[Lock Manager] Queueing lock request of {} from {}.",
                key, id
            );
            self.queue.push_back((id, key));
            return false;
        }

        println!(
            "[Lock Manager] Granted exclusive lock of {} to {}.",
            key, id
        );

        self.locks.insert(key, id);
        self.granted.push(id);

        true
    }

    pub fn release(&mut self, key: Key) {
        let lock_holder = self.locks.get(&key).unwrap();
        println!(
            "[Lock Manager] Releasing exclusive lock of {} from {}.",
            key, lock_holder
        );

        self.locks.remove(&key);
    }

    pub fn has(&mut self, id: TransactionId, key: &str) -> bool {
        &id == self.locks.get(key).unwrap_or(&0u32)
    }
}

pub struct TimestampManager {
    arrivals: HashMap<TransactionId, SystemTime>,
    validations: HashMap<TransactionId, SystemTime>,
    finish: HashMap<TransactionId, SystemTime>,
}

impl TimestampManager {
    pub fn new() -> Self {
        Self {
            arrivals: HashMap::new(),
            validations: HashMap::new(),
            finish: HashMap::new(),
        }
    }

    pub fn arrive(&mut self, id: TransactionId) {
        self.arrivals.insert(id, SystemTime::now());
    }

    pub fn get_arrival(&self, id: &TransactionId) -> &SystemTime {
        self.arrivals.get(id).expect("Id should exists")
    }

    pub fn validate(&mut self, id: TransactionId) {
        self.validations.insert(id, SystemTime::now());
    }

    pub fn finish(&mut self, id: TransactionId) {
        self.finish.insert(id, SystemTime::now());
    }

    pub fn finished(&self) -> &HashMap<TransactionId, SystemTime> {
        &self.finish
    }

    pub fn finished_after_start_of(&self, lhs: TransactionId, rhs: TransactionId) -> bool {
        let lhs_finish = self.finish.get(&lhs);
        let rhs_start = self.arrivals.get(&rhs).expect("Invalid transaction id");
        match lhs_finish {
            None => false,
            Some(finish) => finish > rhs_start,
        }
    }
}
