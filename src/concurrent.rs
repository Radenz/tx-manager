use std::{
    collections::{HashMap, VecDeque},
    mem,
    time::SystemTime,
    vec,
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

    pub fn release_all(&mut self, id: TransactionId) {
        println!("[Lock Manager] Releasing all locks granted to {}.", id);
        let mut keys = vec![];
        for (key, value) in self.locks.iter() {
            if value == &id {
                keys.push(key.to_owned());
            }
        }

        for key in keys.into_iter() {
            self.release(key);
        }
    }

    fn release(&mut self, key: Key) {
        self.locks.remove(&key);
        self.try_grant(key);
    }

    pub fn try_grant(&mut self, key: Key) {
        let mut id = 0;
        let mut index = 0;
        let mut found = false;

        for (i, entry) in self.queue.iter().enumerate() {
            if entry.1 == *key {
                index = i;
                id = entry.0;
                found = true;
                break;
            }
        }

        if found {
            self.queue.remove(index as usize);
            self.granted.push(id);
            println!("[Lock Manager] Granted exclusive lock of {} to {}", key, id);
        }
    }

    pub fn has(&mut self, id: TransactionId, key: &str) -> bool {
        &id == self.locks.get(key).unwrap_or(&0u32)
    }

    pub fn get_grantee(&self, key: &str) -> TransactionId {
        *self.locks.get(key).unwrap_or(&0u32)
    }

    pub fn get_granted(&mut self) -> Vec<TransactionId> {
        mem::take(&mut self.granted)
    }

    pub fn remove(&mut self, id: TransactionId) {
        let queue = mem::take(&mut self.queue);
        self.queue = queue
            .into_iter()
            .filter(|(tx_id, _)| tx_id != &id)
            .collect();
        let pos = self.granted.iter().position(|tx_id| tx_id == &id);
        if pos.is_some() {
            self.granted.remove(pos.unwrap());
        }
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

    pub fn validate(&mut self, id: TransactionId) {
        self.validations.insert(id, SystemTime::now());
    }

    pub fn finish(&mut self, id: TransactionId) {
        self.finish.insert(id, SystemTime::now());
    }

    pub fn is_earlier(&self, lhs: TransactionId, rhs: TransactionId) -> bool {
        let lhs_time = self.arrivals.get(&lhs).expect("Invalid transaction id");
        let rhs_time = self.arrivals.get(&rhs).expect("Invalid transaction id");

        lhs_time < rhs_time
    }
}
