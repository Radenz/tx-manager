use std::{
    collections::{HashMap, VecDeque},
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
    granted: VecDeque<TransactionId>,
}

impl LockManager {
    pub fn new() -> Self {
        Self {
            locks: HashMap::new(),
            queue: VecDeque::new(),
            granted: VecDeque::new(),
        }
    }

    pub fn request(&mut self, id: TransactionId, key: &str) -> bool {
        let key = key.to_owned();

        if self.locks.contains_key(&key) {
            println!("[!] Queueing lock request of {} from {}.", key, id);
            self.queue.push_back((id, key));
            return false;
        }

        println!("[!] Granted exclusive lock of {} to {}.", key, id);

        self.locks.insert(key, id);
        self.granted.push_back(id);

        true
    }

    pub fn release_all(&mut self, id: TransactionId) {
        println!("[!] Releasing all locks granted to {}.", id);
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
            self.granted.push_back(id);
            println!("[!] Granted exclusive lock of {} to {}", key, id);
        }
    }

    pub fn has(&mut self, id: TransactionId, key: &str) -> bool {
        &id == self.locks.get(key).unwrap_or(&0u32)
    }
}

pub struct TimestampManager {}

impl TimestampManager {
    pub fn new() -> Self {
        // unimplemented!()
        Self {}
    }
}
