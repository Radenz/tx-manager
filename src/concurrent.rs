use std::collections::{HashMap, VecDeque};

use crate::{
    storage::util::Key,
    tx::{Op, TransactionId},
};

pub struct Lock(String);
pub type OpQueue = VecDeque<(u32, Op)>;

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

    pub fn request(&mut self, id: TransactionId, key: Key) {
        if self.locks.contains_key(&key) {
            self.queue.push_back((id, key));
            return;
        }

        self.locks.insert(key, id);
        self.granted.push_back(id);
    }

    pub fn release(&mut self, _: TransactionId, key: Key) {
        self.locks.remove(&key);
        self.try_grant(key);
    }

    pub fn try_grant(&mut self, key: Key) {
        let mut id = 0;
        let mut index = 0;
        let mut found = false;

        for (i, entry) in self.queue.iter().enumerate() {
            if entry.1 == key {
                index = i;
                id = entry.0;
                found = true;
                break;
            }
        }

        if found {
            self.queue.remove(index as usize);
            self.granted.push_back(id);
        }
    }

    pub fn has(&mut self, _: TransactionId, key: Key) -> bool {
        self.locks.contains_key(&key)
    }
}

pub struct TimestampManager {}

impl TimestampManager {
    pub fn new() -> Self {
        unimplemented!()
    }
}
