use regex::Regex;

use crate::concurrent::{LockManager, Protocol, TimestampManager};
use crate::storage::StorageManager;
use std::sync::mpsc;
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::thread::JoinHandle;
use std::{collections::HashMap, ops::Deref};
use std::{mem, thread, vec};

pub type TransactionId = u32;
type OpEntry = (u32, Op);

#[derive(Clone, PartialEq, Eq, Debug)]
pub enum Op {
    Read(String),
    Write(String, String),
    Assign(String, String),
    Commit,
}

pub enum OpMessage {
    Ok(String),
    Abort,
}

pub struct Transaction {
    id: TransactionId,
    operations: Vec<Op>,
    frame: StorageManager,
    sender: SyncSender<OpEntry>,
    receiver: Receiver<OpMessage>,
}

impl Transaction {
    pub fn new(
        id: TransactionId,
        ops: Vec<Op>,
        sender: SyncSender<OpEntry>,
        receiver: Receiver<OpMessage>,
    ) -> Self {
        Self {
            id,
            operations: ops,
            frame: StorageManager::new(),
            sender,
            receiver,
        }
    }

    pub fn exec(&mut self) {
        let ops = mem::take(&mut self.operations);

        for op in ops.iter() {
            match op {
                Op::Read(key) => self.read(key),
                Op::Write(key, _) => self.write(key),
                Op::Assign(key, value) => self.assign(key, value),
                Op::Commit => self.commit(),
            }
        }

        self.operations = ops;
    }

    pub fn abort(&mut self) {
        unimplemented!()
    }

    pub fn commit(&mut self) {
        unimplemented!()
    }

    pub fn read(&mut self, key: &str) {
        self.sender
            .send((self.id, Op::Read(key.to_owned())))
            .unwrap();

        let message = self.receiver.recv().unwrap();

        match message {
            OpMessage::Abort => self.abort(),
            OpMessage::Ok(value) => self.frame.write(key, value.as_str()),
        }
    }

    pub fn write(&mut self, key: &str) {
        let value = self.frame.read(key).unwrap();
        self.sender
            .send((self.id, Op::Write(key.to_owned(), value.to_owned())))
            .unwrap();

        let message = self.receiver.recv().unwrap();

        match message {
            OpMessage::Abort => self.abort(),
            OpMessage::Ok(value) => self.frame.write(key, value.as_str()),
        }
    }

    pub fn assign(&mut self, key: &str, value: &str) {
        self.frame.write(key, value);
    }
}

impl Deref for Transaction {
    type Target = Vec<Op>;

    fn deref(&self) -> &Self::Target {
        &self.operations
    }
}

pub fn parse_ops(raw: &str) -> Vec<Op> {
    let mut ops = Vec::new();

    let multiline_regex = Regex::new(r"(\r?\n)+").unwrap();
    let whitespaces_regex = Regex::new(r"\s+").unwrap();

    let lines: Vec<&str> = multiline_regex.split(raw).collect();
    let entries: Vec<Vec<&str>> = lines
        .into_iter()
        .map(|line| whitespaces_regex.split(line.trim()).collect())
        .collect();

    for entry in entries {
        let len = entry.len();
        if len == 0 {
            continue;
        }

        let op = entry[0];

        match op {
            "W" | "w" | "Write" => {
                if len == 2 {
                    let key = entry[1];
                    ops.push(Op::Write(key.to_owned(), String::new()));
                }
            }
            "R" | "r" | "Read" => {
                if len == 2 {
                    let key = entry[1];
                    ops.push(Op::Read(key.to_owned()));
                }
            }
            "A" | "a" | "Assign" => {
                if len == 3 {
                    let key = entry[1];
                    let value = entry[2];
                    ops.push(Op::Assign(key.to_owned(), value.to_owned()));
                }
            }
            _ => continue,
        }
    }

    ops
}

pub struct TransactionManager {
    lock_manager: LockManager,
    ts_manager: TimestampManager,
    storage_manager: StorageManager,
    last_id: TransactionId,
    remote_sender: SyncSender<OpEntry>,
    receiver: Receiver<OpEntry>,
    senders: HashMap<u32, Sender<OpMessage>>,
    commited: u32,
    alg: Protocol,
    pool: Vec<JoinHandle<()>>,
}

impl TransactionManager {
    pub fn new(storage_manager: StorageManager, alg: Protocol) -> Self {
        let (sender, receiver) = sync_channel::<OpEntry>(mem::size_of::<OpEntry>() * 8);

        Self {
            lock_manager: LockManager::new(),
            ts_manager: TimestampManager::new(),
            storage_manager,
            last_id: 0,
            remote_sender: sender,
            receiver,
            senders: HashMap::new(),
            commited: 0,
            alg,
            pool: vec![],
        }
    }

    pub fn exec(&mut self, ops: Vec<Op>) {
        let id = self.generate_id();
        let sender = self.remote_sender.clone();
        let (tx, rx) = mpsc::channel::<OpMessage>();

        self.senders.insert(id, tx);

        self.pool.push(thread::spawn(move || {
            Transaction::new(id, ops, sender, rx).exec();
        }));
    }

    pub fn run(&mut self) {
        let mut commited = self.commited;
        let txs = self.last_id;

        // while not all transactions has been committed
        while commited != txs {
            let (id, op) = self.receiver.recv().unwrap();

            match op {
                Op::Read(key) => self.handle_read(id, key),
                Op::Write(key, value) => self.handle_write(id, key, value),
                Op::Commit => self.handle_commit(id),
                _ => {}
            }

            commited = self.commited;
        }

        let pool = mem::replace(&mut self.pool, vec![]);

        for handle in pool.into_iter() {
            handle.join().unwrap();
        }
    }

    pub fn handle_read(&mut self, id: TransactionId, key: String) {
        match self.alg {
            Protocol::Lock => {
                let granted = self.lock_manager.request(id, &key);

                if granted {
                    let value = self.storage_manager.read(&key).unwrap().to_owned();
                    let sender = self.senders.get(&id).unwrap();

                    sender.send(OpMessage::Ok(value)).unwrap();
                }
            }
            Protocol::Validation => {}
            Protocol::Timestamp => {}
        }

        todo!("Check lock or timestamp")
    }

    pub fn handle_write(&mut self, id: TransactionId, key: String, value: String) {
        todo!("Check lock or timestamp")
    }

    pub fn handle_commit(&mut self, id: TransactionId) {
        match self.alg {
            Protocol::Lock => {
                self.release_all_locks(id);
                self.commited += 1;
            }
            Protocol::Validation => {}
            Protocol::Timestamp => {}
        }

        todo!("Validate and check released lock")
    }

    pub fn generate_id(&mut self) -> TransactionId {
        self.last_id += 1;
        return self.last_id;
    }

    fn release_all_locks(&mut self, id: TransactionId) {
        self.lock_manager.release_all(id);
    }
}

#[cfg(test)]
mod tests {
    use crate::tx::{parse_ops, Op};

    #[test]
    fn test_parse() {
        let ops = parse_ops(
            "
                Below is an empty line. This line will not be parsed.

                Correct read
                R A
                Read A
                r A
                Wrong read
                R
                R A B
                Correct write
                W B
                w B
                Write B
                Wrong write
                W 
                W S abc
                Correct assign
                A C abc
                a C abc
                Assign C abc
                Wrong assign
                Assign M 
                ",
        );

        let read_op = Op::Read(String::from("A"));
        let write_op = Op::Write(String::from("B"), String::from(""));
        let assign_op = Op::Assign(String::from("C"), String::from("abc"));

        assert_eq!(ops.len(), 9);
        assert_eq!(ops.get(0), Some(&read_op));
        assert_eq!(ops.get(1), Some(&read_op));
        assert_eq!(ops.get(2), Some(&read_op));

        assert_eq!(ops.get(3), Some(&write_op));
        assert_eq!(ops.get(4), Some(&write_op));
        assert_eq!(ops.get(5), Some(&write_op));

        assert_eq!(ops.get(6), Some(&assign_op));
        assert_eq!(ops.get(7), Some(&assign_op));
        assert_eq!(ops.get(8), Some(&assign_op));

        assert_eq!(ops.get(9), None);
    }
}
