use crate::concurrent::{LockManager, TimestampManager};
use crate::storage::StorageManager;
use std::sync::mpsc;
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::{collections::HashMap, ops::Deref};
use std::{mem, thread};

pub type TransactionId = u32;
type OpEntry = (u32, Op);

#[derive(Clone)]
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
                Op::Write(key, value) => self.write(key, value),
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

    pub fn write(&mut self, key: &str, value: &str) {
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

pub struct TransactionManager {
    lock_manager: LockManager,
    ts_manager: TimestampManager,
    last_id: TransactionId,
    remote_sender: SyncSender<OpEntry>,
    receiver: Receiver<OpEntry>,
    senders: HashMap<u32, Sender<OpMessage>>,
    commited: u32,
}

impl TransactionManager {
    pub fn new() -> Self {
        let (sender, receiver) = sync_channel::<OpEntry>(mem::size_of::<OpEntry>() * 8);

        Self {
            lock_manager: LockManager::new(),
            ts_manager: TimestampManager::new(),
            last_id: 0,
            remote_sender: sender,
            receiver,
            senders: HashMap::new(),
            commited: 0,
        }
    }

    pub fn exec(&mut self, ops: Vec<Op>) {
        let id = self.generate_id();
        let sender = self.remote_sender.clone();
        let (tx, rx) = mpsc::channel::<OpMessage>();

        self.senders.insert(id, tx);

        thread::spawn(move || {
            Transaction::new(id, ops, sender, rx).exec();
        });
    }

    pub fn run(&mut self) {
        let mut commited = self.commited;
        let txs = self.last_id;

        while commited != txs {
            let (id, op) = self.receiver.recv().unwrap();

            match op {
                Op::Read(key) => self.handle_read(id, key),
                Op::Write(_, _) => {}
                Op::Commit => {}
                _ => {}
            }
        }
    }

    pub fn handle_read(&mut self, id: TransactionId, key: String) {
        todo!("Check lock or timestamp")
    }

    pub fn handle_write(&mut self, id: TransactionId, key: String, value: String) {
        todo!("Check lock or timestamp")
    }

    pub fn handle_commit(&mut self, id: TransactionId, key: String, value: String) {
        todo!("Validate")
    }

    pub fn generate_id(&mut self) -> TransactionId {
        self.last_id += 1;
        return self.last_id;
    }
}
