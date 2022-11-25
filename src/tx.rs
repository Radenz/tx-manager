use regex::Regex;

use crate::concurrent::{LockManager, Protocol, TimestampManager};
use crate::storage::{Log, StorageManager};
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::sync::mpsc;
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::thread::JoinHandle;
use std::{collections::HashMap, ops::Deref};
use std::{fs, mem, thread, vec};

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
                Op::Commit => {}
            }
        }

        self.operations = ops;
        self.commit();
    }

    pub fn abort(&mut self) {
        unimplemented!()
    }

    pub fn commit(&mut self) {
        self.sender
            .send((self.id, Op::Commit))
            .expect("Sender read error");
    }

    pub fn read(&mut self, key: &str) {
        self.sender
            .send((self.id, Op::Read(key.to_owned())))
            .expect("Sender read error");

        let message = self.receiver.recv().expect("Receiver manager read error");

        match message {
            OpMessage::Abort => self.abort(),
            OpMessage::Ok(value) => self.frame.write(key, value.as_str()),
        }
    }

    pub fn write(&mut self, key: &str) {
        let value = self.frame.read(key).unwrap();
        self.sender
            .send((self.id, Op::Write(key.to_owned(), value.to_owned())))
            .expect("Sender write error");

        let message = self.receiver.recv().expect("Receiver manager write error");

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
    log: Vec<Log>,
    op_queue: VecDeque<OpEntry>,
}

impl TransactionManager {
    pub fn new(storage_manager: StorageManager, alg: Protocol) -> Self {
        // TODO: cap channel at 1 OpEntry
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
            log: vec![],
            op_queue: VecDeque::new(),
        }
    }

    pub fn exec(&mut self, tx_ops: Vec<Vec<Op>>) {
        for ops in tx_ops {
            let id = self.generate_id();
            let sender = self.remote_sender.clone();
            let (tx, rx) = mpsc::channel::<OpMessage>();

            self.senders.insert(id, tx);

            self.ts_manager.arrive(id);
            self.pool.push(thread::spawn(move || {
                Transaction::new(id, ops, sender, rx).exec();
            }));
        }
    }

    pub fn run(&mut self) {
        let mut commited = self.commited;

        while commited != self.last_id {
            let (id, op) = self.receiver.recv().expect("Receiver error");

            self.handle(id, op);

            commited = self.commited;
        }

        let pool = mem::replace(&mut self.pool, vec![]);

        for handle in pool.into_iter() {
            handle.join().unwrap();
        }
    }

    pub fn handle(&mut self, id: TransactionId, op: Op) {
        match op {
            Op::Read(key) => self.handle_read(id, key),
            Op::Write(key, value) => self.handle_write(id, key, value),
            Op::Commit => self.handle_commit(id),
            _ => {}
        }
    }

    pub fn handle_read(&mut self, id: TransactionId, key: String) {
        match self.alg {
            Protocol::Lock => {
                if self.has_lock(id, &key) {
                    let value = self.storage_manager.read(&key).unwrap().to_owned();
                    println!("[!] Read {} = {} for {}.", key, value, id);

                    let sender = self.senders.get(&id).unwrap();
                    sender
                        .send(OpMessage::Ok(value))
                        .expect("Sender manager read error");
                } else {
                    // Wait-die scheme

                    let grantee = self.lock_manager.get_grantee(&key);
                    if self.ts_manager.is_earlier(grantee, id) {
                        // Die
                        println!("[!] Aborting {} by wait-die scheme.", id);
                        let sender = self.senders.get(&id).unwrap();
                        sender
                            .send(OpMessage::Abort)
                            .expect("Sender manager read error");
                        self.release_all_locks(id);
                    } else {
                        // Wait
                        self.op_queue.push_back((id, Op::Read(key)));
                    }
                }
            }
            Protocol::Validation => {}
            Protocol::Timestamp => {}
        }
    }

    pub fn handle_write(&mut self, id: TransactionId, key: String, value: String) {
        let init_value = self.storage_manager.read(&key).unwrap().to_owned();

        match self.alg {
            Protocol::Lock => {
                if self.has_lock(id, &key) {
                    self.storage_manager.write(&key, &value);
                    println!("[!] Wrote {} = {} by {}.", key, value, id);
                    let sender = self.senders.get(&id).unwrap();

                    sender
                        .send(OpMessage::Ok(value))
                        .expect("Sender manager read error");
                } else {
                    let grantee = self.lock_manager.get_grantee(&key);
                    if self.ts_manager.is_earlier(grantee, id) {
                        // Die
                        println!("[!] Aborting {} by wait-die scheme.", id);
                        // let sender = self.senders.get(&id).unwrap();
                        // sender
                        //     .send(OpMessage::Abort)
                        //     .expect("Sender manager read error");
                        self.release_all_locks(id);
                    } else {
                        // Wait
                        self.op_queue.push_back((id, Op::Write(key, value)));
                        return;
                    }
                }
            }
            Protocol::Validation => {}
            Protocol::Timestamp => {}
        }

        let written_value = self.storage_manager.read(&key).unwrap().to_owned();
        self.log
            .push(Log::write(key).from(init_value).to(written_value).by(id));
    }

    pub fn handle_commit(&mut self, id: TransactionId) {
        match self.alg {
            Protocol::Lock => {
                self.commited += 1;
                println!("[!] {} successfully commited.", id);
                self.release_all_locks(id);
            }
            Protocol::Validation => {}
            Protocol::Timestamp => {}
        }
    }

    pub fn generate_id(&mut self) -> TransactionId {
        self.last_id += 1;
        return self.last_id;
    }

    fn has_lock(&mut self, id: TransactionId, key: &str) -> bool {
        self.lock_manager.has(id, key) || self.lock_manager.request(id, key)
    }

    fn release_all_locks(&mut self, id: TransactionId) {
        self.lock_manager.release_all(id);
        self.handle_queued();
    }

    fn handle_queued(&mut self) {
        let granted = self.lock_manager.get_granted();
        let mut entry_count = self.op_queue.len();
        let mut index = 0;

        while index < entry_count {
            let entry = self.op_queue.get(index).unwrap();
            let is_granted = granted.iter().position(|&id| id == entry.0).is_some();

            if is_granted {
                let (id, op) = self.op_queue.remove(index).unwrap();
                self.handle(id, op);
                entry_count -= 1;
            } else {
                index += 1;
            }
        }
    }
}

pub type ParseResult = (Option<StorageManager>, Vec<Vec<Op>>);

pub fn parse_dir(dir: String) -> ParseResult {
    let dir = fs::read_dir(dir).expect("Failed to read the directory.");
    let mut tx_ops = vec![];
    let mut storage_manager = None;

    for entry in dir {
        let entry = entry.expect("Failed to read directory entry.");
        let path = entry.path();
        let file_name = path.file_name().expect("Failed to extract path file name.");
        let is_init = file_name == OsStr::new("init.txt");
        let content = fs::read_to_string(path).expect("Failed to read file.");

        if is_init {
            storage_manager = Some(StorageManager::parse(&content));
            continue;
        }

        tx_ops.push(parse_ops(&content));
    }

    (storage_manager, tx_ops)
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
