use regex::Regex;

use crate::concurrent::{LockManager, Protocol, TimestampManager};
use crate::storage::util::Key;
use crate::storage::{Log, StorageManager, VersionedStorageManager};
use std::collections::VecDeque;
use std::ffi::OsStr;
use std::sync::mpsc;
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::thread::JoinHandle;
use std::{collections::HashMap, ops::Deref};
use std::{fs, mem, thread, vec};

pub type TransactionId = u32;
type OpEntry = (u32, Op);

#[derive(PartialEq, Eq, Debug)]
pub enum Op {
    Read(String),
    Write(String, String),
    Assign(String, String),
    Commit(StorageManager),
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
    aborted: bool,
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
            aborted: false,
        }
    }

    pub fn exec(&mut self) {
        let ops = mem::take(&mut self.operations);

        for op in ops.iter() {
            match op {
                Op::Read(key) => self.read(key),
                Op::Write(key, _) => self.write(key),
                Op::Assign(key, value) => self.assign(key, value),
                Op::Commit(_) => {}
            }

            if self.aborted {
                break;
            }
        }

        self.operations = ops;

        if !self.aborted {
            self.commit();
        }
    }

    pub fn abort(&mut self) {
        self.aborted = true;
    }

    pub fn commit(&mut self) {
        self.sender
            .send((self.id, Op::Commit(mem::take(&mut self.frame))))
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
    versioned_storage_manager: VersionedStorageManager,
    last_id: TransactionId,
    remote_sender: SyncSender<OpEntry>,
    receiver: Receiver<OpEntry>,
    senders: HashMap<u32, Sender<OpMessage>>,
    commited: u32,
    aborted: u32,
    alg: Protocol,
    pool: Vec<JoinHandle<()>>,
    log: Vec<Log>,
    op_queue: VecDeque<OpEntry>,
    active_writers: HashMap<Key, Vec<TransactionId>>,
    writers: HashMap<Key, Vec<TransactionId>>,
    readers: HashMap<TransactionId, Vec<Key>>,
    commit_dependencies: HashMap<TransactionId, Vec<TransactionId>>,
}

impl TransactionManager {
    pub fn new(storage_manager: StorageManager, alg: Protocol) -> Self {
        let (sender, receiver) = sync_channel::<OpEntry>(mem::size_of::<OpEntry>());
        let versioned_storage_manager = VersionedStorageManager::new(&storage_manager);

        Self {
            lock_manager: LockManager::new(),
            ts_manager: TimestampManager::new(),
            storage_manager,
            versioned_storage_manager,
            last_id: 0,
            remote_sender: sender,
            receiver,
            senders: HashMap::new(),
            commited: 0,
            aborted: 0,
            alg,
            pool: vec![],
            log: vec![],
            op_queue: VecDeque::new(),
            active_writers: HashMap::new(),
            writers: HashMap::new(),
            readers: HashMap::new(),
            commit_dependencies: HashMap::new(),
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
        println!("Initial storage");
        self.storage_manager.print();
        println!();

        let mut finished = self.commited + self.aborted;

        while finished != self.last_id {
            let (id, op) = self.receiver.recv().expect("Receiver error");

            self.handle(id, op);

            finished = self.commited + self.aborted;
        }

        let pool = mem::replace(&mut self.pool, vec![]);

        for handle in pool.into_iter() {
            handle.join().unwrap();
        }

        println!("Storage after all transactions commited/aborted");
        self.storage_manager.print();
        println!();
    }

    pub fn handle(&mut self, id: TransactionId, op: Op) {
        match op {
            Op::Read(key) => self.handle_read(id, key),
            Op::Write(key, value) => self.handle_write(id, key, value),
            Op::Commit(_) => self.handle_commit(id),
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
                    let active_writer = self.active_writers.get(&key);

                    if !self.commit_dependencies.contains_key(&id) {
                        self.commit_dependencies.insert(id, vec![]);
                    }

                    let dependecies = self
                        .commit_dependencies
                        .get_mut(&id)
                        .expect("Dependencies should never be None.");

                    if active_writer.is_some() {
                        let active_writer = active_writer.unwrap();
                        for writer_id in active_writer.iter() {
                            if writer_id == &id {
                                continue;
                            }
                            if !dependecies.contains(writer_id) {
                                dependecies.push(*writer_id);
                            }
                        }
                    }

                    if !self.readers.contains_key(&id) {
                        self.readers.insert(id, vec![]);
                    }
                    let reader = self
                        .readers
                        .get_mut(&id)
                        .expect("Reader should never be None.");
                    if !reader.contains(&key) {
                        reader.push(key);
                    }

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
                        self.handle_abort(id);
                    } else {
                        // Wait
                        self.op_queue.push_back((id, Op::Read(key)));
                    }
                }
            }
            Protocol::Validation => {
                let value = self.storage_manager.read(&key).unwrap().to_owned();
                println!("[!] Read {} = {} for {}.", key, value, id);

                let sender = self.senders.get(&id).unwrap();
                let active_writer = self.active_writers.get(&key);

                if !self.commit_dependencies.contains_key(&id) {
                    self.commit_dependencies.insert(id, vec![]);
                }

                let dependecies = self
                    .commit_dependencies
                    .get_mut(&id)
                    .expect("Dependencies should never be None.");

                if active_writer.is_some() {
                    let active_writer = active_writer.unwrap();
                    for writer_id in active_writer.iter() {
                        if writer_id == &id {
                            continue;
                        }
                        if !dependecies.contains(writer_id) {
                            dependecies.push(*writer_id);
                        }
                    }
                }

                if !self.readers.contains_key(&id) {
                    self.readers.insert(id, vec![]);
                }
                let reader = self
                    .readers
                    .get_mut(&id)
                    .expect("Reader should never be None.");
                if !reader.contains(&key) {
                    reader.push(key);
                }

                sender
                    .send(OpMessage::Ok(value))
                    .expect("Sender manager read error");
            }
            Protocol::Timestamp => {
                let tx_timestamp = self.ts_manager.get_arrival(&id);
                let (value, last_writer_id) = self
                    .versioned_storage_manager
                    .read(&key, *tx_timestamp)
                    .to_owned();
                let sender = self.senders.get(&id).unwrap();
                let value = value.to_owned();

                sender
                    .send(OpMessage::Ok(value))
                    .expect("Sender manager read error");

                if last_writer_id != 0 {
                    self.add_commit_dependency(id, last_writer_id);
                }
            }
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
                        let sender = self.senders.get(&id).unwrap();
                        sender
                            .send(OpMessage::Abort)
                            .expect("Sender manager read error");
                        self.handle_abort(id);
                    } else {
                        // Wait
                        self.op_queue.push_back((id, Op::Write(key, value)));
                        return;
                    }
                }
            }
            Protocol::Validation => {
                let sender = self.senders.get(&id).unwrap();
                sender
                    .send(OpMessage::Ok(value))
                    .expect("Sender manager read error");
            }
            Protocol::Timestamp => {}
        }

        let written_value = self.storage_manager.read(&key).unwrap().to_owned();

        if !self.active_writers.contains_key(&key) {
            self.active_writers.insert(key.clone(), Vec::new());
            self.writers.insert(key.clone(), Vec::new());
        }
        let active_writer = self
            .active_writers
            .get_mut(&key)
            .expect("Active writer should never be None.");
        let writer = self
            .writers
            .get_mut(&key)
            .expect("Writer should never be None.");
        if !active_writer.contains(&id) {
            active_writer.push(id);
            writer.push(id);
        }

        self.log
            .push(Log::write(key).from(init_value).to(written_value).by(id));
    }

    pub fn handle_commit(&mut self, id: TransactionId) {
        match self.alg {
            Protocol::Lock => {
                self.commited += 1;
                println!("[!] {} successfully commited.", id);
                self.remove_writer(id);
                self.release_all_locks(id);
            }
            Protocol::Validation => {
                println!("[!] Validating {}", id);

                let read_keys = self.readers.get(&id).unwrap();
                let mut writer_ids = vec![];

                for key in read_keys.iter() {
                    let default = vec![];
                    let writers = self.writers.get(key).unwrap_or(&default);
                    for writer in writers.iter() {
                        writer_ids.push(*writer);
                    }
                }

                let mut valid = true;
                for (finished_tx, _) in self.ts_manager.finished().iter() {
                    if self.ts_manager.finished_after_start_of(*finished_tx, id)
                        && writer_ids.contains(finished_tx)
                    {
                        valid = false;
                        break;
                    }
                }

                if valid {
                    println!("[!] Validation success.");
                    println!("[!] {} successfully commited.", id);
                    self.ts_manager.validate(id);
                    self.remove_writer(id);
                    self.commited += 1;
                } else {
                    println!("[!] Validation failed.");
                    self.handle_abort(id);
                }
            }
            Protocol::Timestamp => {}
        }
    }

    fn handle_abort(&mut self, id: TransactionId) {
        let mut aborted_txs = vec![id];

        loop {
            let mut more_aborted_txs = vec![];
            for id in aborted_txs.iter() {
                let dependencies = self.commit_dependencies.get(id);
                if dependencies.is_some() {
                    for dependency in dependencies.unwrap().iter() {
                        more_aborted_txs.push(*dependency);
                    }
                }
            }

            if more_aborted_txs.len() == 0 {
                break;
            }

            for aborted_tx in more_aborted_txs.iter() {
                if !aborted_txs.contains(aborted_tx) {
                    aborted_txs.push(*aborted_tx);
                }
            }
        }

        for log in self.log.iter().rev() {
            if aborted_txs.contains(&log.writer()) {
                let key = log.key();
                let value = log.initial_value();

                println!("Rolling back {} to {}", key, value);

                self.storage_manager.write(key, value);
            }
        }

        for aborted_tx in aborted_txs.iter() {
            self.remove_writer(*aborted_tx);
            self.release_all_locks_unhandled(*aborted_tx);
            self.lock_manager.remove(*aborted_tx);

            let op_queue = mem::take(&mut self.op_queue);
            self.op_queue = op_queue
                .into_iter()
                .filter(|(id, _)| id == aborted_tx)
                .collect();

            self.aborted += 1;
        }

        self.handle_queued();
    }

    fn add_commit_dependency(&mut self, dependant: TransactionId, dependency: TransactionId) {
        if !self.commit_dependencies.contains_key(&dependant) {
            self.commit_dependencies.insert(dependant, vec![dependency]);
        } else {
            self.commit_dependencies
                .get_mut(&dependant)
                .unwrap()
                .push(dependency);
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

    fn release_all_locks_unhandled(&mut self, id: TransactionId) {
        self.lock_manager.release_all(id);
    }

    fn remove_writer(&mut self, id: TransactionId) {
        for (_, writer) in self.active_writers.iter_mut() {
            let index = writer.iter().position(|writer_id| writer_id == &id);
            if index.is_some() {
                writer.remove(index.unwrap());
            }
        }
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
