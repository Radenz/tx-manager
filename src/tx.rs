use regex::Regex;

use crate::concurrent::{LockManager, Protocol, TimestampManager};
use crate::storage::util::Key;
use crate::storage::{Log, StorageManager, VersionedStorageManager};
use std::collections::HashMap;
use std::collections::HashSet;
use std::ffi::OsStr;
use std::sync::mpsc;
use std::sync::mpsc::{sync_channel, Receiver, Sender, SyncSender};
use std::thread::JoinHandle;
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
    aborted_txs: HashSet<TransactionId>,
    commited_txs: HashSet<TransactionId>,
    alg: Protocol,
    pool: Vec<JoinHandle<()>>,
    log: Vec<Log>,
    read_sets: HashMap<TransactionId, HashSet<Key>>,
    write_sets: HashMap<TransactionId, HashSet<Key>>,
    commit_dependencies: HashMap<TransactionId, Vec<TransactionId>>,
    names: HashMap<TransactionId, String>,
    commit_queue: Vec<(TransactionId, StorageManager)>,
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
            aborted_txs: HashSet::new(),
            commited_txs: HashSet::new(),
            alg,
            pool: vec![],
            log: vec![],
            read_sets: HashMap::new(),
            write_sets: HashMap::new(),
            commit_dependencies: HashMap::new(),
            names: HashMap::new(),
            commit_queue: vec![],
        }
    }

    pub fn exec(&mut self, tx_ops: Vec<Vec<Op>>, mut names: Vec<String>) {
        for ops in tx_ops {
            let id = self.generate_id();
            let name = names.remove(0);
            let sender = self.remote_sender.clone();
            let (tx, rx) = mpsc::channel::<OpMessage>();

            self.read_sets.insert(id, HashSet::new());
            self.write_sets.insert(id, HashSet::new());
            self.senders.insert(id, tx);
            self.names.insert(id, name);

            self.ts_manager.arrive(id);
            self.pool.push(thread::spawn(move || {
                Transaction::new(id, ops, sender, rx).exec();
            }));
        }
    }

    pub fn run(&mut self) {
        println!("\x1b[1;96m[INITIALIZATION]\x1b[0m");
        println!("\x1b[1;34m[?]\x1b[0m Initial storage");
        self.storage_manager.print();
        println!();

        println!("\x1b[1;96m[EXECUTION]\x1b[0m");
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

        println!();
        println!("\x1b[1;96m[SUMMARY]\x1b[0m");
        if self.aborted == 0 {
            println!("\x1b[1;34m[?]\x1b[0m All transactions commited.");
        } else {
            let commited_txs_str: Vec<String> = self
                .commited_txs
                .iter()
                .map(|tx| self.get_name(*tx).clone())
                .collect();
            let aborted_txs_str: Vec<String> = self
                .aborted_txs
                .iter()
                .map(|tx| self.get_name(*tx).clone())
                .collect();
            println!(
                "\x1b[1;34m[?]\x1b[0m Commited transactions: {}",
                commited_txs_str.join(", ")
            );
            println!(
                "\x1b[1;34m[?]\x1b[0m Aborted transactions: {}",
                aborted_txs_str.join(", ")
            );
        }
        println!("\x1b[1;34m[?]\x1b[0m Storage after all transactions commited/aborted");
        self.storage_manager.print();
        println!();
    }

    pub fn handle(&mut self, id: TransactionId, op: Op) {
        match op {
            Op::Read(key) => self.handle_read(id, key),
            Op::Write(key, value) => self.handle_write(id, key, value),
            Op::Commit(frame) => self.handle_commit(id, frame),
            _ => {}
        }
    }

    pub fn handle_read(&mut self, id: TransactionId, key: String) {
        match self.alg {
            Protocol::Lock => {
                if self.has_lock(id, &key) {
                    let value = self.storage_manager.read(&key).unwrap().to_owned();
                    self.print_read(&key, &value, id);

                    let sender = self.senders.get(&id).unwrap();
                    sender
                        .send(OpMessage::Ok(value))
                        .expect("Sender manager read error");

                    self.lock_manager.release(key);
                }
            }
            Protocol::Validation => {
                let value = self.storage_manager.read(&key).unwrap().to_owned();
                self.print_read(&key, &value, id);
                self.put_to_read_set(id, key);

                let sender = self.senders.get(&id).unwrap();

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
                self.print_read(&key, &value, id);

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
                    self.print_write(&key, &value, id);
                    let sender = self.senders.get(&id).unwrap();

                    sender
                        .send(OpMessage::Ok(value))
                        .expect("Sender manager read error");
                    self.lock_manager.release(key);
                }
                return;
            }
            Protocol::Validation => {
                let sender = self.senders.get(&id).unwrap();
                sender
                    .send(OpMessage::Ok(value))
                    .expect("Sender manager read error");
                self.put_to_write_set(id, key);

                return;
            }
            Protocol::Timestamp => {
                let tx_timestamp = self.ts_manager.get_arrival(&id);
                let res =
                    self.versioned_storage_manager
                        .write(&key, id, *tx_timestamp, value.clone());

                if res.is_err() {
                    self.handle_abort(id, true);
                } else {
                    let sender = self.senders.get(&id).unwrap();
                    sender
                        .send(OpMessage::Ok(value))
                        .expect("Sender manager read error");
                    self.put_to_write_set(id, key.clone());
                }
            }
        }

        let written_value = self.storage_manager.read(&key).unwrap().to_owned();

        self.log
            .push(Log::write(key).from(init_value).to(written_value).by(id));
    }

    pub fn handle_commit(&mut self, id: TransactionId, frame: StorageManager) {
        if self.aborted_txs.contains(&id) {
            return;
        }

        match self.alg {
            Protocol::Lock => {
                self.commited += 1;
                self.print_commit(id);
                self.commited_txs.insert(id);
            }
            Protocol::Validation => {
                println!("\x1b[1;34m[?]\x1b[0m Validating {}", self.get_name(id));

                let mut valid = true;
                let read_set = self.read_sets.get(&id).unwrap();

                for (finished_tx, _) in self.ts_manager.finished().iter() {
                    if self.ts_manager.finished_after_start_of(*finished_tx, id) {
                        let write_set = self.write_sets.get(finished_tx).unwrap();

                        if read_set.intersection(write_set).count() > 0 {
                            valid = false;
                            break;
                        }
                    }
                }

                if valid {
                    println!("\x1b[1;92m[!]\x1b[0m Validation success.");

                    let write_set = self.write_sets.get(&id).unwrap();
                    for (key, value) in frame.iter() {
                        if write_set.contains(key) {
                            self.storage_manager.write(key, value);
                            self.print_write(key, value, id);
                        }
                    }

                    self.ts_manager.validate(id);
                    self.commited += 1;
                    self.print_commit(id);
                    self.commited_txs.insert(id);
                } else {
                    println!("\x1b[1;91m[-]\x1b[0m Validation failed.");
                    self.handle_abort(id, false);
                }
            }
            Protocol::Timestamp => {
                self.try_commit(id, frame);
            }
        }

        self.ts_manager.finish(id);
    }

    fn try_commit(&mut self, id: TransactionId, frame: StorageManager) {
        if self.has_commit_dependencies(id) {
            if self
                .commit_queue
                .iter()
                .position(|(tx_id, _)| *tx_id == id)
                .is_none()
            {
                println!("\x1b[1;93m[-]\x1b[0m \x1b[93mDeferring commit of {} because of dependencies.\x1b[0m", self.get_name(id));
                self.commit_queue.push((id, frame));
            }
            return;
        }

        let write_set = self.write_sets.get(&id).unwrap();
        for (key, value) in frame.iter() {
            if write_set.contains(key) {
                self.storage_manager.write(key, value);
                self.print_write(key, value, id);
            }
        }

        self.ts_manager.validate(id);
        self.commited += 1;
        self.print_commit(id);
        self.commited_txs.insert(id);

        if let Some(commit_queue_index) =
            self.commit_queue.iter().position(|(tx_id, _)| *tx_id == id)
        {
            // Remove from commit queue
            self.commit_queue.remove(commit_queue_index);
        }

        // Try commit dependants
        let mut try_commit_list = vec![];
        if let Some(dependants) = self.commit_dependencies.get(&id) {
            for dependant in dependants.iter() {
                if let Some(commit_queue_index) = self
                    .commit_queue
                    .iter()
                    .position(|(tx_id, _)| tx_id == dependant)
                {
                    try_commit_list.push(self.commit_queue.remove(commit_queue_index));
                }
            }
        }

        for (id, frame) in try_commit_list.into_iter() {
            self.try_commit(id, frame);
        }
    }

    fn has_commit_dependencies(&self, id: TransactionId) -> bool {
        for (dependency, dependants) in self.commit_dependencies.iter() {
            if !self.commited_txs.contains(&dependency) && dependants.contains(&id) {
                return true;
            }
        }

        false
    }

    fn handle_abort(&mut self, id: TransactionId, with_send: bool) {
        let mut aborted_txs = HashSet::from([id]);

        loop {
            let mut more_aborted_txs = vec![];
            for id in aborted_txs.iter() {
                let dependencies = self.commit_dependencies.get(id);
                if dependencies.is_some() {
                    for dependency in dependencies.unwrap().iter() {
                        if !aborted_txs.contains(dependency) {
                            more_aborted_txs.push(*dependency);
                        }
                    }
                }
            }

            if more_aborted_txs.len() == 0 {
                break;
            }

            for aborted_tx in more_aborted_txs.iter() {
                aborted_txs.insert(*aborted_tx);
            }
        }

        for log in self.log.iter().rev() {
            if aborted_txs.contains(&log.writer()) {
                let key = log.key();
                let value = log.initial_value();

                println!("\x1b[1;91m[-]\x1b[0m Rolling back {} to {}", key, value);

                self.storage_manager.write(key, value);
            }
        }

        for aborted_tx in aborted_txs.iter() {
            if with_send {
                let sender = self.senders.get(&aborted_tx).unwrap();
                let _ = sender.send(OpMessage::Abort);
            }

            self.aborted_txs.insert(*aborted_tx);
            self.print_abort(*aborted_tx);
            self.aborted += 1;
        }
    }

    fn add_commit_dependency(&mut self, dependant: TransactionId, dependency: TransactionId) {
        if !self.commit_dependencies.contains_key(&dependency) {
            self.commit_dependencies.insert(dependency, vec![dependant]);
        } else {
            self.commit_dependencies
                .get_mut(&dependency)
                .unwrap()
                .push(dependant);
        }
    }

    fn put_to_read_set(&mut self, reader: TransactionId, key: Key) {
        let read_set = self.read_sets.get_mut(&reader).unwrap();
        read_set.insert(key);
    }

    fn put_to_write_set(&mut self, writer: TransactionId, key: Key) {
        let write_set = self.write_sets.get_mut(&writer).unwrap();
        write_set.insert(key);
    }

    pub fn generate_id(&mut self) -> TransactionId {
        self.last_id += 1;
        return self.last_id;
    }

    fn has_lock(&mut self, id: TransactionId, key: &str) -> bool {
        self.lock_manager.has(id, key) || self.lock_manager.request(id, key)
    }

    fn get_name(&self, id: TransactionId) -> &String {
        self.names.get(&id).unwrap()
    }

    fn print_read(&self, key: &str, value: &str, id: TransactionId) {
        println!(
            "\x1b[1;93m[-]\x1b[0m \x1b[93mRead {} = {} for {}.\x1b[0m",
            key,
            value,
            self.get_name(id)
        );
    }

    fn print_write(&self, key: &str, value: &str, id: TransactionId) {
        println!(
            "\x1b[1;95m[=]\x1b[0m \x1b[95mWrote {} = {} by {}.\x1b[0m",
            key,
            value,
            self.get_name(id)
        );
    }

    fn print_commit(&self, id: TransactionId) {
        println!(
            "\x1b[1;92m[+]\x1b[0m \x1b[92m{} successfully commited.\x1b[0m",
            self.get_name(id)
        );
    }

    fn print_abort(&self, id: TransactionId) {
        println!(
            "\x1b[1;91m[-]\x1b[0m \x1b[91m{} aborted.\x1b[0m",
            self.get_name(id)
        );
    }
}

pub type ParseResult = (Option<StorageManager>, Vec<Vec<Op>>, Vec<String>);

pub fn parse_dir(dir: String) -> ParseResult {
    let dir = fs::read_dir(dir).expect("Failed to read the directory.");
    let mut tx_ops = vec![];
    let mut storage_manager = None;
    let mut names = Vec::new();

    for entry in dir {
        let entry = entry.expect("Failed to read directory entry.");
        let path = entry.path();
        let file_name = path.file_name().expect("Failed to extract path file name.");
        let is_init = file_name == OsStr::new("init.txt");
        let name = path
            .file_stem()
            .unwrap()
            .to_os_string()
            .into_string()
            .unwrap();
        let content = fs::read_to_string(path).expect("Failed to read file.");

        if is_init {
            storage_manager = Some(StorageManager::parse(&content));
            continue;
        }

        tx_ops.push(parse_ops(&content));
        names.push(name);
    }

    (storage_manager, tx_ops, names)
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
