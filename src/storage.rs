use self::util::{Key, Value};
use crate::tx::TransactionId;
use regex::Regex;
use std::mem;
use std::{collections::HashMap, ops::Deref, time::SystemTime};

pub mod util;

type Timestamp = SystemTime;
type ReadTimestamp = Timestamp;
type WriteTimestamp = Timestamp;
type WriterId = TransactionId;

#[derive(Debug, Eq, Default)]
pub struct StorageManager {
    storage: HashMap<Key, Value>,
}

impl PartialEq for StorageManager {
    fn eq(&self, _: &StorageManager) -> bool {
        true
    }
}

impl StorageManager {
    pub fn new() -> Self {
        Self {
            storage: HashMap::new(),
        }
    }

    pub fn parse(config: &str) -> Self {
        let mut manager = Self::new();

        let multiline_regex = Regex::new(r"(\r?\n)+").unwrap();
        let whitespaces_regex = Regex::new(r"\s+").unwrap();

        let lines: Vec<&str> = multiline_regex.split(&config).collect();
        let entries: Vec<Vec<&str>> = lines
            .into_iter()
            .map(|line| whitespaces_regex.split(line.trim()).collect())
            .collect();

        for entry in entries {
            if entry.len() == 2 {
                let key = entry[0];
                let value = entry[1];

                manager.write(key, value);
            }
        }

        manager
    }

    pub fn write(&mut self, key: &str, value: &str) {
        self.storage.insert(key.to_owned(), value.to_owned());
    }

    pub fn read(&self, key: &str) -> Option<&str> {
        self.storage.get(key).map(|v| v.as_str())
    }

    pub fn load(&mut self, entries: &HashMap<Key, Value>) {
        for (key, value) in entries.iter() {
            self.write(key.as_str(), value.as_str());
        }
    }

    pub fn print(&self) {
        for (key, value) in self.storage.iter() {
            println!("{} = {}", key, value);
        }
    }
}

impl Deref for StorageManager {
    type Target = HashMap<Key, Value>;

    fn deref(&self) -> &Self::Target {
        &self.storage
    }
}

pub struct VersionedStorageManager {
    storage: Vec<(Key, ReadTimestamp, WriterId, WriteTimestamp, Value)>,
}

impl VersionedStorageManager {
    pub fn new(storage_manager: &StorageManager) -> Self {
        let mut storage = Vec::new();
        let timestamp = SystemTime::now();

        for (key, value) in storage_manager.storage.iter() {
            storage.push((key.clone(), timestamp, 0, timestamp, value.clone()));
        }

        Self { storage }
    }

    pub fn read(&mut self, key: &str, tx_timestamp: Timestamp) -> (&Value, WriterId) {
        let index = self.find_latest_version_index(key, tx_timestamp);
        let entry = self.storage.get_mut(index).unwrap();

        if tx_timestamp > entry.1 {
            entry.1 = tx_timestamp;
        }

        let (_, _, writer_id, _, value) = entry;

        (value, *writer_id)
    }

    pub fn write(
        &mut self,
        key: &str,
        writer_id: TransactionId,
        tx_timestamp: Timestamp,
        value: Value,
    ) -> Result<(), ()> {
        let index = self.find_latest_version_index(key, tx_timestamp);
        let entry = self.storage.get_mut(index).unwrap();
        let (_, read_timestamp, entry_writer_id, _, _) = entry;
        if *read_timestamp > tx_timestamp {
            return Err(());
        }

        if *entry_writer_id != writer_id {
            self.storage
                .push((key.to_owned(), tx_timestamp, writer_id, tx_timestamp, value));
        } else {
            entry.4 = value;
        }

        Ok(())
    }

    pub fn remove(&mut self, aborted_id: TransactionId) {
        let storage = mem::take(&mut self.storage);
        self.storage = storage
            .into_iter()
            .filter(|(_, _, writer_id, _, _)| *writer_id != aborted_id)
            .collect();
    }

    fn find_latest_version_index(&mut self, key: &str, tx_timestamp: Timestamp) -> usize {
        let mut index = self.storage.len();
        let mut latest_timestamp = SystemTime::UNIX_EPOCH;

        for (i, (entry_key, _, _, write_timestamp, _)) in self.storage.iter().enumerate() {
            if key == entry_key
                && latest_timestamp < *write_timestamp
                && *write_timestamp < tx_timestamp
            {
                index = i;
                latest_timestamp = *write_timestamp;
            }
        }

        index
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::util::{Key, Value};

    use super::StorageManager;
    use std::collections::HashMap;

    #[test]
    fn test_read_write() {
        let mut manager = StorageManager::new();
        manager.write("A", "2");
        manager.write("C", "Database Management");
        manager.write("D", "false");

        assert_eq!(manager.read("A"), Some("2"));
        assert_eq!(manager.read("B"), None);
        assert_eq!(manager.read("C"), Some("Database Management"));
        assert_eq!(manager.read("D"), Some("false"));
    }

    #[test]
    fn test_parse() {
        let manager = StorageManager::parse(
            "
                A 1
                this line will not be parsed
                aswellasthisline
                B text

                D mn",
        );

        assert_eq!(manager.read("A"), Some("1"));
        assert_eq!(manager.read("B"), Some("text"));
        assert_eq!(manager.read("D"), Some("mn"));
        assert_eq!(manager.read("this"), None);
        assert_eq!(manager.read("aswellasthisline"), None);
    }

    #[test]
    fn test_load() {
        let mut manager = StorageManager::new();
        let mut entries = HashMap::new();
        entries.insert("A", "2");
        entries.insert("C", "Database Management");
        entries.insert("D", "false");

        let entries: HashMap<Key, Value> = entries
            .into_iter()
            .map(|(key, value)| (key.into(), value.into()))
            .collect();

        manager.load(&entries);

        assert_eq!(manager.read("A"), Some("2"));
        assert_eq!(manager.read("B"), None);
        assert_eq!(manager.read("C"), Some("Database Management"));
        assert_eq!(manager.read("D"), Some("false"));
    }
}
