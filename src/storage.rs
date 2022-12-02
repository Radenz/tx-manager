use crate::tx::TransactionId;

use self::util::{Key, Value};
use regex::Regex;
use std::collections::HashMap;

pub mod util;

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

#[derive(Debug)]
pub struct Log {
    tx: TransactionId,
    key: Key,
    from: Value,
    to: Value,
}

impl Log {
    pub fn write(key: Key) -> Self {
        Self {
            tx: 0,
            key,
            from: String::default(),
            to: String::default(),
        }
    }

    pub fn from(self, value: Value) -> Self {
        Self {
            from: value,
            ..self
        }
    }

    pub fn to(self, value: Value) -> Self {
        Self { to: value, ..self }
    }

    pub fn by(self, tx: TransactionId) -> Self {
        Self { tx, ..self }
    }

    pub fn is_done_by(&self, tx: TransactionId) -> bool {
        self.tx == tx
    }

    pub fn writer(&self) -> TransactionId {
        self.tx
    }

    pub fn key(&self) -> &Key {
        &self.key
    }

    pub fn initial_value(&self) -> &Value {
        &self.from
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
