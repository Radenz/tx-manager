#![allow(warnings, unused)]

use clap::{Parser, ValueEnum};
use tx_manager::{
    concurrent,
    storage::StorageManager,
    tx::{parse_dir, Transaction, TransactionManager},
};
fn main() {
    // let storage_manager = StorageManager::new();
    // let mut manager = TransactionManager::new(storage_manager, Protocol::Lock);

    let cli = Cli::parse();

    let protocol = cli.protocol.into();

    let (mut sm, tx_ops) = parse_dir(cli.dir);

    if sm.is_none() {
        println!("Warning: init.txt is missing. Intializing empty storage...");
        sm = Some(StorageManager::new());
    }

    let sm = sm.unwrap();

    let mut tx_manager = TransactionManager::new(sm, protocol);

    for ops in tx_ops {
        tx_manager.exec(ops);
    }
}

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    dir: String,

    #[arg(value_enum, default_value_t=Protocol::Sl)]
    protocol: Protocol,
}

#[derive(Copy, Clone, PartialEq, Eq, PartialOrd, Ord, ValueEnum)]
enum Protocol {
    /// Simple locking protocol
    Sl,
    /// Serial optimistic concurrency control
    Occ,
    /// Multiversion timestamp ordering concurrency control
    Mvcc,
}

impl Into<concurrent::Protocol> for Protocol {
    fn into(self) -> concurrent::Protocol {
        match self {
            Protocol::Sl => concurrent::Protocol::Lock,
            Protocol::Occ => concurrent::Protocol::Validation,
            Protocol::Mvcc => concurrent::Protocol::Timestamp,
        }
    }
}
