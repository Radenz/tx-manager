tx-manager
====================================================

#### Table of Contents
- [About The Project](#about-the-project)
- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation)
- [Usage](#usage)

## About The Project
This is a simple CLI program to simulate a transaction manager
in a key-value based DBMS handling multiple concurrent transactions at a
given time. The program provides 3 concurrency control protocol:
simple locking protocol (with only exclusive locks), optimistic
concurrency control protocol (using validation based protocol),
and multiversion timestamp ordering protocol. This project is made
to fulfill `Tugas Besar 2 IF3140 Manajemen Basis Data`.

## Getting Started

### Prerequisites

To build the program from source, you will need rust toolchains including
cargo and rust compiler (rustc) which can be installed using
[rustup](https://www.rust-lang.org/tools/install).

### Installation
You can now build the binary executable using cargo by running
```bash
cargo build -r
```
You can also run the program using cargo with `run` subcommand.
```bash
cargo run [...args]
```

## Usage
To start a simulation, you will need to prepare a directory which
contains an `init.txt` file (optional) representing the initial content
of the database storage and several text files representing the
transactions that you want to execute concurrently, with 1 transaction
represented in a single file.
Both `init.txt` file and transaction file are similar in format. Since
the simulation is based on key-value data, each line in the `init.txt`
file should contain two strings (without whitespaces) separated by spaces.
Any line that doesn't follow this format will be ignored by the parser
inside the program. For example, an `init.txt` file containing this content
```
A 2

X Lorem
Hello World
abc
I am me
```
will initialize the data with key `A` to `2`, `X` to `Lorem`, and `Hello`
to `World` in the storage, while the other lines are ignored.

Similar to the initialization file, the transaction definition file
consists of lines with non-whitespace-containing strings separated by
spaces. There are 3 available instructions that can be executed within
a transaction: `Read`, `Write`, and `Assign`.
- The `Read` instruction allows you to read the value of a data with
a specific key from the database storage and bring it to the transaction
context.
- The `Write` instruction allows you to write the value of a data with
a specific key in the transaction context to the database storage
(do make sure it is available in the context, otherwise the program may
panic).
- The `Assign` instruction allows you to assign a value to a specific key
in the transaction context with a specific value (doesn't need to be
initialized/read from the storage first).

To write your own transaction, you may arrange multiple instructions
on various data key in the transaction file with these format.
```
R <key>
r <key>
Read <key>
```
```
W <key>
w <key>
Write <key>
```
```
A <key> <value>
a <key> <value>
Assign <key> <value>
```

Lastly, keep in mind that all files in the prepared directory will be
considered as a transaction, even though it contains no instructions
at all (all lines are ignored). Only put transaction files and
initialization file in this directory to ensure correct behavior and
output of the simulation.

If you have prepared the directory and have written your transactions
to execute concurrently, simply run the program to start the
simulation. (You may want to move the binary out of the target
directory first.)
```
tx-manager <dir> [protocol]
protocol:   sl      Simple locking protocol
            occ     Optimistic concurrently control protocol
            mvcc    Multiversion timestamp ordering protocol
``` 