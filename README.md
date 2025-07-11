# C-SQL: A Simple B-Tree Database in C

This project is a simple, from-scratch database implementation in C. It uses a B-Tree to store data, persists records to a file, and provides a basic SQL-like command-line interface (REPL). The primary purpose of this project is educational, demonstrating core database concepts like data structures, file I/O, serialization, and command parsing.

## Features

- **CRUD Operations**: `insert`, `select` (all or by ID), and `drop` (by ID). 
- **B-Tree for Indexing**: Data is stored and indexed in a B-Tree, allowing for efficient range queries and lookups. The primary key is an integer `id`.
- **File-Based Persistence**: The database is saved to a single file, which can be reloaded in subsequent sessions.
- **File-Based Import/Export**: The database can be imported or exported using a csv file.
- **In-Memory Page Cache**: A pager manages reading and writing fixed-size pages from the file into memory to reduce I/O overhead.
- **Interactive REPL**: A simple Read-Eval-Print Loop for interacting with the database.
- **Meta-Commands**: Special commands for inspecting the database state (e.g., printing the B-Tree structure).

## How to Build and Run

### Prerequisites

You need a C compiler (`MinGW` to use getline() function). It is implemented to work in UNIX Environments like linux, MacOS or WSL.

### Compilation

```bash
make
```

### Runing

```bash
make run
```

## Usage and Commands

### SQL-like Commands

- `insert {id} {username} {email}`  
  Inserts a new user record.  
  - `id`: non-negative integer  
  - `username`: max 32 characters  
  - `email`: max 255 characters  
  **Example:**  
  ```bash
  insert 1 alice alice@example.com
  ```

- `select`  
  Retrieves and prints all records, sorted by `id`.  
  **Example:**  
  ```bash
  select
  ```

- `select {id}`  
  Retrieves a single record with the given `id`.  
  **Example:**  
  ```bash
  select 1
  ```

- `drop {id}`  
  Deletes the record with the given `id`.  
  **Example:**  
  ```bash
  drop 1
  ```

- `import '{file.csv}'`
 Imports content of csv file with name `file.csv`.
 **Example:**
 ```bash
 import 'example.csv'
 ```

- `export '{file.csv}'`
 Exports records to csv file with name `file.csv`.
 **Example:**
 ```bash
 export 'records.csv'
 ```

### Meta-Commands

- `.exit`  
  Flushes changes and exits the program.

- `.btree`  
  Displays the B-Tree structure.

- `.constants`  
  Shows database constants (node size, page capacity, etc.)

- `.commands`  
  Prints a list of available commands.

## Architecture Overview

### 1. Pager and File Format

- Database file is divided into **4096-byte pages**.
- A `DbPager` handles:
  - Reading pages from disk to memory.
  - Writing modified pages back to disk.
- Reduces disk I/O through in-memory caching.

### 2. B-Tree Implementation

- Supports efficient **lookups**, **insertions**, and **ordered scans**.
- **Node Types**:
  - **LEAF**: Holds (key, value) pairs. Value is serialized `UserRow`.
  - **INTERNAL**: Guides traversal with keys and child pointers.

#### Operations

- **Insertion**:  
  - Find correct leaf node.
  - Insert key; split node if full.
  - Splits may propagate up, creating a new root.

- **Search**:  
  - Starts at root.
  - Traverses internal nodes based on key comparisons.

### 3. Command Processing (REPL)

- The main loop:
  1. Reads input.
  2. Parses into a `Statement` (`prepare_statement`).
  3. Executes using `execute_statement`.
  4. Interacts with the B-Tree using `TableCursor`.

### 4. Cursor Abstraction

- `TableCursor` points to specific row in the table.
- Simplifies traversal of the B-Tree.
- Supports row access and iteration.

## Limitations and Future Work

- ❌ No Transactions – risk of corruption on crash during B-Tree operations  
- ❌ No Concurrency – not safe for multi-threaded or multi-process access  
- ❌ Fixed Schema – only supports `{id, username, email}`  
- ❌ Limited Query Language – no `WHERE`, `JOIN`, or aggregation  
- ❌ No Secondary Indexes – queries on non-primary keys are inefficient

## License

This project is licensed under the [MIT License](https://opensource.org/licenses/MIT).
