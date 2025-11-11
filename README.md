# Go-Bitcask: A Log-Structured Hash Table in Go

## Abstract

This repository contains a Go implementation of a Key-Value (KV) store based on the design principles of Bitcask. Bitcask is a highly efficient embedded database model, described as a Log-Structured Hash Table for Fast Key/Value Data.

In simple terms, all data is written sequentially to an append-only log file. A separate in-memory index, the DataMap, holds a pointer for each key, directing to the precise byte-offset of its value in the log. This design minimizes disk seeks and provides exceptionally high read/write throughput, making it ideal for production-grade traffic.

## Setup and Usage

### Prerequisites
- Go 1.22 or higher

### Installation
To get started, clone the repository:
```sh
git clone https://github.com/your-username/go-bitcask.git
cd go-bitcask
```

### Basic Usage
Here's a simple example of how to use the `FileStore` to store and retrieve data:
```go
package main

import (
	"fmt"
	"log"

	"bitcask/internal/store"
)

func main() {
	// Open the store. The directory will be created if it doesn't exist.
	s, err := store.Open("./data")
	if err != nil {
		log.Fatalf("failed to open store: %v", err)
	}
	defer s.Close()

	// Put a key-value pair.
	if err := s.Put("hello", "world"); err != nil {
		log.Fatalf("failed to put key: %v", err)
	}

	// Get the value for a key.
	val, err := s.Get("hello")
	if err != nil {
		log.Fatalf("failed to get key: %v", err)
	}
	fmt.Println(string(val)) // Output: world

	// Delete a key.
	if err := s.Delete("hello"); err != nil {
		log.Fatalf("failed to delete key: %v", err)
	}
}
```

## Command-Line Tool

This repository includes a command-line tool for benchmarking and profiling the Bitcask store.

### Building the Tool
To build the `bitcaskctl` tool, run the following command from the root of the repository:
```sh
go build ./cmd/bitcaskctl
```

### Usage
The `bitcaskctl` tool can be used to run a configurable workload against the store. Here's an example of how to run a workload with default settings:
```sh
./bitcaskctl -data ./my-data
```

For a full list of available flags and their descriptions, use the `-h` or `-help` flag:
```sh
./bitcaskctl -h
```

## Core Design Principles

This implementation, like the Bitcask paper, is built on two core components: a directory of datafiles and an in-memory index.

### 1. The Datafile: An Append-Only Log
All write operations (Create, Update, Delete) are translated into entries appended to a single active log file. When this file is written to, it is never modified in place. An "update" is simply a new entry for the same key written to the end of the file; the old entry is now dangling. A "delete" is a special entry with an empty value (a "tombstone").

#### On-Disk Format
To ensure we can rebuild the index after a crash or restart, each entry in the log file follows a precise binary format:
`[Key Size: 4 bytes] [Value Size: 4 bytes] [Key: N bytes] [Value: M bytes]`

- **Key Size**: A `uint32` (4 bytes) indicating the length of the key.
- **Value Size**: A `uint32` (4 bytes) indicating the length of the value.
- **Key**: The raw bytes of the key itself.
- **Value**: The raw bytes of the value.

### 2. The DataMap: An In-Memory Index
The DataMap is an in-memory hash table (map) that stores all keys present in the database. Each key maps to an `Entry` struct that tells the store exactly where to find the value and how large it is, enabling a read operation with a single disk seek.

This implementation is defined by these core Go structs:
```go
package store

// FileStore represents the Bitcask store.
type FileStore struct {
    DataMap     map[string]Entry
    Files       map[int]*os.File
    ActiveSegID int
    NextSegID   int
    Path        string
    RWMux       sync.RWMutex
}

// Entry represents the metadata for a value stored in a segment file.
type Entry struct {
    Offset int64
    Size   int64
    SegID  int
}
```

## Operations

### Writing Data (Put)
When a new KV pair is submitted, the engine appends it to the active datafile and then updates the DataMap with the new position. This requires just one disk write (an append, which is very fast) and an in-memory map update.

### Reading Data (Get)
Reading a value is extremely fast. It requires one O(1) lookup in the in-memory DataMap followed by a single disk seek and read.

### Deleting Data (Delete)
Deletion is a special case of a Put. To delete a key, we simply Put a new entry for that key with a special "tombstone" value (an empty string).

## Crash Recovery & Startup (Open)
If the store is shut down or crashes, the in-memory DataMap is lost. The `Open` function rebuilds the entire DataMap by reading the log files from beginning to end. Because later entries for the same key supersede earlier ones, this process correctly reconstructs the final state of the database. For faster startups, a "hint" file can be used, which contains a snapshot of the DataMap.

## Strengths and Weaknesses

### Strengths
- **Low Latency**: Read and write operations are extremely fast.
- **High Write Throughput**: All writes are sequential appends.
- **Fast Reads**: A single disk seek is all that's required to retrieve any value.
- **Fast Crash Recovery**: Rebuilding the index is fast and bounded by disk read speed.
- **Simple Backups**: Backing up is as simple as copying the data files.

### Weaknesses
- **RAM Constraint**: The DataMap holds all keys in memory. This implementation's primary limiting factor is the amount of system RAM available.

## Compaction
As the store is used, old data entries are left dangling in the log files, consuming disk space. The compaction process reclaims this space by creating new segment files containing only the live data. This implementation includes a `CompactOnce` function that can be used to compact the oldest sealed segment.
