# Go-Bitcask: A Log-Structured Hash Table in Go

## Abstract

This repository contains a Go implementation of a Key-Value (KV) store based on the design principles of Bitcask. Bitcask is a highly efficient embedded database model, described as a Log-Structured Hash Table for Fast Key/Value Data.

In simple terms, all data is written sequentially to an append-only log file. A separate in-memory index, the DataMap, holds a pointer for each key, directing to the precise byte-offset of its value in the log. This design minimizes disk seeks and provides exceptionally high read/write throughput, making it ideal for production-grade traffic.

### Core Design Principles

This implementation, like the Bitcask paper, is built on two core components: a directory of datafiles (in our case, a single active datafile) and an in-memory index.

1. The Datafile: An Append-Only Log

All write operations (Create, Update, Delete) are translated into entries appended to a single active log file. When this file is written to, it is never modified in place. An "update" is simply a new entry for the same key written to the end of the file; the old entry is now dangling. A "delete" is a special entry with an empty value (a "tombstone").

On-Disk Format

To ensure we can rebuild the index after a crash or restart, each entry in the log file follows a precise binary format:

[Key Size: 4 bytes] [Value Size: 4 bytes] [Key: N bytes] [Value: M bytes]

Key Size: A uint32 (4 bytes) indicating the length of the key.

Value Size: A uint32 (4 bytes) indicating the length of the value.

Key: The raw bytes of the key itself.

Value: The raw bytes of the value.

2. The DataMap: An In-Memory Index

The DataMap is the secret sauce. It is an in-memory hash table (map) that stores all keys present in the database. Each key maps to an Entry struct that tells the store exactly where to find the value and how large it is, enabling a read operation with a single disk seek.

This implementation is defined by these core Go structs:

package store

// FileStore holds the active file handle and the in-memory index.
type FileStore struct {
DataMap map[string]Entry
File   *os.File
Path   string
}

// Entry stores the metadata for a key, allowing for
// a direct read from the log file.
type Entry struct {
Offset int64 // The byte offset where the *value* begins.
Size   int64 // The size of the value in bytes.
}


A-Tomicity and Operations

Here is how the core operations are implemented, linking the Bitcask theory to this project's code.

Writing Data (Put)

When a new KV pair is submitted, the engine appends it to the active datafile and then updates the DataMap with the new position. This requires just one disk write (an append, which is very fast) and an in-memory map update.

The Put function serializes the key and value according to the on-disk format:

func (s *FileStore) Put(key string, value string) error {
offset, err := s.File.Seek(0, io.SeekEnd)
if err != nil {
return fmt.Errorf("failed to seek EOF: %w", err)
}

    // ... (Error handling) ...

    buf := new(bytes.Buffer)
    keyLen := uint32(len(key))
    valLen := uint32(len(value))

    // write the keylen and valuelen in binary format
    if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
       return fmt.Errorf("failed to write key length: %w", err)
    }
    if err := binary.Write(buf, binary.LittleEndian, valLen); err != nil {
       return fmt.Errorf("failed to write value length: %w", err)
    }

    // write the key and value in binary format
    if _, err := buf.Write([]byte(key)); err != nil {
       return fmt.Errorf("failed to write key bytes: %w", err)
    }
    if _, err := buf.Write([]byte(value)); err != nil {
       return fmt.Errorf("failed to write value bytes: %w", err)
    }

    if _, err := s.File.Write(buf.Bytes()); err != nil {
       return fmt.Errorf("failed to write record: %w", err)
    }

    // ... (Sync to disk) ...

    // Finally, update the in-memory index.
    entry := Entry{
       Size:   int64(len(value)),
       // Note: The offset points *past* the header and key, directly to the value.
       Offset: offset + 8 + int64(len(key)), 
    }
    s.DataMap[key] = entry
    return nil
}


Reading Data (Get)

Reading a value is extremely fast. It requires one O(1) lookup in the in-memory DataMap followed by a single disk seek and read. We find the entry, seek to the Offset, and read Size bytes.

func (s *FileStore) Get(key string) ([]byte, error) {
if s.File == nil {
return nil, ErrFileNotOpen
}

    // 1. O(1) hash map lookup
    entry, ok := s.DataMap[key]
    if !ok {
       return nil, ErrKeyNotFound
    }

    // 2. Single disk read
    size := entry.Size
    offset := entry.Offset

    buf := make([]byte, size)
    _, err := s.File.ReadAt(buf, offset)
    if err != nil && err != io.EOF {
       return nil, err
    }
    return buf, nil
}


Deleting Data (Delete)

Deletion is a special case of a Put. To delete a key, we simply Put a new entry for that key with a special "tombstone" value—in this case, an empty string (""). We then remove the key from the in-memory DataMap.

The tombstone entry in the log file is crucial for the compaction process.

func (s *FileStore) Delete(key string) error {
// Write a tombstone record.
err := s.Put(key, "")
if err != nil {
return err
}
// Remove from the in-memory index.
delete(s.DataMap, key)
return nil
}


Crash Recovery & Startup (Open)

The most critical piece of the design is recovery. If the store is shut down or crashes, the in-memory DataMap is lost. The Open function rebuilds the entire DataMap by reading the log file from beginning to end, one entry at a time.

Because later entries for the same key supersede earlier ones, this process correctly reconstructs the final state of the database.

func Open(path string) (*FileStore, error) {
// ... (File opening logic) ...

    // we will need to rebuild the map dir from the contents of the file
    keyDir := make(map[string]Entry)
    var offset int64 = 0

    for {
       // 1. Read the 8-byte header
       header := make([]byte, 8)
       n, err := file.ReadAt(header, offset)
       if err == io.EOF || n == 0 {
          break // reached end of file
       }
       // ... (Error handling) ...

       // 2. decode keylen and valuelen
       keyLen := binary.LittleEndian.Uint32(header[0:4])
       valueLen := binary.LittleEndian.Uint32(header[4:8])

       // 3. Read the key
       keyBuf := make([]byte, keyLen)
       _, err = file.ReadAt(keyBuf, offset+8)
       // ... (Error handling) ...

       if valueLen == 0 {
          // encountered a tombstone
          delete(keyDir, string(keyBuf)) // tombstone → remove key
       } else {
          // This is a live entry. Add it to the map.
          keyDir[string(keyBuf)] = Entry{offset + 8 + int64(keyLen), int64(valueLen)}
       }
       
       // Move offset to the beginning of the next entry
       offset += 8 + int64(keyLen) + int64(valueLen)
    }

    store := &FileStore{
       File:   file,
       DataMap: keyDir,
       Path:   path,
    }

    return store, nil
}


Strengths and Weaknesses

Strengths

Low Latency: Read and write operations are extremely fast.

High Write Throughput: All writes are sequential appends, the most efficient way to write to disk.

Fast Reads: A single disk seek is all that's required to retrieve any value.

Fast Crash Recovery: Rebuilding the index is fast and bounded by disk read speed.

Simple Backups: Backing up is as simple as copying the data file.

Weaknesses

RAM Constraint: The DataMap holds all keys in memory. This implementation's primary limiting factor is the amount of system RAM available to hold the entire keyspace.

Future Considerations

Merge and Compaction

As seen in the Update and Delete operations, old data entries are left dangling in the log file, consuming disk space. The Compact() function is a stub for the "Merge Process."

A future implementation of Compact would:

Iterate over all keys in the current DataMap.

Write each live key-value pair to a new log file.

Once complete, atomically swap the new file for the old one.

This process would reclaim all the space lost to old updates and tombstones, and it can be run in the background without blocking reads or writes.