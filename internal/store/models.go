package store

import (
	"os"
	"sync"
)

// FileStore represents the Bitcask store.
// It holds the in-memory index, file handles, and metadata for the store's operation.
type FileStore struct {
	DataMap     map[string]Entry // The in-memory index (key -> Entry).
	Files       map[int]*os.File // A map of segment IDs to their file handles.
	ActiveSegID int              // The ID of the current active segment file for writes.
	NextSegID   int              // The ID to be used for the next segment file.
	Path        string           // The directory path where the data files are stored.
	RWMux       sync.RWMutex     // A read-write mutex to protect concurrent access.
}

// Entry represents the metadata for a value stored in a segment file.
// It contains the necessary information to locate and read the value from disk.
type Entry struct {
	Offset int64 // The byte offset within the segment file where the value begins.
	Size   int64 // The size of the value in bytes.
	SegID  int   // The ID of the segment file where the value is stored.
}
