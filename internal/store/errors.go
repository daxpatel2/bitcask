package store

import "errors"

// ErrKeyNotFound is returned when a key is not found in the store.
var ErrKeyNotFound = errors.New("key not found")

// ErrFileNotOpen is returned when an operation is attempted on a file that is not open.
var ErrFileNotOpen = errors.New("file not open")

// ErrSeekingIO is returned when an error occurs while seeking to a position in a file.
var ErrSeekingIO = errors.New("encountered an error seeking to positon in file")

// ErrWritingData is returned when an error occurs while writing data to a file.
var ErrWritingData = errors.New("error writing data")

// ErrClosingFile is returned when an error occurs while closing a file.
var ErrClosingFile = errors.New("error closing file")

// ErrSyncingFile is returned when an error occurs while syncing a file to disk.
var ErrSyncingFile = errors.New("error syncing file")

// ErrFailedToOpenFile is returned when a file fails to open.
var ErrFailedToOpenFile = errors.New("failed to open file")

// ErrCompactionNotPossible is returned when compaction cannot be performed.
var ErrCompactionNotPossible = errors.New("compaction not possible")
