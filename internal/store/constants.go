package store

// headerSize is the fixed size of a record's header in bytes (key length + value length).
const headerSize = 8

// hintFileName is the name of the hint file, used for faster startup.
const hintFileName = "bitcask.hint"

// tmpExtension is the file extension for temporary files used during compaction.
const tmpExtension = ".tmp"

// segmentExt is the file extension for segment files.
const segmentExt = ".data"

// compactExt is the file extension for compacted segment files.
const compactExt = ".compact"

// maxSegmentBytes is the maximum size of a segment file before it is rotated.
const maxSegmentBytes = 1024 * 1024 // 1 MB
