package store

const headerSize = 8
const hintFileName = "bitcask.hint"
const tmpExtension = ".tmp"
const segmentExt = ".data"
const compactExt = ".compact"
const maxSegmentBytes = 1024 * 1024 // 1 MB
