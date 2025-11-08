package store

import "errors"

var ErrKeyNotFound = errors.New("key not found")
var ErrFileNotOpen = errors.New("file not open")
var ErrSeekingIO = errors.New("encountered an error seeking to positon in file")
var ErrWritingData = errors.New("error writing data")
var ErrClosingFile = errors.New("error closing file")
