package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Store struct {
	KeyDir map[string]Entry
	File   *os.File
	Path   string
}

type Entry struct {
	Offset int64
	Size   int64
}

func (s *Store) Put(key string, value string) error {
	offset, err := s.File.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek EOF: %w", err)
	}

	/**

	We need to store key len , value len and key value on the file because if the program crashes or restarts we have no
	way of knowing what key goest to what value. the file will exist yes, but only with the written values, the in memory hashmap will be wiped
	so we won't be able to build back the keyDir

	For that, store, key, value, (key len, value len) -> so that when we read we know how many bytes to go past instantly

	Header -> keylen + valuelen
	payload -> key + value
	*/

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

	// Ensure it’s flushed to disk
	if err := s.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	entry := Entry{
		Size:   int64(len(value)),            // how much the actual value is (we can simply just read this much instantly
		Offset: offset + 8 + int64(len(key)), // offset (beggining of our header) + 8 bytes (keylen + valuelen) + len(key) (since we also write the key)
		// we can start reading straight from the value part and skip parsing the header again
	}
	s.KeyDir[key] = entry
	return nil
}

func Open(path string) (*Store, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return nil, fmt.Errorf("failed to open file %s: %w", path, err)
	}

	// we will need to rebuild the map dir from the contents of the file, if it's not empty
	keyDir := make(map[string]Entry)

	var offset int64 = 0
	for {
		header := make([]byte, 8)
		n, err := file.ReadAt(header, offset)
		if err == io.EOF || n == 0 {
			break // reached end of file
		}
		if err != nil {
			return nil, fmt.Errorf("read header: %w", err)
		}

		// 2. decode keylen and valuelen
		// first 4 -> keylen
		// last 4 -> valuelen
		keyLen := binary.LittleEndian.Uint32(header[0:4])
		valueLen := binary.LittleEndian.Uint32(header[4:8])

		// Step 3: read key
		keyBuf := make([]byte, keyLen)
		_, err = file.ReadAt(keyBuf, offset+8)
		if err != nil {
			return nil, fmt.Errorf("read key: %w", err)
		}
		keyDir[string(keyBuf)] = Entry{offset + 8 + int64(keyLen), int64(valueLen)}
		offset += 8 + int64(keyLen) + int64(valueLen)
	}

	store := &Store{
		File:   file,
		KeyDir: keyDir,
		Path:   path,
	}

	return store, nil
}

func (s *Store) Close() error {
	if s.File != nil {
		if err := s.File.Close(); err != nil {
			return fmt.Errorf("failed to close file: %w", err)
		}
	}
	// once the file is closed, we can dereference all the data that was used in memory

	// Wipe memory-heavy data structures -> they will get GC'd and cleaned
	s.KeyDir = nil
	s.File = nil
	s.Path = ""

	return nil
}

var ErrKeyNotFound = errors.New("key not found")
var ErrFileNotOpen = errors.New("file not open")

func (s *Store) Get(key string) ([]byte, error) {
	if s.File == nil {
		return nil, ErrFileNotOpen
	}

	entry, ok := s.KeyDir[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	// to get the value, read amount (size) from file starting at offset
	size := entry.Size
	offset := entry.Offset

	buf := make([]byte, size)
	_, err := s.File.ReadAt(buf, offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}
