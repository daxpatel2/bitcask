package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
)

type Store struct {
	KeyDir map[string]Entry
	File   *os.File
	Path   string
	Mutex  sync.RWMutex
}

type Entry struct {
	Offset int64
	Size   int64
}

const HEADERSIZE = 8
const hintExt = ".hint"
const hintTmpExt = ".tmp"

var ErrKeyNotFound = errors.New("key not found")
var ErrFileNotOpen = errors.New("file not open")

func (s *Store) Put(key string, value string) error {
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

	// if the value is empty ->"", this is an indication of a tombstone
	// we need to remove this value from memory

	s.Mutex.Lock()
	defer s.Mutex.Unlock() // this will unlock mutexes when function returns if we don't do mutex unlock a
	offset, err := s.File.Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek EOF: %w", err)
	}
	//write only keyLen,valLen, key, and 0 to file
	// create the buffer for keylen and valuelen in binary format firs
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

	// finally write the PAYLOAD to the file
	if _, err := s.File.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	// Ensure it’s flushed to disk
	if err := s.File.Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if valLen != 0 {
		entry := Entry{
			Size:   int64(len(value)),                     // how much the actual value is (we can simply just read this much instantly
			Offset: offset + HEADERSIZE + int64(len(key)), // offset (beggining of our header) + 8 bytes (keylen + valuelen) + len(key) (since we also write the key)
			// we can start reading straight from the value part and skip parsing the header again
		}
		s.KeyDir[key] = entry
	} else {
		delete(s.KeyDir, key)
	}

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

	// check if this was an empty file
	inf, err := os.Stat(path)
	if err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("failed to stat file %s: %w", path, err)
	}

	// check if hint exists, if yes than load everything into memory
	hintPath := path + hintExt
	if hintInfo, err := os.Stat(hintPath); err == nil {
		// use hint if its newer than datafile
		if !hintInfo.ModTime().Before(inf.ModTime()) {
			if kd, err := loadHintFile(hintPath); err == nil {
				return &Store{
					KeyDir: kd,
					File:   file,
					Path:   path,
				}, nil
			}
		}
	}

	// if the datafile is empty (potentially new instance etc...)
	// then there is no keydir to return
	// just return a store
	if inf.Size() == 0 {
		return &Store{
			File:   file,
			KeyDir: make(map[string]Entry),
			Path:   path,
		}, nil
	}

	// we will need to rebuild the map dir from the contents of the file, if it's not empty
	keyDir := make(map[string]Entry)

	var offset int64 = 0

	for {
		header := make([]byte, HEADERSIZE)
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

		// create a buffer to read key
		keyBuf := make([]byte, keyLen)
		// Step 3: read key
		_, err = file.ReadAt(keyBuf, offset+HEADERSIZE)
		if err != nil {
			return nil, fmt.Errorf("read key: %w", err)
		}

		if valueLen == 0 {
			// encountered a tombstone
			delete(keyDir, string(keyBuf)) // tombstone → remove key
		} else {
			keyDir[string(keyBuf)] = Entry{offset + HEADERSIZE + int64(keyLen), int64(valueLen)}
		}
		offset += HEADERSIZE + int64(keyLen) + int64(valueLen)

	}

	return &Store{
		File:   file,
		KeyDir: keyDir,
		Path:   path,
	}, nil
}

func (s *Store) Close() error {
	if s.File != nil {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()

		_ = s.writeHintLocked()

		if err := s.File.Close(); err != nil {
			s.Mutex.Unlock()
			return fmt.Errorf("failed to close file: %w", err)
		}
		// once the file is closed, we can dereference all the data that was used in memory

		// Wipe memory-heavy data structures -> they will get GC'd and cleaned
		s.KeyDir = nil
		s.File = nil
		s.Path = ""
	}

	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	if s.File == nil {
		return nil, ErrFileNotOpen
	}

	s.Mutex.RLock()
	defer s.Mutex.RUnlock()
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

func (s *Store) Delete(key string) error {
	return s.Put(key, "")
}

func (s *Store) Compact() error {
	// lock mu to pause any operations new writes while we compact
	s.Mutex.Lock()
	defer s.Mutex.Unlock()

	if s.File == nil {
		return ErrFileNotOpen
	}

	// 1) Create temp file in the SAME directory for atomic rename.
	tmpPath := s.Path + ".compact"
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file %s: %w", tmpPath, err)
	}

	//new in mem map
	newKeyDir := make(map[string]Entry, len(s.KeyDir))
	// loop through the current open file
	for key, entry := range s.KeyDir {
		// Read the current value directly from the old file using stored offsets.
		val := make([]byte, entry.Size)
		if entry.Size > 0 {
			if _, err := s.File.ReadAt(val, entry.Offset); err != nil && err != io.EOF {
				_ = tmpFile.Close()
				return fmt.Errorf("read value for key %q: %w", key, err)
			}
		}

		// Prepare one record: [keyLen][valLen][key][value]=empty is tombstone.
		keyBytes := []byte(key)
		keyLen := uint32(len(keyBytes))
		valLen := uint32(len(val))

		// Find where we'll write in the temp file (to compute value offset).
		off, err := tmpFile.Seek(0, io.SeekEnd)
		if err != nil {
			_ = tmpFile.Close()
			return fmt.Errorf("seek compact file EOF: %w", err)
		}

		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			_ = tmpFile.Close()
			return fmt.Errorf("write keyLen: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, valLen); err != nil {
			_ = tmpFile.Close()
			return fmt.Errorf("write valLen: %w", err)
		}
		if _, err := buf.Write(keyBytes); err != nil {
			_ = tmpFile.Close()
			return fmt.Errorf("write key bytes: %w", err)
		}
		if valLen > 0 {
			if _, err := buf.Write(val); err != nil {
				_ = tmpFile.Close()
				return fmt.Errorf("write value bytes: %w", err)
			}
		}

		if _, err := tmpFile.Write(buf.Bytes()); err != nil {
			_ = tmpFile.Close()
			return fmt.Errorf("append compact record: %w", err)
		}
		if err := tmpFile.Sync(); err != nil {
			_ = tmpFile.Close()
			return fmt.Errorf("sync compact record: %w", err)
		}

		// Record new value location in the compacted file.
		newValOffset := off + HEADERSIZE + int64(len(keyBytes))
		newKeyDir[key] = Entry{Offset: newValOffset, Size: int64(valLen)}
	}

	// 3) Finish temp writes.
	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		return fmt.Errorf("final sync compact file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("close compact file: %w", err)
	}

	// 4) Close old file
	if err := s.File.Close(); err != nil {
		return fmt.Errorf("close old file: %w", err)
	}

	// 5) Atomically replace old with compacted file.
	//    (On same filesystem, Rename is atomic.)
	if err := os.Rename(tmpPath, s.Path); err != nil {
		// Best-effort recovery: try to reopen the old file so the store remains usable.
		if f, openErr := os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); openErr == nil {
			s.File = f
		}
		return fmt.Errorf("rename compact file: %w", err)
	}

	// 6) Reopen the new canonical file and swap in the new index.
	f, err := os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("reopen compacted file: %w", err)
	}
	s.File = f
	s.KeyDir = newKeyDir

	if err := s.writeHintLocked(); err != nil {
		//nothing
	}

	return nil
}

// Format per record: [u32 keyLen][i64 offset][i64 size][keyBytes]
func writeHintFile(hintPath string, keyDir map[string]Entry) error {

	tmp := hintPath + hintTmpExt
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("open hint file: %w", err)
	}

	for k, e := range keyDir {
		buf := new(bytes.Buffer)
		keyBytes := []byte(k)
		keyLen := uint32(len(keyBytes))

		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			_ = f.Close()
			return fmt.Errorf("write keyLen: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, e.Offset); err != nil {
			_ = f.Close()
			return fmt.Errorf("write valLen: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, e.Size); err != nil {
			_ = f.Close()
			return fmt.Errorf("write key bytes: %w", err)
		}
		if _, err := buf.Write(keyBytes); err != nil {
			_ = f.Close()
			return fmt.Errorf("write key bytes: %w", err)
		}

		if _, err := f.Write(buf.Bytes()); err != nil {
			_ = f.Close()
			return fmt.Errorf("write value bytes: %w", err)
		}
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("final sync compact file: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close compact file: %w", err)
	}

	if err := os.Rename(tmp, hintPath); err != nil {
		return fmt.Errorf("rename compact file: %w", err)
	}

	return nil
}

// loadHintFile reads a hint file into a fresh KeyDir map.
func loadHintFile(hintPath string) (map[string]Entry, error) {
	f, err := os.Open(hintPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	keyDir := make(map[string]Entry)
	for {
		var keyLen uint32
		if err := binary.Read(f, binary.LittleEndian, &keyLen); err != nil {
			if err == io.EOF {
				break
			}
			return nil, fmt.Errorf("read keyLen: %w", err)
		}
		var off int64
		if err := binary.Read(f, binary.LittleEndian, &off); err != nil {
			return nil, fmt.Errorf("read offset: %w", err)
		}
		var sz int64
		if err := binary.Read(f, binary.LittleEndian, &sz); err != nil {
			return nil, fmt.Errorf("read size: %w", err)
		}

		kb := make([]byte, keyLen)
		if _, err := io.ReadFull(f, kb); err != nil {
			return nil, fmt.Errorf("read key bytes: %w", err)
		}
		keyDir[string(kb)] = Entry{Offset: off, Size: sz}
	}
	return keyDir, nil
}

// WriteHint writes the current KeyDir to <data>.hint under a read lock
// so writers are paused and the snapshot is consistent.
func (s *Store) WriteHint() error {
	hintPath := s.Path + hintExt

	s.Mutex.RLock()
	defer s.Mutex.RUnlock()

	if s.File == nil {
		return ErrFileNotOpen
	}
	return writeHintFile(hintPath, s.KeyDir)
}

// writeHintLocked writes a hint while the caller already holds s.Mutex (write lock).
func (s *Store) writeHintLocked() error {
	hintPath := s.Path + hintExt
	return writeHintFile(hintPath, s.KeyDir)
}
