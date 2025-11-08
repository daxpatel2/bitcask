package store

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Store struct {
	KeyDir      map[string]Entry
	Files       map[int]*os.File
	ActiveSegID int
	NextSegID   int
	Path        string
	Mutex       sync.RWMutex
}

type Entry struct {
	Offset int64
	Size   int64
	SegID  int //since there are more files that can be operated on, we need to know which file its in
}

const headerSize = 8
const hintFileName = "bitcask.hint"
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
	offset, err := s.Files[s.ActiveSegID].Seek(0, io.SeekEnd)
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
	if _, err := s.Files[s.ActiveSegID].Write(buf.Bytes()); err != nil {
		return fmt.Errorf("failed to write record: %w", err)
	}

	// Ensure it’s flushed to disk
	if err := s.Files[s.ActiveSegID].Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if valLen != 0 {
		entry := Entry{
			Size:   int64(len(value)),                     // how much the actual value is (we can simply just read this much instantly
			Offset: offset + headerSize + int64(len(key)), // offset (beggining of our header) + 8 bytes (keylen + valuelen) + len(key) (since we also write the key)
			// we can start reading straight from the value part and skip parsing the header again
			SegID: s.ActiveSegID,
		}
		s.KeyDir[key] = entry
	} else {
		delete(s.KeyDir, key)
	}

	activeSize, err := s.Files[s.ActiveSegID].Seek(0, io.SeekEnd)
	if err != nil {
		return fmt.Errorf("failed to seek EOF: %w", err)
	}

	if maybeRotate(activeSize) {
		err = rotate(s)
		if err != nil {
			return err
		}
	}

	return nil
}

func Open(dir string) (*Store, error) {
	files := make(map[int]*os.File)
	keyDir := make(map[string]Entry)
	segIds := filterSegmentFiles(dir)

	// there are no previous data files
	if len(segIds) == 0 {
		// create a datafile, name it, and return the store
		segId := 1
		p := filepath.Join(dir, fmt.Sprintf("%06d%s", segId, segmentExt))
		file, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", dir, err)
		}
		files[segId] = file
		return &Store{
			KeyDir:      keyDir,
			Files:       files,
			ActiveSegID: segId,
			NextSegID:   2,
			Path:        p,
		}, nil
	}

	for i, segId := range segIds {
		p := filepath.Join(dir, fmt.Sprintf("%06d%s", segId, segmentExt))
		var flags int
		// the latest file gets append permissions (latest = highest segId number)
		if i == len(segIds)-1 {
			flags = os.O_RDWR | os.O_APPEND
		} else {
			// the rest are read only
			flags = os.O_RDONLY
		}
		f, err := os.OpenFile(p, flags, 0644)
		if err != nil {
			return nil, fmt.Errorf("failed to open file %s: %w", dir, err)
		}
		files[segId] = f
	}

	active := segIds[len(segIds)-1]

	//check for hintfile and loadkey dir from there, otherwise fallback to scanning
	hp := hintPath(dir)
	if hi, err := os.Stat(hp); err == nil {
		if newest, err := latestSegmentMTime(dir, segIds); err == nil {
			// If hint is at least as new as the newest .data segment, trust it.
			if !hi.ModTime().Before(newest) {
				if kd, err := loadHintFile(hp); err == nil {
					return &Store{
						KeyDir:      kd,
						Files:       files,  // already opened map[int]*os.File
						ActiveSegID: active, // highest segID you opened RW|APPEND
						NextSegID:   active + 1,
						Path:        dir,
					}, nil
				}
			}
		}
		var offset int64 = 0
		for {
			header := make([]byte, headerSize)
			n, err := f.ReadAt(header, offset)
			if err == io.EOF && n == 0 {
				break // reached end of file
			}
			if err != nil {
				return nil, fmt.Errorf("read header seg %06d of %d:%w", segId, offset, err)
			}

			if n < headerSize {
				// Incomplete tail; stop at last good offset.
				break
			}

			keyLen := binary.LittleEndian.Uint32(header[0:4])
			valueLen := binary.LittleEndian.Uint32(header[4:8])

			if keyLen == 0 {
				break
			}

			// create a buffer to read key
			keyBuf := make([]byte, keyLen)
			// Step 3: read key
			_, err = f.ReadAt(keyBuf, offset+headerSize)
			if err != nil {
				return nil, fmt.Errorf("read key: %w", err)
			}

			if valueLen == 0 {
				// encountered a tombstone
				delete(keyDir, string(keyBuf)) // tombstone → remove key
			} else {
				valOff := offset + int64(headerSize) + int64(keyLen)
				keyDir[string(keyBuf)] = Entry{
					Offset: valOff,
					Size:   int64(valueLen),
					SegID:  segId,
				}
			}
			offset += headerSize + int64(keyLen) + int64(valueLen)
		}
	}

	return &Store{
		KeyDir:      keyDir,
		Files:       files,
		ActiveSegID: active,
		NextSegID:   active + 1,
		Path:        dir,
	}, nil

}

func (s *Store) Close() error {
	if s.Files[s.ActiveSegID] != nil {
		s.Mutex.Lock()
		defer s.Mutex.Unlock()

		_ = s.writeHintLocked()

		for segId, _ := range s.Files {
			err := s.Files[segId].Close()
			if err != nil {
				return fmt.Errorf("failed to close file: %w", err)
			}
		}

		// Wipe memory-heavy data structures -> they will get GC'd and cleaned
		s.KeyDir = nil
		s.Files = nil
		s.Path = ""
	}

	return nil
}

func (s *Store) Get(key string) ([]byte, error) {
	if s.Files[s.ActiveSegID] == nil {
		return nil, ErrFileNotOpen
	}

	s.Mutex.RLock()
	entry, ok := s.KeyDir[key]
	if !ok {
		s.Mutex.RUnlock()
		return nil, ErrKeyNotFound
	}
	s.Mutex.RUnlock()

	// to get the value, read amount (size) from file starting at offset
	size := entry.Size
	offset := entry.Offset

	f := s.Files[entry.SegID]
	if f == nil {
		return nil, ErrFileNotOpen
	}

	buf := make([]byte, size)
	_, err := s.Files[entry.SegID].ReadAt(buf, offset)
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

	if s.Files[s.ActiveSegID] == nil {
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
			if _, err := s.Files[s.ActiveSegID].ReadAt(val, entry.Offset); err != nil && err != io.EOF {
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
		newValOffset := off + headerSize + int64(len(keyBytes))
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
	if err := s.Files[s.ActiveSegID].Close(); err != nil {
		return fmt.Errorf("close old file: %w", err)
	}

	// 5) Atomically replace old with compacted file.
	//    (On same filesystem, Rename is atomic.)
	if err := os.Rename(tmpPath, s.Path); err != nil {
		// Best-effort recovery: try to reopen the old file so the store remains usable.
		if f, openErr := os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644); openErr == nil {
			s.Files[s.ActiveSegID] = f
		}
		return fmt.Errorf("rename compact file: %w", err)
	}

	// 6) Reopen the new canonical file and swap in the new index.
	f, err := os.OpenFile(s.Path, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("reopen compacted file: %w", err)
	}
	s.Files[s.ActiveSegID] = f
	s.KeyDir = newKeyDir

	if err := s.writeHintLocked(); err != nil {
		//nothing
	}

	return nil
}
