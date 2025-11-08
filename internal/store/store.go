package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (s *FileStore) Put(key string, value string) error {
	/**

	We need to store key len , value len and key value on the file because if the program crashes or restarts we have no
	way of knowing what key goest to what value. the file will exist yes, but only with the written values, the in memory hashmap will be wiped
	so we won't be able to build back the keyDir

	For that, store, key, value, (key len, value len) -> so that when we read we know how many bytes to go past instantly

	Header -> keylen + valuelen
	payload -> key + value
	*/

	// use a buffer to store the data we want to write to file
	buf := new(bytes.Buffer)
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

	s.RWMux.Lock()
	defer s.RWMux.Unlock()

	// calculate offset by moving io to EOF, we want data to be at this position
	offset, err := s.Files[s.ActiveSegID].Seek(0, io.SeekEnd)
	if err != nil {
		return ErrSeekingIO
	}

	// write keylen and valuelen to buffer
	if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
		return fmt.Errorf("%w,%v", ErrWritingData, err)
	}
	if err := binary.Write(buf, binary.LittleEndian, valLen); err != nil {
		return fmt.Errorf("%w,%v", ErrWritingData, err)
	}

	// write the key and value to buffer
	if _, err := buf.Write([]byte(key)); err != nil {
		return fmt.Errorf("%w,%v", ErrWritingData, err)
	}
	if _, err := buf.Write([]byte(value)); err != nil {
		return fmt.Errorf("%w,%v", ErrWritingData, err)
	}

	// Write the buffer to active file
	if _, err := s.Files[s.ActiveSegID].Write(buf.Bytes()); err != nil {
		return fmt.Errorf("%w,%v", ErrWritingData, err)
	}

	// Ensure it’s flushed to disk
	if err := s.Files[s.ActiveSegID].Sync(); err != nil {
		return fmt.Errorf("failed to sync file: %w", err)
	}

	if valLen != 0 {
		entry := Entry{
			Size:   int64(len(value)),                     // we can just read entry.Size when reading data from file
			Offset: offset + headerSize + int64(len(key)), // where to begin reading data
			SegID:  s.ActiveSegID,
		}
		s.DataMap[key] = entry
	} else {
		// if the value is empty ->"", this is an indication of a tombstone
		// we need to remove this value from memory
		delete(s.DataMap, key)
	}

	// run file rotation
	// return nil if success, err otherwise
	err = maybeRotate(s)
	if err != nil {
		return err
	}
	// everything succeeded return out
	return nil
}

func Open(dir string) (*FileStore, error) {
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
		return &FileStore{
			DataMap:     keyDir,
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

	// check for Hint File and load DataMap from there, otherwise fallback to scanning
	hp := hintPath(dir)
	if hi, err := os.Stat(hp); err == nil {
		if newest, err := latestSegmentMTime(dir, segIds); err == nil {
			// If hint is at least as new as the newest .data segment, trust it.
			if !hi.ModTime().Before(newest) {
				if kd, err := loadHintFile(hp); err == nil {
					return &FileStore{
						DataMap:     kd,
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

	return &FileStore{
		DataMap:     keyDir,
		Files:       files,
		ActiveSegID: active,
		NextSegID:   active + 1,
		Path:        dir,
	}, nil
}

func (s *FileStore) Close() error {
	if s.Files[s.ActiveSegID] != nil {
		s.RWMux.Lock()
		defer s.RWMux.Unlock()

		_ = s.writeHintLocked()

		for segId, _ := range s.Files {
			err := s.Files[segId].Close()
			if err != nil {
				return ErrClosingFile
			}
		}

		// Wipe memory-heavy data structures
		s.DataMap = nil
		s.Files = nil
		s.Path = ""
	}

	return nil
}

func (s *FileStore) Get(key string) ([]byte, error) {
	if s.Files[s.ActiveSegID] == nil {
		return nil, ErrFileNotOpen
	}

	s.RWMux.RLock()
	entry, ok := s.DataMap[key]
	if !ok {
		s.RWMux.RUnlock()
		return nil, ErrKeyNotFound
	}
	s.RWMux.RUnlock()

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

func (s *FileStore) Delete(key string) error {
	return s.Put(key, "")
}
