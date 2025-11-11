package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

func (fs *FileStore) Put(key string, value string) error {
	// use a buffer to store the data we want to write to file
	buf := new(bytes.Buffer)
	keyLen := uint32(len(key))
	valLen := uint32(len(value))

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

	fs.RWMux.Lock()
	defer fs.RWMux.Unlock()

	f := fs.Files[fs.ActiveSegID]
	if f == nil {
		return ErrFileNotOpen
	}

	// calculate offset by moving io to EOF, we want data to be at this position
	offset, err := f.Seek(0, io.SeekEnd)
	if err != nil {
		return ErrSeekingIO
	}

	// Write the buffer to active file
	if _, err := f.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("%w,%v", ErrWritingData, err)
	}

	// Ensure it’s flushed to disk
	if err := f.Sync(); err != nil {
		return ErrSyncingFile
	}

	if valLen != 0 {
		fs.DataMap[key] = Entry{
			Size:   int64(len(value)),                            // we can just read entry.Size when reading data from file
			Offset: offset + int64(headerSize) + int64(len(key)), // where to begin reading data
			SegID:  fs.ActiveSegID,
		}
	} else {
		// if the value is empty ->"", this is an indication of a tombstone
		// we need to remove this value from memory
		delete(fs.DataMap, key)
	}

	// rotation after a successful write return nil if success, err otherwise
	if err := maybeRotate(fs); err != nil {
		return err
	}
	// everything succeeded return out
	return nil
}

func Open(dir string) (*FileStore, error) {
	files := make(map[int]*os.File)
	keyDir := make(map[string]Entry)
	segIds := filterSegmentFiles(dir)

	//delete the .compact files

	// there are no previous data files, also no hint files
	if len(segIds) == 0 {
		// create a datafile, name it, and return the store
		segId := 1
		p := filepath.Join(dir, fmt.Sprintf("%06d%s", segId, segmentExt))
		file, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
		if err != nil {
			return nil, ErrFailedToOpenFile
		}
		files[segId] = file
		return &FileStore{
			DataMap:     keyDir,
			Files:       files,
			ActiveSegID: segId,
			NextSegID:   2,
			Path:        dir,
		}, nil
	}

	for i, segId := range segIds {
		p := filepath.Join(dir, fmt.Sprintf("%06d%s", segId, segmentExt))
		flags := os.O_RDONLY
		// the latest file gets append permissions (latest = highest segId number)
		if i == len(segIds)-1 {
			flags = os.O_RDWR | os.O_APPEND
		}
		f, err := os.OpenFile(p, flags, 0644)
		if err != nil {
			return nil, ErrFailedToOpenFile
		}
		files[segId] = f
	}

	active := segIds[len(segIds)-1]

	// check for Hint File and load DataMap from there, otherwise fallback to scanning
	hp := hintPath(dir)
	if hi, err := os.Stat(hp); err == nil {
		if newest, err := latestSegmentMTime(dir, segIds); err == nil && !hi.ModTime().Before(newest) {
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

	for _, segId := range segIds {
		f := files[segId]
		if err := scanSegment(segId, f, keyDir); err != nil {
			return nil, err
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

func (fs *FileStore) Close() error {
	fs.RWMux.Lock()
	defer fs.RWMux.Unlock()

	_ = fs.writeHintLocked()

	for segId, f := range fs.Files {
		if f != nil {
			if err := f.Close(); err != nil {
				return ErrClosingFile
			}
			fs.Files[segId] = nil
		}
	}

	fs.DataMap = nil
	fs.Files = nil
	fs.Path = ""

	return nil
}

func (fs *FileStore) Get(key string) ([]byte, error) {
	fs.RWMux.RLock()
	defer fs.RWMux.RUnlock()
	entry, ok := fs.DataMap[key]
	if !ok {
		return nil, ErrKeyNotFound
	}

	f := fs.Files[entry.SegID]
	if f == nil {
		return nil, ErrFileNotOpen
	}

	buf := make([]byte, entry.Size)
	_, err := f.ReadAt(buf, entry.Offset)
	if err != nil && err != io.EOF {
		return nil, err
	}
	return buf, nil
}

func (fs *FileStore) Delete(key string) error {
	return fs.Put(key, "")
}
