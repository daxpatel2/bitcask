package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

func (s *FileStore) Compact() error {
	// lock mu to pause any operations new writes while we compact
	s.RWMux.Lock()
	defer s.RWMux.Unlock()

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
	newKeyDir := make(map[string]Entry, len(s.DataMap))
	// loop through the current open file
	for key, entry := range s.DataMap {
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
	s.DataMap = newKeyDir

	if err := s.writeHintLocked(); err != nil {
		//nothing
	}

	return nil
}
