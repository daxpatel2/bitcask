package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

// CompactOnce compacts the oldest sealed segment (segID < ActiveSegID).
// Returns (didWork, error).
func (fs *FileStore) CompactOnce() (bool, error) {
	//don't compact if there is only one active file or no files
	if len(fs.Files) == 0 || fs.ActiveSegID == 1 {
		return false, ErrCompactionNotPossible
	}

	fs.RWMux.RLock()
	// find the candidate to compact
	candId, ok := SelectCompactCandidate(fs)
	if !ok {
		fs.RWMux.RUnlock()
		return false, ErrCompactionNotPossible
	}

	// get all the data inside the canidate file
	plan := make(map[string]Entry)
	//take a snapshot of the entries inside the canidate file
	for key, entry := range fs.DataMap {
		// if entry lives in the canidate file, we need to account for it
		if entry.SegID == candId {
			plan[key] = entry
		}
	}

	candFile := fs.Files[candId]
	dir := fs.Path
	fs.RWMux.RUnlock()

	if candFile == nil {
		// candidate disappeared (e.g., closed elsewhere); nothing to do
		return false, nil
	}

	//new in mem map size equal to plan
	wroteAnything := false
	newKeyDir := make(map[string]Entry, len(plan))

	// 1) Create temp file in the SAME directory
	tmpPath := fs.Path + ".compact"
	tmpFile, err := os.OpenFile(tmpPath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return false, ErrOpeningFile
	}

	var off int64 = 0
	// loop through the plan and write it to the compact
	for k, e := range plan {
		// Read the current value directly from the seg file using stored offsets.
		val := make([]byte, e.Size)
		if e.Size > 0 {
			if _, err := candFile.ReadAt(val, e.Offset); err != nil && err != io.EOF {
				_ = tmpFile.Close()
				return false, fmt.Errorf("read value for key %q: %w", k, err)
			}
		}

		// Prepare one record: [keyLen][valLen][key][value]=empty is tombstone.
		keyBytes := []byte(k)
		keyLen := uint32(len(keyBytes))
		valLen := uint32(len(val))
		buf := new(bytes.Buffer)

		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			_ = tmpFile.Close()
			return false, ErrWritingData
		}
		if err := binary.Write(buf, binary.LittleEndian, valLen); err != nil {
			_ = tmpFile.Close()
			return false, ErrWritingData
		}
		if _, err := buf.Write(keyBytes); err != nil {
			_ = tmpFile.Close()
			return false, ErrWritingData
		}

		if valLen > 0 {
			if _, err := buf.Write(val); err != nil {
				_ = tmpFile.Close()
				_ = os.Remove(tmpPath)
				return false, ErrWritingData
			}
		}

		n, err := tmpFile.Write(buf.Bytes())
		if err != nil {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return false, fmt.Errorf("append compact record: %w", err)
		}

		if n != len(buf.Bytes()) {
			_ = tmpFile.Close()
			_ = os.Remove(tmpPath)
			return false, fmt.Errorf("short write to %s", tmpPath)
		}

		// Record new value location in the compacted file.
		newValOffset := off + int64(headerSize) + int64(len(keyBytes))
		newKeyDir[k] = Entry{Offset: newValOffset, Size: int64(valLen), SegID: fs.NextSegID}
		off += int64(n)
		wroteAnything = true
	}

	if err := tmpFile.Sync(); err != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmpPath)
		return false, ErrCompactionNotPossible
	}
	if err := tmpFile.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return false, ErrClosingFile
	}
	// 4) Commit under a short write lock.
	fs.RWMux.Lock()
	defer fs.RWMux.Unlock()
	// If candidate is no longer sealed, abort.
	if candId >= fs.ActiveSegID {
		_ = os.Remove(tmpPath)
		return false, nil
	}

	if !wroteAnything {
		_ = os.Remove(tmpPath)
		if f := fs.Files[candId]; f != nil {
			_ = f.Close()
			delete(fs.Files, candId)
		}
		_ = os.Remove(segmentPath(dir, candId))
		_ = fs.writeHintLocked() // best-effort
		return true, nil
	}

	finalPath := segmentPath(dir, fs.NextSegID)
	if err := os.Rename(tmpPath, finalPath); err != nil {
		_ = os.Remove(tmpPath)
		return false, fmt.Errorf("rename compact file: %w", err)
	}

	// Open the new compacted segment as sealed (RO) and register it.
	newF, err := os.OpenFile(finalPath, os.O_RDONLY, 0644)
	if err != nil {
		return false, fmt.Errorf("open compacted segment: %w", err)
	}
	fs.Files[fs.NextSegID] = newF

	// Update DataMap for keys that STILL point to the candidate seg.
	for k, ne := range newKeyDir {
		cur, ok := fs.DataMap[k]
		if ok && cur.SegID == candId {
			fs.DataMap[k] = ne
		}
	}
	// Remove the old candidate file.
	if f := fs.Files[candId]; f != nil {
		_ = f.Close()
		delete(fs.Files, candId)
	}
	_ = os.Remove(segmentPath(dir, candId))

	// Advance segment id counter, refresh hint best-effort.
	fs.NextSegID++
	_ = fs.writeHintLocked()

	return true, nil
}

func SelectCompactCandidate(fs *FileStore) (candidateID int, ok bool) {
	segIDs := nonActiveSegments(fs)
	if len(segIDs) == 0 {
		return 0, false
	}
	return segIDs[0], true // oldest (smallest segID)
}
