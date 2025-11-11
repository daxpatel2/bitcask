package store

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// writeHintFile creates a new hint file from the provided in-memory index (keyDir).
// The hint file is used to speed up the startup process by avoiding a full scan of all segment files.
// The format for each record in the hint file is: [keyLen u32][segID i32][offset i64][size i64][key bytes].
//
// Parameters:
//   hintPath: The full path where the hint file should be written.
//   keyDir: The map containing the in-memory index to be written to the hint file.
//
// Returns:
//   An error if any file operations fail, otherwise nil.
func writeHintFile(hintPath string, keyDir map[string]Entry) error {

	// create or open the global hint file if it doesn't exist
	tmp := hintPath + tmpExtension
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error opening hint file: %w", err)
	}

	for k, e := range keyDir {
		buf := new(bytes.Buffer)
		// represents the key in byte form
		keyBytes := []byte(k)
		keyLen := uint32(len(keyBytes))

		if err := binary.Write(buf, binary.LittleEndian, keyLen); err != nil {
			_ = f.Close()
			return fmt.Errorf("write keyLen: %w", err)
		}
		seg := int32(e.SegID)
		if err := binary.Write(buf, binary.LittleEndian, seg); err != nil {
			_ = f.Close()
			return fmt.Errorf("write segID: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, e.Offset); err != nil {
			_ = f.Close()
			return fmt.Errorf("write valLen: %w", err)
		}
		if err := binary.Write(buf, binary.LittleEndian, e.Size); err != nil {
			_ = f.Close()
			return fmt.Errorf("write key bytes: %w", err)
		}
		// write the key to the buffer
		if _, err := buf.Write(keyBytes); err != nil {
			_ = f.Close()
			return fmt.Errorf("write key bytes: %w", err)
		}
		// write the buffer to file
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

// loadHintFile reads a hint file and reconstructs the in-memory index (DataMap).
// This allows for a much faster startup as it avoids scanning all segment files.
//
// Parameters:
//   hintPath: The path to the hint file.
//
// Returns:
//   A map representing the in-memory index and an error if the file cannot be read.
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
		var segID int32
		if err := binary.Read(f, binary.LittleEndian, &segID); err != nil {
			return nil, fmt.Errorf("read key segID: %w", err)
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
		keyDir[string(kb)] = Entry{Offset: off, Size: sz, SegID: int(segID)}
	}
	return keyDir, nil
}

// WriteHint writes the current in-memory index (DataMap) to a hint file.
// It acquires a read lock to ensure a consistent snapshot of the DataMap.
//
// Returns:
//   An error if the store has no open files or if the hint file write fails.
func (fs *FileStore) WriteHint() error {
	fs.RWMux.RLock()
	defer fs.RWMux.RUnlock()
	if len(fs.Files) == 0 {
		return ErrFileNotOpen
	}
	return writeHintFile(HintPath(fs.Path), fs.DataMap)
}

// writeHintLocked is an internal helper that writes a hint file
// while the caller already holds a write lock on the FileStore.
func (fs *FileStore) writeHintLocked() error {
	return writeHintFile(HintPath(fs.Path), fs.DataMap)
}

// scanDir reads the contents of a directory.
// It creates the directory if it does not exist.
//
// Parameters:
//   dir: The path to the directory.
//
// Returns:
//   A slice of directory entries and an error if the directory cannot be read.
func scanDir(dir string) ([]os.DirEntry, error) {
	// ensure the directory exits
	// creates it if it doesn't
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Discover all files: 000001.data, 000002.data, ...
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	return ents, nil
}

// filterSegmentFiles scans a directory for segment files (e.g., "000001.data")
// and returns a sorted slice of their integer IDs.
//
// Parameters:
//   dir: The directory to scan.
//
// Returns:
//   A sorted slice of segment IDs.
func filterSegmentFiles(dir string) []int {
	ents, err := scanDir(dir)
	if err != nil {
		return nil
	}
	var segIds []int
	for _, ent := range ents {
		// filter out non files
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		// filter out non .data files
		if !strings.HasSuffix(name, segmentExt) {
			continue
		}
		base := strings.TrimSuffix(name, segmentExt)
		id, err := strconv.Atoi(base)
		if err != nil {
			continue
		}
		segIds = append(segIds, id)
	}
	sort.Ints(segIds)

	return segIds
}

// HintPath returns the full path to the hint file for a given directory.
func HintPath(dir string) string {
	return filepath.Join(dir, hintFileName)
}

// latestSegmentMTime finds the modification time of the most recently changed segment file.
//
// Parameters:
//   dir: The directory containing the segment files.
//   segIDs: A slice of segment IDs to check.
//
// Returns:
//   The latest modification time and an error if any file's status cannot be read.
func latestSegmentMTime(dir string, segIDs []int) (time.Time, error) {
	var latest time.Time
	for _, id := range segIDs {
		p := filepath.Join(dir, fmt.Sprintf("%06d%s", id, segmentExt))
		fi, err := os.Stat(p)
		if err != nil {
			return time.Time{}, fmt.Errorf("stat segment %s: %w", p, err)
		}
		if fi.ModTime().After(latest) {
			latest = fi.ModTime()
		}
	}
	return latest, nil
}

// scanSegment rebuilds the in-memory index (keyDir) by reading a single segment file.
// It iterates through the file, parsing each record to extract the key and its metadata.
//
// Parameters:
//   segID: The ID of the segment being scanned.
//   f: An open file handle to the segment file.
//   keyDir: The map to populate with the index data.
//
// Returns:
//   An error if there is an issue reading the file.
func scanSegment(segID int, f *os.File, keyDir map[string]Entry) error {
	var off int64 = 0
	hdr := make([]byte, headerSize)

	for {
		n, err := f.ReadAt(hdr, off)
		if err == io.EOF && n == 0 {
			break // clean end
		}
		if err != nil && err != io.EOF {
			return fmt.Errorf("read header seg %06d off %d: %w", segID, off, err)
		}
		if n < headerSize {
			// Incomplete tail → stop at last good boundary.
			break
		}

		keyLen := binary.LittleEndian.Uint32(hdr[0:4])
		valLen := binary.LittleEndian.Uint32(hdr[4:8])

		// If you disallow empty keys, this can be treated as a format error.
		if keyLen == 0 {
			break
		}

		kb := make([]byte, keyLen)
		if _, err := f.ReadAt(kb, off+int64(headerSize)); err != nil {
			return fmt.Errorf("read key seg %06d off %d: %w", segID, off, err)
		}

		if valLen == 0 {
			delete(keyDir, string(kb)) // tombstone
		} else {
			valOff := off + int64(headerSize) + int64(keyLen)
			keyDir[string(kb)] = Entry{
				SegID:  segID,
				Offset: valOff,
				Size:   int64(valLen),
			}
		}

		off += int64(headerSize) + int64(keyLen) + int64(valLen)
	}
	return nil
}

// deleteOrphanFiles removes any leftover .compact files that may not have been cleaned up.
//
// Parameters:
//   dir: The directory to scan for orphan files.
//
// Returns:
//   An error if any file cannot be removed.
func deleteOrphanFiles(dir string) error {
	ents, err := scanDir(dir)
	if err != nil {
		return nil
	}

	for _, ent := range ents {
		// filter out non files
		if ent.IsDir() {
			continue
		}
		name := ent.Name()
		// filter out non .compact files
		if !strings.HasSuffix(name, compactExt) {
			continue
		}
		if err := os.Remove(filepath.Join(dir, name)); err != nil {
			return fmt.Errorf("remove file %s: %w", name, err)
		}
		fmt.Printf("removed file %s\n", name)
	}
	return nil
}
