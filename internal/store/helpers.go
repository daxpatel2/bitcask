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

// Format per record: [u32 keyLen][i64 offset][i64 size][keyBytes]
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

// loadHintFile reads a hint file into a fresh DataMap map.
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

// WriteHint writes the current DataMap to <data>.hint under a read lock
// so writers are paused and the snapshot is consistent.
func (s *FileStore) WriteHint() error {
	hint := hintPath(s.Path)

	s.RWMux.RLock()
	defer s.RWMux.RUnlock()

	if s.Files[s.ActiveSegID] == nil {
		return ErrFileNotOpen
	}
	return writeHintFile(hint, s.DataMap)
}

// writeHintLocked writes a hint while the caller already holds s.RWMux (write lock).
func (s *FileStore) writeHintLocked() error {
	hintPath := hintPath(s.Path)
	return writeHintFile(hintPath, s.DataMap)
}

func scanDir(dir string) ([]os.DirEntry, error) {
	// ensure the directory exits
	// creates it if it doesn't
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create directory %s: %w", dir, err)
	}

	// Discover segment files: 000001.data, 000002.data, ...
	ents, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to read directory %s: %w", dir, err)
	}

	return ents, nil
}

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

func hintPath(dir string) string {
	return filepath.Join(dir, hintFileName)
}

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
