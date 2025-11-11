package store

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
)

// segmentPath returns the full path for a segment file given a directory and segment ID.
func segmentPath(dir string, segId int) string {
	return path.Join(fmt.Sprintf("%s/%06d%s", dir, segId, segmentExt))
}

// maybeRotate checks if the active segment file has reached its maximum size
// and rotates it if necessary.
func maybeRotate(s *FileStore) error {
	activeSize, err := s.Files[s.ActiveSegID].Seek(0, io.SeekEnd)
	if err != nil {
		return ErrSeekingIO
	}

	if activeSize > maxSegmentBytes {
		err = rotate(s)
		if err != nil {
			return err
		}
	}

	return nil
}

// rotate closes the current active segment file and opens a new one for writing.
func rotate(s *FileStore) error {
	// close the active file
	_ = s.Files[s.ActiveSegID].Close()

	nxtFile, err := os.OpenFile(segmentPath(s.Path, s.NextSegID), os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	s.ActiveSegID = s.NextSegID
	s.NextSegID++
	s.Files[s.ActiveSegID] = nxtFile
	return nil
}

// nonActiveSegments returns a sorted slice of segment IDs that are not the active segment.
// These are the segments that are candidates for compaction.
func nonActiveSegments(fs *FileStore) []int {
	fs.RWMux.RLock()
	defer fs.RWMux.RUnlock()
	//take the files
	segIds := make([]int, 0, len(fs.Files))

	for segId := range fs.Files {
		if segId < fs.ActiveSegID {
			segIds = append(segIds, segId)
		}
	}
	sort.Ints(segIds)
	return segIds
}

// fileSizeForSegment returns the size of a segment file in bytes.
func (fs *FileStore) fileSizeForSegment(segId int) (int64, error) {
	//build the dir
	segPath := filepath.Join(fs.Path, fmt.Sprintf("%06d%s", segId, segmentExt))
	fi, err := os.Stat(segPath)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", segPath, err)
	}
	return fi.Size(), nil
}

// liveBytesForSegment calculates the total size of live (not deleted or updated)
// data in a segment and the number of live keys.
func (fs *FileStore) liveBytesForSegment(segId int) (live int64, keyCount int) {
	fs.RWMux.RLock()
	defer fs.RWMux.RUnlock()

	var total int64
	var count int

	for _, entry := range fs.DataMap {
		if entry.SegID == segId {
			total += entry.Size
			count++
		}
	}
	return total, count
}

// fileToCompact returns the path to the segment file that is a candidate for compaction.
func (fs *FileStore) fileToCompact(segId int) string {
	return segmentPath(fs.Path, segId)
}
