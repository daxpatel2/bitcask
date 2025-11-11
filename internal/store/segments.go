package store

import (
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"sort"
)

func segmentPath(dir string, segId int) string {
	return path.Join(fmt.Sprintf("%s/%06d%s", dir, segId, segmentExt))
}

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

/*
This function returns a list of all non-active segments which are segIds < ActiveID
*/
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

func (fs *FileStore) fileSizeForSegment(segId int) (int64, error) {
	//build the dir
	segPath := filepath.Join(fs.Path, fmt.Sprintf("%06d%s", segId, segmentExt))
	fi, err := os.Stat(segPath)
	if err != nil {
		return 0, fmt.Errorf("stat %s: %w", segPath, err)
	}
	return fi.Size(), nil
}

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

func (fs *FileStore) fileToCompact(segId int) string {
	return segmentPath(fs.Path, segId)
}
