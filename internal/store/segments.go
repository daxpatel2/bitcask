package store

import (
	"fmt"
	"os"
	"path"
)

const segmentExt = ".data"
const maxSegmentBytes = 1024 * 1024

func segmentPath(dir string, segId int) string {
	return path.Join(fmt.Sprintf("%s/%06d%s", dir, segId, segmentExt))
}

func maybeRotate(activeSize int64) bool {
	if activeSize > maxSegmentBytes {
		return true
	}
	return false
}

func rotate(s *Store) error {
	// close the active file
	_ = s.Files[s.ActiveSegID].Close()

	nxtFile, err := os.OpenFile(segmentPath(s.Path, s.NextSegID), os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	s.ActiveSegID = s.NextSegID
	s.NextSegID++
	s.Files[s.ActiveSegID] = nxtFile
	return nil
}
