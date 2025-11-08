package store

import (
	"fmt"
	"io"
	"os"
	"path"
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

	nxtFile, err := os.OpenFile(segmentPath(s.Path, s.NextSegID), os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	s.ActiveSegID = s.NextSegID
	s.NextSegID++
	s.Files[s.ActiveSegID] = nxtFile
	return nil
}
