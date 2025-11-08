package store

import (
	"os"
	"sync"
)

type FileStore struct {
	DataMap     map[string]Entry // key, store t
	Files       map[int]*os.File // map to all the data files
	ActiveSegID int              // id of the active segment file
	NextSegID   int              // id of the next segment file
	Path        string           // directory path
	RWMux       sync.RWMutex
}

type Entry struct {
	Offset int64 // where the data value lives
	Size   int64 // size of the data value
	SegID  int   // id of the segment file where data lives
}
