package keydb

import (
	"bytes"
	"github.com/nightlyone/lockfile"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"sync"
	"sync/atomic"
)

// Database reference is obtained via Open()
type Database struct {
	sync.Mutex
	tables       map[string]*internalTable
	open         bool
	closing      bool
	transactions map[uint64]*Transaction
	path         string
	wg           sync.WaitGroup
	nextSegID    uint64
	lockfile     lockfile.Lockfile

	// if non-nil an asynchronous error has occurred, and the database cannot be used
	err error
}

type internalTable struct {
	sync.Mutex
	segments     []segment
	transactions int
	name         string
}

// LookupIterator iterator interface for table scanning. all iterators should be read until completion
type LookupIterator interface {
	// returns EndOfIterator when complete, if err is nil, then key and value are valid
	Next() (key []byte, value []byte, err error)
	// returns the next non-deleted key in the index
	peekKey() ([]byte, error)
}

var dblock sync.RWMutex

// Open a database. The database can only be opened by a single process, but the *Database
// reference can be shared across Go routines. The path is a directory name.
// if createIfNeeded is true, them if the db doesn't exist it will be created
// Additional tables can be added on subsequent opens, but there is no current way to delete a table,
// except for deleting the table related files from the directory
func Open(path string, createIfNeeded bool) (*Database, error) {
	dblock.Lock()
	defer dblock.Unlock()

	db, err := open(path)
	if err == NoDatabaseFound && createIfNeeded == true {
		return create(path)
	}
	return db, err
}

func open(path string) (*Database, error) {

	path = filepath.Clean(path)

	err := IsValidDatabase(path)
	if err != nil {
		return nil, err
	}

	abs, err := filepath.Abs(path + "/lockfile")
	if err != nil {
		return nil, err
	}
	lf, err := lockfile.New(abs)
	if err != nil {
		return nil, err
	}
	err = lf.TryLock()
	if err != nil {
		return nil, DatabaseInUse
	}

	db := &Database{path: path, open: true}
	db.lockfile = lf
	db.transactions = make(map[uint64]*Transaction)
	db.tables = make(map[string]*internalTable)

	db.wg.Add(1)
	go mergeDiskSegments(db)

	return db, nil
}

func create(path string) (*Database, error) {
	path = filepath.Clean(path)

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return open(path)
}

// Remove the database, deleting all files. the caller must be able to
// gain exclusive multi to the database
func Remove(path string) error {
	dblock.Lock()
	defer dblock.Unlock()

	path = filepath.Clean(path)

	err := IsValidDatabase(path)
	if err != nil {
		return err
	}

	abs, err := filepath.Abs(path + "/lockfile")
	if err != nil {
		return err
	}
	lf, err := lockfile.New(abs)
	if err != nil {
		return err
	}
	err = lf.TryLock()
	if err != nil {
		return DatabaseInUse
	}

	return os.RemoveAll(path)
}

// IsValidDatabase checks if the path points to a valid database or empty directory (which is also valid)
func IsValidDatabase(path string) error {
	fi, err := os.Stat(path)
	if err != nil {
		return NoDatabaseFound
	}

	if !fi.IsDir() {
		return NotADirectory
	}

	infos, err := ioutil.ReadDir(path)
	if err != nil {
		return err
	}

	for _, f := range infos {
		if "lockfile" == f.Name() {
			continue
		}
		if f.Name() == filepath.Base(path) {
			continue
		}
		if matched, _ := regexp.Match(".*\\.(keys|data)\\..*", []byte(f.Name())); !matched {
			return NotValidDatabase
		}
	}
	return nil
}

// Close the database. any memory segments are persisted to disk.
// The resulting segments are merged until the default maxSegments is reached
func (db *Database) Close() error {
	dblock.Lock()
	defer dblock.Unlock()
	if !db.open {
		return DatabaseClosed
	}
	if len(db.transactions) > 0 {
		return DatabaseHasOpenTransactions
	}

	db.Lock()
	db.closing = true
	db.Unlock()

	db.wg.Wait()

	err := mergeDiskSegments0(db, maxSegments)

	for _, table := range db.tables {
		for _, segment := range table.segments {
			segment.Close()
		}
	}

	db.lockfile.Unlock()
	db.open = false

	return err
}

// CloseWithMerge closes the database with control of the segment count. if segmentCount is 0, then
// the merge process is skipped
func (db *Database) CloseWithMerge(segmentCount int) error {
	dblock.Lock()
	defer dblock.Unlock()
	if !db.open {
		return DatabaseClosed
	}
	if db.err != nil {
		return db.err
	}
	if len(db.transactions) > 0 {
		return DatabaseHasOpenTransactions
	}

	db.Lock()
	db.closing = true
	db.Unlock()

	db.wg.Wait()

	if segmentCount > 0 {
		mergeDiskSegments0(db, segmentCount)
	}

	for _, table := range db.tables {
		for _, segment := range table.segments {
			segment.Close()
		}
	}

	db.lockfile.Unlock()
	db.open = false

	return nil
}

func (db *Database) nextSegmentID() uint64 {
	return atomic.AddUint64(&db.nextSegID, 1)
}

func less(a []byte, b []byte) bool {
	return bytes.Compare(a, b) < 0
}
func equal(a []byte, b []byte) bool {
	return bytes.Equal(a, b)
}
