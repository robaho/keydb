package keydb

import (
	"errors"
	"github.com/nightlyone/lockfile"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

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
}

type Table struct {
	Name    string
	Compare KeyCompare
}

type internalTable struct {
	sync.Mutex
	table    Table
	segments []segment
}

type LookupIterator interface {
	// returns EndOfIterator when complete
	Next() (key []byte, value []byte, err error)
	// returns the next non-deleted key in the index
	peekKey() ([]byte, error)
}

var dblock sync.RWMutex

// if createIfNeeded is true, them if the db doesn't exist it will be created
func Open(path string, tables []Table, createIfNeeded bool) (*Database, error) {
	dblock.Lock()
	defer dblock.Unlock()

	db, err := open(path, tables)
	if err == NoDatabaseFound && createIfNeeded == true {
		return create(path, tables)
	}
	return db, err
}

func open(path string, tables []Table) (*Database, error) {

	path = filepath.Clean(path)

	fi, err := os.Stat(path)
	if err != nil {
		return nil, NoDatabaseFound
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
	case mode.IsRegular():
		return nil, NoDatabaseFound
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
	for _, v := range tables {
		it := &internalTable{table: v, segments: loadDiskSegments(path, v.Name, v.Compare)}
		db.tables[v.Name] = it
	}

	db.wg.Add(1)
	go mergeDiskSegments(db)

	return db, nil
}

func create(path string, tables []Table) (*Database, error) {
	path = filepath.Clean(path)

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return open(path, tables)
}

func Remove(path string) error {
	dblock.Lock()
	defer dblock.Unlock()

	path = filepath.Clean(path)

	fi, err := os.Stat(path)
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

	switch mode := fi.Mode(); {
	case mode.IsDir():
	case mode.IsRegular():
		return errors.New("path is not a directory")
	}

	return os.RemoveAll(path)
}

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
	db.Lock()

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
