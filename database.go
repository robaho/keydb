package keydb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type Database struct {
	sync.Mutex
	tables       map[string]*internalTable
	open         bool
	transactions map[uint64]*Transaction
	count        int
	path         string
	wg           sync.WaitGroup
	nextSegID    uint64
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
var openDBs = make(map[string]*Database)

func Open(path string, tables []Table) (*Database, error) {
	dblock.Lock()
	defer dblock.Unlock()

	return open(path, tables)
}

func open(path string, tables []Table) (*Database, error) {

	path = filepath.Clean(path)

	if db, ok := openDBs[path]; ok {
		db.count++
		return db, nil
	}

	fi, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	switch mode := fi.Mode(); {
	case mode.IsDir():
	case mode.IsRegular():
		return nil, errors.New("path is not a directory")
	}

	db := &Database{path: path, count: 1, open: true}
	db.transactions = make(map[uint64]*Transaction)
	db.tables = make(map[string]*internalTable)
	for _, v := range tables {
		it := &internalTable{table: v, segments: []segment{}}
		db.tables[v.Name] = it
	}
	openDBs[path] = db
	return db, nil
}

func Create(path string, tables []Table) (*Database, error) {
	dblock.Lock()
	defer dblock.Unlock()

	path = filepath.Clean(path)

	if _, ok := openDBs[path]; ok {
		return nil, DatabaseInUse
	}

	err := os.MkdirAll(path, os.ModePerm)
	if err != nil {
		return nil, err
	}

	return open(path, tables)
}

func Remove(path string) error {
	dblock.Lock()
	defer dblock.Unlock()

	if _, ok := openDBs[path]; ok {
		return DatabaseInUse
	}

	path = filepath.Clean(path)

	fi, err := os.Stat(path)
	if err != nil {
		return err
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
	if db.count == 1 && len(db.transactions) > 0 {
		return DatabaseHasOpenTransactions
	}
	db.count--
	if db.count == 0 {
		db.wg.Wait()
		db.open = false
		delete(openDBs, db.path)
	}
	return nil
}
func (db *Database) nextSegmentID() uint64 {
	return atomic.AddUint64(&db.nextSegID, 1)
}
