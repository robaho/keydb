package keydb

import (
	"errors"
	"sync/atomic"
	"time"
)

var txID uint64

type Transaction struct {
	table  string
	open   bool
	db     *Database
	id     uint64
	access *multiSegment
}

// create a transaction for a database table.
// a Transaction can only be used by a single Go routine.
// each transaction should be completed with either Commit, or Rollback
func (db *Database) BeginTX(table string) (*Transaction, error) {
	db.Lock()
	defer db.Unlock()

	if db.closing {
		return nil, DatabaseClosed
	}

	it, ok := db.tables[table]
	if !ok {
		return nil, errors.New("unknown table")
	}

	for { // wait to start transaction if table has too many segments
		if len(it.segments) > maxSegments*10 {
			db.Unlock()
			time.Sleep(100 * time.Millisecond)
			db.Lock()
		} else {
			break
		}
	}

	it.transactions++

	tx := &Transaction{db: db, table: table, open: true}
	tx.id = atomic.AddUint64(&txID, 1)

	memory := newMemorySegment(it.table.Compare)

	tx.access = &multiSegment{append(it.segments, memory), memory, it.table.Compare}

	db.transactions[tx.id] = tx

	return tx, nil
}

// retrieve a value from the table
func (tx *Transaction) Get(key []byte) ([]byte, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	return tx.access.Get(key)
}

// put a value into the table. empty keys are not supported.
func (tx *Transaction) Put(key []byte, value []byte) error {
	if !tx.open {
		return TransactionClosed
	}
	if len(key) > 1024 {
		return KeyTooLong
	}
	if len(key) == 0 {
		return EmptyKey
	}
	return tx.access.Put(key, value)

}

// remove a key and its value from the table. empty keys are not supported.
func (tx *Transaction) Remove(key []byte) ([]byte, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	return tx.access.Remove(key)
}

// find matching record between lower and upper inclusive. lower or upper can be nil and
// then the range is unbounded on that side. Using the iterator after the transaction has
// been Commit/Rollback is not supported.
func (tx *Transaction) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	return tx.access.Lookup(lower, upper)
}

// persist any changes to the table. after Commit the transaction can no longer be used
func (tx *Transaction) Commit() error {
	tx.db.Lock()
	delete(tx.db.transactions, tx.id)
	table := tx.db.tables[tx.table]
	tx.open = false
	tx.db.Unlock()

	table.Lock()
	defer table.Unlock()

	table.transactions--
	table.segments = append(table.segments, tx.access.writable)

	tx.db.wg.Add(1)

	go writeSegmentToDisk(tx.db, tx.table, tx.access.writable)

	return nil
}

// discard any changes to the table. after Rollback the transaction can no longer be used
func (tx *Transaction) Rollback() error {
	tx.db.Lock()
	defer tx.db.Unlock()

	tx.access = nil
	tx.open = false

	delete(tx.db.transactions, tx.id)

	table := tx.db.tables[tx.table]
	table.transactions--

	return nil
}
