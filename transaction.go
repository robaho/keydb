package keydb

import (
	"errors"
	"sync/atomic"
)

var txID uint64

type Transaction struct {
	table  string
	open   bool
	db     *Database
	id     uint64
	access *multiSegment
}

// a Transaction can only be used by a single Go routine
func (db *Database) BeginTX(table string) (*Transaction, error) {
	dblock.Lock()
	defer dblock.Unlock()

	if !db.open {
		return nil, errors.New("database is not open")
	}

	it, ok := db.tables[table]
	if !ok {
		return nil, errors.New("unknown table")
	}

	tx := &Transaction{db: db, table: table, open: true}
	tx.id = atomic.AddUint64(&txID, 1)

	memory := newMemorySegment(it.table.Compare)

	tx.access = &multiSegment{append(it.segments, memory), memory, it.table.Compare}

	db.transactions[tx.id] = tx

	return tx, nil
}

func (tx *Transaction) Get(key []byte) ([]byte, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	return tx.access.Get(key)
}
func (tx *Transaction) Put(key []byte, value []byte) error {
	if !tx.open {
		return TransactionClosed
	}
	if len(key) > 1024 {
		return KeyTooLong
	}
	return tx.access.Put(key, value)

}
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
// then the range is unbounded on that side
func (tx *Transaction) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	return tx.access.Lookup(lower, upper)
}
func (tx *Transaction) Commit() error {
	tx.db.Lock()
	defer tx.db.Unlock()

	delete(tx.db.transactions, tx.id)

	table := tx.db.tables[tx.table]

	table.segments = tx.access.segments

	tx.db.tables[tx.table] = table

	tx.db.wg.Add(1)

	go writeSegmentToDisk(tx.db, tx.table, tx.access.writable)

	return nil
}
func (tx *Transaction) Rollback() error {
	tx.db.Lock()
	defer tx.db.Unlock()

	tx.access = nil
	tx.open = false

	delete(tx.db.transactions, tx.id)

	return nil
}
