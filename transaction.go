package keydb

import (
	"errors"
	"sync/atomic"
	"time"
)

var txID uint64

// Transaction for keydb operations
type Transaction struct {
	table  string
	open   bool
	db     *Database
	id     uint64
	multi  *multiSegment
	memory segment
}

type transactionLookup struct {
	LookupIterator
}

// skip removed records
func (tl *transactionLookup) Next() (key, value []byte, err error) {
	for {
		key, value, err = tl.LookupIterator.Next()
		if value == nil && err == nil {
			continue
		}
		return
	}
}

// GetID returns the internal transaction identifier
func (tx *Transaction) GetID() uint64 {
	return tx.id
}

// BeginTX starts a transaction for a database table.
// a Transaction can only be used by a single Go routine.
// each transaction should be completed with either Commit, or Rollback
func (db *Database) BeginTX(table string) (*Transaction, error) {
	db.Lock()
	defer db.Unlock()

	if db.err != nil {
		return nil, db.err
	}

	if db.closing {
		return nil, DatabaseClosed
	}

	it, ok := db.tables[table]
	if !ok {
		it = &internalTable{name: table, segments: loadDiskSegments(db.path, table)}
		db.tables[table] = it
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

	it.Lock()
	defer it.Unlock()
	it.transactions++

	tx := &Transaction{db: db, table: table, open: true}
	tx.id = atomic.AddUint64(&txID, 1)

	tx.memory = newMemorySegment()

	tx.multi = newMultiSegment(append(it.segments, tx.memory))

	db.transactions[tx.id] = tx

	return tx, nil
}

// Get a value for a key, error is non-nil if the key was not found or an error occurred
func (tx *Transaction) Get(key []byte) (value []byte, err error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	value, err = tx.multi.Get(key)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, KeyNotFound
	}
	return
}

// Put a key/value pair into the table, overwriting any existing entry. empty keys are not supported.
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
	return tx.memory.Put(key, value)
}

// Remove a key and its value from the table. empty keys are not supported.
func (tx *Transaction) Remove(key []byte) ([]byte, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	if len(key) > 1024 {
		return nil, KeyTooLong
	}
	value, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	tx.memory.Remove(key)
	return value, nil
}

// Lookup finds matching record between lower and upper inclusive. lower or upper can be nil and
// and then the range is unbounded on that side. Using the iterator after the transaction has
// been Commit/Rollback is not supported.
func (tx *Transaction) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	if !tx.open {
		return nil, TransactionClosed
	}
	itr, err := tx.multi.Lookup(lower, upper)
	if err != nil {
		return nil, err
	}
	return &transactionLookup{itr}, nil
}

// Commit persists any changes to the table. after Commit the transaction can no longer be used
func (tx *Transaction) Commit() error {
	tx.db.Lock()
	delete(tx.db.transactions, tx.id)
	table := tx.db.tables[tx.table]
	tx.open = false
	tx.db.Unlock()

	table.Lock()
	defer table.Unlock()

	table.transactions--
	table.segments = append(table.segments, tx.memory)

	tx.db.wg.Add(1)

	go func() {
		err := writeSegmentToDisk(tx.db, tx.table, tx.memory)
		if err != nil {
			tx.db.Lock()
			tx.db.err = errors.New("transaction failed: " + err.Error())
			tx.db.Unlock()
		}
	}()

	return nil
}

// CommitSync persists any changes to the table, waiting for disk segment to be written. note that synchronous writes are not used,
// so that a hard OS failure could leave the database in a corrupted state. after Commit the transaction can no longer be used
func (tx *Transaction) CommitSync() error {
	tx.db.Lock()
	delete(tx.db.transactions, tx.id)
	table := tx.db.tables[tx.table]
	tx.open = false

	err := tx.db.err

	tx.db.Unlock()

	if err != nil {
		return err
	}

	table.Lock()

	table.transactions--
	table.segments = append(table.segments, tx.memory)

	tx.db.wg.Add(1)

	table.Unlock()

	err = writeSegmentToDisk(tx.db, tx.table, tx.memory)

	return err
}

// Rollback discards any changes to the table. after Rollback the transaction can no longer be used
func (tx *Transaction) Rollback() error {
	tx.db.Lock()
	defer tx.db.Unlock()

	table := tx.db.tables[tx.table]
	table.Lock()

	tx.multi = nil
	tx.open = false

	delete(tx.db.transactions, tx.id)

	defer table.Unlock()
	table.transactions--

	return nil
}
