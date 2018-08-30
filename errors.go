package keydb

import "errors"

var KeyNotFound = errors.New("key not found")
var KeyRemoved = errors.New("key removed")
var KeyTooLong = errors.New("key too long, max 1024")
var TransactionClosed = errors.New("transaction closed")
var DatabaseClosed = errors.New("database closed")
var DatabaseInUse = errors.New("database in use")
var DatabaseHasOpenTransactions = errors.New("database has open transactions")
var EndOfIterator = errors.New("end of iterator")
var ReadOnlySegment = errors.New("read only segment")
