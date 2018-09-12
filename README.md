# keydb

high performance key value database written in Go

bulk insert and sequential read \< 3 micro secs

random access read of disk based record, approx. 13 us

uses LSM trees, see https://en.wikipedia.org/wiki/Log-structured_merge-tree

limitation of max 1024 byte keys, to allow efficient on disk index searching, but has
compressed keys which allows for very efficient storage of time series data
(market tick data) in the same table

use the dbdump and dbload utilities to save/restore databases to a single file, but just zipping up the directory works as
well...
      
# TODOs

make some settings configurable

purge removed key/value, it currently stores an empty []byte 

# How To Use

	db, err := keydb.Open("test/mydb", true)
	if err != nil {
		t.Fatal("unable to create database", err)
	}
	tx, err := db.BeginTX("main")
	if err != nil {
		t.Fatal("unable to create transaction", err)
	}
	err = tx.Put([]byte("mykey"), []byte("myvalue"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
    err = tx.Commit()
    if err != nil {
        t.Fatal("unable to commit transaction", err)
    }
    err = db.Close()
    if err != nil {
        t.Fatal("unable to close database", err)
    }

