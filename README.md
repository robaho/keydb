# keydb

## keydb is deprecated, use [robaho/leveldb](http://www.github.com/robaho/leveldb) which is more stable and uses the Google LevelDB api (similar).

high performance key value database written in Go

bulk insert and sequential read < 1 micro sec

random access read of disk based record < 4 micro secs

uses LSM trees, see https://en.wikipedia.org/wiki/Log-structured_merge-tree

limitation of max 1024 byte keys, to allow efficient on disk index searching, but has
compressed keys which allows for very efficient storage of time series data
(market tick data) in the same table

use the dbdump and dbload utilities to save/restore databases to a single file, but just zipping up the directory works as
well...

see the related http://github.com/robaho/keydbr which allows remote access to a keydb instance, and allows a keydb database to be shared by multiple processes
      
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

# Performance

Using example/performance.go

<pre>
Using Go 1.15.5:

insert time  10000000 records =  17890 ms, usec per op  1.7890143
close time  8477 ms
scan time  2887 ms, usec per op  0.2887559
scan time 50%  81 ms, usec per op  0.162584
random access time  3.508029 us per get
close with merge 1 time  0.148 ms
scan time  2887 ms, usec per op  0.2887248
scan time 50%  85 ms, usec per op  0.171406
random access time  3.487226 us per get
</pre>
