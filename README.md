# keydb

high performance key value database written in Go

bulk insert and sequential read \< 3 microsecs 

uses LSM trees, see https://en.wikipedia.org/wiki/Log-structured_merge-tree

limitation of max 1024 byte keys, to allow efficient on disk index searching, but has
compressed keys which allows for very efficient storage of time series data (market tick data)in the
same table

# TODOs

make some settings configurable

dbdump and dbload utilities
      
# How To Use

	tables := []keydb.Table{keydb.Table{"main", keydb.DefaultKeyCompare{}}}

	db, err := keydb.Open("test/mydb", tables, true)
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
        t.Fatal("unable to commit traabsaction", err)
    }
    err = db.Close()
    if err != nil {
        t.Fatal("unable to close database", err)
    }

