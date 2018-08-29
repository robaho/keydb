package keydb_test

import (
	"bytes"
	"testing"
)
import "github.com/robaho/keydb"

func TestDatabase(t *testing.T) {
	tables := []keydb.Table{keydb.Table{"main", keydb.DefaultKeyCompare{}}}
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", tables)
	if err == nil {
		t.Fatal("database should not exist", err)
	}

	db, err = keydb.Create("test/mydb", tables)
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
	_, err = tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}
	err = tx.Put([]byte("mykey2"), []byte("myvalue2"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	_, err = tx.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	large := make([]byte, 1025)
	err = tx.Put(large, []byte("myvalue"))
	if err == nil {
		t.Fatal("should not of been able to Put a large key")
	}
	_, err = tx.Remove([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to remove by key", err)
	}
	_, err = tx.Get([]byte("mykey"))
	if err == nil {
		t.Fatal("should not of found removed key")
	}
	tx.Commit()
	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}

}

func TestCommit(t *testing.T) {
	tables := []keydb.Table{keydb.Table{"main", keydb.DefaultKeyCompare{}}}
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", tables)
	if err == nil {
		t.Fatal("database should not exist", err)
	}

	db, err = keydb.Create("test/mydb", tables)
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
	_, err = tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}
	err = tx.Put([]byte("mykey2"), []byte("myvalue2"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	_, err = tx.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	tx.Commit()

	tx, err = db.BeginTX("main")
	if err != nil {
		t.Fatal("unable to create transaction", err)
	}
	_, err = tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}
	_, err = tx.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	tx.Commit()

	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}

}

func TestDatabaseIterator(t *testing.T) {
	tables := []keydb.Table{keydb.Table{"main", keydb.DefaultKeyCompare{}}}
	keydb.Remove("test/mydb")

	db, err := keydb.Create("test/mydb", tables)
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
	err = tx.Put([]byte("mykey2"), []byte("myvalue2"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	err = tx.Put([]byte("mykey3"), []byte("myvalue3"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	itr, err := tx.Lookup([]byte("mykey2"), nil)

	key, value, err := itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	if !bytes.Equal(key, []byte("mykey2")) {
		t.Fatal("wrong key", string(key), "mykey2")
	}
	if !bytes.Equal(value, []byte("myvalue2")) {
		t.Fatal("wrong Value", string(key), "myvalue2")
	}
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	if !bytes.Equal(key, []byte("mykey3")) {
		t.Fatal("wrong key", string(key), "mykey3")
	}
	if !bytes.Equal(value, []byte("myvalue3")) {
		t.Fatal("wrong Value", string(key), "myvalue3")
	}
	itr, err = tx.Lookup(nil, []byte("mykey2"))
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	itr, err = tx.Lookup([]byte("mykey2"), []byte("mykey2"))
	key, value, err = itr.Next()
	if err != nil {
		t.Fatal("iterator failed", err)
	}
	itr, err = tx.Lookup([]byte("mykey4"), nil)

	tx.Commit()
	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}
}

func TestPersistence(t *testing.T) {
	tables := []keydb.Table{keydb.Table{"main", keydb.DefaultKeyCompare{}}}
	keydb.Remove("mydb")

	db, err := keydb.Open("mydb", tables)
	if err == nil {
		t.Fatal("database should not exist", err)
	}

	db, err = keydb.Create("mydb", tables)
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

	tx.Commit()

	db.Close()

	db, err = keydb.Open("mydb", tables)
	if err != nil {
		t.Fatal("database did not exist", err)
	}

	tx, err = db.BeginTX("main")
	if err != nil {
		t.Fatal("unable to create transaction", err)
	}
	_, err = tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	tx.Commit()

	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}
}
