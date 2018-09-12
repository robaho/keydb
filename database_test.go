package keydb_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"strings"
	"testing"
	"time"
)
import "github.com/robaho/keydb"

func TestDatabase(t *testing.T) {
	keydb.Remove("test/mydb")

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
	if err != keydb.KeyNotFound {
		t.Fatal("should not of found removed key")
	}
	tx.Commit()
	err = db.CloseWithMerge(1)
	if err != nil {
		t.Fatal("unable to close database", err)
	}

	db, err = keydb.Open("test/mydb", true)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	tx, err = db.BeginTX("main")
	if err != nil {
		t.Fatal("unable to create transaction", err)
	}
	_, err = tx.Get([]byte("mykey"))
	if err != keydb.KeyNotFound {
		t.Fatal("should not of found removed key")
	}
	tx.Commit()
}

func TestCommit(t *testing.T) {
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", true)
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

func TestCommitSync(t *testing.T) {
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", true)
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
	err = tx.Put([]byte("mykey3"), []byte("myvalue3"))
	if err != nil {
		t.Fatal("unable to put key/Value", err)
	}
	_, err = tx.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	tx.CommitSync()

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
	_, err = tx.Get([]byte("mykey3"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}

	tx.CommitSync()

	err = db.Close()
	if err != nil {
		t.Fatal("unable to close database", err)
	}

}

func TestDatabaseIterator(t *testing.T) {
	keydb.Remove("test/mydb")

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

func TestSegmentMerge(t *testing.T) {
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", true)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	for i := 0; i < 100; i++ {
		tx, err := db.BeginTX("main")
		if err != nil {
			t.Fatal("unable to create transaction", err)
		}
		err = tx.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
		if err != nil {
			t.Fatal("unable to put key/Value", err)
		}

		tx.Commit()
	}

	var count = 0
	for {
		count0 := countFiles("test/mydb")
		time.Sleep(2 * time.Second)
		count1 := countFiles("test/mydb")
		if count0 == count1 {
			count = count0
			break
		}
	}

	db.CloseWithMerge(1)

	countX := countFiles("test/mydb")
	if countX < count {
		count = countX
	}

	if count != 2 { // there are two files for every segment
		t.Fatal("there should only be NaxSegments*2 files at this point, count is ", count)
	}

	db, err = keydb.Open("test/mydb", false)
	if err != nil {
		t.Fatal("unable to open database", err)
	}
	tx, err := db.BeginTX("main")
	itr, err := tx.Lookup(nil, nil)
	count = 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 100 {
		t.Fatal("incorrect count, should be 100, is ", count)
	}
}

func countFiles(path string) int {
	files, _ := ioutil.ReadDir(path)
	count := 0
	for _, file := range files {
		if strings.Index(file.Name(), ".keys.") >= 0 || strings.Index(file.Name(), ".data.") >= 0 {
			count++
		}
	}
	return count
}

func TestPersistence(t *testing.T) {
	keydb.Remove("test/mydb")

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

	tx.Commit()

	db.Close()

	db, err = keydb.Open("test/mydb", false)
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

func TestRemovedKeys(t *testing.T) {
	keydb.Remove("test/mydb")

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
	_, err = tx.Get([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to get by key", err)
	}
	tx.Commit()

	err = db.CloseWithMerge(1)
	if err != nil {
		t.Fatal("unable to close database", err)
	}

	db, err = keydb.Open("test/mydb", true)
	if err != nil {
		t.Fatal("unable to create database", err)
	}

	tx, err = db.BeginTX("main")
	if err != nil {
		t.Fatal("unable to create transaction", err)
	}
	_, err = tx.Remove([]byte("mykey"))
	if err != nil {
		t.Fatal("unable to remove key", err)
	}
	_, err = tx.Get([]byte("mykey"))
	if err != keydb.KeyNotFound {
		t.Fatal("should not of found key", err)
	}
	tx.Commit()
	err = db.CloseWithMerge(1)
	db, err = keydb.Open("test/mydb", true)
	if err != nil {
		t.Fatal("unable to create database", err)
	}
	tx, err = db.BeginTX("main")
	if err != nil {
		t.Fatal("unable to create transaction", err)
	}
	_, err = tx.Get([]byte("mykey"))
	if err != keydb.KeyNotFound {
		t.Fatal("should not of found key", err)
	}
	itr, err := tx.Lookup(nil, nil)
	if err != nil {
		t.Fatal("unable to open iterator", err)
	}
	_, _, err = itr.Next()
	if err != keydb.EndOfIterator {
		t.Fatal("iterator should be empty", err)
	}
	tx.Commit()
	err = db.CloseWithMerge(1)
}
