package keydb

import (
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestDiskSegment(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m := newMemorySegment()
	m.Put([]byte("mykey"), []byte("myvalue"))
	m.Put([]byte("mykey2"), []byte("myvalue2"))
	m.Put([]byte("mykey3"), []byte("myvalue3"))
	itr, err := m.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := writeAndLoadSegment("test/keyfile", "test/datafile", itr)

	itr, err = ds.Lookup(nil, nil)
	if err != nil {
		t.Fatal("unable to lookup", err)
	}
	count := 0
	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 3 {
		t.Fatal("incorrect count", count)
	}

	value, err := ds.Get([]byte("mykey"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("myvalue")) {
		t.Fatal("incorrect values")
	}
	value, err = ds.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("myvalue2")) {
		t.Fatal("incorrect values")
	}
	value, err = ds.Get([]byte("mykey3"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("myvalue3")) {
		t.Fatal("incorrect values")
	}
	value, err = ds.Get([]byte("mykey4"))
	if err == nil {
		t.Fatal("key should not be found")
	}

}

func TestLargeDiskSegment(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m := newMemorySegment()
	for i := 0; i < 1000000; i++ {
		m.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	itr, err := m.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := writeAndLoadSegment("test/keyfile", "test/datafile", itr)

	itr, err = ds.Lookup(nil, nil)
	count := 0
	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 1000000 {
		t.Fatal("incorrect count", count)
	}

	value, err := ds.Get([]byte("mykey1"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("myvalue1")) {
		t.Fatal("incorrect values")
	}
	value, err = ds.Get([]byte("mykey2"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("myvalue2")) {
		t.Fatal("incorrect values")
	}
	value, err = ds.Get([]byte("mykey3"))
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(value, []byte("myvalue3")) {
		t.Fatal("incorrect values")
	}
	value, err = ds.Get([]byte("mykey1000000"))
	if err == nil {
		t.Fatal("key should not be found")
	}

	itr, err = ds.Lookup([]byte("mykey1"), []byte("mykey1"))
	key, data, err := itr.Next()
	if err != nil {
		t.Fatal("key should be found")
	}
	if string(key) != "mykey1" {
		t.Fatal("key is not mykey1")
	}
	if string(data) != "myvalue1" {
		t.Fatal("value is not myvalue1")
	}

}

func TestEmptySegment(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m := newMemorySegment()
	m.Put([]byte("mykey"), []byte("myvalue"))
	m.Remove([]byte("mykey"))
	itr, err := m.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := writeAndLoadSegment("test/keyfile", "test/datafile", itr)

	itr, err = ds.Lookup(nil, nil)
	count := 0
	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}
	// the segment must return the empty array for the key, so that removes are accurate in the case of multi segment multi
	if count != 1 {
		t.Fatal("incorrect count", count)
	}
}
