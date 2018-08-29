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
	m := newMemorySegment(DefaultKeyCompare{})
	m.Put([]byte("mykey"), []byte("myvalue"))
	m.Put([]byte("mykey2"), []byte("myvalue2"))
	m.Put([]byte("mykey3"), []byte("myvalue3"))
	itr, err := m.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := writeAndLoadSegment("test/keyfile", "test/datafile", itr, m.getKeyCompare())
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
	m := newMemorySegment(DefaultKeyCompare{})
	for i := 0; i < 100000; i++ {
		m.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	itr, err := m.Lookup(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	ds, err := writeAndLoadSegment("test/keyfile", "test/datafile", itr, m.getKeyCompare())
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
	value, err = ds.Get([]byte("mykey200000"))
	if err == nil {
		t.Fatal("key should not be found")
	}

}
