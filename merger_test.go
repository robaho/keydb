package keydb

import (
	"fmt"
	"os"
	"testing"
)

func TestMerger(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m1 := newMemorySegment()
	for i := 0; i < 100000; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemorySegment()
	for i := 100000; i < 200000; i++ {
		m2.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	merged, err := mergeDiskSegments1("test", "testtable", 0, []segment{m1, m2})
	if err != nil {
		t.Fatal(err)
	}

	itr, err := merged.Lookup(nil, nil)
	count := 0

	for {
		_, _, err := itr.Next()
		if err != nil {
			break
		}
		count++
	}

	if count != 200000 {
		t.Fatal("wrong number of records", count)
	}
}

func TestMergerRemove(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m1 := newMemorySegment()
	for i := 0; i < 100000; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemorySegment()
	for i := 0; i < 100000; i++ {
		m2.Remove([]byte(fmt.Sprint("mykey", i)))
	}

	merged, err := mergeDiskSegments1("test", "testtable", 0, []segment{m1, m2})
	if err != nil {
		t.Fatal(err)
	}

	itr, err := merged.Lookup(nil, nil)
	count := 0

	for {
		_, v, err := itr.Next()
		if err != nil {
			break
		}
		if len(v) > 0 {
			count++
		}
	}

	if count != 0 {
		t.Fatal("wrong number of records", count)
	}
}
