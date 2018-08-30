package keydb

import (
	"fmt"
	"os"
	"testing"
)

func TestMerger(t *testing.T) {
	os.RemoveAll("test")
	os.Mkdir("test", os.ModePerm)
	m1 := newMemorySegment(DefaultKeyCompare{})
	for i := 0; i < 100000; i++ {
		m1.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}
	m2 := newMemorySegment(DefaultKeyCompare{})
	for i := 100000; i < 200000; i++ {
		m2.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
	}

	merged, err := mergeDiskSegments0("test", "testtable", 0, m1, m2)
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
