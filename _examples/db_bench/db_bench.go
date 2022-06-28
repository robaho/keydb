package main

import (
	"fmt"
	"github.com/robaho/keydb"
	"io/ioutil"
	"log"
	"math/rand"
	"runtime"
	"time"
)

// benchmark similar in scope to leveldb db_bench.cc, uses 16 byte keys and 100 byte values

const nr = 1000000

var value []byte

func main() {

	value = make([]byte, 100)
	rand.Read(value)

	runtime.GOMAXPROCS(4)

	testWrite()
	testWriteSync()

	testRead()

	db, err := keydb.Open("test/mydb", false)
	if err != nil {
		log.Fatal("unable to open database", err)
	}
	start := time.Now()
	db.CloseWithMerge(1)
	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("close with merge 1 time ", float64(duration)/1000, "ms")

	testRead()
}

func testWrite() {
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", true)
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	tx, err := db.BeginTX("main")
	if err != nil {
		panic(err)
	}
	for i := 0; i < nr; i++ {
		tx.Put([]byte(fmt.Sprintf("%07d.........", i)), value)
		if i%10000 == 0 {
			tx.CommitSync()
			tx, err = db.BeginTX("main")
			if err != nil {
				panic(err)
			}
		}
	}
	tx.CommitSync()

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("insert time ", nr, "records = ", duration/1000, "ms, usec per op ", float64(duration)/nr)
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", duration/1000.0, "ms")
	if err != nil {
		panic(err)
	}

	fmt.Println("database size ", dbsize("test/mydb"))
}

func testWriteSync() {
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", true)
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	for i := 0; i < nr; i++ {
		tx, err := db.BeginTX("main")
		if err != nil {
			panic(err)
		}
		tx.Put([]byte(fmt.Sprintf("%07d.........", i)), value)
		tx.CommitSync()
	}

	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("insert sync time ", nr, "records = ", duration/1000, "ms, usec per op ", float64(duration)/nr)
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("close time ", duration/1000.0, "ms")
	if err != nil {
		panic(err)
	}

	fmt.Println("database size ", dbsize("test/mydb"))
}

func dbsize(path string) string {
	files, err := ioutil.ReadDir(path)
	if err != nil {
		log.Fatal(err)
	}
	var size int64 = 0
	for _, file := range files {
		size += file.Size()
	}
	return fmt.Sprintf("%.1dM", size/(1024*1024))
}

func testRead() {
	db, err := keydb.Open("test/mydb", false)
	if err != nil {
		log.Fatal("unable to open database", err)
	}
	start := time.Now()
	tx, err := db.BeginTX("main")
	if err != nil {
		panic(err)
	}
	itr, err := tx.Lookup(nil, nil)
	count := 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != nr {
		log.Fatal("incorrect count != ", nr, ", count is ", count)
	}
	end := time.Now()
	duration := end.Sub(start).Microseconds()

	fmt.Println("scan time ", duration/1000, "ms, usec per op ", float64(duration)/nr)

	start = time.Now()
	itr, err = tx.Lookup([]byte("0300000........."), []byte("0799999........."))
	count = 0
	for {
		_, _, err = itr.Next()
		if err != nil {
			break
		}
		count++
	}
	if count != 500000 {
		log.Fatal("incorrect count != 500000, count is ", count)
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("scan time 50% ", duration/1000, "ms, usec per op ", float64(duration)/500000)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	start = time.Now()

	for i := 0; i < nr/10; i++ {
		index := r.Intn(nr / 10)
		_, err := tx.Get([]byte(fmt.Sprintf("%07d.........", index)))
		if err != nil {
			panic(err)
		}
	}
	end = time.Now()
	duration = end.Sub(start).Microseconds()

	fmt.Println("random access time ", float64(duration)/(nr/10), "us per get")

	tx.Rollback()
	db.Close()
}
