package main

import (
	"fmt"
	"github.com/robaho/keydb"
	"log"
	"math/rand"
	"runtime"
	"time"
)

const nr = 1000000

func main() {

	runtime.GOMAXPROCS(4)

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
		tx.Put([]byte(fmt.Sprintf("mykey%7d", i)), []byte(fmt.Sprint("myvalue", i)))
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
	duration := end.Sub(start).Nanoseconds()

	fmt.Println("insert time ", nr, "records = ", duration/1000000.0, "ms, usec per op ", (duration/1000)/nr)
	start = time.Now()
	err = db.Close()
	end = time.Now()
	duration = end.Sub(start).Nanoseconds()

	fmt.Println("close time ", duration/1000000.0, "ms")
	if err != nil {
		panic(err)
	}

	testRead()

	db, err = keydb.Open("test/mydb", false)
	if err != nil {
		log.Fatal("unable to open database", err)
	}
	start = time.Now()
	db.CloseWithMerge(1)
	end = time.Now()
	duration = end.Sub(start).Nanoseconds()

	fmt.Println("close with merge 1 time ", duration/1000000.0, "ms")

	testRead()
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
	duration := end.Sub(start).Nanoseconds()

	fmt.Println("scan time ", duration/1000000.0, "ms, usec per op ", (duration/1000)/nr)

	start = time.Now()
	itr, err = tx.Lookup([]byte("mykey 300000"), []byte("mykey 799999"))
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
	duration = end.Sub(start).Nanoseconds()

	fmt.Println("scan time 50% ", duration/1000000.0, "ms, usec per op ", (duration/1000)/500000)

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	start = time.Now()

	for i := 0; i < nr/10; i++ {
		index := r.Intn(nr / 10)
		_, err := tx.Get([]byte(fmt.Sprintf("mykey%7d", index)))
		if err != nil {
			panic(err)
		}
	}
	end = time.Now()
	duration = end.Sub(start).Nanoseconds()

	fmt.Println("random access time ", (duration/1000.0)/int64(nr/10.0), "us per get")

	tx.Rollback()

	db.Close()
}
