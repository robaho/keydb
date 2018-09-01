package main

import (
	"fmt"
	"github.com/robaho/keydb"
	"log"
	"math/rand"
	"runtime"
	"time"
)

func main() {

	runtime.GOMAXPROCS(4)

	tables := []keydb.Table{keydb.Table{"main", keydb.DefaultKeyCompare{}}}
	keydb.Remove("test/mydb")

	db, err := keydb.Open("test/mydb", tables, true)
	if err != nil {
		log.Fatal("unable to create database", err)
	}

	start := time.Now()
	tx, err := db.BeginTX("main")
	if err != nil {
		panic(err)
	}
	for i := 0; i < 1000000; i++ {
		tx.Put([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myvalue", i)))
		if i%10000 == 0 {
			tx.Commit()
			tx, err = db.BeginTX("main")
			if err != nil {
				panic(err)
			}
		}
	}
	tx.Commit()

	fmt.Println("insert time ", (time.Now().Sub(start)).Nanoseconds()/1000000.0, "ms")
	err = db.Close()
	if err != nil {
		panic(err)
	}

	db, err = keydb.Open("test/mydb", tables, false)
	if err != nil {
		log.Fatal("unable to create database", err)
	}
	start = time.Now()
	tx, err = db.BeginTX("main")
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
	if count != 1000000 {
		log.Fatal("incorrect count != 1000000, count is ", count)
	}
	fmt.Println("scan time ", (time.Now().Sub(start)).Nanoseconds()/1000000.0, "ms")

	start = time.Now()

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 100000; i++ {
		index := r.Intn(1000000)
		_, err := tx.Get([]byte(fmt.Sprint("mykey", index)))
		if err != nil {
			panic(err)
		}
	}

	fmt.Println("random access time ", ((time.Now().Sub(start)).Nanoseconds()/1000.0)/100000.0, "us per get")

	tx.Rollback()

	db.Close()

}
