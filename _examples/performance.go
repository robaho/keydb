package main

import (
	"fmt"
	"github.com/robaho/keydb"
	"log"
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
		}
		tx, err = db.BeginTX("main")
		if err != nil {
			panic(err)
		}
	}
	fmt.Println("time ", (time.Now().Sub(start)).Nanoseconds()/1000000.0, "ms")
	db.Close()

}
