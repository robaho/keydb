package main

import (
	"bufio"
	"encoding/hex"
	"flag"
	"fmt"
	"github.com/robaho/keydb"
	"html"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// dump a database to stdout
func main() {
	asStrings := flag.Bool("s", false, "use strings for keys and values")
	path := flag.String("path", "", "set the database path")
	out := flag.String("out", "dbdump.xml", "set output file")

	flag.Parse()

	dbpath := filepath.Clean(*path)

	outfile, err := os.Create(*out)
	if err != nil {
		log.Fatal("unable to open output file ", err)
	}
	defer outfile.Close()

	w := bufio.NewWriter(outfile)

	fi, err := os.Stat(dbpath)
	if err != nil {
		log.Fatalln("unable to open database directory", err)
	}

	if !fi.IsDir() {
		log.Fatalln("path is not a directory")
	}

	tableNames := findTableNames(dbpath)
	if tableNames == nil || len(tableNames) == 0 {
		log.Fatal("database contains zero tables")
	}
	var compare keydb.KeyCompare = keydb.DefaultKeyCompare{}
	if *asStrings {
		compare = keydb.StringKeyCompare{}
	}

	tables := make([]keydb.Table, len(tableNames))
	for i, v := range tableNames {
		table := keydb.Table{Name: v, Compare: compare}
		tables[i] = table
	}

	db, err := keydb.Open(dbpath, tables, false)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(w, "<db path=\"%s\" strings=\"%t\">\n", html.EscapeString(dbpath), *asStrings)
	fmt.Fprintln(w, "\t<tables>")
	for _, v := range tables {
		fmt.Fprintf(w, "\t\t<table name=\"%s\"></table>\n", v.Name)
	}
	fmt.Fprintln(w, "\t</tables>")
	for _, v := range tables {
		name := v.Name
		fmt.Fprintf(w, "\t<tabledata name=\"%s\">\n", name)
		tx, err := db.BeginTX(name)
		if err != nil {
			log.Fatal("unable to open tx on ", v.Name, " ", err)
		}
		itr, err := tx.Lookup(nil, nil)
		if err != nil {
			log.Fatal("unable to open iterator on ", name, " ", err)
		}
		for {
			if key, value, err := itr.Next(); err == nil {
				if *asStrings {
					fmt.Fprintf(w, "\t\t<entry><key>%s</key> <value>%s</value></entry>\n", html.EscapeString(string(key)), html.EscapeString(string(value)))
				} else {
					fmt.Fprintln(w, "\t\t<entry><key>", hex.EncodeToString(key), "</key>", "<value>", hex.EncodeToString(value), "</value></entry>")
				}
			} else {
				if err == keydb.EndOfIterator {
					break
				}
				if err != nil {
					log.Fatal("error processing table ", name)
				}
			}
		}
		fmt.Fprintln(w, "\t</tabledata>")
	}
	fmt.Fprintln(w, "</db>")

	err = w.Flush()
	if err != nil {
		log.Fatal("unable to flush writer, io errors,", err)
	}
}

func findTableNames(dbpath string) []string {
	names := make(map[string]bool)

	infos, err := ioutil.ReadDir(dbpath)
	if err != nil {
		log.Fatal("unable to read directory", err)
	}

	for _, fi := range infos {
		if strings.Index(fi.Name(), ".keys.") >= 0 {
			table := strings.Split(fi.Name(), ".")[0]
			names[table] = true
		}
	}
	var namesS []string
	for k, _ := range names {
		namesS = append(namesS, k)

	}
	return namesS
}
