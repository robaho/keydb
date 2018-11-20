package main

import (
	"bufio"
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

	tables := findTableNames(dbpath)
	if tables == nil || len(tables) == 0 {
		log.Fatal("database contains zero tables")
	}

	db, err := keydb.Open(dbpath, false)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Fprintf(w, "<db path=\"%s\">\n", html.EscapeString(dbpath))
	for _, v := range tables {
		name := v
		fmt.Fprintf(w, "\t<tabledata name=\"%s\">\n", name)
		tx, err := db.BeginTX(name)
		if err != nil {
			log.Fatal("unable to open tx on ", v, " ", err)
		}
		itr, err := tx.Lookup(nil, nil)
		if err != nil {
			log.Fatal("unable to open iterator on ", name, " ", err)
		}
		for {
			if key, value, err := itr.Next(); err == nil {
				fmt.Fprintf(w, "\t\t<entry><key>%s</key> <value>%s</value></entry>\n", html.EscapeString(string(key)), html.EscapeString(string(value)))
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
	for k := range names {
		namesS = append(namesS, k)

	}
	return namesS
}
