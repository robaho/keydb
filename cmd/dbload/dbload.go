package main

import (
	"bufio"
	"encoding/hex"
	"encoding/xml"
	"flag"
	"fmt"
	"github.com/robaho/keydb"
	"log"
	"os"
	"path/filepath"
	"strconv"
)

// load a database a dbdump file
func main() {
	path := flag.String("path", "", "set the database path")
	in := flag.String("in", "dbdump.xml", "set the input file")
	remove := flag.Bool("remove", true, "remove existing db")
	create := flag.Bool("create", true, "create if doesn't exist")

	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	dbpath := filepath.Clean(*path)

	infile, err := os.Open(*in)
	if err != nil {
		log.Fatal("unable to open output file ", err)
	}
	defer infile.Close()

	if *remove {
		err = keydb.Remove(dbpath)
		if err != keydb.NoDatabaseFound {
			log.Fatal("unable to remove ")
		}
	}

	r := bufio.NewReader(infile)

	var asStrings bool
	var tableNames []string

	var db *keydb.Database
	var compare keydb.KeyCompare = keydb.DefaultKeyCompare{}

	decoder := xml.NewDecoder(r)
	var tx *keydb.Transaction

	type KeyValueElement struct {
		Key   string
		Value string
	}

	var inElement string
	for {
		// Read tokens from the XML document in a stream.
		t, _ := decoder.Token()
		if t == nil {
			break
		}
		// Inspect the type of the token just read.
		switch se := t.(type) {
		case xml.StartElement:
			// If we just read a StartElement token
			inElement = se.Name.Local
			// ...and its name is "page"
			if inElement == "db" {
				asStrings, _ = strconv.ParseBool(getAttr("strings", se.Attr))
				if asStrings {
					compare = keydb.StringKeyCompare{}
				}
			} else if inElement == "table" {
				tableNames = append(tableNames, getAttr("name", se.Attr))
			} else if inElement == "tabledata" {
				tx, err = db.BeginTX(getAttr("name", se.Attr))
				if err != nil {
					log.Fatal(err)
				}
			} else if inElement == "key" {
				e := KeyValueElement{}

				decoder.DecodeElement(e, t.(*xml.StartElement))

				fmt.Println("kv ", e)

				if asStrings {
					err := tx.Put([]byte(e.Key), []byte(e.Value))
					if err != nil {
						log.Fatal(err)
					}
				} else {
					key, _ := hex.DecodeString(e.Key)
					value, _ := hex.DecodeString(e.Key)
					err := tx.Put(key, value)
					if err != nil {
						log.Fatal(err)
					}
				}
			}
		case xml.EndElement:
			outElement := se.Name.Local
			if outElement == "tables" {
				// at this ppint we know the tables, so create the database
				tables := make([]keydb.Table, len(tableNames))
				for i, v := range tableNames {
					tables[i] = keydb.Table{v, compare}
				}
				db, err = keydb.Open(dbpath, tables, *create)
				if err != nil {
					log.Fatal(err)
				}
			}
			if outElement == "tabledata" {
				tx.Commit()
			}
		default:
		}

	}

}

func getAttr(name string, attrs []xml.Attr) string {
	for _, v := range attrs {
		if v.Name.Local == name {
			return v.Value
		}
	}
	return ""
}
