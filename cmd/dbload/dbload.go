package main

import (
	"bufio"
	"encoding/hex"
	"encoding/xml"
	"flag"
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
	remove := flag.Bool("remove", true, "remove existing db if it exists")
	create := flag.Bool("create", true, "create database if it doesn't exist")

	flag.Parse()

	if *path == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	dbpath := filepath.Clean(*path)

	infile, err := os.Open(*in)
	if err != nil {
		log.Fatal("unable to open input file ", err)
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

	var db *keydb.Database

	decoder := xml.NewDecoder(r)
	var tx *keydb.Transaction

	type EntryElement struct {
		Key   string `xml:"key"`
		Value string `xml:"value"`
	}

	db, err = keydb.Open(dbpath, *create)
	if err != nil {
		panic(err)
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
			} else if inElement == "tabledata" {
				tx, err = db.BeginTX(getAttr("name", se.Attr))
				if err != nil {
					log.Fatal(err)
				}
			} else if inElement == "entry" {
				e := EntryElement{}

				decoder.DecodeElement(&e, &se)

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
			if outElement == "tabledata" {
				tx.Commit()
			}
		default:
		}

	}

	err = db.Close()
	if err != nil {
		panic(err)
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
