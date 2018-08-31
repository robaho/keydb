package keydb

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const keyBlockSize = 4096
const maxKeySize = 1024
const removed = int64(-1)
const endOfBlock uint16 = 0xFFFF

var emptySegment = errors.New("empty segment")

// called to write a memory segment to disk
func writeSegmentToDisk(db *Database, table string, seg segment) {
	defer db.wg.Done() // allows database to close with no writers pending

	var err error

	itr, err := seg.Lookup(nil, nil)
	if err != nil {
		return
	}

	id := db.nextSegmentID()

	keyFilename := filepath.Join(db.path, fmt.Sprint(table, ".keys.", id))
	dataFilename := filepath.Join(db.path, fmt.Sprint(table, ".data.", id))

	ds, err := writeAndLoadSegment(keyFilename, dataFilename, itr, seg.getKeyCompare())
	if err != nil && err != emptySegment {
		panic(err)
	}

	db.tables[table].Lock()
	defer db.tables[table].Unlock()

	segments := make([]segment, 0)
	for _, v := range db.tables[table].segments {
		if v == seg {
			if ds != nil {
				segments = append(segments, ds)
			}
		} else {
			segments = append(segments, v)
		}
	}

	db.tables[table].segments = segments
}

func writeAndLoadSegment(keyFilename, dataFilename string, itr LookupIterator, compare KeyCompare) (segment, error) {

	keyFilenameTmp := keyFilename + ".tmp"
	dataFilenameTmp := dataFilename + ".tmp"

	err := writeSegmentFiles(keyFilenameTmp, dataFilenameTmp, itr)
	if err != nil {
		os.Remove(keyFilenameTmp)
		os.Remove(dataFilenameTmp)
		return nil, err
	}

	os.Rename(keyFilenameTmp, keyFilename)
	os.Rename(dataFilenameTmp, dataFilename)

	return newDiskSegment(keyFilename, dataFilename, compare), nil
}

func writeSegmentFiles(keyFName, dataFName string, itr LookupIterator) error {

	keyF, err := os.OpenFile(keyFName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println("unable to create key segments", err)
		return err
	}
	defer keyF.Close()

	dataF, err := os.OpenFile(dataFName, os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		fmt.Println("unable to create data file", err)
		return err
	}
	defer dataF.Close()

	keyW := bufio.NewWriter(keyF)
	dataW := bufio.NewWriter(dataF)

	var dataOffset int64
	var keyBlockLen int
	var keyCount = 0

	var zeros = make([]byte, keyBlockSize)

	for {
		key, value, err := itr.Next()
		if err != nil {
			break
		}
		keyCount++
		keylen := len(key)
		dataW.Write(value)
		if keyBlockLen+2+keylen+8+4 >= keyBlockSize-2 { // need to leave room for 'end of block marker'
			// key won't fit in block so move to next
			keyW.WriteByte(0xFF)
			keyW.WriteByte(0xFF)
			keyBlockLen += 2
			keyW.Write(zeros[:keyBlockSize-keyBlockLen])
			keyBlockLen = 0
		}

		var dataLen uint32
		if value == nil {
			dataLen = 0
		} else {
			dataLen = uint32(len(value))
		}

		var data = []interface{}{
			uint16(keylen),
			key,
			int64(dataOffset),
			uint32(dataLen)}
		buf := new(bytes.Buffer)
		for _, v := range data {
			err = binary.Write(buf, binary.LittleEndian, v)
			if err != nil {
				goto failed
			}
		}
		keyBlockLen += 2 + keylen + 8 + 4
		keyW.Write(buf.Bytes())
		dataOffset += int64(dataLen)
	}

	// pad key file to block size
	if keyBlockLen > 0 && keyBlockLen < keyBlockSize {
		// key won't fit in block so move to next
		keyW.WriteByte(0xFF)
		keyW.WriteByte(0xFF)
		keyBlockLen += 2
		keyW.Write(zeros[:keyBlockSize-keyBlockLen])
		keyBlockLen = 0
	}

	keyW.Flush()
	dataW.Flush()

	if keyCount == 0 {
		return emptySegment
	}

	return nil

failed:
	fmt.Println("unable to write segment", err)
	return err
}
