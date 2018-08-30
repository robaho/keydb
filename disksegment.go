package keydb

import (
	"bytes"
	"encoding/binary"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// the key file uses 4096 byte blocks, the format is
// keylen uint16
// key []byte
// dataoffset int64
// datalen uint32 (if datalen is 0, the key is "removed"
//
// the data file can only be read in conjunction with the key
// file since there is no length attribute, it is a raw appended
// byte array with the offset and length in the key file
//

type diskSegment struct {
	keyFile   *os.File
	keyBlocks int64
	dataFile  *os.File
	compare   KeyCompare
	id        uint64
}

type diskSegmentIterator struct {
	segment      *diskSegment
	lower        []byte
	upper        []byte
	buffer       []byte
	block        int64
	bufferOffset int
	key          []byte
	data         []byte
	isValid      bool
	err          error
	finished     bool
}

func loadDiskSegments(directory string, table string, compare KeyCompare) []segment {
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		panic(err)
	}
	segments := []segment{}
	for _, file := range files {
		if strings.HasPrefix(file.Name(), table) {
			id := getSegmentID(file.Name())
			keyFilename := filepath.Join(directory, table+".keys."+strconv.FormatUint(id, 10))
			dataFilename := filepath.Join(directory, table+".data."+strconv.FormatUint(id, 10))
			segments = append(segments, newDiskSegment(keyFilename, dataFilename, compare))
		}
	}
	sort.Slice(segments, func(i, j int) bool {
		return segments[i].(*diskSegment).id < segments[j].(*diskSegment).id
	})
	return segments
}

func getSegmentID(filename string) uint64 {
	base := filepath.Base(filename)
	index := strings.LastIndex(base, ".")
	if index >= 0 {
		id, err := strconv.Atoi(base[index+1:])
		if err == nil {
			return uint64(id)
		}
	}
	return 0
}

func newDiskSegment(keyFilename, dataFilename string, compare KeyCompare) segment {

	segmentID := getSegmentID(keyFilename)

	ds := &diskSegment{}
	kf, err := os.Open(keyFilename)
	if err != nil {
		panic(err)
	}
	df, err := os.Open(dataFilename)
	if err != nil {
		panic(err)
	}
	ds.keyFile = kf
	ds.dataFile = df

	fi, err := kf.Stat()
	if err != nil {
		panic(err)
	}

	ds.keyBlocks = (fi.Size()-1)/keyBlockSize + 1
	ds.compare = compare
	ds.id = segmentID

	return ds
}

func (dsi *diskSegmentIterator) Next() (key []byte, value []byte, err error) {
	if dsi.isValid {
		dsi.isValid = false
		return dsi.key, dsi.data, dsi.err
	}
	dsi.nextKeyValue()
	return dsi.key, dsi.data, dsi.err
}

func (dsi *diskSegmentIterator) peekKey() ([]byte, error) {
	if dsi.isValid {
		return dsi.key, dsi.err
	}
	dsi.nextKeyValue()
	return dsi.key, dsi.err
}

func (dsi *diskSegmentIterator) nextKeyValue() error {
	if dsi.finished {
		return EndOfIterator
	}
	for {
		keylen := binary.LittleEndian.Uint16(dsi.buffer[dsi.bufferOffset:])
		if keylen == 0xFFFF {
			dsi.block++
			if dsi.block == dsi.segment.keyBlocks {
				dsi.finished = true
				dsi.err = EndOfIterator
				dsi.key = nil
				dsi.data = nil
				dsi.isValid = true
				return dsi.err
			}
			dsi.segment.keyFile.ReadAt(dsi.buffer, dsi.block*keyBlockSize)
			dsi.bufferOffset = 0
			continue
		}
		dsi.bufferOffset += 2
		key := dsi.buffer[dsi.bufferOffset : dsi.bufferOffset+int(keylen)]
		dsi.bufferOffset += int(keylen)
		dataoffset := binary.LittleEndian.Uint64(dsi.buffer[dsi.bufferOffset:])
		dsi.bufferOffset += 8
		datalen := binary.LittleEndian.Uint32(dsi.buffer[dsi.bufferOffset:])
		dsi.bufferOffset += 4

		if dsi.lower != nil {
			if dsi.segment.compare.Less(key, dsi.lower) {
				continue
			}
			if bytes.Equal(key, dsi.lower) {
				goto found
			}
		}
		if dsi.upper != nil {
			if bytes.Equal(key, dsi.upper) {
				goto found
			}
			if !dsi.segment.compare.Less(key, dsi.upper) {
				dsi.finished = true
				dsi.isValid = true
				dsi.key = nil
				dsi.data = nil
				dsi.err = EndOfIterator
				return EndOfIterator
			}
		}
	found:
		dsi.data = make([]byte, datalen)
		_, err := dsi.segment.dataFile.ReadAt(dsi.data, int64(dataoffset))
		dsi.key = key
		dsi.isValid = true
		return err
	}
}

func (ds *diskSegment) getKeyCompare() KeyCompare {
	return ds.compare
}
func (ds *diskSegment) Put(key []byte, value []byte) error {
	panic("disk segments are not mutable, unable to Put")
}

func (ds *diskSegment) Get(key []byte) ([]byte, error) {
	offset, len, err := binarySearch(ds, key)
	if err != nil {
		return nil, err
	}
	buffer := make([]byte, len)
	_, err = ds.dataFile.ReadAt(buffer, offset)
	if err != nil {
		return nil, err
	}
	return buffer, nil
}

func binarySearch(ds *diskSegment, key []byte) (offset int64, len uint32, err error) {
	buffer := make([]byte, keyBlockSize)

	block, err := binarySearch0(ds, 0, ds.keyBlocks-1, key, buffer)
	if err != nil {
		return 0, 0, err
	}
	// just scan 2 blocks
	offset, len, err = scanBlock(ds, block, key, buffer)
	if err == nil {
		return
	}
	return scanBlock(ds, block+1, key, buffer)

}

// returns the block that may contain the key, or possible the next block - since we do not have a 'last key' of the block
func binarySearch0(ds *diskSegment, lowblock int64, highBlock int64, key []byte, buffer []byte) (int64, error) {
	if highBlock-lowblock <= 1 {
		return lowblock, nil
	}

	block := (highBlock-lowblock)/2 + lowblock

	ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	keylen := binary.LittleEndian.Uint16(buffer)
	skey := buffer[2 : 2+keylen]

	if ds.compare.Less(key, skey) {
		return binarySearch0(ds, lowblock, block, key, buffer)
	} else {
		return binarySearch0(ds, block, highBlock, key, buffer)
	}
}
func scanBlock(ds *diskSegment, block int64, key []byte, buffer []byte) (offset int64, len uint32, err error) {
	ds.keyFile.ReadAt(buffer, block*keyBlockSize)

	index := 0
	for {
		keylen := binary.LittleEndian.Uint16(buffer[index:])
		if keylen == 0xFFFF {
			return 0, 0, KeyNotFound
		}
		endkey := index + 2 + int(keylen)
		_key := buffer[index+2 : endkey]
		if bytes.Equal(_key, key) {
			offset = int64(binary.LittleEndian.Uint64(buffer[endkey:]))
			len = binary.LittleEndian.Uint32(buffer[endkey+8:])
			if len == 0 {
				err = KeyRemoved
			}
			return
		}
		if !ds.compare.Less(_key, key) {
			return 0, 0, KeyNotFound
		}
		index = endkey + 12
	}
}

func (ds *diskSegment) Remove(key []byte) ([]byte, error) {
	panic("disk segments are immutable, unable to Remove")
}

func (ds *diskSegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	buffer := make([]byte, keyBlockSize)
	var block int64 = 0
	if lower != nil {
		startBlock, err := binarySearch0(ds, 0, ds.keyBlocks-1, lower, buffer)
		if err != nil {
			return nil, err
		}
		block = startBlock
	}
	ds.keyFile.ReadAt(buffer, block*keyBlockSize)
	return &diskSegmentIterator{segment: ds, lower: lower, upper: upper, buffer: buffer, block: block}, nil
}

func (ds *diskSegment) Close() {
	ds.keyFile.Close()
	ds.dataFile.Close()
}
