package keydb

import (
	"bytes"
	"encoding/binary"
	"os"
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
}

type diskSegmentIterator struct {
}

func newDiskSegment(keyFilename, dataFilename string, compare KeyCompare) segment {
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

	return ds
}

func (dsi *diskSegmentIterator) HasNext() bool {
	panic("implement me")
}

func (dsi *diskSegmentIterator) Next() (key []byte, value []byte, err error) {
	panic("implement me")
}

func (dsi *diskSegmentIterator) peekKey() []byte {
	panic("implement me")
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
	panic("disk segments are not mutable, unable to Remove")
}

func (ds *diskSegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	panic("implement me")
}
