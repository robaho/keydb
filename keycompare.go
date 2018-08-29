package keydb

import "bytes"

type KeyCompare interface {
	Less(keyA []byte, keyB []byte) bool
}

type DefaultKeyCompare struct{}

func (DefaultKeyCompare) Less(a []byte, b []byte) bool {
	return bytes.Compare(a, b) < 0
}
