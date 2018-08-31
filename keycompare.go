package keydb

import (
	"bytes"
	"strings"
)

// interface for comparing keys
type KeyCompare interface {
	// return true if keyA is "less than" keyB
	Less(keyA []byte, keyB []byte) bool
}

// default key compare that before a byte by byte comparison
type DefaultKeyCompare struct{}

func (DefaultKeyCompare) Less(a []byte, b []byte) bool {
	return bytes.Compare(a, b) < 0
}

// key compare that handles the keys as strings
type StringKeyCompare struct{}

func (StringKeyCompare) Less(a []byte, b []byte) bool {
	return strings.Compare(string(a), string(b)) < 0
}
