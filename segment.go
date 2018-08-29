package keydb

type segment interface {
	Put(key []byte, value []byte) error
	Get(key []byte) ([]byte, error)
	Remove(key []byte) ([]byte, error)
	Lookup(lower []byte, upper []byte) (LookupIterator, error)
	getKeyCompare() KeyCompare
}
