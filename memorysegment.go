package keydb

//
// memorySegment wraps an im-memory binary Tree, so the number of items that can be inserted or removed
// in a transaction is limited by available memory. the Tree uses a nil Value to designate a key that
// has been removed from the table
//

type memorySegment struct {
	tree *Tree
}

func newMemorySegment(compare KeyCompare) segment {
	ms := new(memorySegment)
	ms.tree = &Tree{Compare: compare}

	return ms
}

func (ms *memorySegment) getKeyCompare() KeyCompare {
	return ms.tree.Compare
}

func (ms *memorySegment) Put(key []byte, value []byte) error {
	ms.tree.Insert(key, value)
	return nil
}
func (ms *memorySegment) Get(key []byte) ([]byte, error) {
	value, ok := ms.tree.Find(key)
	if !ok {
		return nil, KeyNotFound
	}
	if value == nil {
		return nil, KeyRemoved
	}
	return value, nil

}
func (ms *memorySegment) Remove(key []byte) ([]byte, error) {
	value, ok := ms.tree.Remove(key)
	if ok {
		return value, nil
	}
	return nil, KeyRemoved
}

func (ms *memorySegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	return &entrySetIterator{results: ms.tree.FindNodes(lower, upper), index: 0}, nil
}

func (ms *memorySegment) Close() error {
	return nil
}

type entrySetIterator struct {
	results []TreeEntry
	index   int
}

func (es *entrySetIterator) Next() (key []byte, value []byte, err error) {
	if es.index >= len(es.results) {
		return nil, nil, EndOfIterator
	}
	key = es.results[es.index].Key
	value = es.results[es.index].Value
	es.index++
	return key, value, nil
}
func (es *entrySetIterator) peekKey() ([]byte, error) {
	if es.index >= len(es.results) {
		return nil, EndOfIterator
	}
	key := es.results[es.index].Key
	return key, nil
}
