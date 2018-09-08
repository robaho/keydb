package keydb

// multiSegment presents multiple segments as a single segment. The segments are ordered, since the different segments
// may contain the same key with different values (due to an update or a remove)
type multiSegment struct {
	segments []segment
	writable segment
	compare  KeyCompare
}

type multiSegmentIterator struct {
	iterators []LookupIterator
	compare   KeyCompare
}

func (msi *multiSegmentIterator) peekKey() ([]byte, error) {
	panic("peekKey called on multiSegmentIterator")
}

func (msi *multiSegmentIterator) Next() (key []byte, value []byte, err error) {
	var currentIndex = -1
	var lowest []byte = nil

	// find the lowest next non-deleted key in any of the iterators

	for i := len(msi.iterators) - 1; i >= 0; i-- {
		iterator := msi.iterators[i]

		var key []byte
		var err error
		for {
			key, err = iterator.peekKey()
			if err == nil && key == nil {
				iterator.Next()
			} else {
				break
			}
		}

		if err != nil {
			continue
		}

		if lowest == nil || msi.compare.Less(key, lowest) {
			lowest = make([]byte, len(key))
			copy(lowest, key)
			currentIndex = i
		}
	}

	if currentIndex == -1 {
		return nil, nil, EndOfIterator
	}

	key, value, err = msi.iterators[currentIndex].Next()

	// advance all of the iterators past the current
	for i := len(msi.iterators) - 1; i >= 0; i-- {
		if i == currentIndex {
			continue
		}
		iterator := msi.iterators[i]
		for {
			key, err := iterator.peekKey()
			if err != nil {
				break
			}
			if key == nil || !msi.compare.Less(lowest, key) {
				msi.Next()
			} else {
				break
			}
		}
	}

	return
}

func newMultiSegment(segments []segment, writable segment, compare KeyCompare) *multiSegment {
	return &multiSegment{segments: segments, writable: writable, compare: compare}
}

func (ms *multiSegment) Put(key []byte, value []byte) error {
	if ms.writable == nil {
		panic(ReadOnlySegment)
	}
	return ms.writable.Put(key, value)
}

func (ms *multiSegment) Get(key []byte) ([]byte, error) {
	// segments are in chronological order, so search in reverse
	for i := len(ms.segments) - 1; i >= 0; i-- {
		s := ms.segments[i]
		val, err := s.Get(key)
		if err == KeyRemoved {
			return nil, KeyRemoved
		}
		if err == nil {
			return val, nil
		}
	}
	return nil, KeyNotFound
}

func (ms *multiSegment) Remove(key []byte) ([]byte, error) {
	if ms.writable == nil {
		panic(ReadOnlySegment)
	}
	return ms.writable.Remove(key)
}

func (ms *multiSegment) Lookup(lower []byte, upper []byte) (LookupIterator, error) {
	iterators := make([]LookupIterator, 0)
	for _, v := range ms.segments {
		iterator, err := v.Lookup(lower, upper)
		if err != nil {
			return nil, err
		}
		iterators = append(iterators, iterator)
	}
	return &multiSegmentIterator{iterators: iterators, compare: ms.compare}, nil
}
