package keydb

import "golang.org/x/exp/mmap"

type memoryMappedFile struct {
	file   *mmap.ReaderAt
	length int
	name   string
}

func newMemoryMappedFile(filename string) (*memoryMappedFile, error) {
	f := memoryMappedFile{}
	file, err := mmap.Open(filename)
	if err != nil {
		return nil, err
	}
	f.file = file
	f.length = file.Len()
	f.name = filename
	return &f, nil
}

func (f *memoryMappedFile) Length() int64 {
	return int64(f.length)
}

func (f *memoryMappedFile) ReadAt(buffer []byte, off int64) (int, error) {
	return f.file.ReadAt(buffer, off)
}

func (f *memoryMappedFile) Close() error {
	return f.file.Close()
}

func (f *memoryMappedFile) Name() string {
	return f.name
}
