package keydb

import (
	"os"
	"path/filepath"
	"strconv"
	"time"
)

func mergeDiskSegments(db *Database) {
	defer db.wg.Done()

	for {
		db.Lock()
		if db.closing {
			db.Unlock()
			return
		}

	again:
		db.Unlock()

		for _, table := range db.tables {

			db.Lock()
			segments := table.segments
			db.Unlock()

			if len(segments) > 1 {
				seg0, ok := segments[0].(*diskSegment)
				if !ok {
					continue
				}
				seg1, ok := segments[1].(*diskSegment)
				if !ok {
					continue
				}

				newseg, err := mergeDiskSegments0(db.path, table.table.Name, seg1.id, seg0, seg1)
				if err != nil {
					continue
				}

				for {
					db.Lock()
					if len(db.transactions) < 0 {
						goto tryAgain
					}

					seg0.keyFile.Close()
					seg0.dataFile.Close()
					seg1.keyFile.Close()
					seg1.dataFile.Close()

					os.Remove(seg0.keyFile.Name())
					os.Remove(seg0.dataFile.Name())
					os.Remove(seg1.keyFile.Name())
					os.Remove(seg1.dataFile.Name())

					//fmt.Println("merged segments", seg0.keyFile.Name(), seg1.keyFile.Name())

					break

				tryAgain:
					db.Unlock()
					time.Sleep(100 * time.Millisecond)
				}

				segments = table.segments
				if newseg != nil && len(segments) > 1 && seg0 == segments[0] && seg1 == segments[1] {
					table.segments = append([]segment{newseg}, segments[2:]...)
				}

				goto again
			}
		}

		time.Sleep(1 * time.Second)
	}
}

// returns with the database locked if the segment is non-nil

func mergeDiskSegments0(dbpath string, table string, id uint64, seg0 segment, seg1 segment) (segment, error) {

	base := filepath.Join(dbpath, "merged."+table)

	sid := strconv.FormatUint(id, 10)

	keyFilename := base + ".keys." + sid
	dataFilename := base + ".data." + sid

	ms := newMultiSegment([]segment{seg0, seg1}, nil, seg0.getKeyCompare())
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		panic(err)
	}

	return writeAndLoadSegment(keyFilename, dataFilename, itr, seg0.getKeyCompare())

}
