package keydb

import (
	"log"
	"os"
	"path/filepath"
	"strconv"
	"sync/atomic"
	"time"
)

var maxSegments = 8

// merge on disk segments for the database
func mergeDiskSegments(db *Database) {
	defer db.wg.Done()
	//defer fmt.Println("merger complete on "+db.path)

	for {
		db.Lock()
		if db.closing {
			db.Unlock()
			return
		}

		// the following prevents a Close from occurring while this
		// routine is running

		db.wg.Add(1)

		db.Unlock()

		mergeDiskSegments0(db, maxSegments)

		db.wg.Done()

		time.Sleep(1 * time.Second)
	}
}

func mergeDiskSegments0(db *Database, segmentCount int) {
	for _, table := range db.tables {
		mergeTableSegments(db, table, segmentCount)
	}
}

func mergeTableSegments(db *Database, table *internalTable, segmentCount int) {

	var index = 0

	for {

		table.Lock()
		segments := table.segments
		table.Unlock()

		if len(segments) <= segmentCount {
			return
		}

		if index+1 >= len(segments) {
			index = 0
		}

		// ensure that only valid disk segments are merged

		seg0, ok := segments[index].(*diskSegment)
		if !ok {
			index = 0
			continue
		}
		seg1, ok := segments[index+1].(*diskSegment)
		if !ok {
			index = 0
			continue
		}

		newseg, err := mergeDiskSegments1(db.path, table.table.Name, seg1.id, seg0, seg1)
		if err != nil {
			panic(err)
		}

		for {
			table.Lock()
			if table.transactions > 0 {
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
			table.Unlock()
			time.Sleep(100 * time.Millisecond)
		}

		segments = table.segments
		if newseg != nil && len(segments) > 1 && seg0 == segments[index] && seg1 == segments[index+1] {
			table.segments = append(append(segments[:index], newseg), segments[index+2:]...)
		} else {
			log.Fatalln("unexpected segment change,", seg0, segments[index], seg1, segments[index+1])
		}

		table.Unlock()

		index += 2
	}
}

var mergeSeq uint64

func mergeDiskSegments1(dbpath string, table string, id uint64, seg0 segment, seg1 segment) (segment, error) {

	base := filepath.Join(dbpath, table+".merged.")

	sid := strconv.FormatUint(id, 10)

	seq := atomic.AddUint64(&mergeSeq, 1)
	sseq := strconv.FormatUint(seq, 10)

	keyFilename := base + "." + sseq + ".keys." + sid
	dataFilename := base + "." + sseq + ".data." + sid

	ms := newMultiSegment([]segment{seg0, seg1}, nil, seg0.getKeyCompare())
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		panic(err)
	}

	return writeAndLoadSegment(keyFilename, dataFilename, itr, seg0.getKeyCompare())

}
