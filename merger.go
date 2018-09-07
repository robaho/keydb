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

		// ensure that only valid disk segments are merged

		mergable := make([]*diskSegment, 0)

		for _, s := range segments[index:] {
			ds, ok := s.(*diskSegment)

			if ok {
				mergable = append(mergable, ds)
				if len(mergable) == len(segments)/2 {
					break
				}
			} else {
				break
			}
		}

		if len(mergable) < 2 {
			return
		}

		id := mergable[len(mergable)-1].id
		segments = segments[index : index+len(mergable)]

		newseg, err := mergeDiskSegments1(db.path, table.table.Name, id, segments)
		if err != nil {
			panic(err)
		}

		table.Lock()
		for table.transactions > 0 {
			table.Unlock()
			time.Sleep(100 * time.Millisecond)
			table.Lock()
		}

		segments = table.segments

		for i, s := range mergable {
			if s != segments[i+index] {
				log.Fatalln("unexpected segment change,", s, segments[i])
			}
		}

		for _, s := range mergable {
			s.keyFile.Close()
			s.dataFile.Close()
			os.Remove(s.keyFile.Name())
			os.Remove(s.dataFile.Name())
		}

		newsegments := []segment{}

		newsegments = append(newsegments, segments[:index]...)
		newsegments = append(newsegments, newseg)
		newsegments = append(newsegments, segments[index+len(mergable):]...)

		table.segments = newsegments

		index += 1
		table.Unlock()
		time.Sleep(100 * time.Millisecond)
	}
}

var mergeSeq uint64

func mergeDiskSegments1(dbpath string, table string, id uint64, segments []segment) (segment, error) {

	base := filepath.Join(dbpath, table+".merged.")

	sid := strconv.FormatUint(id, 10)

	seq := atomic.AddUint64(&mergeSeq, 1)
	sseq := strconv.FormatUint(seq, 10)

	keyFilename := base + "." + sseq + ".keys." + sid
	dataFilename := base + "." + sseq + ".data." + sid

	compare := segments[0].getKeyCompare()

	ms := newMultiSegment(segments, nil, compare)
	itr, err := ms.Lookup(nil, nil)
	if err != nil {
		panic(err)
	}

	return writeAndLoadSegment(keyFilename, dataFilename, itr, compare)

}
