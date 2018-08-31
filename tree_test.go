package keydb

import (
	"log"
	"strings"
	"testing"
)

func TestTree(t *testing.T) {
	// The values are sorted in a way that causes two single rotations and a double rotation.
	values := []string{"d", "b", "g", "g", "c", "e", "a", "h", "f", "i", "j", "l", "k"}
	data := []string{"delta", "bravo", "golang", "golf", "charlie", "echo", "alpha", "hotel", "foxtrot", "india", "juliett", "lima", "kilo"}

	tree := &Tree{Compare: StringKeyCompare{}}
	for i := 0; i < len(values); i++ {
		tree.Insert([]byte(values[i]), []byte(data[i]))
	}

	nodes := tree.FindNodes(nil, nil)

	var prev = ""
	for _, v := range nodes {
		if prev == "" {
			prev = string(v.Key)
		} else {
			s := string(v.Key)
			if strings.Compare(prev, s) >= 0 {
				log.Fatalln("keys are out of order ", prev, s)
			}
		}
	}
}
