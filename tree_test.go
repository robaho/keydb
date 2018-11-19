package keydb

import (
	"fmt"
	"log"
	"math"
	"strings"
	"testing"
)

func TestTree(t *testing.T) {
	// The values are sorted in a way that causes two single rotations and a double rotation.
	values := []string{"d", "b", "g", "g", "c", "e", "a", "h", "f", "i", "j", "l", "k"}
	data := []string{"delta", "bravo", "golang", "golf", "charlie", "echo", "alpha", "hotel", "foxtrot", "india", "juliett", "lima", "kilo"}

	tree := &Tree{}
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

// check balancing
func TestTree2(t *testing.T) {
	// The values are sorted in a way that causes two single rotations and a double rotation.

	tree := &Tree{}
	for i := 0; i < 150000; i++ {
		tree.Insert([]byte(fmt.Sprint("mykey", i)), []byte(fmt.Sprint("myval", i)))
	}

	height := tree.bfsDump()

	if float64(height) > math.Log2(150000)*math.Phi {
		log.Fatalln("height should be log2(150000) * phi, height is ", height)
	}
}
