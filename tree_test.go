package keydb

import (
	"fmt"
	"testing"
)

func TestTree(t *testing.T) {
	// The values are sorted in a way that causes two single rotations and a double rotation.
	values := []string{"d", "b", "g", "g", "c", "e", "a", "h", "f", "i", "j", "l", "k"}
	data := []string{"delta", "bravo", "golang", "golf", "charlie", "echo", "alpha", "hotel", "foxtrot", "india", "juliett", "lima", "kilo"}

	tree := &Tree{Compare: DefaultKeyCompare{}}
	for i := 0; i < len(values); i++ {
		fmt.Println("insert " + values[i] + ": " + data[i])
		tree.Insert([]byte(values[i]), []byte(data[i]))
	}

	fmt.Print("Sorted values: | ")

	nodes := tree.FindNodes(nil, nil)

	for _, v := range nodes {
		fmt.Print("(", string(v.Key), ",", string(v.Value), ") ")
	}
	fmt.Println()
}
