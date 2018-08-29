package keydb

import "fmt"

func main() {
	// The values are sorted in a way that causes two single rotations and a double rotation.
	values := []string{"d", "b", "g", "g", "c", "e", "a", "h", "f", "i", "j", "l", "k"}
	data := []string{"delta", "bravo", "golang", "golf", "charlie", "echo", "alpha", "hotel", "foxtrot", "india", "juliett", "lima", "kilo"}

	tree := &tree{compare: DefaultKeyCompare{}}
	for i := 0; i < len(values); i++ {
		fmt.Println("insert " + values[i] + ": " + data[i])
		tree.Insert([]byte(values[i]), []byte(data[i]))
		tree.Dump()
		fmt.Println()
	}

	fmt.Print("Sorted values: | ")
	tree.Traverse(tree.Root, func(n *Node) bool { fmt.Print(n.key, ": ", n.data, " | "); return false })
	fmt.Println()
}
