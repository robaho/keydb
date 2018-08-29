package keydb

import (
	"bytes"
	"fmt"
	"strings"
)

// `Node` gets a new field, `bal`, to store the height difference between the node's subtrees.
type Node struct {
	key   []byte
	data  []byte
	left  *Node
	right *Node
	bal   int // height(n.right) - height(n.left)
	tree  *tree
}

/* ### The modified `insert` function
 */

// `insert` takes a search value and some data and inserts a new node (unless a node with the given
// search value already exists, in which case `insert` only replaces the data).
//
// It returns:
//
// * `true` if the height of the tree has increased.
// * `false` otherwise.
func (n *Node) insert(key, data []byte) bool {
	// The following actions depend on whether the new search key is equal, less, or greater than
	// the current node's search key.

	if bytes.Equal(key, n.key) {
		// node already exists nothing changes
		n.data = data
		return false
	}

	compare := n.tree.compare

	if compare.Less(key, n.key) {
		// If there is no left child, create a new one.
		if n.left == nil {
			// Create a new node.
			n.left = &Node{key: key, data: data, tree: n.tree}
			// If there is no right child, the new child node has increased the height of this subtree.
			if n.right == nil {
				// The new left child is the only child.
				n.bal = -1
			} else {
				// There is a left and a right child. The right child cannot have children;
				// otherwise the tree would already have been out of balance at `n`.
				n.bal = 0
			}
		} else {
			// The left child is not nil. Continue in the left subtree.
			if n.left.insert(key, data) {
				// If the subtree's balance factor has become either -2 or 2, the subtree must be rebalanced.
				if n.left.bal < -1 || n.left.bal > 1 {
					n.rebalance(n.left)
				} else {
					// If no rebalancing occurred, the left subtree has grown by one: Decrease the balance of the current node by one.
					n.bal--
				}
			}
		}
	} else {
		if n.right == nil {
			n.right = &Node{key: key, data: data, tree: n.tree}
			if n.left == nil {
				n.bal = 1
			} else {
				n.bal = 0
			}
		} else {
			if n.right.insert(key, data) {
				if n.right.bal < -1 || n.right.bal > 1 {
					n.rebalance(n.right)
				} else {
					n.bal++
				}
			}
		}
	}
	if n.bal != 0 {
		return true
	}
	// No more adjustments to the ancestor nodes required.
	return false
}

/* ### The new `rebalance()` method and its helpers `rotateLeft()`, `rotateRight()`, `rotateLeftRight()`, and `rotateRightLeft`.

 **Important note: Many of the assumptions about balances, left and right children, etc, as well as much of the logic usde in the functions below, apply to the `insert` operation only. For `Delete` operations, different rules and operations apply.** As noted earlier, this article focuses on `insert` only, to keep the code short and clear.
 */

// `rotateLeft` takes a child node and rotates the child node's subtree to the left.
func (n *Node) rotateLeft(c *Node) {
	// Save `c`'s right child.
	r := c.right
	// `r`'s left subtree gets reassigned to `c`.
	c.right = r.left
	// `c` becomes the left child of `r`.
	r.left = c
	// Make the parent node (that is, the current one) point to the new root node.
	if c == n.left {
		n.left = r
	} else {
		n.right = r
	}
	// Finally, adjust the balances. After a single rotation, the subtrees are always of the same height.
	c.bal = 0
	r.bal = 0
}

// `rotateRight` is the mirrored version of `rotateLeft`.
func (n *Node) rotateRight(c *Node) {
	l := c.left
	c.left = l.right
	l.right = c
	if c == n.left {
		n.left = l
	} else {
		n.right = l
	}
	c.bal = 0
	l.bal = 0
}

// `rotateRightLeft` first rotates the right child of `c` to the right, then `c` to the left.
func (n *Node) rotateRightLeft(c *Node) {
	// `rotateRight` assumes that the left child has a left child, but as part of the rotate-right-left process,
	// the left child of `c.right` is a leaf. We therefore have to tweak the balance factors before and after
	// calling `rotateRight`.
	// If we did not do that, we would not be able to reuse `rotateRight` and `rotateLeft`.
	c.right.left.bal = 1
	c.rotateRight(c.right)
	c.right.bal = 1
	n.rotateLeft(c)
}

// `rotateLeftRight` first rotates the left child of `c` to the left, then `c` to the right.
func (n *Node) rotateLeftRight(c *Node) {
	c.left.right.bal = -1 // The considerations from rotateRightLeft also apply here.
	c.rotateLeft(c.left)
	c.left.bal = -1
	n.rotateRight(c)
}

// `rebalance` brings the (sub-)tree with root node `c` back into a balanced state.
func (n *Node) rebalance(c *Node) {
	switch {
	// left subtree is too high, and left child has a left child.
	case c.bal == -2 && c.left.bal == -1:
		n.rotateRight(c)
		// right subtree is too high, and right child has a right child.
	case c.bal == 2 && c.right.bal == 1:
		n.rotateLeft(c)
		// left subtree is too high, and left child has a right child.
	case c.bal == -2 && c.left.bal == 1:
		n.rotateLeftRight(c)
		// right subtree is too high, and right child has a left child.
	case c.bal == 2 && c.right.bal == -1:
		n.rotateRightLeft(c)
	}
}

func (n *Node) Find(key []byte) ([]byte, bool) {

	if n == nil {
		return nil, false
	}

	compare := n.tree.compare

	if bytes.Equal(key, n.key) {
		return n.data, true
	}

	if compare.Less(key, n.key) {
		return n.left.Find(key)
	} else {
		return n.right.Find(key)
	}
}

func (n *Node) Remove(key []byte) ([]byte, bool) {

	if n == nil {
		return nil, false
	}

	compare := n.tree.compare

	if bytes.Equal(key, n.key) {
		prev := n.data
		n.data = nil
		return prev, true
	}

	if compare.Less(key, n.key) {
		return n.left.Remove(key)
	} else {
		return n.right.Remove(key)
	}
}

// `Dump` dumps the structure of the subtree starting at node `n`, including node search values and balance factors.
// Parameter `i` sets the line indent. `lr` is a prefix denoting the left or the right child, respectively.
func (n *Node) Dump(i int, lr string) {
	if n == nil {
		return
	}
	indent := ""
	if i > 0 {
		//indent = strings.Repeat(" ", (i-1)*4) + "+" + strings.Repeat("-", 3)
		indent = strings.Repeat(" ", (i-1)*4) + "+" + lr + "--"
	}
	fmt.Printf("%s%s[%d]\n", indent, n.key, n.bal)
	n.left.Dump(i+1, "L")
	n.right.Dump(i+1, "R")
}

/*
## tree

Changes to the tree type:

* `insert` now takes care of rebalancing the root node if necessary.
* A new method, `Dump`, exist for invoking `Node.Dump`.
* `Delete` is gone.

*/

//
type tree struct {
	Root    *Node
	compare KeyCompare
}

func (t *tree) Insert(key, data []byte) {
	if t.Root == nil {
		t.Root = &Node{key: key, data: data, tree: t}
		return
	}
	t.Root.insert(key, data)
	// If the root node gets out of balance,
	if t.Root.bal < -1 || t.Root.bal > 1 {
		t.rebalance()
	}
}

// `Node`'s `rebalance` method is invoked from the parent node of the node that needs rebalancing.
// However, the root node of a tree has no parent node.
// Therefore, `tree`'s `rebalance` method creates a fake parent node for rebalancing the root node.
func (t *tree) rebalance() {
	fakeParent := &Node{left: t.Root, key: []byte{}, tree: t}
	fakeParent.rebalance(t.Root)
	// Fetch the new root node from the fake parent node
	t.Root = fakeParent.left
}

func (t *tree) Find(key []byte) ([]byte, bool) {
	if t.Root == nil {
		return nil, false
	}
	return t.Root.Find(key)
}

func (t *tree) Remove(key []byte) ([]byte, bool) {
	old, ok := t.Root.Remove(key)
	if !ok {
		t.Insert(key, nil)
		return nil, false
	} else {
		return old, true
	}
}

func (t *tree) Traverse(n *Node, f func(*Node) bool) bool {
	if n == nil {
		return false
	}
	if !t.Traverse(n.left, f) {
		return false
	}
	f(n)
	if !t.Traverse(n.right, f) {
		return false
	}
	return false
}

type Entry struct {
	key   []byte
	value []byte
}

/* The functions prints all the keys which in the given range [k1..k2].
   The function assumes than lower < upper, or lower/upper is nil */
func FindNodes(node *Node, compare KeyCompare, lower []byte, upper []byte, fn func(*Node)) {
	if node == nil {
		return
	}

	/* Since the desired o/p is sorted, recurse for left subtree first
	   If node.key is greater than lower, then only we can get o/p keys
	   in left subtree */
	if lower == nil || compare.Less(lower, node.key) {
		FindNodes(node.left, compare, lower, upper, fn)
	}

	if isNodeInRange(node, compare, lower, upper) {
		fn(node)
	}

	/* If node.key is smaller than upper, then only we can get o/p keys
	in right subtree */
	if upper == nil || compare.Less(node.key, upper) {
		FindNodes(node.right, compare, lower, upper, fn)
	}
}

func (t *tree) FindNodes(lower []byte, upper []byte) []Entry {
	if t.Root == nil {
		return nil
	}

	compare := t.compare

	results := make([]Entry, 0)

	nodeInRange := func(n *Node) {
		results = append(results, Entry{n.key, n.data})
	}
	FindNodes(t.Root, compare, lower, upper, nodeInRange)
	return results
}

func isNodeInRange(n *Node, compare KeyCompare, lower []byte, upper []byte) bool {
	if n == nil {
		return false
	}
	if bytes.Equal(n.key, lower) || bytes.Equal(n.key, upper) {
		return true
	} else {
		return (upper == nil || compare.Less(n.key, upper)) && (lower == nil || compare.Less(lower, n.key))
	}
}

// `Dump` dumps the tree structure.
func (t *tree) Dump() {
	t.Root.Dump(0, "")
}
