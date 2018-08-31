package keydb

import (
	"bytes"
)

// auto balancing binary Tree, based on code from 'applied go', but modified for []byte key and values,
// and range searching
type Tree struct {
	root    *node
	Compare KeyCompare
}

type node struct {
	key   []byte
	data  []byte
	left  *node
	right *node
	bal   int // height(n.right) - height(n.left)
	tree  *Tree
}

// `insert` takes a search Value and some data and inserts a new node (unless a node with the given
// search Value already exists, in which case `insert` only replaces the data).
//
// It returns:
//
// * `true` if the height of the Tree has increased.
// * `false` otherwise.
func (n *node) insert(key, data []byte) bool {
	// The following actions depend on whether the new search key is equal, less, or greater than
	// the current node's search key.

	if bytes.Equal(key, n.key) {
		// node already exists nothing changes
		n.data = data
		return false
	}

	compare := n.tree.Compare

	if compare.Less(key, n.key) {
		// If there is no left child, create a new one.
		if n.left == nil {
			// Create a new node.
			n.left = &node{key: key, data: data, tree: n.tree}
			// If there is no right child, the new child node has increased the height of this subtree.
			if n.right == nil {
				// The new left child is the only child.
				n.bal = -1
			} else {
				// There is a left and a right child. The right child cannot have children;
				// otherwise the Tree would already have been out of balance at `n`.
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
			n.right = &node{key: key, data: data, tree: n.tree}
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

// `rotateLeft` takes a child node and rotates the child node's subtree to the left.
func (n *node) rotateLeft(c *node) {
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
func (n *node) rotateRight(c *node) {
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
func (n *node) rotateRightLeft(c *node) {
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
func (n *node) rotateLeftRight(c *node) {
	c.left.right.bal = -1 // The considerations from rotateRightLeft also apply here.
	c.rotateLeft(c.left)
	c.left.bal = -1
	n.rotateRight(c)
}

// `rebalance` brings the (sub-)Tree with root node `c` back into a balanced state.
func (n *node) rebalance(c *node) {
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

func (n *node) Find(key []byte) ([]byte, bool) {

	if n == nil {
		return nil, false
	}

	compare := n.tree.Compare

	if bytes.Equal(key, n.key) {
		return n.data, true
	}

	if compare.Less(key, n.key) {
		return n.left.Find(key)
	} else {
		return n.right.Find(key)
	}
}

// Remove does not actual remove the node, but instead stores a 'nil' Value. This is essential to allow the
// memory index to track removals for other segments
func (n *node) Remove(key []byte) ([]byte, bool) {

	if n == nil {
		return nil, false
	}

	compare := n.tree.Compare

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

func (t *Tree) Insert(key, data []byte) {
	if t.root == nil {
		t.root = &node{key: key, data: data, tree: t}
		return
	}
	t.root.insert(key, data)
	// If the root node gets out of balance,
	if t.root.bal < -1 || t.root.bal > 1 {
		t.rebalance()
	}
}

// `node`'s `rebalance` method is invoked from the parent node of the node that needs rebalancing.
// However, the root node of a Tree has no parent node.
// Therefore, `Tree`'s `rebalance` method creates a fake parent node for rebalancing the root node.
func (t *Tree) rebalance() {
	fakeParent := &node{left: t.root, key: []byte{}, tree: t}
	fakeParent.rebalance(t.root)
	// Fetch the new root node from the fake parent node
	t.root = fakeParent.left
}

// return the value for a key, ok is true if the key was found
func (t *Tree) Find(key []byte) (value []byte, ok bool) {
	if t.root == nil {
		return nil, false
	}
	return t.root.Find(key)
}

// remove the value for a key, returning it. ok is true if the node existed and was found. If the key was not
// found a 'nil' value is inserted into the tree
func (t *Tree) Remove(key []byte) (value []byte, ok bool) {
	old, ok := t.root.Remove(key)
	if !ok {
		t.Insert(key, nil)
		return nil, false
	} else {
		return old, true
	}
}

type TreeEntry struct {
	Key   []byte
	Value []byte
}

// The functions finds all nodes within the provided key range, call function fn on each found node
func FindNodes(node *node, compare KeyCompare, lower []byte, upper []byte, fn func(*node)) {
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

func (t *Tree) FindNodes(lower []byte, upper []byte) []TreeEntry {
	if t.root == nil {
		return nil
	}

	compare := t.Compare

	results := make([]TreeEntry, 0)

	nodeInRange := func(n *node) {
		results = append(results, TreeEntry{n.key, n.data})
	}
	FindNodes(t.root, compare, lower, upper, nodeInRange)
	return results
}

func isNodeInRange(n *node, compare KeyCompare, lower []byte, upper []byte) bool {
	if n == nil {
		return false
	}
	if bytes.Equal(n.key, lower) || bytes.Equal(n.key, upper) {
		return true
	} else {
		return (upper == nil || compare.Less(n.key, upper)) && (lower == nil || compare.Less(lower, n.key))
	}
}
