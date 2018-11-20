package keydb

import (
	"bytes"
	"fmt"
)

// Tree is an auto balancing CVL tree, based on code from 'applied go', but modified for []byte key and values,
// and range searching
type Tree struct {
	root *node
}

type node struct {
	key   []byte
	data  []byte
	left  *node
	right *node
	h     int
}

func (n *node) height() int {
	if n == nil {
		return 0
	}
	return n.h
}

func (n *node) balance() int {
	return n.right.height() - n.left.height()
}

func (n *node) insert(key, data []byte) *node {

	if n == nil {
		return &node{key: key, data: data, h: 1}
	}

	if bytes.Equal(key, n.key) {
		// node already exists nothing changes
		n.data = data
		return n
	}

	if less(key, n.key) {
		n.left = n.left.insert(key, data)
	} else {
		n.right = n.right.insert(key, data)
	}

	n.h = max(n.left.height(), n.right.height()) + 1

	bf := n.balance()

	if bf < -1 {
		if n.left.balance() >= 0 {
			n.left = n.left.rotateLeft()
		}
		n = n.rotateRight()
	} else if bf > 1 {
		if n.right.balance() <= 0 {
			n.right = n.right.rotateRight()
		}
		n = n.rotateLeft()
	}

	return n
}

// `rotateLeft` takes a child node and rotates the child node's subtree to the left.
func (n *node) rotateLeft() *node {
	// Save `c`'s right child.
	r := n.right
	// `r`'s left subtree gets reassigned to `c`.
	n.right = r.left
	// `c` becomes the left child of `r`.
	r.left = n

	n.h = max(n.left.height(), n.right.height()) + 1
	r.h = max(r.left.height(), r.right.height()) + 1

	return r
}

// `rotateRight` is the mirrored version of `rotateLeft`.
func (n *node) rotateRight() *node {
	l := n.left
	n.left = l.right
	l.right = n

	n.h = max(n.left.height(), n.right.height()) + 1
	l.h = max(l.left.height(), l.right.height()) + 1

	return l
}

func (n *node) Find(key []byte) ([]byte, bool) {

	if n == nil {
		return nil, false
	}

	if equal(key, n.key) {
		return n.data, true
	}

	if less(key, n.key) {
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

	if bytes.Equal(key, n.key) {
		prev := n.data
		n.data = nil
		return prev, true
	}

	if less(key, n.key) {
		return n.left.Remove(key)
	} else {
		return n.right.Remove(key)
	}
}

// Insert a key value pair into the Tree
func (t *Tree) Insert(key, data []byte) {
	t.root = t.root.insert(key, data)
}

// Find the value for a given key, ok is true if the key was found
func (t *Tree) Find(key []byte) (value []byte, ok bool) {
	if t.root == nil {
		return nil, false
	}
	return t.root.Find(key)
}

// Remove the value for a key, returning it. ok is true if the node existed and was found. If the key was not
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

// TreeEntry is node returned by FindNodes
type TreeEntry struct {
	Key   []byte
	Value []byte
}

// FindNodes calls function fn on nodes with key between lower and upper inclusive
func FindNodes(node *node, lower []byte, upper []byte, fn func(*node)) {
	if node == nil {
		return
	}

	/* Since the desired o/p is sorted, recurse for left subtree first
	   If node.key is greater than lower, then only we can get o/p keys
	   in left subtree */
	if lower == nil || less(lower, node.key) {
		FindNodes(node.left, lower, upper, fn)
	}

	if isNodeInRange(node, lower, upper) {
		fn(node)
	}

	/* If node.key is smaller than upper, then only we can get o/p keys
	in right subtree */
	if upper == nil || less(node.key, upper) {
		FindNodes(node.right, lower, upper, fn)
	}
}

// FindNodes returns a slice of nodes with the keys in range lower and upper inclusive
func (t *Tree) FindNodes(lower []byte, upper []byte) []TreeEntry {
	if t.root == nil {
		return nil
	}

	results := make([]TreeEntry, 0)

	nodeInRange := func(n *node) {
		results = append(results, TreeEntry{n.key, n.data})
	}
	FindNodes(t.root, lower, upper, nodeInRange)
	return results
}

func isNodeInRange(n *node, lower []byte, upper []byte) bool {
	if n == nil {
		return false
	}
	if equal(n.key, lower) || equal(n.key, upper) {
		return true
	} else {
		return (upper == nil || less(n.key, upper)) && (lower == nil || less(lower, n.key))
	}
}

type queue struct {
	values []*node
}

func newQueue() *queue {
	queue := &queue{}
	return queue
}

func (q *queue) enqueue(node *node) {
	q.values = append(q.values, node)
}

func (q *queue) dequeue() *node {
	var val node
	if q.isEmpty() {
		return nil
	}
	val = *q.values[0]
	q.values = q.values[1:]
	return &val
}

func (q *queue) drain() []*node {
	nodes := q.values[0:]
	q.values = make([]*node, 0)
	return nodes
}

func (q *queue) isEmpty() bool {
	return len(q.values) == 0
}

func (t *Tree) bfsDump() int {
	q := newQueue()
	q.enqueue(t.root)

	height := 0

	for !q.isEmpty() {
		nodes := q.drain()
		for _, n := range nodes {
			fmt.Print(string(n.key), "(", n.balance(), "/", n.height(), ") ")
			if n.left != nil {
				q.enqueue(n.left)
			}
			if n.right != nil {
				q.enqueue(n.right)
			}
		}
		fmt.Println()
		height++
	}
	return height
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
