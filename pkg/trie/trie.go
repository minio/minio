/*
 * Minio Cloud Storage, (C) 2014, 2015, 2016, 2017 Minio, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// Package trie implements a simple trie tree for minio server/tools borrows
// idea from - https://godoc.org/golang.org/x/text/internal/triegen.
package trie

// Node trie tree node container carries value and children.
type Node struct {
	exists bool
	value  interface{}
	child  map[rune]*Node // runes as child.
}

// newNode create a new trie node.
func newNode() *Node {
	return &Node{
		exists: false,
		value:  nil,
		child:  make(map[rune]*Node),
	}
}

// Trie is a trie container.
type Trie struct {
	root *Node
	size int
}

// Root returns root node.
func (t *Trie) Root() *Node {
	return t.root
}

// Insert insert a key.
func (t *Trie) Insert(key string) {
	curNode := t.root
	for _, v := range key {
		if curNode.child[v] == nil {
			curNode.child[v] = newNode()
		}
		curNode = curNode.child[v]
	}

	if !curNode.exists {
		// increment when new rune child is added.
		t.size++
		curNode.exists = true
	}
	// value is stored for retrieval in future.
	curNode.value = key
}

// PrefixMatch - prefix match.
func (t *Trie) PrefixMatch(key string) []interface{} {
	node, _ := t.findNode(key)
	if node != nil {
		return t.Walk(node)
	}
	return []interface{}{}
}

// Walk the tree.
func (t *Trie) Walk(node *Node) (ret []interface{}) {
	if node.exists {
		ret = append(ret, node.value)
	}
	for _, v := range node.child {
		ret = append(ret, t.Walk(v)...)
	}
	return
}

// find nodes corresponding to key.
func (t *Trie) findNode(key string) (node *Node, index int) {
	curNode := t.root
	f := false
	for k, v := range key {
		if f {
			index = k
			f = false
		}
		if curNode.child[v] == nil {
			return nil, index
		}
		curNode = curNode.child[v]
		if curNode.exists {
			f = true
		}
	}

	if curNode.exists {
		index = len(key)
	}

	return curNode, index
}

// NewTrie create a new trie.
func NewTrie() *Trie {
	return &Trie{
		root: newNode(),
		size: 0,
	}
}
