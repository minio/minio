/*
 * Minio Cloud Storage, (C) 2015 Minio, Inc.
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

package cmd

// This package borrows idea from - https://godoc.org/golang.org/x/text/internal/triegen

// Trie trie container
type Trie struct {
	root *trieNode
	size int
}

// newTrie get new trie
func newTrie() *Trie {
	return &Trie{
		root: newTrieNode(),
		size: 0,
	}
}

// trieNode trie tree node container carries value and children
type trieNode struct {
	exists bool
	value  interface{}
	child  map[rune]*trieNode // runes as child
}

func newTrieNode() *trieNode {
	return &trieNode{
		exists: false,
		value:  nil,
		child:  make(map[rune]*trieNode),
	}
}

// Insert insert a key
func (t *Trie) Insert(key string) {
	curNode := t.root
	for _, v := range key {
		if curNode.child[v] == nil {
			curNode.child[v] = newTrieNode()
		}
		curNode = curNode.child[v]
	}

	if !curNode.exists {
		// increment when new rune child is added
		t.size++
		curNode.exists = true
	}
	// value is stored for retrieval in future
	curNode.value = key
}

// PrefixMatch - prefix match
func (t *Trie) PrefixMatch(key string) []interface{} {
	node, _ := t.findNode(key)
	if node != nil {
		return t.walk(node)
	}
	return []interface{}{}
}

// walk the tree
func (t *Trie) walk(node *trieNode) (ret []interface{}) {
	if node.exists {
		ret = append(ret, node.value)
	}
	for _, v := range node.child {
		ret = append(ret, t.walk(v)...)
	}
	return
}

// find nodes corresponding to key
func (t *Trie) findNode(key string) (node *trieNode, index int) {
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
