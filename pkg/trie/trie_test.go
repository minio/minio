/*
 * MinIO Cloud Storage, (C) 2016, 2017 MinIO, Inc.
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

package trie

import (
	"testing"
)

// Simply make sure creating a new tree works.
func TestNewTrie(t *testing.T) {
	trie := NewTrie()

	if trie.size != 0 {
		t.Errorf("expected size 0, got: %d", trie.size)
	}
}

// Ensure that we can insert new keys into the tree, then check the size.
func TestInsert(t *testing.T) {
	trie := NewTrie()

	// We need to have an empty tree to begin with.
	if trie.size != 0 {
		t.Errorf("expected size 0, got: %d", trie.size)
	}

	trie.Insert("key")
	trie.Insert("keyy")

	// After inserting, we should have a size of two.
	if trie.size != 2 {
		t.Errorf("expected size 2, got: %d", trie.size)
	}
}

// Ensure that PrefixMatch gives us the correct two keys in the tree.
func TestPrefixMatch(t *testing.T) {
	trie := NewTrie()

	// Feed it some fodder: only 'minio' and 'miny-os' should trip the matcher.
	trie.Insert("minio")
	trie.Insert("amazon")
	trie.Insert("cheerio")
	trie.Insert("miny-o's")

	matches := trie.PrefixMatch("min")
	if len(matches) != 2 {
		t.Errorf("expected two matches, got: %d", len(matches))
	}

	if matches[0] != "minio" && matches[1] != "minio" {
		t.Errorf("expected one match to be 'minio', got: '%s' and '%s'", matches[0], matches[1])
	}
}
