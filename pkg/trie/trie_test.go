// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

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
