merkletree
----------

merkletree is a Go package for working with [Merkle
trees](http://en.wikipedia.org/wiki/Merkle_tree). Specifically, this package is
designed to facilitate the generation and verification of "Merkle proofs" —
cryptographic proofs that a given subset of data "belongs" to a larger set.
BitTorrent, for example, requires downloading many small pieces of a file from
many untrusted peers; Merkle proofs allow the downloader to verify that each
piece is part of the full file.

When sha256 is used as the hashing algorithm, the implementation matches the
merkle tree described in RFC 6962, 'Certificate Transparency'.

Usage
-----

```go
package main

import (
    "crypto/sha256"
    "log"
    "os"

    "github.com/NebulousLabs/merkletree"
)

// All error checking is ignored in the following examples.
func main() {
	// Example 1: Get the merkle root of a file.
	segmentSize := 4096 // bytes per leaf
	file, _ := os.Open("myfile")
	merkleRoot, _ := merkletree.ReaderRoot(file, sha256.New(), segmentSize)

	// Example 2: Build and verify a proof that the element at segment 7 is in
	// the merkle root.
	file.Seek(0, 0) // Offset needs to be set back to 0.
	proofIndex := uint64(7)
	merkleRoot, proof, numLeaves, _ := merkletree.BuildReaderProof(file, sha256.New(), segmentSize, proofIndex)
	verified := VerifyProof(sha256.New(), merkleRoot, proof, proofIndex, numLeaves)

	// Example 3: Using a Tree to build a merkle tree and get a proof for a
	// specific index for non-file objects.
	tree := merkletree.New(sha256.New())
	tree.SetIndex(1)
	tree.Push([]byte("an object - the tree will hash the data after it is pushed"))
	tree.Push([]byte("another object"))
	// The merkle root could be obtained by calling tree.Root(), but will also
	// be provided by tree.Prove()
	merkleRoot, proof, proofIndex, numLeaves := tree.Prove()

	////////////////////////////////////////////////
	/// Remaining examples deal with cached trees //
	////////////////////////////////////////////////

	// Example 4: Creating a cached set of Merkle roots and then using them in
	// a cached tree. The cached tree is height 1, meaning that all elements of
	// the cached tree will be Merkle roots of data with 2 leaves.
	cachedTree := merkletree.NewCachedTree(sha256.New(), 1)
	subtree1 := merkletree.New(sha256.New())
	subtree1.Push([]byte("first leaf, first subtree"))
	subtree1.Push([]byte("second leaf, first subtree"))
	subtree2 := merkletree.New(sha256.New())
	subtree2.Push([]byte("first leaf, second subtree"))
	subtree2.Push([]byte("second leaf, second subtree"))
	// Using the cached tree, build the merkle root of the 4 leaves.
	cachedTree.Push(subtree1.Root())
	cachedTree.Push(subtree2.Root())
	collectiveRoot := cachedTree.Root()

	// Example 5: Modify the data pushed into subtree 2 and create the Merkle
	// root, without needing to rehash the data in any other subtree.
	revisedSubtree2 := merkletree.New(sha256.New())
	revisedSubtree2.Push([]byte("first leaf, second subtree"))
	revisedSubtree2.Push([]byte("second leaf, second subtree, revised"))
	// Using the cached tree, build the merkle root of the 4 leaves - without
	// needing to rehash any of the data in subtree1.
	cachedTree = merkletree.NewCachedTree(sha256.New(), 1)
	cachedTree.Push(subtree1.Root())
	cachedTree.Push(revisedSubtree2.Root())
	revisedRoot := cachedTree.Root()

	// Exapmle 6: Create a proof that leaf 3 (index 2) of the revised root,
	// found in revisedSubtree2 (at index 0 of the revised subtree), is a part of
	// the cached set. This is a two stage process - first we must get a proof
	// that the leaf is a part of revisedSubtree2, and then we must get provide
	// that proof as input to the cached tree prover.
	cachedTree = merkletree.NewCachedTree(sha256.New(), 1)
	cachedTree.SetIndex(2) // leaf at index 2, or the third element which gets inserted.
	revisedSubtree2 = merkletree.New(sha256.New())
	revisedSubtree2.SetIndex(0)
	revisedSubtree2.Push([]byte("first leaf, second subtree"))
	revisedSubtree2.Push([]byte("second leaf, second subtree, revised"))
	_, subtreeProof, _, _ := revisedSubtree2.Prove()
	// Now we can create the full proof for the cached tree, without having to
	// rehash any of the elements from subtree1.
	_, fullProof, _, _ := cachedTree.Prove(subtreeProof)
}
```

For more extensive documentation, refer to the
[godoc](http://godoc.org/github.com/NebulousLabs/merkletree).

Notes
-----

This implementation does not retain the entire Merkle tree in memory. Rather,
as each new leaf is added to the tree, is it pushed onto a stack as a "subtree
of depth 1." If the next element on the stack also has depth 1, the two are
combined into a "subtree of depth 2." This process continues until no adjacent
elements on the stack have the same depth. (For a nice visual representation of
this, play a round of [2048](http://gabrielecirulli.github.io/2048).) This
gives a space complexity of O(log(n)), making this implementation suitable for
generating Merkle proofs on very large files. (It is not as suitable for
generating "batches" of many Merkle proofs on the same file.)

Different Merkle tree implementations handle "orphan" leaves in different ways.
Our trees conform to the diagrams below; orphan leaves are not duplicated or
hashed multiple times.
```
     ┌───┴──┐       ┌────┴───┐         ┌─────┴─────┐
  ┌──┴──┐   │    ┌──┴──┐     │      ┌──┴──┐     ┌──┴──┐
┌─┴─┐ ┌─┴─┐ │  ┌─┴─┐ ┌─┴─┐ ┌─┴─┐  ┌─┴─┐ ┌─┴─┐ ┌─┴─┐   │
   (5-leaf)         (6-leaf)             (7-leaf)
```

When using the Reader functions (ReaderRoot and BuildReaderProof), the last
segment will not be padded if there are not 'segmentSize' bytes remaining.
