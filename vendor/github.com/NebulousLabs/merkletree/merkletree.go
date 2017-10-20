// Package merkletree provides tools for calculating the Merkle root of a
// dataset, for creating a proof that a piece of data is in a Merkle tree of a
// given root, and for verifying proofs that a piece of data is in a Merkle
// tree of a given root. The tree is implemented according to the specification
// for Merkle trees provided in RFC 6962.
//
// Package merkletree also supports building roots and proofs from cached
// subroots of the Merkle tree. For example, a large file could be cached by
// building the Merkle root for each 4MB sector and remembering the Merkle
// roots of each sector. Using a cached tree, the Merkle root of the whole file
// can be computed by passing the cached tree each of the roots of the 4MB
// sector. Building proofs using these cached roots is also supported. A proof
// must be built within the target sector using a normal Tree, requiring the
// whole sector to be hashed. The results of that proof can then be passed into
// the Prove() function of a cached tree, which will create the full proof
// without needing to hash the entire file. Caching also makes it inexpensive
// to update the Merkle root of the file after changing or deleting segments of
// the larger file.
//
// Examples can be found in the README for the package.
package merkletree
