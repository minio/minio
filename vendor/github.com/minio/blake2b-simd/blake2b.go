// Written in 2012 by Dmitry Chestnykh.
//
// To the extent possible under law, the author have dedicated all copyright
// and related and neighboring rights to this software to the public domain
// worldwide. This software is distributed without any warranty.
// http://creativecommons.org/publicdomain/zero/1.0/

// Package blake2b implements BLAKE2b cryptographic hash function.
package blake2b

import (
	"encoding/binary"
	"errors"
	"hash"
)

const (
	BlockSize  = 128 // block size of algorithm
	Size       = 64  // maximum digest size
	SaltSize   = 16  // maximum salt size
	PersonSize = 16  // maximum personalization string size
	KeySize    = 64  // maximum size of key
)

type digest struct {
	h  [8]uint64       // current chain value
	t  [2]uint64       // message bytes counter
	f  [2]uint64       // finalization flags
	x  [BlockSize]byte // buffer for data not yet compressed
	nx int             // number of bytes in buffer

	ih         [8]uint64       // initial chain value (after config)
	paddedKey  [BlockSize]byte // copy of key, padded with zeros
	isKeyed    bool            // indicates whether hash was keyed
	size       uint8           // digest size in bytes
	isLastNode bool            // indicates processing of the last node in tree hashing
}

// Initialization values.
var iv = [8]uint64{
	0x6a09e667f3bcc908, 0xbb67ae8584caa73b,
	0x3c6ef372fe94f82b, 0xa54ff53a5f1d36f1,
	0x510e527fade682d1, 0x9b05688c2b3e6c1f,
	0x1f83d9abfb41bd6b, 0x5be0cd19137e2179,
}

// Config is used to configure hash function parameters and keying.
// All parameters are optional.
type Config struct {
	Size   uint8  // digest size (if zero, default size of 64 bytes is used)
	Key    []byte // key for prefix-MAC
	Salt   []byte // salt (if < 16 bytes, padded with zeros)
	Person []byte // personalization (if < 16 bytes, padded with zeros)
	Tree   *Tree  // parameters for tree hashing
}

// Tree represents parameters for tree hashing.
type Tree struct {
	Fanout        uint8  // fanout
	MaxDepth      uint8  // maximal depth
	LeafSize      uint32 // leaf maximal byte length (0 for unlimited)
	NodeOffset    uint64 // node offset (0 for first, leftmost or leaf)
	NodeDepth     uint8  // node depth (0 for leaves)
	InnerHashSize uint8  // inner hash byte length
	IsLastNode    bool   // indicates processing of the last node of layer
}

var (
	defaultConfig = &Config{Size: Size}
	config256     = &Config{Size: 32}
)

func verifyConfig(c *Config) error {
	if c.Size > Size {
		return errors.New("digest size is too large")
	}
	if len(c.Key) > KeySize {
		return errors.New("key is too large")
	}
	if len(c.Salt) > SaltSize {
		// Smaller salt is okay: it will be padded with zeros.
		return errors.New("salt is too large")
	}
	if len(c.Person) > PersonSize {
		// Smaller personalization is okay: it will be padded with zeros.
		return errors.New("personalization is too large")
	}
	if c.Tree != nil {
		if c.Tree.Fanout == 1 {
			return errors.New("fanout of 1 is not allowed in tree mode")
		}
		if c.Tree.MaxDepth < 2 {
			return errors.New("incorrect tree depth")
		}
		if c.Tree.InnerHashSize < 1 || c.Tree.InnerHashSize > Size {
			return errors.New("incorrect tree inner hash size")
		}
	}
	return nil
}

// New returns a new hash.Hash configured with the given Config.
// Config can be nil, in which case the default one is used, calculating 64-byte digest.
// Returns non-nil error if Config contains invalid parameters.
func New(c *Config) (hash.Hash, error) {
	if c == nil {
		c = defaultConfig
	} else {
		if c.Size == 0 {
			// Set default size if it's zero.
			c.Size = Size
		}
		if err := verifyConfig(c); err != nil {
			return nil, err
		}
	}
	d := new(digest)
	d.initialize(c)
	return d, nil
}

// initialize initializes digest with the given
// config, which must be non-nil and verified.
func (d *digest) initialize(c *Config) {
	// Create parameter block.
	var p [BlockSize]byte
	p[0] = c.Size
	p[1] = uint8(len(c.Key))
	if c.Salt != nil {
		copy(p[32:], c.Salt)
	}
	if c.Person != nil {
		copy(p[48:], c.Person)
	}
	if c.Tree != nil {
		p[2] = c.Tree.Fanout
		p[3] = c.Tree.MaxDepth
		binary.LittleEndian.PutUint32(p[4:], c.Tree.LeafSize)
		binary.LittleEndian.PutUint64(p[8:], c.Tree.NodeOffset)
		p[16] = c.Tree.NodeDepth
		p[17] = c.Tree.InnerHashSize
	} else {
		p[2] = 1
		p[3] = 1
	}

	// Initialize.
	d.size = c.Size
	for i := 0; i < 8; i++ {
		d.h[i] = iv[i] ^ binary.LittleEndian.Uint64(p[i*8:])
	}
	if c.Tree != nil && c.Tree.IsLastNode {
		d.isLastNode = true
	}

	// Process key.
	if c.Key != nil {
		copy(d.paddedKey[:], c.Key)
		d.Write(d.paddedKey[:])
		d.isKeyed = true
	}
	// Save a copy of initialized state.
	copy(d.ih[:], d.h[:])
}

// New512 returns a new hash.Hash computing the BLAKE2b 64-byte checksum.
func New512() hash.Hash {
	d := new(digest)
	d.initialize(defaultConfig)
	return d
}

// New256 returns a new hash.Hash computing the BLAKE2b 32-byte checksum.
func New256() hash.Hash {
	d := new(digest)
	d.initialize(config256)
	return d
}

// NewMAC returns a new hash.Hash computing BLAKE2b prefix-
// Message Authentication Code of the given size in bytes
// (up to 64) with the given key (up to 64 bytes in length).
func NewMAC(outBytes uint8, key []byte) hash.Hash {
	d, err := New(&Config{Size: outBytes, Key: key})
	if err != nil {
		panic(err.Error())
	}
	return d
}

// Reset resets the state of digest to the initial state
// after configuration and keying.
func (d *digest) Reset() {
	copy(d.h[:], d.ih[:])
	d.t[0] = 0
	d.t[1] = 0
	d.f[0] = 0
	d.f[1] = 0
	d.nx = 0
	if d.isKeyed {
		d.Write(d.paddedKey[:])
	}
}

// Size returns the digest size in bytes.
func (d *digest) Size() int { return int(d.size) }

// BlockSize returns the algorithm block size in bytes.
func (d *digest) BlockSize() int { return BlockSize }

func (d *digest) Write(p []byte) (nn int, err error) {
	nn = len(p)
	left := BlockSize - d.nx
	if len(p) > left {
		// Process buffer.
		copy(d.x[d.nx:], p[:left])
		p = p[left:]
		compress(d, d.x[:])
		d.nx = 0
	}
	// Process full blocks except for the last one.
	if len(p) > BlockSize {
		n := len(p) &^ (BlockSize - 1)
		if n == len(p) {
			n -= BlockSize
		}
		compress(d, p[:n])
		p = p[n:]
	}
	// Fill buffer.
	d.nx += copy(d.x[d.nx:], p)
	return
}

// Sum returns the calculated checksum.
func (d *digest) Sum(in []byte) []byte {
	// Make a copy of d so that caller can keep writing and summing.
	d0 := *d
	hash := d0.checkSum()
	return append(in, hash[:d0.size]...)
}

func (d *digest) checkSum() [Size]byte {
	// Do not create unnecessary copies of the key.
	if d.isKeyed {
		for i := 0; i < len(d.paddedKey); i++ {
			d.paddedKey[i] = 0
		}
	}

	dec := BlockSize - uint64(d.nx)
	if d.t[0] < dec {
		d.t[1]--
	}
	d.t[0] -= dec

	// Pad buffer with zeros.
	for i := d.nx; i < len(d.x); i++ {
		d.x[i] = 0
	}
	// Set last block flag.
	d.f[0] = 0xffffffffffffffff
	if d.isLastNode {
		d.f[1] = 0xffffffffffffffff
	}
	// Compress last block.
	compress(d, d.x[:])

	var out [Size]byte
	j := 0
	for _, s := range d.h[:(d.size-1)/8+1] {
		out[j+0] = byte(s >> 0)
		out[j+1] = byte(s >> 8)
		out[j+2] = byte(s >> 16)
		out[j+3] = byte(s >> 24)
		out[j+4] = byte(s >> 32)
		out[j+5] = byte(s >> 40)
		out[j+6] = byte(s >> 48)
		out[j+7] = byte(s >> 56)
		j += 8
	}
	return out
}

// Sum512 returns a 64-byte BLAKE2b hash of data.
func Sum512(data []byte) [64]byte {
	var d digest
	d.initialize(defaultConfig)
	d.Write(data)
	return d.checkSum()
}

// Sum256 returns a 32-byte BLAKE2b hash of data.
func Sum256(data []byte) (out [32]byte) {
	var d digest
	d.initialize(config256)
	d.Write(data)
	sum := d.checkSum()
	copy(out[:], sum[:32])
	return
}
