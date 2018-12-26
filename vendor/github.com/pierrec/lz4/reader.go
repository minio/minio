package lz4

import (
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/pierrec/lz4/internal/xxh32"
)

// Reader implements the LZ4 frame decoder.
// The Header is set after the first call to Read().
// The Header may change between Read() calls in case of concatenated frames.
type Reader struct {
	Header

	buf      [8]byte       // Scrap buffer.
	pos      int64         // Current position in src.
	src      io.Reader     // Source.
	zdata    []byte        // Compressed data.
	data     []byte        // Uncompressed data.
	idx      int           // Index of unread bytes into data.
	checksum xxh32.XXHZero // Frame hash.
}

// NewReader returns a new LZ4 frame decoder.
// No access to the underlying io.Reader is performed.
func NewReader(src io.Reader) *Reader {
	r := &Reader{src: src}
	return r
}

// readHeader checks the frame magic number and parses the frame descriptoz.
// Skippable frames are supported even as a first frame although the LZ4
// specifications recommends skippable frames not to be used as first frames.
func (z *Reader) readHeader(first bool) error {
	defer z.checksum.Reset()

	buf := z.buf[:]
	for {
		magic, err := z.readUint32()
		if err != nil {
			z.pos += 4
			if !first && err == io.ErrUnexpectedEOF {
				return io.EOF
			}
			return err
		}
		if magic == frameMagic {
			break
		}
		if magic>>8 != frameSkipMagic>>8 {
			return ErrInvalid
		}
		skipSize, err := z.readUint32()
		if err != nil {
			return err
		}
		z.pos += 4
		m, err := io.CopyN(ioutil.Discard, z.src, int64(skipSize))
		if err != nil {
			return err
		}
		z.pos += m
	}

	// Header.
	if _, err := io.ReadFull(z.src, buf[:2]); err != nil {
		return err
	}
	z.pos += 8

	b := buf[0]
	if v := b >> 6; v != Version {
		return fmt.Errorf("lz4: invalid version: got %d; expected %d", v, Version)
	}
	if b>>5&1 == 0 {
		return fmt.Errorf("lz4: block dependency not supported")
	}
	z.BlockChecksum = b>>4&1 > 0
	frameSize := b>>3&1 > 0
	z.NoChecksum = b>>2&1 == 0

	bmsID := buf[1] >> 4 & 0x7
	bSize, ok := bsMapID[bmsID]
	if !ok {
		return fmt.Errorf("lz4: invalid block max size ID: %d", bmsID)
	}
	z.BlockMaxSize = bSize

	// Allocate the compressed/uncompressed buffers.
	// The compressed buffer cannot exceed the uncompressed one.
	if n := 2 * bSize; cap(z.zdata) < n {
		z.zdata = make([]byte, n, n)
	}
	if debugFlag {
		debug("header block max size id=%d size=%d", bmsID, bSize)
	}
	z.zdata = z.zdata[:bSize]
	z.data = z.zdata[:cap(z.zdata)][bSize:]
	z.idx = len(z.data)

	z.checksum.Write(buf[0:2])

	if frameSize {
		buf := buf[:8]
		if _, err := io.ReadFull(z.src, buf); err != nil {
			return err
		}
		z.Size = binary.LittleEndian.Uint64(buf)
		z.pos += 8
		z.checksum.Write(buf)
	}

	// Header checksum.
	if _, err := io.ReadFull(z.src, buf[:1]); err != nil {
		return err
	}
	z.pos++
	if h := byte(z.checksum.Sum32() >> 8 & 0xFF); h != buf[0] {
		return fmt.Errorf("lz4: invalid header checksum: got %x; expected %x", buf[0], h)
	}

	z.Header.done = true
	if debugFlag {
		debug("header read: %v", z.Header)
	}

	return nil
}

// Read decompresses data from the underlying source into the supplied buffer.
//
// Since there can be multiple streams concatenated, Header values may
// change between calls to Read(). If that is the case, no data is actually read from
// the underlying io.Reader, to allow for potential input buffer resizing.
func (z *Reader) Read(buf []byte) (int, error) {
	if debugFlag {
		debug("Read buf len=%d", len(buf))
	}
	if !z.Header.done {
		if err := z.readHeader(true); err != nil {
			return 0, err
		}
		if debugFlag {
			debug("header read OK compressed buffer %d / %d uncompressed buffer %d : %d index=%d",
				len(z.zdata), cap(z.zdata), len(z.data), cap(z.data), z.idx)
		}
	}

	if len(buf) == 0 {
		return 0, nil
	}

	if z.idx == len(z.data) {
		// No data ready for reading, process the next block.
		if debugFlag {
			debug("reading block from writer")
		}
		// Block length: 0 = end of frame, highest bit set: uncompressed.
		bLen, err := z.readUint32()
		if err != nil {
			return 0, err
		}
		z.pos += 4

		if bLen == 0 {
			// End of frame reached.
			if !z.NoChecksum {
				// Validate the frame checksum.
				checksum, err := z.readUint32()
				if err != nil {
					return 0, err
				}
				if debugFlag {
					debug("frame checksum got=%x / want=%x", z.checksum.Sum32(), checksum)
				}
				z.pos += 4
				if h := z.checksum.Sum32(); checksum != h {
					return 0, fmt.Errorf("lz4: invalid frame checksum: got %x; expected %x", h, checksum)
				}
			}

			// Get ready for the next concatenated frame and keep the position.
			pos := z.pos
			z.Reset(z.src)
			z.pos = pos

			// Since multiple frames can be concatenated, check for more.
			return 0, z.readHeader(false)
		}

		if debugFlag {
			debug("raw block size %d", bLen)
		}
		if bLen&compressedBlockFlag > 0 {
			// Uncompressed block.
			bLen &= compressedBlockMask
			if debugFlag {
				debug("uncompressed block size %d", bLen)
			}
			if int(bLen) > cap(z.data) {
				return 0, fmt.Errorf("lz4: invalid block size: %d", bLen)
			}
			z.data = z.data[:bLen]
			if _, err := io.ReadFull(z.src, z.data); err != nil {
				return 0, err
			}
			z.pos += int64(bLen)

			if z.BlockChecksum {
				checksum, err := z.readUint32()
				if err != nil {
					return 0, err
				}
				z.pos += 4

				if h := xxh32.ChecksumZero(z.data); h != checksum {
					return 0, fmt.Errorf("lz4: invalid block checksum: got %x; expected %x", h, checksum)
				}
			}

		} else {
			// Compressed block.
			if debugFlag {
				debug("compressed block size %d", bLen)
			}
			if int(bLen) > cap(z.data) {
				return 0, fmt.Errorf("lz4: invalid block size: %d", bLen)
			}
			zdata := z.zdata[:bLen]
			if _, err := io.ReadFull(z.src, zdata); err != nil {
				return 0, err
			}
			z.pos += int64(bLen)

			if z.BlockChecksum {
				checksum, err := z.readUint32()
				if err != nil {
					return 0, err
				}
				z.pos += 4

				if h := xxh32.ChecksumZero(zdata); h != checksum {
					return 0, fmt.Errorf("lz4: invalid block checksum: got %x; expected %x", h, checksum)
				}
			}

			n, err := UncompressBlock(zdata, z.data)
			if err != nil {
				return 0, err
			}
			z.data = z.data[:n]
		}

		if !z.NoChecksum {
			z.checksum.Write(z.data)
			if debugFlag {
				debug("current frame checksum %x", z.checksum.Sum32())
			}
		}
		z.idx = 0
	}

	n := copy(buf, z.data[z.idx:])
	z.idx += n
	if debugFlag {
		debug("copied %d bytes to input", n)
	}

	return n, nil
}

// Reset discards the Reader's state and makes it equivalent to the
// result of its original state from NewReader, but reading from r instead.
// This permits reusing a Reader rather than allocating a new one.
func (z *Reader) Reset(r io.Reader) {
	z.Header = Header{}
	z.pos = 0
	z.src = r
	z.zdata = z.zdata[:0]
	z.data = z.data[:0]
	z.idx = 0
	z.checksum.Reset()
}

// readUint32 reads an uint32 into the supplied buffer.
// The idea is to make use of the already allocated buffers avoiding additional allocations.
func (z *Reader) readUint32() (uint32, error) {
	buf := z.buf[:4]
	_, err := io.ReadFull(z.src, buf)
	x := binary.LittleEndian.Uint32(buf)
	return x, err
}
