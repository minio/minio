package lz4

import (
	"encoding/binary"
	"fmt"
	"io"

	"github.com/pierrec/lz4/internal/xxh32"
)

// Writer implements the LZ4 frame encoder.
type Writer struct {
	Header

	buf       [19]byte      // magic number(4) + header(flags(2)+[Size(8)+DictID(4)]+checksum(1)) does not exceed 19 bytes
	dst       io.Writer     // Destination.
	checksum  xxh32.XXHZero // Frame checksum.
	zdata     []byte        // Compressed data.
	data      []byte        // Data to be compressed.
	idx       int           // Index into data.
	hashtable [winSize]int  // Hash table used in CompressBlock().
}

// NewWriter returns a new LZ4 frame encoder.
// No access to the underlying io.Writer is performed.
// The supplied Header is checked at the first Write.
// It is ok to change it before the first Write but then not until a Reset() is performed.
func NewWriter(dst io.Writer) *Writer {
	return &Writer{dst: dst}
}

// writeHeader builds and writes the header (magic+header) to the underlying io.Writer.
func (z *Writer) writeHeader() error {
	// Default to 4Mb if BlockMaxSize is not set.
	if z.Header.BlockMaxSize == 0 {
		z.Header.BlockMaxSize = bsMapID[7]
	}
	// The only option that needs to be validated.
	bSize := z.Header.BlockMaxSize
	bSizeID, ok := bsMapValue[bSize]
	if !ok {
		return fmt.Errorf("lz4: invalid block max size: %d", bSize)
	}
	// Allocate the compressed/uncompressed buffers.
	// The compressed buffer cannot exceed the uncompressed one.
	if n := 2 * bSize; cap(z.zdata) < n {
		z.zdata = make([]byte, n, n)
	}
	z.zdata = z.zdata[:bSize]
	z.data = z.zdata[:cap(z.zdata)][bSize:]
	z.idx = 0

	// Size is optional.
	buf := z.buf[:]

	// Set the fixed size data: magic number, block max size and flags.
	binary.LittleEndian.PutUint32(buf[0:], frameMagic)
	flg := byte(Version << 6)
	flg |= 1 << 5 // No block dependency.
	if z.Header.BlockChecksum {
		flg |= 1 << 4
	}
	if z.Header.Size > 0 {
		flg |= 1 << 3
	}
	if !z.Header.NoChecksum {
		flg |= 1 << 2
	}
	buf[4] = flg
	buf[5] = bSizeID << 4

	// Current buffer size: magic(4) + flags(1) + block max size (1).
	n := 6
	// Optional items.
	if z.Header.Size > 0 {
		binary.LittleEndian.PutUint64(buf[n:], z.Header.Size)
		n += 8
	}

	// The header checksum includes the flags, block max size and optional Size.
	buf[n] = byte(xxh32.ChecksumZero(buf[4:n]) >> 8 & 0xFF)
	z.checksum.Reset()

	// Header ready, write it out.
	if _, err := z.dst.Write(buf[0 : n+1]); err != nil {
		return err
	}
	z.Header.done = true
	if debugFlag {
		debug("wrote header %v", z.Header)
	}

	return nil
}

// Write compresses data from the supplied buffer into the underlying io.Writer.
// Write does not return until the data has been written.
func (z *Writer) Write(buf []byte) (int, error) {
	if !z.Header.done {
		if err := z.writeHeader(); err != nil {
			return 0, err
		}
	}
	if debugFlag {
		debug("input buffer len=%d index=%d", len(buf), z.idx)
	}

	zn := len(z.data)
	var n int
	for len(buf) > 0 {
		if z.idx == 0 && len(buf) >= zn {
			// Avoid a copy as there is enough data for a block.
			if err := z.compressBlock(buf[:zn]); err != nil {
				return n, err
			}
			n += zn
			buf = buf[zn:]
			continue
		}
		// Accumulate the data to be compressed.
		m := copy(z.data[z.idx:], buf)
		n += m
		z.idx += m
		buf = buf[m:]
		if debugFlag {
			debug("%d bytes copied to buf, current index %d", n, z.idx)
		}

		if z.idx < len(z.data) {
			// Buffer not filled.
			if debugFlag {
				debug("need more data for compression")
			}
			return n, nil
		}

		// Buffer full.
		if err := z.compressBlock(z.data); err != nil {
			return n, err
		}
		z.idx = 0
	}

	return n, nil
}

// compressBlock compresses a block.
func (z *Writer) compressBlock(data []byte) error {
	if !z.NoChecksum {
		z.checksum.Write(data)
	}

	// The compressed block size cannot exceed the input's.
	var zn int
	var err error

	if level := z.Header.CompressionLevel; level != 0 {
		zn, err = CompressBlockHC(data, z.zdata, level)
	} else {
		zn, err = CompressBlock(data, z.zdata, z.hashtable[:])
	}

	var zdata []byte
	var bLen uint32
	if debugFlag {
		debug("block compression %d => %d", len(data), zn)
	}
	if err == nil && zn > 0 && zn < len(data) {
		// Compressible and compressed size smaller than uncompressed: ok!
		bLen = uint32(zn)
		zdata = z.zdata[:zn]
	} else {
		// Uncompressed block.
		bLen = uint32(len(data)) | compressedBlockFlag
		zdata = data
	}
	if debugFlag {
		debug("block compression to be written len=%d data len=%d", bLen, len(zdata))
	}

	// Write the block.
	if err := z.writeUint32(bLen); err != nil {
		return err
	}
	if _, err := z.dst.Write(zdata); err != nil {
		return err
	}

	if z.BlockChecksum {
		checksum := xxh32.ChecksumZero(zdata)
		if debugFlag {
			debug("block checksum %x", checksum)
		}
		if err := z.writeUint32(checksum); err != nil {
			return err
		}
	}
	if debugFlag {
		debug("current frame checksum %x", z.checksum.Sum32())
	}

	return nil
}

// Flush flushes any pending compressed data to the underlying writer.
// Flush does not return until the data has been written.
// If the underlying writer returns an error, Flush returns that error.
func (z *Writer) Flush() error {
	if debugFlag {
		debug("flush with index %d", z.idx)
	}
	if z.idx == 0 {
		return nil
	}

	return z.compressBlock(z.data[:z.idx])
}

// Close closes the Writer, flushing any unwritten data to the underlying io.Writer, but does not close the underlying io.Writer.
func (z *Writer) Close() error {
	if !z.Header.done {
		if err := z.writeHeader(); err != nil {
			return err
		}
	}

	if err := z.Flush(); err != nil {
		return err
	}

	if debugFlag {
		debug("writing last empty block")
	}
	if err := z.writeUint32(0); err != nil {
		return err
	}
	if !z.NoChecksum {
		checksum := z.checksum.Sum32()
		if debugFlag {
			debug("stream checksum %x", checksum)
		}
		if err := z.writeUint32(checksum); err != nil {
			return err
		}
	}
	return nil
}

// Reset clears the state of the Writer z such that it is equivalent to its
// initial state from NewWriter, but instead writing to w.
// No access to the underlying io.Writer is performed.
func (z *Writer) Reset(w io.Writer) {
	z.Header = Header{}
	z.dst = w
	z.checksum.Reset()
	z.zdata = z.zdata[:0]
	z.data = z.data[:0]
	z.idx = 0
}

// writeUint32 writes a uint32 to the underlying writer.
func (z *Writer) writeUint32(x uint32) error {
	buf := z.buf[:4]
	binary.LittleEndian.PutUint32(buf, x)
	_, err := z.dst.Write(buf)
	return err
}
