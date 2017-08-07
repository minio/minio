package binarydist

import (
	"bytes"
	"compress/bzip2"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
)

var ErrCorrupt = errors.New("corrupt patch")

// Patch applies patch to old, according to the bspatch algorithm,
// and writes the result to new.
func Patch(old io.Reader, new io.Writer, patch io.Reader) error {
	var hdr header
	err := binary.Read(patch, signMagLittleEndian{}, &hdr)
	if err != nil {
		return err
	}
	if hdr.Magic != magic {
		return ErrCorrupt
	}
	if hdr.CtrlLen < 0 || hdr.DiffLen < 0 || hdr.NewSize < 0 {
		return ErrCorrupt
	}

	ctrlbuf := make([]byte, hdr.CtrlLen)
	_, err = io.ReadFull(patch, ctrlbuf)
	if err != nil {
		return err
	}
	cpfbz2 := bzip2.NewReader(bytes.NewReader(ctrlbuf))

	diffbuf := make([]byte, hdr.DiffLen)
	_, err = io.ReadFull(patch, diffbuf)
	if err != nil {
		return err
	}
	dpfbz2 := bzip2.NewReader(bytes.NewReader(diffbuf))

	// The entire rest of the file is the extra block.
	epfbz2 := bzip2.NewReader(patch)

	obuf, err := ioutil.ReadAll(old)
	if err != nil {
		return err
	}

	nbuf := make([]byte, hdr.NewSize)

	var oldpos, newpos int64
	for newpos < hdr.NewSize {
		var ctrl struct{ Add, Copy, Seek int64 }
		err = binary.Read(cpfbz2, signMagLittleEndian{}, &ctrl)
		if err != nil {
			return err
		}

		// Sanity-check
		if newpos+ctrl.Add > hdr.NewSize {
			return ErrCorrupt
		}

		// Read diff string
		_, err = io.ReadFull(dpfbz2, nbuf[newpos:newpos+ctrl.Add])
		if err != nil {
			return ErrCorrupt
		}

		// Add old data to diff string
		for i := int64(0); i < ctrl.Add; i++ {
			if oldpos+i >= 0 && oldpos+i < int64(len(obuf)) {
				nbuf[newpos+i] += obuf[oldpos+i]
			}
		}

		// Adjust pointers
		newpos += ctrl.Add
		oldpos += ctrl.Add

		// Sanity-check
		if newpos+ctrl.Copy > hdr.NewSize {
			return ErrCorrupt
		}

		// Read extra string
		_, err = io.ReadFull(epfbz2, nbuf[newpos:newpos+ctrl.Copy])
		if err != nil {
			return ErrCorrupt
		}

		// Adjust pointers
		newpos += ctrl.Copy
		oldpos += ctrl.Seek
	}

	// Write the new file
	for len(nbuf) > 0 {
		n, err := new.Write(nbuf)
		if err != nil {
			return err
		}
		nbuf = nbuf[n:]
	}

	return nil
}
