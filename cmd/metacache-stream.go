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

package cmd

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"

	jsoniter "github.com/json-iterator/go"
	"github.com/klauspost/compress/s2"
	"github.com/minio/minio/internal/logger"
	"github.com/tinylib/msgp/msgp"
	"github.com/valyala/bytebufferpool"
)

// metadata stream format:
//
// The stream is s2 compressed.
// https://github.com/klauspost/compress/tree/master/s2#s2-compression
// This ensures integrity and reduces the size typically by at least 50%.
//
// All stream elements are msgpack encoded.
//
// 1 Integer, metacacheStreamVersion of the writer.
// This can be used for managing breaking changes.
//
// For each element:
// 1. Bool. If false at end of stream.
// 2. String. Name of object. Directories contains a trailing slash.
// 3. Binary. Blob of metadata. Length 0 on directories.
// ... Next element.
//
// Streams can be assumed to be sorted in ascending order.
// If the stream ends before a false boolean it can be assumed it was truncated.

const metacacheStreamVersion = 1

// metacacheWriter provides a serializer of metacache objects.
type metacacheWriter struct {
	mw        *msgp.Writer
	creator   func() error
	closer    func() error
	blockSize int

	streamErr error
	streamWg  sync.WaitGroup
}

// newMetacacheWriter will create a serializer that will write objects in given order to the output.
// Provide a block size that affects latency. If 0 a default of 128KiB will be used.
// Block size can be up to 4MiB.
func newMetacacheWriter(out io.Writer, blockSize int) *metacacheWriter {
	if blockSize < 8<<10 {
		blockSize = 128 << 10
	}
	w := metacacheWriter{
		mw:        nil,
		blockSize: blockSize,
	}
	w.creator = func() error {
		s2w := s2.NewWriter(out, s2.WriterBlockSize(blockSize), s2.WriterConcurrency(2))
		w.mw = msgp.NewWriter(s2w)
		w.creator = nil
		if err := w.mw.WriteByte(metacacheStreamVersion); err != nil {
			return err
		}

		w.closer = func() (err error) {
			defer func() {
				cerr := s2w.Close()
				if err == nil && cerr != nil {
					err = cerr
				}
			}()
			if w.streamErr != nil {
				return w.streamErr
			}
			if err = w.mw.WriteBool(false); err != nil {
				return err
			}
			return w.mw.Flush()
		}
		return nil
	}
	return &w
}

// write one or more objects to the stream in order.
// It is favorable to send as many objects as possible in a single write,
// but no more than math.MaxUint32
func (w *metacacheWriter) write(objs ...metaCacheEntry) error {
	if w == nil {
		return errors.New("metacacheWriter: nil writer")
	}
	if len(objs) == 0 {
		return nil
	}
	if w.creator != nil {
		err := w.creator()
		w.creator = nil
		if err != nil {
			return fmt.Errorf("metacacheWriter: unable to create writer: %w", err)
		}
		if w.mw == nil {
			return errors.New("metacacheWriter: writer not initialized")
		}
	}
	for _, o := range objs {
		if len(o.name) == 0 {
			return errors.New("metacacheWriter: no name provided")
		}
		// Indicate EOS
		err := w.mw.WriteBool(true)
		if err != nil {
			return err
		}
		err = w.mw.WriteString(o.name)
		if err != nil {
			return err
		}
		err = w.mw.WriteBytes(o.metadata)
		if err != nil {
			return err
		}
	}

	return nil
}

// stream entries to the output.
// The returned channel should be closed when done.
// Any error is reported when closing the metacacheWriter.
func (w *metacacheWriter) stream() (chan<- metaCacheEntry, error) {
	if w.creator != nil {
		err := w.creator()
		w.creator = nil
		if err != nil {
			return nil, fmt.Errorf("metacacheWriter: unable to create writer: %w", err)
		}
		if w.mw == nil {
			return nil, errors.New("metacacheWriter: writer not initialized")
		}
	}
	var objs = make(chan metaCacheEntry, 100)
	w.streamErr = nil
	w.streamWg.Add(1)
	go func() {
		defer w.streamWg.Done()
		for o := range objs {
			if len(o.name) == 0 || w.streamErr != nil {
				continue
			}
			// Indicate EOS
			err := w.mw.WriteBool(true)
			if err != nil {
				w.streamErr = err
				continue
			}
			err = w.mw.WriteString(o.name)
			if err != nil {
				w.streamErr = err
				continue
			}
			err = w.mw.WriteBytes(o.metadata)
			if err != nil {
				w.streamErr = err
				continue
			}
		}
	}()

	return objs, nil
}

// Close and release resources.
func (w *metacacheWriter) Close() error {
	if w == nil || w.closer == nil {
		return nil
	}
	w.streamWg.Wait()
	err := w.closer()
	w.closer = nil
	return err
}

// Reset and start writing to new writer.
// Close must have been called before this.
func (w *metacacheWriter) Reset(out io.Writer) {
	w.streamErr = nil
	w.creator = func() error {
		s2w := s2.NewWriter(out, s2.WriterBlockSize(w.blockSize), s2.WriterConcurrency(2))
		w.mw = msgp.NewWriter(s2w)
		w.creator = nil
		if err := w.mw.WriteByte(metacacheStreamVersion); err != nil {
			return err
		}

		w.closer = func() error {
			if w.streamErr != nil {
				return w.streamErr
			}
			if err := w.mw.WriteBool(false); err != nil {
				return err
			}
			if err := w.mw.Flush(); err != nil {
				return err
			}
			return s2w.Close()
		}
		return nil
	}
}

var s2DecPool = sync.Pool{New: func() interface{} {
	// Default alloc block for network transfer.
	return s2.NewReader(nil, s2.ReaderAllocBlock(16<<10))
}}

// metacacheReader allows reading a cache stream.
type metacacheReader struct {
	mr      *msgp.Reader
	current metaCacheEntry
	err     error // stateful error
	closer  func()
	creator func() error
}

// newMetacacheReader creates a new cache reader.
// Nothing will be read from the stream yet.
func newMetacacheReader(r io.Reader) *metacacheReader {
	dec := s2DecPool.Get().(*s2.Reader)
	dec.Reset(r)
	mr := msgp.NewReader(dec)
	return &metacacheReader{
		mr: mr,
		closer: func() {
			dec.Reset(nil)
			s2DecPool.Put(dec)
		},
		creator: func() error {
			v, err := mr.ReadByte()
			if err != nil {
				return err
			}
			switch v {
			case metacacheStreamVersion:
			default:
				return fmt.Errorf("metacacheReader: Unknown version: %d", v)
			}
			return nil
		},
	}
}

func (r *metacacheReader) checkInit() {
	if r.creator == nil || r.err != nil {
		return
	}
	r.err = r.creator()
	r.creator = nil
}

// peek will return the name of the next object.
// Will return io.EOF if there are no more objects.
// Should be used sparingly.
func (r *metacacheReader) peek() (metaCacheEntry, error) {
	r.checkInit()
	if r.err != nil {
		return metaCacheEntry{}, r.err
	}
	if r.current.name != "" {
		return r.current, nil
	}
	if more, err := r.mr.ReadBool(); !more {
		switch err {
		case nil:
			r.err = io.EOF
			return metaCacheEntry{}, io.EOF
		case io.EOF:
			r.err = io.ErrUnexpectedEOF
			return metaCacheEntry{}, io.ErrUnexpectedEOF
		}
		r.err = err
		return metaCacheEntry{}, err
	}

	var err error
	if r.current.name, err = r.mr.ReadString(); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		r.err = err
		return metaCacheEntry{}, err
	}
	r.current.metadata, err = r.mr.ReadBytes(r.current.metadata[:0])
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	r.err = err
	return r.current, err
}

// next will read one entry from the stream.
// Generally not recommended for fast operation.
func (r *metacacheReader) next() (metaCacheEntry, error) {
	r.checkInit()
	if r.err != nil {
		return metaCacheEntry{}, r.err
	}
	var m metaCacheEntry
	var err error
	if r.current.name != "" {
		m.name = r.current.name
		m.metadata = r.current.metadata
		r.current.name = ""
		r.current.metadata = nil
		return m, nil
	}
	if more, err := r.mr.ReadBool(); !more {
		switch err {
		case nil:
			r.err = io.EOF
			return m, io.EOF
		case io.EOF:
			r.err = io.ErrUnexpectedEOF
			return m, io.ErrUnexpectedEOF
		}
		r.err = err
		return m, err
	}
	if m.name, err = r.mr.ReadString(); err != nil {
		if err == io.EOF {
			err = io.ErrUnexpectedEOF
		}
		r.err = err
		return m, err
	}
	m.metadata, err = r.mr.ReadBytes(nil)
	if err == io.EOF {
		err = io.ErrUnexpectedEOF
	}
	r.err = err
	return m, err
}

// next will read one entry from the stream.
// Generally not recommended for fast operation.
func (r *metacacheReader) nextEOF() bool {
	r.checkInit()
	if r.err != nil {
		return r.err == io.EOF
	}
	if r.current.name != "" {
		return false
	}
	_, err := r.peek()
	if err != nil {
		r.err = err
		return r.err == io.EOF
	}
	return false
}

// forwardTo will forward to the first entry that is >= s.
// Will return io.EOF if end of stream is reached without finding any.
func (r *metacacheReader) forwardTo(s string) error {
	r.checkInit()
	if r.err != nil {
		return r.err
	}

	if s == "" {
		return nil
	}
	if r.current.name != "" {
		if r.current.name >= s {
			return nil
		}
		r.current.name = ""
		r.current.metadata = nil
	}
	// temporary name buffer.
	var tmp = make([]byte, 0, 256)
	for {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				r.err = io.EOF
				return io.EOF
			case io.EOF:
				r.err = io.ErrUnexpectedEOF
				return io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		// Read name without allocating more than 1 buffer.
		sz, err := r.mr.ReadStringHeader()
		if err != nil {
			r.err = err
			return err
		}
		if cap(tmp) < int(sz) {
			tmp = make([]byte, 0, sz+256)
		}
		tmp = tmp[:sz]
		_, err = r.mr.R.ReadFull(tmp)
		if err != nil {
			r.err = err
			return err
		}
		if string(tmp) >= s {
			r.current.name = string(tmp)
			r.current.metadata, r.err = r.mr.ReadBytes(nil)
			return r.err
		}
		// Skip metadata
		err = r.mr.Skip()
		if err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
	}
}

// readN will return all the requested number of entries in order
// or all if n < 0.
// Will return io.EOF if end of stream is reached.
// If requesting 0 objects nil error will always be returned regardless of at end of stream.
// Use peek to determine if at end of stream.
func (r *metacacheReader) readN(n int, inclDeleted, inclDirs bool, prefix string) (metaCacheEntriesSorted, error) {
	r.checkInit()
	if n == 0 {
		return metaCacheEntriesSorted{}, nil
	}
	if r.err != nil {
		return metaCacheEntriesSorted{}, r.err
	}

	var res metaCacheEntries
	if n > 0 {
		res = make(metaCacheEntries, 0, n)
	}
	if prefix != "" {
		if err := r.forwardTo(prefix); err != nil {
			return metaCacheEntriesSorted{}, err
		}
	}
	next, err := r.peek()
	if err != nil {
		return metaCacheEntriesSorted{}, err
	}
	if !next.hasPrefix(prefix) {
		return metaCacheEntriesSorted{}, io.EOF
	}

	if r.current.name != "" {
		if (inclDeleted || !r.current.isLatestDeletemarker()) && r.current.hasPrefix(prefix) && (inclDirs || r.current.isObject()) {
			res = append(res, r.current)
		}
		r.current.name = ""
		r.current.metadata = nil
	}

	for n < 0 || len(res) < n {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				r.err = io.EOF
				return metaCacheEntriesSorted{o: res}, io.EOF
			case io.EOF:
				r.err = io.ErrUnexpectedEOF
				return metaCacheEntriesSorted{o: res}, io.ErrUnexpectedEOF
			}
			r.err = err
			return metaCacheEntriesSorted{o: res}, err
		}
		var err error
		var meta metaCacheEntry
		if meta.name, err = r.mr.ReadString(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return metaCacheEntriesSorted{o: res}, err
		}
		if !meta.hasPrefix(prefix) {
			r.mr.R.Skip(1)
			return metaCacheEntriesSorted{o: res}, io.EOF
		}
		if meta.metadata, err = r.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return metaCacheEntriesSorted{o: res}, err
		}
		if !inclDirs && meta.isDir() {
			continue
		}
		if !inclDeleted && meta.isLatestDeletemarker() {
			continue
		}
		res = append(res, meta)
	}
	return metaCacheEntriesSorted{o: res}, nil
}

// readAll will return all remaining objects on the dst channel and close it when done.
// The context allows the operation to be canceled.
func (r *metacacheReader) readAll(ctx context.Context, dst chan<- metaCacheEntry) error {
	r.checkInit()
	if r.err != nil {
		return r.err
	}
	defer close(dst)
	if r.current.name != "" {
		select {
		case <-ctx.Done():
			r.err = ctx.Err()
			return ctx.Err()
		case dst <- r.current:
		}
		r.current.name = ""
		r.current.metadata = nil
	}
	for {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case io.EOF:
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}

		var err error
		var meta metaCacheEntry
		if meta.name, err = r.mr.ReadString(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		if meta.metadata, err = r.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		select {
		case <-ctx.Done():
			r.err = ctx.Err()
			return ctx.Err()
		case dst <- meta:
		}
	}
}

// readFn will return all remaining objects
// and provide a callback for each entry read in order
// as long as true is returned on the callback.
func (r *metacacheReader) readFn(fn func(entry metaCacheEntry) bool) error {
	r.checkInit()
	if r.err != nil {
		return r.err
	}
	if r.current.name != "" {
		fn(r.current)
		r.current.name = ""
		r.current.metadata = nil
	}
	for {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case io.EOF:
				r.err = io.ErrUnexpectedEOF
				return io.ErrUnexpectedEOF
			case nil:
				r.err = io.EOF
				return io.EOF
			}
			return err
		}

		var err error
		var meta metaCacheEntry
		if meta.name, err = r.mr.ReadString(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		if meta.metadata, err = r.mr.ReadBytes(nil); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		// Send it!
		if !fn(meta) {
			return nil
		}
	}
}

// readNames will return all the requested number of names in order
// or all if n < 0.
// Will return io.EOF if end of stream is reached.
func (r *metacacheReader) readNames(n int) ([]string, error) {
	r.checkInit()
	if r.err != nil {
		return nil, r.err
	}
	if n == 0 {
		return nil, nil
	}
	var res []string
	if n > 0 {
		res = make([]string, 0, n)
	}
	if r.current.name != "" {
		res = append(res, r.current.name)
		r.current.name = ""
		r.current.metadata = nil
	}
	for n < 0 || len(res) < n {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				r.err = io.EOF
				return res, io.EOF
			case io.EOF:
				r.err = io.ErrUnexpectedEOF
				return res, io.ErrUnexpectedEOF
			}
			return res, err
		}

		var err error
		var name string
		if name, err = r.mr.ReadString(); err != nil {
			r.err = err
			return res, err
		}
		if err = r.mr.Skip(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return res, err
		}
		res = append(res, name)
	}
	return res, nil
}

// skip n entries on the input stream.
// If there are less entries left io.EOF is returned.
func (r *metacacheReader) skip(n int) error {
	r.checkInit()
	if r.err != nil {
		return r.err
	}
	if n <= 0 {
		return nil
	}
	if r.current.name != "" {
		n--
		r.current.name = ""
		r.current.metadata = nil
	}
	for n > 0 {
		if more, err := r.mr.ReadBool(); !more {
			switch err {
			case nil:
				r.err = io.EOF
				return io.EOF
			case io.EOF:
				r.err = io.ErrUnexpectedEOF
				return io.ErrUnexpectedEOF
			}
			return err
		}

		if err := r.mr.Skip(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		if err := r.mr.Skip(); err != nil {
			if err == io.EOF {
				err = io.ErrUnexpectedEOF
			}
			r.err = err
			return err
		}
		n--
	}
	return nil
}

// Close and release resources.
func (r *metacacheReader) Close() error {
	if r == nil || r.closer == nil {
		return nil
	}
	r.closer()
	r.closer = nil
	r.creator = nil
	return nil
}

// metacacheBlockWriter collects blocks and provides a callaback to store them.
type metacacheBlockWriter struct {
	wg           sync.WaitGroup
	streamErr    error
	blockEntries int
}

// newMetacacheBlockWriter provides a streaming block writer.
// Each block is the size of the capacity of the input channel.
// The caller should close to indicate the stream has ended.
func newMetacacheBlockWriter(in <-chan metaCacheEntry, nextBlock func(b *metacacheBlock) error) *metacacheBlockWriter {
	w := metacacheBlockWriter{blockEntries: cap(in)}
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		var current metacacheBlock
		var n int

		buf := bytebufferpool.Get()
		defer func() {
			buf.Reset()
			bytebufferpool.Put(buf)
		}()

		block := newMetacacheWriter(buf, 1<<20)
		defer block.Close()
		finishBlock := func() {
			if err := block.Close(); err != nil {
				w.streamErr = err
				return
			}
			current.data = buf.Bytes()
			w.streamErr = nextBlock(&current)
			// Prepare for next
			current.n++
			buf.Reset()
			block.Reset(buf)
			current.First = ""
		}
		for o := range in {
			if len(o.name) == 0 || w.streamErr != nil {
				continue
			}
			if current.First == "" {
				current.First = o.name
			}

			if n >= w.blockEntries-1 {
				finishBlock()
				n = 0
			}
			n++

			w.streamErr = block.write(o)
			if w.streamErr != nil {
				continue
			}
			current.Last = o.name
		}
		if n > 0 || current.n == 0 {
			current.EOS = true
			finishBlock()
		}
	}()
	return &w
}

// Close the stream.
// The incoming channel must be closed before calling this.
// Returns the first error the occurred during the writing if any.
func (w *metacacheBlockWriter) Close() error {
	w.wg.Wait()
	return w.streamErr
}

type metacacheBlock struct {
	data  []byte
	n     int
	First string `json:"f"`
	Last  string `json:"l"`
	EOS   bool   `json:"eos,omitempty"`
}

func (b metacacheBlock) headerKV() map[string]string {
	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	v, err := json.Marshal(b)
	if err != nil {
		logger.LogIf(context.Background(), err) // Unlikely
		return nil
	}
	return map[string]string{fmt.Sprintf("%s-metacache-part-%d", ReservedMetadataPrefixLower, b.n): string(v)}
}

// pastPrefix returns true if the given prefix is before start of the block.
func (b metacacheBlock) pastPrefix(prefix string) bool {
	if prefix == "" || strings.HasPrefix(b.First, prefix) {
		return false
	}
	// We have checked if prefix matches, so we can do direct compare.
	return b.First > prefix
}

// endedPrefix returns true if the given prefix ends within the block.
func (b metacacheBlock) endedPrefix(prefix string) bool {
	if prefix == "" || strings.HasPrefix(b.Last, prefix) {
		return false
	}

	// We have checked if prefix matches, so we can do direct compare.
	return b.Last > prefix
}
