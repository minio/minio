/*
 * MinIO Cloud Storage, (C) 2019 MinIO, Inc.
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

package ecc

import (
	"io"
)

// This file contains a reader-like implementation that
// combines a set of data sources and schedules and manages
// concurrent reading from some/all data source in parallel.
// The implementation is optimized for:
// 1) Performing as few read operations as possible overall
//    while doing as many reads in parallel as possible.
// 2) Reading data shards first so that no erasure reconstruction
//    is necessary if all data shards are available.
//
// Before changing the implementation make sure that you understand
// the implications and take a look at the approaches [1] and [2]
// at the end of the file - which are less optimal.

const (
	// ErrReadQuorum is the error returned by the JoinedReaders
	// when too many read operations fail such that the erasure
	// coding won't be able to reconstruct the content data.
	ErrReadQuorum errorType = "ecc: no read quorum: too many data sources are unavailable"

	// errOffline is the error used to mark a data source as offline.
	// The MinIO server may has failed to fetch the metadata (XL.json)
	// from certain data sources before. Such data sources are marked as
	// offline by an io.ReaderAt that is nil.
	errOffline errorType = "ecc: dat source is marked as offline"
)

// JoinReaders combines a set of data sources. It implements reading
// from the sources concurrently. During a read it spawns and waits
// for go routines that read from data sources until the all read
// operations have accumulated enough data to reconstruct it.
func JoinReaders(src []io.ReaderAt, offset int64) *JoinedReaders {
	errors := make([]error, len(src))
	for i := range src {
		if src[i] == nil { // If a data source is nil...
			errors[i] = errOffline // ...mark it as offline.
		}
	}
	return &JoinedReaders{
		src:    src,
		err:    errors,
		offset: offset,
	}
}

// JoinedReaders groups a set of data sources
// and manages reading from them concurrently.
type JoinedReaders struct {
	src    []io.ReaderAt
	err    []error
	offset int64

	rErr error
}

// Read tries to read just as many shards
// as necessary into the buffer such that the
// erasure coding can reconstruct missing data
// shards, if any.
//
// Let d be the number of data shards. Read spawns
// d go routines - each tries to read one complete
// shard from its data source. Whenever a go routine
// completes but returns an error, this data source
// is considered unavailable and another go routine
// is spawned. This new go routine tries to read from
// the next data source. If no more data sources are
// available it returns ErrNoReadQuorum.
//
// Read minimizes the total number of reads / connections
// and tries to read as many data shards before
// it tries to fetch any parity shards. Minimizing
// the number of reads / connections is important to
// avoid any unnecessary I/O or network load. Also, if
// a data shard is available we should prefer fetching it
// instead of reading a parity shard to avoid unnecessary
// erasure coding reconstruction.
func (r *JoinedReaders) Read(b *Buffer) error {
	if len(r.src) != len(b.shards) {
		panic("ecc: number of data sources does not match the number of shards")
	}
	if r.rErr != nil {
		return r.rErr
	}

	b.Reset()
	results, next, err := r.spawn(b)
	if err != nil {
		r.rErr = err
		return r.rErr
	}

	// Mark all shards as missing.
	// When a read from a data source succeeds
	// the particular shard will get marked
	// as available again.
	for i := range b.shards {
		b.shards[i] = b.shards[i][:0]
	}

	return r.join(b, next, results)
}

type readResult struct {
	ID  int   // The ID / index of the shard / data source
	N   int   // The number of bytes read from the data source
	Err error // Any error that happened during reading
}

// spawn creates just as many concurrent go routines as necessary to read
// enough shards to reconstruct the content data. If all data shards are
// available each go routine tries to read a data shard.
// It returns a channel which hold the read results once they are available.
// The returned next index points to the next data source from which a go
// routine should read if any of the spawned reads fails.
func (r *JoinedReaders) spawn(b *Buffer) (results chan readResult, next int, err error) {
	results = make(chan readResult, len(r.src))
	var spawned int
	for spawned < len(b.data) && next < len(b.shards) {
		if r.err[next] == nil {
			go readFrom(r.src[next], next, r.offset, b.shards[next], results)
			spawned++
		}
		next++
	}
	// If we could not spawn a single go routine we should return an error
	// (or panic) since a join would block forever since the results channel
	// would never receive a result. However, that error should never happen
	// since there should be at least 1 (actually `len(b.data)`) non-nil data
	// sources. Otherwise, we never had read quorum in the first place and
	// should have failed somewhere higher up the stack.
	// TODO(aead): Consider a panic.
	if spawned == 0 {
		err = ErrReadQuorum
	}
	return
}

// join joins all go routines created by spawn(). Therefore, it reads
// from the results channel until it has received enough shards. If
// one read fails - i.e. returns an non-EOF error - it re-spawns another
// go routine reading from the next data source.
func (r *JoinedReaders) join(b *Buffer, next int, results chan readResult) error {
	var (
		n      int // Number of bytes read in total
		nReads int // Number of reads that succeeded
		nEOFs  int // Number of cases read returned an EOF error
	)
	for result := range results {
		i, nn, err := result.ID, result.N, result.Err

		// In case the i-th read fails we check whether
		// performing another read "makes sense". (If we
		// have encountered more the `N = parity` errors
		// we won't be able to reconstruct the data - so
		// we return ErrReadQuorum).
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			r.err[i] = err
			if next == len(b.shards) || countErrors(r.err) > len(b.parity) {
				r.rErr = ErrReadQuorum
				return r.rErr
			}
			next = r.respawn(b, next, results)
			continue
		}

		if err == io.EOF || err == io.ErrUnexpectedEOF {
			nEOFs++
		}
		b.shards[i] = b.shards[i][:nn]
		n += nn
		nReads++

		// As soon as we have read enough shards
		// we exit the loop. The GC will take care
		// of the channel and the remaining go routines.
		if nReads >= len(b.data) {
			break
		}
	}
	// If we read no byte at all - i.e. every read
	// returned err == io.EOF then there are no more
	// erasure-encoded data blocks.
	if n == 0 {
		r.rErr = io.EOF
		return r.rErr
	}

	// If every read returns err == io.ErrUnexpected
	// then we should not spawn go routines on the next
	// Read(b *Buffer) call. There won't be more data to
	// read. However, n != 0 => so we only return io.EOF
	// on the next Read call, not this time.
	if nEOFs == nReads {
		r.rErr = io.EOF
	}

	// Adjust the offset for the next Read. This assumes
	// that every read from each data source returned the
	// same number of bytes in case of success. In particular
	// we assume that each `r.src[i].ReadAt(shard)` itself
	// uses io.ReadFull or similar to actually read the data.
	r.offset += int64(n / nReads)
	return nil
}

// Close tries to close each data source if it implements
// io.Closer. It returns the first error it encounters
// during closing the data sources, if any.
func (r *JoinedReaders) Close() error {
	for _, src := range r.src {
		if src != nil {
			if closer, ok := src.(io.Closer); ok {
				if err := closer.Close(); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// respawn finds the next data source that is not marked as
// unavailable, if any exists, and spawns a new go-routine that
// tries to read from that source. The go routine sends its
// result to the results channel.
// respawn returns the (position of the) next data source that
// should be tried when another go routine should be spawned.
// If respawn(...) == len(r.src) then there are no more data
// sources available.
func (r *JoinedReaders) respawn(b *Buffer, next int, results chan<- readResult) int {
	for next < len(b.shards) {
		if r.err[next] == nil { // Invariant: r.err[next] == nil => r.src[next] != nil
			go readFrom(r.src[next], next, r.offset, b.shards[next], results)
			return next+1
			break
		}
		next++
	}
	return next
}

// readFrom reads from the src at the given offset into the
// shard buffer and writes the result to the channel.
func readFrom(src io.ReaderAt, id int, offset int64, shard []byte, results chan<- readResult) {
	nn, err := src.ReadAt(shard[:cap(shard)], offset)
	results <- readResult{ID: id, N: nn, Err: err}
}

// countErrors returns the number of non-nil
// errors in the errors slice.
func countErrors(errors []error) (n int) {
	for _, err := range errors {
		if err != nil {
			n++
		}
	}
	return
}

// Footnotes:
//
// Approaches that don't work / are less optimal than the current
// implementation:
//
// [1]: Reading sequentially
// While reading sequentially from one data source after the other
// is much easier to implement it would cause a significant performance
// hit when the data is fetched over the network. Even with fast network
// connections the round-trip times (a.o.) accumulate.
//
// [2]: Reading from all data sources in parallel
// It might be tempting to just read from all data source in
// parallel and wait for "just enough" results to reconstruct.
// Even though this approach seems easier/less complex at the
// beginning it has some drawbacks and ends up being actually
// more complicated:
//
//    a) Performing N = len(r.src) parallel reads causes
//       unnecessary I/O / network load on the system.
//       Nevertheless, in situations with one or two slow
//       data shard sources this maybe reduces the time-to-first
//       byte slightly. But this does not reflect the avg. case.
//
//    b) Actual complexity. Let's assume there are N = 8 (4/4)
//       data sources and you spawn 8 go routines to read
//       from all in parallel. Now, 5 (3 data, 2 parity) returned
//       successfully and 1 (parity) reported an error while 2 (1 data,
//       1 parity) are still reading and havn't reported any result,
//       yet.
//       Now, you could wait for the 1 go routine with the data shard
//       but if you always wait for the "slowest" data shard you just
//       end up with the implementation from above but with more I/O
//       load due to the extra parity shard reads. Also, you have enough
//       shards to reconstruct the missing one (5 >= 4).
//       However, doing so may lead to the following bug:
//       1. The reconstruction starts while one go routine (data shard)
//          has not finished reading yet.
//       2. During reconstruction the erasure coding may use the data
//          shard as some temporial scratch space.
//       3. The go routine still reads from the data source and may
//          overwrite some of the data written by the erasure coding
//          or vice versa.
//
//          => The still reading go routine causes data corruption
//             due to concurrent writes to the shard buffer.
//       To avoid this you either need to have separate read and
//       erasure coding buffers (requires copying = inefficient) or
//       very sophisticated mutex locking (complex).
