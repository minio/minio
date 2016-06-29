//+build !noasm
//+build !appengine

/*
 * Minio Cloud Storage, (C) 2016 Minio, Inc.
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

package blake2b

//go:noescape
func blockAVX(p []uint8, in, iv, t, f, shffle, out []uint64)

func compressAVX(d *digest, p []uint8) {
	h0, h1, h2, h3, h4, h5, h6, h7 := d.h[0], d.h[1], d.h[2], d.h[3], d.h[4], d.h[5], d.h[6], d.h[7]

	in := make([]uint64, 8, 8)
	out := make([]uint64, 8, 8)

	shffle := make([]uint64, 2, 2)
	// vector for PSHUFB instruction
	shffle[0] = 0x0201000706050403
	shffle[1] = 0x0a09080f0e0d0c0b

	for len(p) >= BlockSize {
		// Increment counter.
		d.t[0] += BlockSize
		if d.t[0] < BlockSize {
			d.t[1]++
		}

		in[0], in[1], in[2], in[3], in[4], in[5], in[6], in[7] = h0, h1, h2, h3, h4, h5, h6, h7

		blockAVX(p, in, iv[:], d.t[:], d.f[:], shffle, out)

		h0, h1, h2, h3, h4, h5, h6, h7 = out[0], out[1], out[2], out[3], out[4], out[5], out[6], out[7]

		p = p[BlockSize:]
	}

	d.h[0], d.h[1], d.h[2], d.h[3], d.h[4], d.h[5], d.h[6], d.h[7] = h0, h1, h2, h3, h4, h5, h6, h7
}

func compress(d *digest, p []uint8) {
	// Verifies if AVX is available, use optimized code path.
	if avx {
		compressAVX(d, p)
		return
	} // else { fallback to generic approach.
	compressGeneric(d, p)
}
