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
func compressAVX2Loop(p []uint8, in, iv, t, f, shffle, out []uint64)

func compressAVX2(d *digest, p []uint8) {
	var (
		in     [8]uint64
		out    [8]uint64
		shffle [8]uint64
	)

	// vector for PSHUFB instruction
	shffle[0] = 0x0201000706050403
	shffle[1] = 0x0a09080f0e0d0c0b
	shffle[2] = 0x0201000706050403
	shffle[3] = 0x0a09080f0e0d0c0b
	shffle[4] = 0x0100070605040302
	shffle[5] = 0x09080f0e0d0c0b0a
	shffle[6] = 0x0100070605040302
	shffle[7] = 0x09080f0e0d0c0b0a

	in[0], in[1], in[2], in[3], in[4], in[5], in[6], in[7] = d.h[0], d.h[1], d.h[2], d.h[3], d.h[4], d.h[5], d.h[6], d.h[7]

	compressAVX2Loop(p, in[:], iv[:], d.t[:], d.f[:], shffle[:], out[:])

	d.h[0], d.h[1], d.h[2], d.h[3], d.h[4], d.h[5], d.h[6], d.h[7] = out[0], out[1], out[2], out[3], out[4], out[5], out[6], out[7]
}
