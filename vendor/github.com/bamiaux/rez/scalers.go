// Copyright 2013 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rez

import (
	"math"
)

const (
	// Bits exports the number of significant bits used by kernels
	Bits = 14
)

func u8(x int) byte {
	if x < 0 {
		x = 0
	}
	if x > 0xFF {
		x = 0xFF
	}
	return byte(x)
}

func copyPlane(dst, src []byte, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		copy(dst[di:di+width], src[si:si+width])
		di += dp
		si += sp
	}
}

func psnrPlane(dst, src []byte, width, height, dp, sp int) float64 {
	mse := 0
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		for x, v := range src[si : si+width] {
			n := int(v) - int(dst[di+x])
			mse += n * n
		}
		di += dp
		si += sp
	}
	fmse := float64(mse) / float64(width*height)
	return 10 * math.Log10(255*255/fmse)
}

func h8scaleNGo(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := 0
			for i, v := range s[xoff : xoff+int16(taps)] {
				pix += int(v) * int(c[i])
			}
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[taps:]
		}
		di += dp
		si += sp
	}
}

func v8scaleNGo(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		for x := range dst[di : di+width] {
			pix := 0
			for i, c := range cof[:taps] {
				pix += int(c) * int(src[sp*i+x])
			}
			dst[di+x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[taps:]
		di += dp
	}
}
