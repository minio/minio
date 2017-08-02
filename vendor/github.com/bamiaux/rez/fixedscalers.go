// Copyright 2013 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rez

// This file is auto-generated - do not modify

func h8scale2Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := int(s[xoff+0])*int(c[0]) +
				int(s[xoff+1])*int(c[1])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[2:]
		}
		di += dp
		si += sp
	}
}

func v8scale2Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		d := dst[di:]
		for x := range d[:width] {
			pix := int(src[sp*0+x])*int(cof[0]) +
				int(src[sp*1+x])*int(cof[1])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[2:]
		di += dp
	}
}

func h8scale4Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := int(s[xoff+0])*int(c[0]) +
				int(s[xoff+1])*int(c[1]) +
				int(s[xoff+2])*int(c[2]) +
				int(s[xoff+3])*int(c[3])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[4:]
		}
		di += dp
		si += sp
	}
}

func v8scale4Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		d := dst[di:]
		for x := range d[:width] {
			pix := int(src[sp*0+x])*int(cof[0]) +
				int(src[sp*1+x])*int(cof[1]) +
				int(src[sp*2+x])*int(cof[2]) +
				int(src[sp*3+x])*int(cof[3])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[4:]
		di += dp
	}
}

func h8scale6Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := int(s[xoff+0])*int(c[0]) +
				int(s[xoff+1])*int(c[1]) +
				int(s[xoff+2])*int(c[2]) +
				int(s[xoff+3])*int(c[3]) +
				int(s[xoff+4])*int(c[4]) +
				int(s[xoff+5])*int(c[5])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[6:]
		}
		di += dp
		si += sp
	}
}

func v8scale6Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		d := dst[di:]
		for x := range d[:width] {
			pix := int(src[sp*0+x])*int(cof[0]) +
				int(src[sp*1+x])*int(cof[1]) +
				int(src[sp*2+x])*int(cof[2]) +
				int(src[sp*3+x])*int(cof[3]) +
				int(src[sp*4+x])*int(cof[4]) +
				int(src[sp*5+x])*int(cof[5])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[6:]
		di += dp
	}
}

func h8scale8Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := int(s[xoff+0])*int(c[0]) +
				int(s[xoff+1])*int(c[1]) +
				int(s[xoff+2])*int(c[2]) +
				int(s[xoff+3])*int(c[3]) +
				int(s[xoff+4])*int(c[4]) +
				int(s[xoff+5])*int(c[5]) +
				int(s[xoff+6])*int(c[6]) +
				int(s[xoff+7])*int(c[7])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[8:]
		}
		di += dp
		si += sp
	}
}

func v8scale8Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		d := dst[di:]
		for x := range d[:width] {
			pix := int(src[sp*0+x])*int(cof[0]) +
				int(src[sp*1+x])*int(cof[1]) +
				int(src[sp*2+x])*int(cof[2]) +
				int(src[sp*3+x])*int(cof[3]) +
				int(src[sp*4+x])*int(cof[4]) +
				int(src[sp*5+x])*int(cof[5]) +
				int(src[sp*6+x])*int(cof[6]) +
				int(src[sp*7+x])*int(cof[7])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[8:]
		di += dp
	}
}

func h8scale10Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := int(s[xoff+0])*int(c[0]) +
				int(s[xoff+1])*int(c[1]) +
				int(s[xoff+2])*int(c[2]) +
				int(s[xoff+3])*int(c[3]) +
				int(s[xoff+4])*int(c[4]) +
				int(s[xoff+5])*int(c[5]) +
				int(s[xoff+6])*int(c[6]) +
				int(s[xoff+7])*int(c[7]) +
				int(s[xoff+8])*int(c[8]) +
				int(s[xoff+9])*int(c[9])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[10:]
		}
		di += dp
		si += sp
	}
}

func v8scale10Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		d := dst[di:]
		for x := range d[:width] {
			pix := int(src[sp*0+x])*int(cof[0]) +
				int(src[sp*1+x])*int(cof[1]) +
				int(src[sp*2+x])*int(cof[2]) +
				int(src[sp*3+x])*int(cof[3]) +
				int(src[sp*4+x])*int(cof[4]) +
				int(src[sp*5+x])*int(cof[5]) +
				int(src[sp*6+x])*int(cof[6]) +
				int(src[sp*7+x])*int(cof[7]) +
				int(src[sp*8+x])*int(cof[8]) +
				int(src[sp*9+x])*int(cof[9])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[10:]
		di += dp
	}
}

func h8scale12Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	si := 0
	for y := 0; y < height; y++ {
		c := cof
		s := src[si:]
		d := dst[di:]
		for x, xoff := range off[:width] {
			pix := int(s[xoff+0])*int(c[0]) +
				int(s[xoff+1])*int(c[1]) +
				int(s[xoff+2])*int(c[2]) +
				int(s[xoff+3])*int(c[3]) +
				int(s[xoff+4])*int(c[4]) +
				int(s[xoff+5])*int(c[5]) +
				int(s[xoff+6])*int(c[6]) +
				int(s[xoff+7])*int(c[7]) +
				int(s[xoff+8])*int(c[8]) +
				int(s[xoff+9])*int(c[9]) +
				int(s[xoff+10])*int(c[10]) +
				int(s[xoff+11])*int(c[11])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
			c = c[12:]
		}
		di += dp
		si += sp
	}
}

func v8scale12Go(dst, src []byte, cof, off []int16,
	taps, width, height, dp, sp int) {
	di := 0
	for _, yoff := range off[:height] {
		src = src[sp*int(yoff):]
		d := dst[di:]
		for x := range d[:width] {
			pix := int(src[sp*0+x])*int(cof[0]) +
				int(src[sp*1+x])*int(cof[1]) +
				int(src[sp*2+x])*int(cof[2]) +
				int(src[sp*3+x])*int(cof[3]) +
				int(src[sp*4+x])*int(cof[4]) +
				int(src[sp*5+x])*int(cof[5]) +
				int(src[sp*6+x])*int(cof[6]) +
				int(src[sp*7+x])*int(cof[7]) +
				int(src[sp*8+x])*int(cof[8]) +
				int(src[sp*9+x])*int(cof[9]) +
				int(src[sp*10+x])*int(cof[10]) +
				int(src[sp*11+x])*int(cof[11])
			d[x] = u8((pix + 1<<(Bits-1)) >> Bits)
		}
		cof = cof[12:]
		di += dp
	}
}
