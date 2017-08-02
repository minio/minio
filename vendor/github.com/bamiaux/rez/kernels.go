// Copyright 2013 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rez

import (
	"math"
	"sort"
)

type kernel struct {
	coeffs   []int16
	offsets  []int16
	size     int
	cofscale int // how many more coeffs do we have
}

func bin(v bool) uint {
	if v {
		return 1
	}
	return 0
}

func clip(v, min, max int) int {
	if v < min {
		return min
	}
	if v > max {
		return max
	}
	return v
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func makeDoubleKernel(cfg *ResizerConfig, filter Filter, field, idx uint) ([]int16, []float64, []float64, int, int) {
	scale := float64(cfg.Output) / float64(cfg.Input)
	step := math.Min(1, scale)
	support := float64(filter.Taps()) / step
	taps := int(math.Ceil(support)) * 2
	if !cfg.Vertical && taps == 6 && hasAsm() && !cfg.DisableAsm {
		taps = 8
	}
	taps = min(taps, (cfg.Input>>field)&^1)
	offsets := make([]int16, cfg.Output)
	sums := make([]float64, cfg.Output)
	weights := make([]float64, cfg.Output*taps)
	xmid := float64(cfg.Input-cfg.Output) / float64(cfg.Output*2)
	xstep := 1 / scale
	// interlaced resize see only one field but still use full res pixel positions
	ftaps := taps << field
	size := (cfg.Output + int(field*(1-idx))) >> field
	step /= float64(1 + field)
	xmid += xstep * float64(field*idx)
	for i := 0; i < size; i++ {
		left := int(math.Ceil(xmid)) - ftaps>>1
		x := clip(left, 0, max(0, cfg.Input-ftaps))
		offsets[i] = int16(x)
		for j := 0; j < ftaps; j++ {
			src := left + j
			if field != 0 && idx^uint(src&1) != 0 {
				continue
			}
			weight := filter.Get(math.Abs(xmid-float64(src)) * step)
			src = clip(src, x, cfg.Input-1) - x
			src >>= field
			weights[i*taps+src] += weight
			sums[i] += weight
		}
		xmid += xstep * float64(1+field)
	}
	return offsets, sums, weights, taps, size
}

type weight struct {
	weight float64
	offset int
}

type weights []weight

func (w weights) Len() int {
	return len(w)
}

func (w weights) Less(i, j int) bool {
	return math.Abs(w[j].weight) < math.Abs(w[i].weight)
}

func (w weights) Swap(i, j int) {
	w[i], w[j] = w[j], w[i]
}

func makeIntegerKernel(taps, size int, cof, sums []float64, pos []int16, field, idx uint) ([]int16, []int16) {
	coeffs := make([]int16, taps*size)
	offsets := make([]int16, size)
	weights := make(weights, taps)
	for i, sum := range sums[:size] {
		for j, w := range cof[:taps] {
			weights[j].weight = w
			weights[j].offset = j
		}
		sort.Sort(weights)
		diff := float64(0)
		scale := 1 << Bits / sum
		for _, it := range weights {
			w := it.weight*scale + diff
			iw := math.Floor(w + 0.5)
			coeffs[i*taps+it.offset] = int16(iw)
			diff = w - iw
		}
		cof = cof[taps:]
		off := pos[i] + int16(field-idx)
		offsets[i] = off >> field
	}
	return coeffs, offsets
}

func makeKernel(cfg *ResizerConfig, filter Filter, idx uint) kernel {
	field := bin(cfg.Interlaced)
	pos, sums, cof, taps, size := makeDoubleKernel(cfg, filter, field, idx)
	coeffs, offsets := makeIntegerKernel(taps, size, cof, sums, pos, field, idx)
	//coeffs, offsets = reduceKernel(coeffs, offsets, taps, size)
	if cfg.Vertical {
		for i := len(offsets) - 1; i > 0; i-- {
			offsets[i] = offsets[i] - offsets[i-1]
		}

	} else if cfg.Pack > 1 {
		coeffs, offsets, taps = unpack(coeffs, offsets, taps, cfg.Pack)
	}
	coeffs, cofscale := prepareCoeffs(cfg, coeffs, size, taps)
	return kernel{coeffs, offsets, taps, cofscale}
}

func prepareCoeffs(cfg *ResizerConfig, cof []int16, size, taps int) ([]int16, int) {
	if !hasAsm() || cfg.DisableAsm {
		return cof, 1
	}
	if cfg.Vertical {
		return prepareVerticalCoeffs(cof, size, taps)
	}
	return prepareHorizontalCoeffs(cof, size*cfg.Pack, taps), 1
}

func prepareVerticalCoeffs(cof []int16, size, taps int) ([]int16, int) {
	xwidth := 16
	dst := make([]int16, size*taps*xwidth>>1)
	si := 0
	di := 0
	for i := 0; i < size; i++ {
		for j := 0; j < taps; j += 2 {
			for k := 0; k < xwidth; k += 2 {
				dst[di+k+0] = cof[si+0]
				dst[di+k+1] = cof[si+1]
			}
			si += 2
			di += xwidth
		}
	}
	return dst, xwidth >> 1
}

func prepareHorizontalCoeffs(cof []int16, size, taps int) []int16 {
	if taps == 2 || taps == 4 || taps == 8 {
		return cof
	}
	xwidth := 16
	dst := make([]int16, len(cof))
	loop := size / xwidth
	left := (size - loop*xwidth) * taps
	si := 0
	di := 0
	// instead of having all taps contiguous for one destination pixel,
	// we store 2 taps per pixel and fill one simd-sized buffer with it, then
	// fill the second register with the following taps until none are left
	// this way we don't care about the simd register size, we will always be
	// able to process N pixels at once
	for i := 0; i < loop; i++ {
		for j := 0; j*2 < taps; j++ {
			for k := 0; k < xwidth; k++ {
				dst[di+k*2+0] = cof[si+k*taps+0]
				dst[di+k*2+1] = cof[si+k*taps+1]
			}
			di += xwidth * 2
			si += 2
		}
		si = di
	}
	copy(dst[di:di+left], cof[si:si+left])
	return dst
}

func unpack(coeffs, offsets []int16, taps, pack int) ([]int16, []int16, int) {
	cof := make([]int16, len(coeffs)*pack*pack)
	off := make([]int16, len(offsets)*pack)
	di := 0
	ci := 0
	oi := 0
	buf := make([]int16, pack*taps*2)
	zero := buf[:pack*taps]
	next := buf[pack*taps:]
	for _, offset := range offsets {
		copy(next, zero)
		for i := 0; i < taps; i++ {
			next[i*pack] = coeffs[ci+i]
		}
		for i := 0; i < pack; i++ {
			off[oi+i] = offset * int16(pack)
			copy(cof[di+pack*taps*i:], next)
			copy(next[i+1:], next[i:])
			copy(next[:i+1], zero)
		}
		di += taps * pack * pack
		ci += taps
		oi += pack
	}
	return cof, off, taps * pack
}
