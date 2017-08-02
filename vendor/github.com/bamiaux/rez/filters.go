// Copyright 2013 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rez

import (
	"math"
)

// Filter is an interpolation filter interface
// It is used to compute weights for every input pixel
type Filter interface {
	Taps() int
	Name() string
	Get(dx float64) float64
}

type bilinear struct{}

func (bilinear) Taps() int    { return 1 }
func (bilinear) Name() string { return "bilinear" }

func (bilinear) Get(x float64) float64 {
	if x < 1 {
		return 1 - x
	}
	return 0
}

// NewBilinearFilter exports a bilinear filter
func NewBilinearFilter() Filter {
	return bilinear{}
}

type bicubic struct {
	a, b, c, d, e, f, g float64
}

func (bicubic) Taps() int {
	return 2
}

func (bicubic) Name() string {
	return "bicubic"
}

func (f *bicubic) Get(x float64) float64 {
	if x < 1 {
		return f.a + x*x*(f.b+x*f.c)
	} else if x < 2 {
		return f.d + x*(f.e+x*(f.f+x*f.g))
	}
	return 0
}

// NewCustomBicubicFilter exports a bicubic filter where <b> and <c> can be
// customized.
// For example, the Mitchell-Netravali bicubic filter is b = c = 1/3
func NewCustomBicubicFilter(b, c float64) Filter {
	f := &bicubic{}
	f.a = 1 - b/3
	f.b = -3 + 2*b + c
	f.c = 2 - 3*b/2 - c
	f.d = 4*b/3 + 4*c
	f.e = -2*b - 8*c
	f.f = b + 5*c
	f.g = -b/6 - c
	return f
}

// NewBicubicFilter exports a classic bicubic filter
func NewBicubicFilter() Filter {
	return NewCustomBicubicFilter(0, 0.5)
}

type lanczos struct {
	alpha float64
}

func (f lanczos) Taps() int {
	return int(f.alpha)
}

func (lanczos) Name() string {
	return "lanczos"
}

func (f lanczos) Get(x float64) float64 {
	if x > f.alpha {
		return 0
	} else if x == 0 {
		return 1
	}
	b := x * math.Pi
	c := b / f.alpha
	return math.Sin(b) * math.Sin(c) / (b * c)
}

// NewLanczosFilter exports a lanczos filter where <alpha> is filter size
func NewLanczosFilter(alpha int) Filter {
	return lanczos{alpha: float64(alpha)}
}
