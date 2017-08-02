// Copyright 2014 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rez

func hasAsm() bool { return true }

func h8scale2Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func h8scale4Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func h8scale8Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func h8scale10Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func h8scale12Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func h8scaleNAmd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scale2Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scale4Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scale6Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scale8Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scale10Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scale12Amd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)
func v8scaleNAmd64(dst, src []byte, cof, off []int16, taps, width, height, dp, sp int)

func getHorizontalScaler(taps int, asm bool) scaler {
	if !asm {
		return getHorizontalScalerGo(taps)
	}
	switch taps {
	case 2:
		return h8scale2Amd64
	case 4:
		return h8scale4Amd64
	case 8:
		return h8scale8Amd64
	case 10:
		return h8scale10Amd64
	case 12:
		return h8scale12Amd64
	}
	return h8scaleNAmd64
}

func getVerticalScaler(taps int, asm bool) scaler {
	if !asm {
		return getVerticalScalerGo(taps)
	}
	switch taps {
	case 2:
		return v8scale2Amd64
	case 4:
		return v8scale4Amd64
	case 6:
		return v8scale6Amd64
	case 8:
		return v8scale8Amd64
	case 10:
		return v8scale10Amd64
	case 12:
		return v8scale12Amd64
	}
	return v8scaleNAmd64
}
