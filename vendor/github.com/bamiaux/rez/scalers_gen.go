// Copyright 2014 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

// +build !amd64

package rez

func hasAsm() bool { return false }

func getHorizontalScaler(taps int, asm bool) scaler {
	return getHorizontalScalerGo(taps)
}

func getVerticalScaler(taps int, asm bool) scaler {
	return getVerticalScalerGo(taps)
}
