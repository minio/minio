// Copyright 2014 Benoît Amiaux. All rights reserved.
// Use of this source code is governed by a MIT-style
// license that can be found in the LICENSE file.

package rez

import (
	"fmt"
	"image"
	"os"
)

func dumpPlane(prefix string, p *Plane, idx int) error {
	fh, err := os.Create(fmt.Sprintf("%v_%v.raw", prefix, idx))
	if err != nil {
		return err
	}
	defer fh.Close()
	si := 0
	for y := 0; y < p.Height; y++ {
		_, err = fh.Write(p.Data[si : si+p.Width])
		if err != nil {
			return err
		}
		si += p.Pitch
	}
	return nil
}

// DumpImage dumps each img planes to disk using the input prefix
func DumpImage(prefix string, img image.Image) error {
	_, src, err := inspect(img, false)
	if err != nil {
		return err
	}
	for i, p := range src {
		err = dumpPlane(prefix, &p, i)
		if err != nil {
			return err
		}
	}
	return nil
}
