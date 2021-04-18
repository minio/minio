// Copyright (c) 2015-2021 MinIO, Inc.
//
// This file is part of MinIO Object Storage stack
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package words

import "math"

// Returns the minimum value of a slice of integers
func minimum(integers []int) (minVal int) {
	minVal = math.MaxInt32
	for _, v := range integers {
		if v < minVal {
			minVal = v
		}
	}
	return
}

// DamerauLevenshteinDistance calculates distance between two strings using an algorithm
// described in https://en.wikipedia.org/wiki/Damerau-Levenshtein_distance
func DamerauLevenshteinDistance(a string, b string) int {
	var cost int
	d := make([][]int, len(a)+1)
	for i := 1; i <= len(a)+1; i++ {
		d[i-1] = make([]int, len(b)+1)
	}
	for i := 0; i <= len(a); i++ {
		d[i][0] = i
	}
	for j := 0; j <= len(b); j++ {
		d[0][j] = j
	}
	for i := 1; i <= len(a); i++ {
		for j := 1; j <= len(b); j++ {
			if a[i-1] == b[j-1] {
				cost = 0
			} else {
				cost = 1
			}
			d[i][j] = minimum([]int{
				d[i-1][j] + 1,
				d[i][j-1] + 1,
				d[i-1][j-1] + cost,
			})
			if i > 1 && j > 1 && a[i-1] == b[j-2] && a[i-2] == b[j-1] {
				d[i][j] = minimum([]int{d[i][j], d[i-2][j-2] + cost}) // transposition
			}
		}
	}
	return d[len(a)][len(b)]
}
