/*
 * Minio Client (C) 2014, 2015, 2016, 2017 Minio, Inc.
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
