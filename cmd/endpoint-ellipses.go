/*
 * Minio Cloud Storage, (C) 2017 Minio, Inc.
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

package cmd

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/minio/minio-go/pkg/set"
)

// This file implements and supports ellipses pattern for
// `minio server` command line arguments.

var (
	// Regex to extract ellipses syntax for distributed setup.
	regexpTwoEllipses = regexp.MustCompile(`(^https?://.*)({[0-9]*\.\.\.[0-9]*})(.*)({[0-9]*\.\.\.[0-9]*})`)

	// Regex to extract ellipses syntax for standalone setup.
	regexpSingleEllipses = regexp.MustCompile(`(.*)({[0-9]*\.\.\.[0-9]*})`)

	// Ellipses constants
	openBraces  = "{"
	closeBraces = "}"
	ellipses    = "..."
)

// Endpoint set represents parsed ellipses values, also provides
// methods to get the sets of endpoints.
type endpointSet struct {
	ellipsesPattern
	totalSize  uint64
	setIndexes []uint64
}

// Supported set sizes this is used to find the optimal
// single set size.
var setSizes = []uint64{8, 10, 12, 14, 16}

// Function to look for if total size provided, is indeed
// divisible with known set sizes.
func isSetDivisible(setSize uint64) bool {
	for _, i := range setSizes {
		if setSize%i == 0 {
			return true
		}
	}
	return false
}

// getSetIndexes returns list of indexes which provides the set size
// on each index, this function also determines the final set size
// The final set size has the affinity towards choosing smaller
// indexes
func getSetIndexes(totalSize uint64) (setIndexes []uint64, err error) {
	// Total size is lesser than the first element of the known
	// set sizes, then choose the set size to be totalSize.
	if totalSize < setSizes[0] {
		return []uint64{totalSize}, nil
	}

	// Verify if the totalSize is divisible.
	if !isSetDivisible(totalSize) {
		return nil, fmt.Errorf("totalSize %d is not divisible with any %v", totalSize, setSizes)
	}

	var prevD = totalSize / setSizes[0]
	var setSize uint64
	for _, i := range setSizes {
		if totalSize%i == 0 {
			d := totalSize / i
			if d <= prevD {
				prevD = d
				setSize = i
			}
		}
	}

	for i := uint64(0); i < prevD; i++ {
		setIndexes = append(setIndexes, setSize)
	}

	return setIndexes, nil
}

// Get returns the sets representation of the endpoints
// this function also intelligently decides on what will
// be the right set size etc.
func (s endpointSet) Get() (sets [][]string) {
	sets = make([][]string, len(s.setIndexes))

	var endpoints []string
	if len(s.ellipsesPattern) == 2 {
		// Loop through disks first for distributed and exhaust all hosts
		// for better distributed erasure coded behavior.
		for j := s.ellipsesPattern[1].start; j <= s.ellipsesPattern[1].end; j++ {
			for i := s.ellipsesPattern[0].start; i <= s.ellipsesPattern[0].end; i++ {
				endpoint := fmt.Sprintf("%s%d%s%d",
					s.ellipsesPattern[0].prefix, i,
					s.ellipsesPattern[1].prefix, j)
				endpoints = append(endpoints, endpoint)
			}
		}
	} else {
		// In standalone we just loop through parsed pattern and create endpoints.
		for i := s.ellipsesPattern[0].start; i <= s.ellipsesPattern[0].end; i++ {
			endpoint := fmt.Sprintf("%s%d", s.ellipsesPattern[0].prefix, i)
			endpoints = append(endpoints, endpoint)
		}
	}

	var j = uint64(0)
	for i := range s.setIndexes {
		sets[i] = endpoints[j : s.setIndexes[i]+j]
		j = s.setIndexes[i] + j
	}

	return sets
}

type ellipsesSubPattern struct {
	prefix  string
	pattern string
	start   uint64
	end     uint64
}

type ellipsesPattern []ellipsesSubPattern

// Parses an ellipses range pattern of following style
// `{1...64}`
// `{33...64}`
func parseEllipsesRange(pattern string) (start, end uint64, err error) {
	if strings.Index(pattern, openBraces) == -1 {
		return 0, 0, errInvalidArgument
	}
	if strings.Index(pattern, closeBraces) == -1 {
		return 0, 0, errInvalidArgument
	}

	pattern = strings.TrimPrefix(pattern, openBraces)
	pattern = strings.TrimSuffix(pattern, closeBraces)

	ellipsesRange := strings.Split(pattern, "...")
	if len(ellipsesRange) != 2 {
		return 0, 0, errInvalidArgument
	}

	if start, err = strconv.ParseUint(ellipsesRange[0], 10, 64); err != nil {
		return 0, 0, err
	}

	if end, err = strconv.ParseUint(ellipsesRange[1], 10, 64); err != nil {
		return 0, 0, err
	}

	if start > end {
		return 0, 0, fmt.Errorf("Incorrect range start %d cannot be bigger than end %d", start, end)
	}

	return start, end, nil
}

// parses the input args from command line into a structured form as
// endpointSet which carries, parsed values of the input arg.
func parseEndpointSet(arg string) (s endpointSet, err error) {
	var parsedEllipses ellipsesPattern
	// Check if we have two ellipses.
	parts := regexpTwoEllipses.FindStringSubmatch(arg)
	if len(parts) == 0 {
		// We didn't find two ellipses, look if we have atleast one.
		parts = regexpSingleEllipses.FindStringSubmatch(arg)
		if len(parts) == 0 {
			// We throw an error if arg doesn't have any recognizable ellipses pattern.
			return s, fmt.Errorf("Invalid input (%s), no regex pattern found", arg)
		}
	}

	// Regex matches even for more than 2 ellipses, we should simply detect
	// by looking at repeated '...' ellipses patterns.
	if strings.Count(arg, "...") >= 3 {
		return s, fmt.Errorf("Invalid input (%s), contains more than 2 ellipses", arg)
	}

	// For standalone setup we only have one ellipses pattern.
	if len(parts) == 3 {
		parsedEllipses = []ellipsesSubPattern{
			{
				prefix:  parts[1],
				pattern: parts[2],
			},
		}
	} else {
		parsedEllipses = []ellipsesSubPattern{
			{
				prefix:  parts[1],
				pattern: parts[2],
			},
			{
				prefix:  parts[3],
				pattern: parts[4],
			},
		}
	}

	parsedEllipses[0].start, parsedEllipses[0].end, err = parseEllipsesRange(parsedEllipses[0].pattern)
	if err != nil {
		return s, err
	}

	if len(parsedEllipses) == 2 {
		parsedEllipses[1].start, parsedEllipses[1].end, err = parseEllipsesRange(parsedEllipses[1].pattern)
		if err != nil {
			return s, err
		}
	}

	var totalSize uint64 = 1
	for _, p := range parsedEllipses {
		totalSize = totalSize * (p.end - (p.start - 1))
	}

	setIndexes, err := getSetIndexes(totalSize)
	if err != nil {
		return s, err
	}

	return endpointSet{parsedEllipses, totalSize, setIndexes}, nil
}

// Parses all ellipses input arguments, expands them into corresponding
// list of endpoints chunked evenly in accordance with a specific
// set size.
// For example: {1...64} is divided into 4 sets each of size 16.
// This applies to even distributed setup syntax as well.
func getAllSets(args ...string) ([][]string, error) {
	if len(args) == 0 {
		return nil, errInvalidArgument
	}

	var setArgs [][]string
	for _, arg := range args {
		s, err := parseEndpointSet(arg)
		if err != nil {
			return nil, err
		}
		setArgs = append(setArgs, s.Get()...)
	}

	prevSetSize := len(setArgs[0])
	uniqueArgs := set.NewStringSet()
	for i, args := range setArgs {
		for _, arg := range args {
			if uniqueArgs.Contains(arg) {
				return nil, fmt.Errorf("Unable to proceed duplicate sets found, your input might have duplicate ellipses")
			}
			uniqueArgs.Add(arg)
		}
		if len(args) != prevSetSize {
			return nil, fmt.Errorf("Unexpected set sizes found, set %d is of length %d but expected %d", i+1, len(args), prevSetSize)
		}
	}

	return setArgs, nil
}

// Returns true input args match either distributed
// or standalone syntax format.
func hasArgsWithEllipses(args ...string) (ok bool) {
	ok = true
	for _, arg := range args {
		ok = ok && func() bool {
			var parts []string
			if parts = regexpTwoEllipses.FindStringSubmatch(arg); len(parts) == 0 {
				return len(regexpSingleEllipses.FindStringSubmatch(arg)) != 0
			}
			return len(parts) != 0
		}()
	}
	return ok
}

// CreateEndpointsFromEllipses - validates and creates new endpoints for given sets file.
func CreateEndpointsFromEllipses(serverAddr string, args ...string) (string, EndpointList, SetupType, int, int, error) {
	var setEndpoints EndpointList
	var setupType SetupType

	setArgs, err := getAllSets(args...)
	if err != nil {
		return serverAddr, nil, setupType, 0, 0, err
	}

	var endpoints EndpointList
	for i, sets := range setArgs {
		serverAddr, endpoints, setupType, err = CreateEndpoints(serverAddr, sets...)
		if err != nil {
			return serverAddr, setEndpoints, setupType, 0, 0, err
		}

		var newEndpoints EndpointList
		for _, endpoint := range endpoints {
			endpoint.SetIndex = i
			newEndpoints = append(newEndpoints, endpoint)
		}

		setEndpoints = append(setEndpoints, newEndpoints...)
	}

	return serverAddr, setEndpoints, setupType, len(setArgs), len(setArgs[0]), nil
}
