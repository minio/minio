/*
 * MinIO Cloud Storage, (C) 2018-2020 MinIO, Inc.
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
	"sort"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v6/pkg/set"
	"github.com/minio/minio/cmd/config"
	"github.com/minio/minio/pkg/ellipses"
	"github.com/minio/minio/pkg/env"
)

// This file implements and supports ellipses pattern for
// `minio server` command line arguments.

// Endpoint set represents parsed ellipses values, also provides
// methods to get the sets of endpoints.
type endpointSet struct {
	argPatterns []ellipses.ArgPattern
	endpoints   []string   // Endpoints saved from previous GetEndpoints().
	setIndexes  [][]uint64 // All the sets.
}

// Supported set sizes this is used to find the optimal
// single set size.
var setSizes = []uint64{4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

// getDivisibleSize - returns a greatest common divisor of
// all the ellipses sizes.
func getDivisibleSize(totalSizes []uint64) (result uint64) {
	gcd := func(x, y uint64) uint64 {
		for y != 0 {
			x, y = y, x%y
		}
		return x
	}
	result = totalSizes[0]
	for i := 1; i < len(totalSizes); i++ {
		result = gcd(result, totalSizes[i])
	}
	return result
}

// isValidSetSize - checks whether given count is a valid set size for erasure coding.
var isValidSetSize = func(count uint64) bool {
	return (count >= setSizes[0] && count <= setSizes[len(setSizes)-1])
}

func commonSetDriveCount(divisibleSize uint64, setCounts []uint64) (setSize uint64) {
	// prefers setCounts to be sorted for optimal behavior.
	if divisibleSize < setCounts[len(setCounts)-1] {
		return divisibleSize
	}

	// Figure out largest value of total_drives_in_erasure_set which results
	// in least number of total_drives/total_drives_erasure_set ratio.
	prevD := divisibleSize / setCounts[0]
	for _, cnt := range setCounts {
		if divisibleSize%cnt == 0 {
			d := divisibleSize / cnt
			if d <= prevD {
				prevD = d
				setSize = cnt
			}
		}
	}
	return setSize
}

// possibleSetCountsWithSymmetry returns symmetrical setCounts based on the
// input argument patterns, the symmetry calculation is to ensure that
// we also use uniform number of drives common across all ellipses patterns.
func possibleSetCountsWithSymmetry(setCounts []uint64, argPatterns []ellipses.ArgPattern) []uint64 {
	var newSetCounts = make(map[uint64]struct{})
	for _, ss := range setCounts {
		var symmetry bool
		for _, argPattern := range argPatterns {
			for _, p := range argPattern {
				if uint64(len(p.Seq)) > ss {
					symmetry = uint64(len(p.Seq))%ss == 0
				} else {
					symmetry = ss%uint64(len(p.Seq)) == 0
				}
			}
		}
		// With no arg patterns, it is expected that user knows
		// the right symmetry, so either ellipses patterns are
		// provided (recommended) or no ellipses patterns.
		if _, ok := newSetCounts[ss]; !ok && (symmetry || argPatterns == nil) {
			newSetCounts[ss] = struct{}{}
		}
	}

	setCounts = []uint64{}
	for setCount := range newSetCounts {
		setCounts = append(setCounts, setCount)
	}

	// Not necessarily needed but it ensures to the readers
	// eyes that we prefer a sorted setCount slice for the
	// subsequent function to figure out the right common
	// divisor, it avoids loops.
	sort.Slice(setCounts, func(i, j int) bool {
		return setCounts[i] < setCounts[j]
	})

	return setCounts
}

// getSetIndexes returns list of indexes which provides the set size
// on each index, this function also determines the final set size
// The final set size has the affinity towards choosing smaller
// indexes (total sets)
func getSetIndexes(args []string, totalSizes []uint64, customSetDriveCount uint64, argPatterns []ellipses.ArgPattern) (setIndexes [][]uint64, err error) {
	if len(totalSizes) == 0 || len(args) == 0 {
		return nil, errInvalidArgument
	}

	setIndexes = make([][]uint64, len(totalSizes))
	for _, totalSize := range totalSizes {
		// Check if totalSize has minimum range upto setSize
		if totalSize < setSizes[0] || totalSize < customSetDriveCount {
			msg := fmt.Sprintf("Incorrect number of endpoints provided %s", args)
			return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
		}
	}

	commonSize := getDivisibleSize(totalSizes)
	possibleSetCounts := func(setSize uint64) (ss []uint64) {
		for _, s := range setSizes {
			if setSize%s == 0 {
				ss = append(ss, s)
			}
		}
		return ss
	}

	setCounts := possibleSetCounts(commonSize)
	if len(setCounts) == 0 {
		msg := fmt.Sprintf("Incorrect number of endpoints provided %s, number of disks %d is not divisible by any supported erasure set sizes %d", args, commonSize, setSizes)
		return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
	}

	var setSize uint64
	// Custom set drive count allows to override automatic distribution.
	// only meant if you want to further optimize drive distribution.
	if customSetDriveCount > 0 {
		msg := fmt.Sprintf("Invalid set drive count. Acceptable values for %d number drives are %d", commonSize, setCounts)
		var found bool
		for _, ss := range setCounts {
			if ss == customSetDriveCount {
				found = true
			}
		}
		if !found {
			return nil, config.ErrInvalidErasureSetSize(nil).Msg(msg)
		}

		// No automatic symmetry calculation expected, user is on their own
		setSize = customSetDriveCount
		globalCustomErasureDriveCount = true
	} else {
		// Returns possible set counts with symmetry.
		setCounts = possibleSetCountsWithSymmetry(setCounts, argPatterns)

		if len(setCounts) == 0 {
			msg := fmt.Sprintf("No symmetric distribution detected with input endpoints provided %s, disks %d cannot be spread symmetrically by any supported erasure set sizes %d", args, commonSize, setSizes)
			return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
		}

		// Final set size with all the symmetry accounted for.
		setSize = commonSetDriveCount(commonSize, setCounts)
	}

	// Check whether setSize is with the supported range.
	if !isValidSetSize(setSize) {
		msg := fmt.Sprintf("Incorrect number of endpoints provided %s, number of disks %d is not divisible by any supported erasure set sizes %d", args, commonSize, setSizes)
		return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
	}

	for i := range totalSizes {
		for j := uint64(0); j < totalSizes[i]/setSize; j++ {
			setIndexes[i] = append(setIndexes[i], setSize)
		}
	}

	return setIndexes, nil
}

// Returns all the expanded endpoints, each argument is expanded separately.
func (s endpointSet) getEndpoints() (endpoints []string) {
	if len(s.endpoints) != 0 {
		return s.endpoints
	}
	for _, argPattern := range s.argPatterns {
		for _, lbls := range argPattern.Expand() {
			endpoints = append(endpoints, strings.Join(lbls, ""))
		}
	}
	s.endpoints = endpoints
	return endpoints
}

// Get returns the sets representation of the endpoints
// this function also intelligently decides on what will
// be the right set size etc.
func (s endpointSet) Get() (sets [][]string) {
	var k = uint64(0)
	endpoints := s.getEndpoints()
	for i := range s.setIndexes {
		for j := range s.setIndexes[i] {
			sets = append(sets, endpoints[k:s.setIndexes[i][j]+k])
			k = s.setIndexes[i][j] + k
		}
	}

	return sets
}

// Return the total size for each argument patterns.
func getTotalSizes(argPatterns []ellipses.ArgPattern) []uint64 {
	var totalSizes []uint64
	for _, argPattern := range argPatterns {
		var totalSize uint64 = 1
		for _, p := range argPattern {
			totalSize = totalSize * uint64(len(p.Seq))
		}
		totalSizes = append(totalSizes, totalSize)
	}
	return totalSizes
}

// Parses all arguments and returns an endpointSet which is a collection
// of endpoints following the ellipses pattern, this is what is used
// by the object layer for initializing itself.
func parseEndpointSet(customSetDriveCount uint64, args ...string) (ep endpointSet, err error) {
	var argPatterns = make([]ellipses.ArgPattern, len(args))
	for i, arg := range args {
		patterns, perr := ellipses.FindEllipsesPatterns(arg)
		if perr != nil {
			return endpointSet{}, config.ErrInvalidErasureEndpoints(nil).Msg(perr.Error())
		}
		argPatterns[i] = patterns
	}

	ep.setIndexes, err = getSetIndexes(args, getTotalSizes(argPatterns), customSetDriveCount, argPatterns)
	if err != nil {
		return endpointSet{}, config.ErrInvalidErasureEndpoints(nil).Msg(err.Error())
	}

	ep.argPatterns = argPatterns

	return ep, nil
}

// GetAllSets - parses all ellipses input arguments, expands them into
// corresponding list of endpoints chunked evenly in accordance with a
// specific set size.
// For example: {1...64} is divided into 4 sets each of size 16.
// This applies to even distributed setup syntax as well.
func GetAllSets(customSetDriveCount uint64, args ...string) ([][]string, error) {
	var setArgs [][]string
	if !ellipses.HasEllipses(args...) {
		var setIndexes [][]uint64
		// Check if we have more one args.
		if len(args) > 1 {
			var err error
			setIndexes, err = getSetIndexes(args, []uint64{uint64(len(args))}, customSetDriveCount, nil)
			if err != nil {
				return nil, err
			}
		} else {
			// We are in FS setup, proceed forward.
			setIndexes = [][]uint64{{uint64(len(args))}}
		}
		s := endpointSet{
			endpoints:  args,
			setIndexes: setIndexes,
		}
		setArgs = s.Get()
	} else {
		s, err := parseEndpointSet(customSetDriveCount, args...)
		if err != nil {
			return nil, err
		}
		setArgs = s.Get()
	}

	uniqueArgs := set.NewStringSet()
	for _, sargs := range setArgs {
		for _, arg := range sargs {
			if uniqueArgs.Contains(arg) {
				return nil, config.ErrInvalidErasureEndpoints(nil).Msg(fmt.Sprintf("Input args (%s) has duplicate ellipses", args))
			}
			uniqueArgs.Add(arg)
		}
	}

	return setArgs, nil
}

// Override set drive count for manual distribution.
const (
	EnvErasureSetDriveCount = "MINIO_ERASURE_SET_DRIVE_COUNT"
)

var (
	globalCustomErasureDriveCount = false
)

// CreateServerEndpoints - validates and creates new endpoints from input args, supports
// both ellipses and without ellipses transparently.
func createServerEndpoints(serverAddr string, args ...string) (
	endpointZones EndpointZones, setDriveCount int,
	setupType SetupType, err error) {

	if len(args) == 0 {
		return nil, -1, -1, errInvalidArgument
	}

	if v := env.Get(EnvErasureSetDriveCount, ""); v != "" {
		setDriveCount, err = strconv.Atoi(v)
		if err != nil {
			return nil, -1, -1, config.ErrInvalidErasureSetSize(err)
		}
	}

	if !ellipses.HasEllipses(args...) {
		setArgs, err := GetAllSets(uint64(setDriveCount), args...)
		if err != nil {
			return nil, -1, -1, err
		}
		endpointList, newSetupType, err := CreateEndpoints(serverAddr, false, setArgs...)
		if err != nil {
			return nil, -1, -1, err
		}
		endpointZones = append(endpointZones, ZoneEndpoints{
			SetCount:     len(setArgs),
			DrivesPerSet: len(setArgs[0]),
			Endpoints:    endpointList,
		})
		setupType = newSetupType
		return endpointZones, len(setArgs[0]), setupType, nil
	}

	var prevSetupType SetupType
	var foundPrevLocal bool
	for _, arg := range args {
		setArgs, err := GetAllSets(uint64(setDriveCount), arg)
		if err != nil {
			return nil, -1, -1, err
		}
		var endpointList Endpoints
		endpointList, setupType, err = CreateEndpoints(serverAddr, foundPrevLocal, setArgs...)
		if err != nil {
			return nil, -1, -1, err
		}
		if setDriveCount != 0 && setDriveCount != len(setArgs[0]) {
			return nil, -1, -1, fmt.Errorf("All zones should have same drive per set ratio - expected %d, got %d", setDriveCount, len(setArgs[0]))
		}
		if prevSetupType != UnknownSetupType && prevSetupType != setupType {
			return nil, -1, -1, fmt.Errorf("All zones should be of the same setup-type to maintain the original SLA expectations - expected %s, got %s", prevSetupType, setupType)
		}
		if err = endpointZones.Add(ZoneEndpoints{
			SetCount:     len(setArgs),
			DrivesPerSet: len(setArgs[0]),
			Endpoints:    endpointList,
		}); err != nil {
			return nil, -1, -1, err
		}
		foundPrevLocal = endpointList.atleastOneEndpointLocal()
		if setDriveCount == 0 {
			setDriveCount = len(setArgs[0])
		}
		prevSetupType = setupType
	}

	return endpointZones, setDriveCount, setupType, nil
}
