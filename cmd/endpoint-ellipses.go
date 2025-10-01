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

package cmd

import (
	"errors"
	"fmt"
	"net/url"
	"runtime"
	"slices"
	"strings"

	"github.com/cespare/xxhash/v2"
	"github.com/minio/minio-go/v7/pkg/set"
	"github.com/minio/minio/internal/config"
	"github.com/minio/pkg/v3/ellipses"
	"github.com/minio/pkg/v3/env"
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
var setSizes = []uint64{2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

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
	newSetCounts := make(map[uint64]struct{})
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
	slices.Sort(setCounts)

	return setCounts
}

// getSetIndexes returns list of indexes which provides the set size
// on each index, this function also determines the final set size
// The final set size has the affinity towards choosing smaller
// indexes (total sets)
func getSetIndexes(args []string, totalSizes []uint64, setDriveCount uint64, argPatterns []ellipses.ArgPattern) (setIndexes [][]uint64, err error) {
	if len(totalSizes) == 0 || len(args) == 0 {
		return nil, errInvalidArgument
	}

	setIndexes = make([][]uint64, len(totalSizes))
	for _, totalSize := range totalSizes {
		// Check if totalSize has minimum range upto setSize
		if totalSize < setSizes[0] || totalSize < setDriveCount {
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
		msg := fmt.Sprintf("Incorrect number of endpoints provided %s, number of drives %d is not divisible by any supported erasure set sizes %d", args, commonSize, setSizes)
		return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
	}

	var setSize uint64
	// Custom set drive count allows to override automatic distribution.
	// only meant if you want to further optimize drive distribution.
	if setDriveCount > 0 {
		msg := fmt.Sprintf("Invalid set drive count. Acceptable values for %d number drives are %d", commonSize, setCounts)
		var found bool
		for _, ss := range setCounts {
			if ss == setDriveCount {
				found = true
			}
		}
		if !found {
			return nil, config.ErrInvalidErasureSetSize(nil).Msg(msg)
		}

		// No automatic symmetry calculation expected, user is on their own
		setSize = setDriveCount
	} else {
		// Returns possible set counts with symmetry.
		setCounts = possibleSetCountsWithSymmetry(setCounts, argPatterns)

		if len(setCounts) == 0 {
			msg := fmt.Sprintf("No symmetric distribution detected with input endpoints provided %s, drives %d cannot be spread symmetrically by any supported erasure set sizes %d", args, commonSize, setSizes)
			return nil, config.ErrInvalidNumberOfErasureEndpoints(nil).Msg(msg)
		}

		// Final set size with all the symmetry accounted for.
		setSize = commonSetDriveCount(commonSize, setCounts)
	}

	// Check whether setSize is with the supported range.
	if !isValidSetSize(setSize) {
		msg := fmt.Sprintf("Incorrect number of endpoints provided %s, number of drives %d is not divisible by any supported erasure set sizes %d", args, commonSize, setSizes)
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
func (s *endpointSet) getEndpoints() (endpoints []string) {
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
	k := uint64(0)
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
			totalSize *= uint64(len(p.Seq))
		}
		totalSizes = append(totalSizes, totalSize)
	}
	return totalSizes
}

// Parses all arguments and returns an endpointSet which is a collection
// of endpoints following the ellipses pattern, this is what is used
// by the object layer for initializing itself.
func parseEndpointSet(setDriveCount uint64, args ...string) (ep endpointSet, err error) {
	argPatterns := make([]ellipses.ArgPattern, len(args))
	for i, arg := range args {
		patterns, perr := ellipses.FindEllipsesPatterns(arg)
		if perr != nil {
			return endpointSet{}, config.ErrInvalidErasureEndpoints(nil).Msg(perr.Error())
		}
		argPatterns[i] = patterns
	}

	ep.setIndexes, err = getSetIndexes(args, getTotalSizes(argPatterns), setDriveCount, argPatterns)
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
func GetAllSets(setDriveCount uint64, args ...string) ([][]string, error) {
	var setArgs [][]string
	if !ellipses.HasEllipses(args...) {
		var setIndexes [][]uint64
		// Check if we have more one args.
		if len(args) > 1 {
			var err error
			setIndexes, err = getSetIndexes(args, []uint64{uint64(len(args))}, setDriveCount, nil)
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
		s, err := parseEndpointSet(setDriveCount, args...)
		if err != nil {
			return nil, err
		}
		setArgs = s.Get()
	}

	uniqueArgs := set.NewStringSet()
	for _, sargs := range setArgs {
		for _, arg := range sargs {
			if uniqueArgs.Contains(arg) {
				return nil, config.ErrInvalidErasureEndpoints(nil).Msgf("Input args (%s) has duplicate ellipses", args)
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

type node struct {
	nodeName string
	disks    []string
}

type endpointsList []node

func (el *endpointsList) add(arg string) error {
	u, err := url.Parse(arg)
	if err != nil {
		return err
	}
	found := false
	list := *el
	for i := range list {
		if list[i].nodeName == u.Host {
			list[i].disks = append(list[i].disks, u.String())
			found = true
			break
		}
	}
	if !found {
		list = append(list, node{nodeName: u.Host, disks: []string{u.String()}})
	}
	*el = list
	return nil
}

type poolArgs struct {
	args          []string
	setDriveCount uint64
}

// buildDisksLayoutFromConfFile supports with and without ellipses transparently.
func buildDisksLayoutFromConfFile(pools []poolArgs) (layout disksLayout, err error) {
	if len(pools) == 0 {
		return layout, errInvalidArgument
	}

	for _, list := range pools {
		var endpointsList endpointsList

		for _, arg := range list.args {
			switch {
			case ellipses.HasList(arg):
				patterns, err := ellipses.FindListPatterns(arg)
				if err != nil {
					return layout, err
				}
				for _, exp := range patterns.Expand() {
					for _, ep := range exp {
						if err := endpointsList.add(ep); err != nil {
							return layout, err
						}
					}
				}
			case ellipses.HasEllipses(arg):
				patterns, err := ellipses.FindEllipsesPatterns(arg)
				if err != nil {
					return layout, err
				}
				for _, exp := range patterns.Expand() {
					if err := endpointsList.add(strings.Join(exp, "")); err != nil {
						return layout, err
					}
				}
			default:
				if err := endpointsList.add(arg); err != nil {
					return layout, err
				}
			}
		}

		var stopping bool
		var singleNode bool
		var eps []string

		for i := 0; ; i++ {
			for _, node := range endpointsList {
				if node.nodeName == "" {
					singleNode = true
				}

				if len(node.disks) <= i {
					stopping = true
					continue
				}
				if stopping {
					return layout, errors.New("number of disks per node does not match")
				}
				eps = append(eps, node.disks[i])
			}
			if stopping {
				break
			}
		}

		for _, node := range endpointsList {
			if node.nodeName != "" && singleNode {
				return layout, errors.New("all arguments must but either single node or distributed")
			}
		}

		setArgs, err := GetAllSets(list.setDriveCount, eps...)
		if err != nil {
			return layout, err
		}

		h := xxhash.New()
		for _, s := range setArgs {
			for _, d := range s {
				h.WriteString(d)
			}
		}

		layout.pools = append(layout.pools, poolDisksLayout{
			cmdline: fmt.Sprintf("hash:%x", h.Sum(nil)),
			layout:  setArgs,
		})
	}
	return layout, err
}

// mergeDisksLayoutFromArgs supports with and without ellipses transparently.
func mergeDisksLayoutFromArgs(args []string, ctxt *serverCtxt) (err error) {
	if len(args) == 0 {
		return errInvalidArgument
	}

	ok := true
	for _, arg := range args {
		ok = ok && !ellipses.HasEllipses(arg)
	}

	var setArgs [][]string

	v, err := env.GetInt(EnvErasureSetDriveCount, 0)
	if err != nil {
		return err
	}
	setDriveCount := uint64(v)

	// None of the args have ellipses use the old style.
	if ok {
		setArgs, err = GetAllSets(setDriveCount, args...)
		if err != nil {
			return err
		}
		ctxt.Layout = disksLayout{
			legacy: true,
			pools:  []poolDisksLayout{{layout: setArgs, cmdline: strings.Join(args, " ")}},
		}
		return err
	}

	for _, arg := range args {
		if !ellipses.HasEllipses(arg) && len(args) > 1 {
			// TODO: support SNSD deployments to be decommissioned in future
			return fmt.Errorf("all args must have ellipses for pool expansion (%w) args: %s", errInvalidArgument, args)
		}
		setArgs, err = GetAllSets(setDriveCount, arg)
		if err != nil {
			return err
		}
		ctxt.Layout.pools = append(ctxt.Layout.pools, poolDisksLayout{cmdline: arg, layout: setArgs})
	}
	return err
}

// CreateServerEndpoints - validates and creates new endpoints from input args, supports
// both ellipses and without ellipses transparently.
func createServerEndpoints(serverAddr string, poolArgs []poolDisksLayout, legacy bool) (
	endpointServerPools EndpointServerPools, setupType SetupType, err error,
) {
	if len(poolArgs) == 0 {
		return nil, -1, errInvalidArgument
	}

	poolEndpoints, setupType, err := CreatePoolEndpoints(serverAddr, poolArgs...)
	if err != nil {
		return nil, -1, err
	}

	for i, endpointList := range poolEndpoints {
		if err = endpointServerPools.Add(PoolEndpoints{
			Legacy:       legacy,
			SetCount:     len(poolArgs[i].layout),
			DrivesPerSet: len(poolArgs[i].layout[0]),
			Endpoints:    endpointList,
			Platform:     fmt.Sprintf("OS: %s | Arch: %s", runtime.GOOS, runtime.GOARCH),
			CmdLine:      poolArgs[i].cmdline,
		}); err != nil {
			return nil, -1, err
		}
	}

	return endpointServerPools, setupType, nil
}
