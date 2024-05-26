// Copyright (c) 2015-2023 MinIO, Inc.
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

package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/minio/pkg/v3/ellipses"
)

type xl struct {
	This string     `json:"this"`
	Sets [][]string `json:"sets"`
}

type format struct {
	ID string `json:"id"`
	XL xl     `json:"xl"`
}

func getMountMap() (map[string]string, error) {
	result := make(map[string]string)

	mountInfo, err := os.Open("/proc/self/mountinfo")
	if err != nil {
		return nil, err
	}
	defer mountInfo.Close()

	scanner := bufio.NewScanner(mountInfo)
	for scanner.Scan() {
		s := strings.Split(scanner.Text(), " ")
		if len(s) != 11 {
			return nil, errors.New("unsupported /proc/self/mountinfo format")
		}
		result[s[2]] = s[9]
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return result, nil
}

func getDiskUUIDMap() (map[string]string, error) {
	result := make(map[string]string)
	err := filepath.Walk("/dev/disk/by-uuid/",
		func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}
			realPath, err := filepath.EvalSymlinks(path)
			if err != nil {
				return err
			}
			result[realPath] = strings.TrimPrefix(path, "/dev/disk/by-uuid/")
			return nil
		})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type localDisk struct {
	index int
	path  string
}

func getMajorMinor(path string) (string, error) {
	var stat syscall.Stat_t
	if err := syscall.Stat(path, &stat); err != nil {
		return "", fmt.Errorf("unable to stat `%s`: %w", path, err)
	}

	devID := uint64(stat.Dev)
	major := (devID & 0x00000000000fff00) >> 8
	major |= (devID & 0xfffff00000000000) >> 32
	minor := (devID & 0x00000000000000ff) >> 0
	minor |= (devID & 0x00000ffffff00000) >> 12

	return fmt.Sprintf("%d:%d", major, minor), nil
}

func filterLocalDisks(node, args string) ([]localDisk, error) {
	var result []localDisk

	argP, err := ellipses.FindEllipsesPatterns(args)
	if err != nil {
		return nil, err
	}
	exp := argP.Expand()

	if node == "" {
		for index, e := range exp {
			result = append(result, localDisk{index: index, path: strings.Join(e, "")})
		}
	} else {
		for index, e := range exp {
			u, err := url.Parse(strings.Join(e, ""))
			if err != nil {
				return nil, err
			}
			if strings.Contains(u.Host, node) {
				result = append(result, localDisk{index: index, path: u.Path})
			}
		}
	}

	return result, nil
}

func getFormatJSON(path string) (format, error) {
	formatJSON, err := os.ReadFile(filepath.Join(path, ".minio.sys/format.json"))
	if err != nil {
		return format{}, err
	}
	var f format
	err = json.Unmarshal(formatJSON, &f)
	if err != nil {
		return format{}, err
	}

	return f, nil
}

func getDiskLocation(f format) (string, error) {
	for i, set := range f.XL.Sets {
		for j, disk := range set {
			if disk == f.XL.This {
				return fmt.Sprintf("%d-%d", i, j), nil
			}
		}
	}
	return "", errors.New("format.json is corrupted")
}

func main() {
	var node, args string

	flag.StringVar(&node, "local-node-name", "", "the name of the local node")
	flag.StringVar(&args, "args", "", "arguments passed to MinIO server")

	flag.Parse()

	localDisks, err := filterLocalDisks(node, args)
	if err != nil {
		log.Fatal(err)
	}

	if len(localDisks) == 0 {
		log.Fatal("Fix --local-node-name or/and --args to select local disks.")
	}

	format, err := getFormatJSON(localDisks[0].path)
	if err != nil {
		log.Fatal(err)
	}

	setSize := len(format.XL.Sets[0])

	expectedDisksName := make(map[string]string)
	actualDisksName := make(map[string]string)

	// Calculate the set/disk index
	for _, disk := range localDisks {
		expectedDisksName[fmt.Sprintf("%d-%d", disk.index/setSize, disk.index%setSize)] = disk.path
		format, err := getFormatJSON(disk.path)
		if err != nil {
			log.Printf("Unable to read format.json from `%s`, error: %v\n", disk.path, err)
			continue
		}
		foundDiskLoc, err := getDiskLocation(format)
		if err != nil {
			log.Printf("Unable to get disk location of `%s`, error: %v\n", disk.path, err)
			continue
		}
		actualDisksName[foundDiskLoc] = disk.path
	}

	uuidMap, err := getDiskUUIDMap()
	if err != nil {
		log.Fatal("Unable to analyze UUID in /dev/disk/by-uuid/:", err)
	}

	mountMap, err := getMountMap()
	if err != nil {
		log.Fatal("Unable to parse /proc/self/mountinfo:", err)
	}

	for loc, expectedDiskName := range expectedDisksName {
		diskName := actualDisksName[loc]
		if diskName == "" {
			log.Printf("skipping disk location `%s`, err: %v\n", diskName, err)
			continue
		}
		mami, err := getMajorMinor(diskName)
		if err != nil {
			log.Printf("skipping `%s`, err: %v\n", diskName, err)
			continue
		}
		devName := mountMap[mami]
		uuid := uuidMap[devName]
		fmt.Printf("UUID=%s\t%s\txfs\tdefaults,noatime\t0\t2\n", uuid, expectedDiskName)
	}
}
