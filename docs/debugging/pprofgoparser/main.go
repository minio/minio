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
	"bytes"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	re             *regexp.Regexp
	goTime, margin time.Duration
)

func init() {
	re = regexp.MustCompile(`^goroutine [0-9]+ \[[^,]+(, ([0-9]+) minutes)?\]:$`)

	flag.DurationVar(&goTime, "time", 0, "goroutine block age")
	flag.DurationVar(&margin, "margin", 0, "margin time")
}

func parseGoroutineType2(path string) (map[time.Duration][]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	bf := bytes.Buffer{}

	save := func(s string) {
		bf.WriteString(s + "\n")
	}
	reset := func() {
		bf.Reset()
	}

	ret := make(map[time.Duration][]string)

	s := bufio.NewScanner(f)
	s.Split(bufio.ScanLines)

	var (
		t            time.Duration
		skip, record bool
	)

	for s.Scan() {
		line := s.Text()
		switch {
		case skip && line != "":
		case skip && line == "":
			skip = false
		case record && line == "":
			record = false
			ret[t] = append(ret[t], bf.String())
			reset()
		case record:
			save(line)
		default:
			z := re.FindStringSubmatch(line)
			if len(z) == 3 {
				if z[2] != "" {
					a, _ := strconv.Atoi(z[2])
					t = time.Duration(a) * time.Minute
					save(line)
					record = true
				} else {
					skip = true
				}
			}
		}
	}

	return ret, nil
}

const helpUsage = `

At least one argument is required to run this tool.

EXAMPLE:
     ./pprofgoparser --time 50m --margin 1m /path/to/*-goroutines-before,debug=2.txt
`

func main() {
	flag.Parse()

	if len(flag.Args()) == 0 {
		log.Fatal(helpUsage)
	}

	for _, arg := range flag.Args() {
		if !strings.HasSuffix(arg, "-goroutines-before,debug=2.txt") {
			continue
		}
		r, err := parseGoroutineType2(arg)
		if err != nil {
			log.Fatal(err)
		}

		profFName := path.Base(arg)
		fmt.Println(strings.Repeat("=", len(profFName)))
		fmt.Println(profFName)
		fmt.Println(strings.Repeat("=", len(profFName)))
		fmt.Println("")

		for t, stacks := range r {
			if goTime == 0 || math.Abs(float64(t)-float64(goTime)) <= float64(margin) {
				for _, stack := range stacks {
					fmt.Println(stack)
				}
			}
		}
	}
}
