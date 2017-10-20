// Copyright 2017, Google
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package blog implements a private logger, in the manner of glog, without
// polluting the flag namespace or leaving files all over /tmp.
//
// It has almost no features, and a bunch of global state.
package blog

import (
	"log"
	"os"
	"strconv"
)

var level int32

type Verbose bool

func init() {
	lvl := os.Getenv("B2_LOG_LEVEL")
	i, err := strconv.ParseInt(lvl, 10, 32)
	if err != nil {
		return
	}
	level = int32(i)
}

func (v Verbose) Info(a ...interface{}) {
	if v {
		log.Print(a...)
	}
}

func (v Verbose) Infof(format string, a ...interface{}) {
	if v {
		log.Printf(format, a...)
	}
}

func V(target int32) Verbose {
	return Verbose(target <= level)
}
