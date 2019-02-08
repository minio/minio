// Copyright 2014 Gary Burd
//
// Licensed under the Apache License, Version 2.0 (the "License"): you may
// not use this file except in compliance with the License. You may obtain
// a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations
// under the License.

package redis

import (
	"strings"
)

const (
	connectionWatchState = 1 << iota
	connectionMultiState
	connectionSubscribeState
	connectionMonitorState
)

type commandInfo struct {
	// Set or Clear these states on connection.
	Set, Clear int
}

var commandInfos = map[string]commandInfo{
	"WATCH":      {Set: connectionWatchState},
	"UNWATCH":    {Clear: connectionWatchState},
	"MULTI":      {Set: connectionMultiState},
	"EXEC":       {Clear: connectionWatchState | connectionMultiState},
	"DISCARD":    {Clear: connectionWatchState | connectionMultiState},
	"PSUBSCRIBE": {Set: connectionSubscribeState},
	"SUBSCRIBE":  {Set: connectionSubscribeState},
	"MONITOR":    {Set: connectionMonitorState},
}

func init() {
	for n, ci := range commandInfos {
		commandInfos[strings.ToLower(n)] = ci
	}
}

func lookupCommandInfo(commandName string) commandInfo {
	if ci, ok := commandInfos[commandName]; ok {
		return ci
	}
	return commandInfos[strings.ToUpper(commandName)]
}
