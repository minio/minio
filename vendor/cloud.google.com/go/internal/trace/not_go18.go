// Copyright 2018 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !go1.8

package trace

import (
	"golang.org/x/net/context"
)

// OpenCensus only supports go 1.8 and higher.

func StartSpan(ctx context.Context, _ string) context.Context {
	return ctx
}

func EndSpan(context.Context, error) {
}
