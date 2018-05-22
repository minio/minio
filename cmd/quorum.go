/*
 * Minio Cloud Storage, (C) 2018 Minio, Inc.
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
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/minio/minio/cmd/logger"
	"github.com/minio/minio/pkg/quorum"
)

var errReadQuorum = errors.New("quorum is not met on read call")
var errWriteQuorum = errors.New("quorum is not met on write call")

func callFunctions(functions []quorum.Func, quorumValue int, maxExecTime, maxSuccessWait time.Duration, readQuorum bool) error {
	rc, errs := quorum.Call(functions, quorumValue, maxExecTime, maxSuccessWait)

	var err error
	if len(errs) > 0 {
		err = errs[len(errs)-1]
		if _, ok := err.(*quorum.Error); ok {
			if readQuorum {
				err = errReadQuorum
			} else {
				err = errWriteQuorum
			}
		} else {
			errs = errs[:len(errs)-1]
		}
	}

	if len(errs) > 0 {
		logger.LogIf(context.Background(), fmt.Errorf("%v", errs))
	}

	if rc < 0 {
		return err
	}

	return nil
}
