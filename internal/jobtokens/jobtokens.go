// Copyright (c) 2022 MinIO, Inc.
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

package jobtokens

import (
	"errors"
	"sync"
)

// JobTokens provides a bounded semaphore with the ability to wait until all
// concurrent jobs finish.
type JobTokens struct {
	wg     sync.WaitGroup
	tokens chan struct{}
}

// New creates a JobTokens object which allows up to n jobs to proceed
// concurrently. n must be > 0.
func New(n int) (*JobTokens, error) {
	if n <= 0 {
		return nil, errors.New("n must be > 0")
	}

	tokens := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		tokens <- struct{}{}
	}
	return &JobTokens{
		tokens: tokens,
	}, nil
}

// Take is how a job (goroutine) can Take its turn.
func (jt *JobTokens) Take() {
	jt.wg.Add(1)
	<-jt.tokens
}

// Give is how a job (goroutine) can give back its turn once done.
func (jt *JobTokens) Give() {
	jt.wg.Done()
	jt.tokens <- struct{}{}
}

// Wait waits for all ongoing concurrent jobs to complete
func (jt *JobTokens) Wait() {
	jt.wg.Wait()
}
