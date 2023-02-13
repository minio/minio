// Copyright (c) 2022-2023 MinIO, Inc.
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

package workers

import (
	"errors"
	"sync"
)

// Workers provides a bounded semaphore with the ability to wait until all
// concurrent jobs finish.
type Workers struct {
	wg    sync.WaitGroup
	queue chan struct{}
}

// New creates a Workers object which allows up to n jobs to proceed
// concurrently. n must be > 0.
func New(n int) (*Workers, error) {
	if n <= 0 {
		return nil, errors.New("n must be > 0")
	}

	queue := make(chan struct{}, n)
	for i := 0; i < n; i++ {
		queue <- struct{}{}
	}
	return &Workers{
		queue: queue,
	}, nil
}

// Take is how a job (goroutine) can Take its turn.
func (jt *Workers) Take() {
	jt.wg.Add(1)
	<-jt.queue
}

// Give is how a job (goroutine) can give back its turn once done.
func (jt *Workers) Give() {
	jt.queue <- struct{}{}
	jt.wg.Done()
}

// Wait waits for all ongoing concurrent jobs to complete
func (jt *Workers) Wait() {
	jt.wg.Wait()
}
