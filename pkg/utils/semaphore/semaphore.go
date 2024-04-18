// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package semaphore

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/semaphore"
)

var defaultWait = 30 * time.Second

type WeightedSemaphore struct {
	MaxSize int64  // max size of the semaphore
	Name    string // name of the semaphore

	sem  *semaphore.Weighted // internal semaphore
	wait time.Duration
}

/*
Inits a new WeightedSemaphore with max size

max wait for getting a lock is retryCount * singleTryWait(30s), then will error out
*/
func NewDefaultWeightedSemaphore(maxSize int64, name string) *WeightedSemaphore {
	return NewWeightedSemaphore(maxSize, name, defaultWait)
}

/*
Inits a new WeightedSemaphore with max size

max wait for getting a lock is retryCount * wait, then will error out
*/
func NewWeightedSemaphore(maxSize int64, name string, wait time.Duration) *WeightedSemaphore {
	return &WeightedSemaphore{
		MaxSize: maxSize,
		Name:    name,
		sem:     semaphore.NewWeighted(maxSize),
		wait:    wait,
	}
}

/*
Wrapper around semaphore.Acquire using contexts and retries for better visibility

Tries to acquire size from the semaphore. sid is used as a indetifier for log statments

Returns error if sempahore was not acquired at the end or if never possible to acquire needed size
*/
func (s *WeightedSemaphore) TryAcquireWithBackoff(size int64, retryCount int, jobId interface{}) error {
	if size > s.MaxSize {
		return fmt.Errorf("requested %+v, but semaphore was initialized with %+v", size, s.MaxSize)
	}

	for i := 0; i < retryCount; i++ {
		err := s.acquireSingleJob(size, jobId)
		if err != nil {
			log.Errorf("WeightedSemaphore.%+s Failed to acquire resources for job %+v semaphore after %+v. Retrying %d more times",
				s.Name, jobId, s.wait, retryCount-i-1)
		} else {
			return nil
		}
	}
	return fmt.Errorf("failed to acquire resources for job %+v. requested %+v max size %+v", jobId, size, s.MaxSize)
}

func (s *WeightedSemaphore) acquireSingleJob(size int64, jobId interface{}) error {
	ctx, cancel := context.WithTimeout(context.Background(), s.wait)
	defer cancel()
	err := s.sem.Acquire(ctx, size)
	return err
}

/*
Release n resources from the semaphore
*/
func (s *WeightedSemaphore) Release(n int64) {
	s.sem.Release(n)
}

func (s *WeightedSemaphore) TryAcquire(size int64) error {
	if size > s.MaxSize {
		return fmt.Errorf("requested %+v, but semaphore was initialized with %+v", size, s.MaxSize)
	}
	err := s.sem.Acquire(context.TODO(), size)
	if err != nil {
		log.Errorf("WeightedSemaphore.%+s Failed to acquire resources",
			s.Name)
		return fmt.Errorf("failed to acquire resources. Requested %+v max size %+v", size, s.MaxSize)
	} else {
		return nil
	}
}
