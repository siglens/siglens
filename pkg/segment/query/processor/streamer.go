// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"io"
	"sync"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
)

type Streamer interface {
	Fetch() (*iqr.IQR, error)
	Rewind()
	Cleanup()
	String() string
}

type CachedStream struct {
	stream                  Streamer
	unusedDataFromLastFetch *iqr.IQR
	isExhausted             bool
}

type SingleThreadedStream struct {
	stream Streamer
	lock   *sync.Mutex
}

func NewCachedStream(stream Streamer) *CachedStream {
	return &CachedStream{
		stream: stream,
	}
}

func (cs *CachedStream) Fetch() (*iqr.IQR, error) {
	if cs.isExhausted {
		return nil, io.EOF
	}

	if cs.unusedDataFromLastFetch != nil {
		defer func() { cs.unusedDataFromLastFetch = nil }()
		return cs.unusedDataFromLastFetch, nil
	}

	iqr, err := cs.stream.Fetch()
	if err == io.EOF {
		cs.isExhausted = true
	}

	return iqr, err
}

func (cs *CachedStream) Rewind() {
	cs.stream.Rewind()
	cs.unusedDataFromLastFetch = nil
	cs.isExhausted = false
}

func (cs *CachedStream) SetUnusedDataFromLastFetch(iqr *iqr.IQR) {
	cs.unusedDataFromLastFetch = iqr

	if iqr != nil {
		cs.isExhausted = false
	}
}

func (cs *CachedStream) IsExhausted() bool {
	return cs.isExhausted
}

func (cs *CachedStream) Cleanup() {
	cs.stream.Cleanup()
}

func (cs CachedStream) String() string {
	return cs.stream.String()
}

func NewSingleThreadedStream(stream Streamer) *SingleThreadedStream {
	return &SingleThreadedStream{
		stream: stream,
		lock:   &sync.Mutex{},
	}
}

func (sts *SingleThreadedStream) Fetch() (*iqr.IQR, error) {
	sts.lock.Lock()
	defer sts.lock.Unlock()

	return sts.stream.Fetch()
}

func (sts *SingleThreadedStream) Rewind() {
	sts.lock.Lock()
	defer sts.lock.Unlock()

	sts.stream.Rewind()
}

func (sts *SingleThreadedStream) Cleanup() {
	sts.lock.Lock()
	defer sts.lock.Unlock()

	sts.stream.Cleanup()
}

func (sts SingleThreadedStream) String() string {
	return sts.stream.String()
}
