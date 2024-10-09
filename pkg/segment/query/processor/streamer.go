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

package processor

import (
	"io"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
)

type streamer interface {
	Fetch() (*iqr.IQR, error)
	Rewind()
}

type cachedStream struct {
	stream                  streamer
	unusedDataFromLastFetch *iqr.IQR
	isExhausted             bool
}

func (cs *cachedStream) Fetch() (*iqr.IQR, error) {
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

func (cs *cachedStream) Rewind() {
	cs.stream.Rewind()
	cs.unusedDataFromLastFetch = nil
	cs.isExhausted = false
}

func (cs *cachedStream) SetUnusedDataFromLastFetch(iqr *iqr.IQR) {
	cs.unusedDataFromLastFetch = iqr

	if iqr != nil {
		cs.isExhausted = false
	}
}

func (cs *cachedStream) IsExhausted() bool {
	return cs.isExhausted
}
