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

package metrics

import (
	"sort"
	"sync"

	jp "github.com/buger/jsonparser"
	"github.com/cespare/xxhash"
	"github.com/valyala/bytebufferpool"
)

type tagEntry struct {
	tagKey       string
	tagValue     []byte
	tagValueType jp.ValueType
}

type TagsHolder struct {
	idx     int
	len     int
	done    bool
	entries []tagEntry
	buf     *bytebufferpool.ByteBuffer
}

var initialTagCapacity int = 10

var tagsEntryPool = sync.Pool{
	New: func() interface{} {
		// returning &slice causes race conditions, so we return []tagEntry and pay the price of allocating on returning
		// this price is much lower than the cost of creating a new []tagEntry each time
		slice := make([]tagEntry, initialTagCapacity)
		return slice
	},
}

var tagsHolderPool = sync.Pool{
	New: func() interface{} {
		// The Pool's New function should generally only return pointer
		// types, since a pointer can be put into the return interface
		// value without an allocation:
		return &TagsHolder{}
	},
}

/*
Allocates and returns a TagsHolder

Caller is responsible for calling ReturnTagsHolder
*/
func GetTagsHolder() *TagsHolder {
	holder := tagsHolderPool.Get().(*TagsHolder)
	holder.buf = bytebufferpool.Get()
	tagsBuf := tagsEntryPool.Get().([]tagEntry)

	holder.len = initialTagCapacity
	holder.idx = 0
	holder.entries = tagsBuf
	holder.done = false

	return holder
}

/*
Returns allocated tags holder memory back to the pool
*/
func ReturnTagsHolder(th *TagsHolder) {
	bytebufferpool.Put(th.buf)
	tagsEntryPool.Put(th.entries)
	tagsHolderPool.Put(th)
}

func (th *TagsHolder) Insert(key string, value []byte, vType jp.ValueType) {
	th.len = len(th.entries)
	th.entries[th.idx].tagKey = key
	th.entries[th.idx].tagValue = value
	th.entries[th.idx].tagValueType = vType
	th.idx++
	if th.idx >= th.len {
		newBuf := make([]tagEntry, initialTagCapacity)
		th.entries = append(th.entries, newBuf...)
	}
}

func (th *TagsHolder) finish() {
	if th.done {
		return
	}

	th.entries = th.entries[:th.idx]
	if th.idx == 0 {
		return
	} else {
		sort.Slice(th.entries, func(i, j int) bool { return th.entries[i].tagKey > th.entries[j].tagKey })
	}
	th.done = true
}

/*
Gets the TSID given a metric name

Internally, will make sure the tags keys are sorted
*/
func (th *TagsHolder) GetTSID(mName []byte) (uint64, error) {
	th.finish()
	th.buf.Reset()
	_, err := th.buf.Write(mName)
	if err != nil {
		return 0, err
	}

	_, err = th.buf.Write(tags_separator)
	if err != nil {
		return 0, err
	}
	for _, val := range th.entries {
		_, err := th.buf.WriteString(val.tagKey)
		if err != nil {
			return 0, err
		}
		_, err = th.buf.Write(tags_separator)
		if err != nil {
			return 0, err
		}
		if val.tagValue == nil {
			continue
		}
		_, err = th.buf.Write(val.tagValue)
		if err != nil {
			return 0, err
		}
	}
	retVal := xxhash.Sum64(th.buf.Bytes())
	return retVal, nil
}

func (th *TagsHolder) getEntries() []tagEntry {
	return th.entries[:th.idx]
}
