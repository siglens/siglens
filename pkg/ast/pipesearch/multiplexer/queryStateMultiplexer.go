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

package multiplexer

import (
	"fmt"

	"github.com/siglens/siglens/pkg/segment/query"
)

// If any channel gets an error/cancellation/timeout, the multiplexer will send
// that on the output channel and close it. If any channel gets a COMPLETE
// state, the multiplexer will change that to QUERY_UPDATE, unless all channels
// are COMPLETE.
type QueryStateMultiplexer struct {
	input  [2]*chanState
	output chan *QueryStateEnvelope
}

type chanState struct {
	channel    chan *query.QueryStateChanData
	isComplete bool
}

type QueryStateEnvelope struct {
	*query.QueryStateChanData
	channelIndex int
}

func NewQueryStateMultiplexer(query1Chan, query2Chan chan *query.QueryStateChanData) *QueryStateMultiplexer {
	return &QueryStateMultiplexer{
		input: [2]*chanState{
			{channel: query1Chan},
			{channel: query2Chan},
		},
		output: make(chan *QueryStateEnvelope),
	}
}

// This should only be called once per instance of QueryStateMultiplexer.
func (q *QueryStateMultiplexer) Multiplex() <-chan *QueryStateEnvelope {
	go func() {
		defer close(q.output)

		for {
			outputClosed := false
			select {
			case data, ok := <-q.input[0].channel:
				outputClosed = q.handleMessage(data, ok, 0)
			case data, ok := <-q.input[1].channel:
				outputClosed = q.handleMessage(data, ok, 1)
			}

			if q.allChannelsAreComplete() || outputClosed {
				return
			}
		}
	}()

	return q.output
}

func (q *QueryStateMultiplexer) allChannelsAreComplete() bool {
	return q.input[0].isComplete && q.input[1].isComplete
}

func (q *QueryStateMultiplexer) handleMessage(data *query.QueryStateChanData, ok bool, chanIndex int) bool {
	state := q.input[chanIndex]
	if !ok {
		if state.isComplete {
			return false
		}

		q.errorAndClose(fmt.Errorf("Channel closed unexpectedly"), chanIndex)
		return true
	}

	return q.handleData(data, chanIndex)
}

func (q *QueryStateMultiplexer) handleData(data *query.QueryStateChanData, chanIndex int) bool {
	switch data.StateName {
	case query.READY:
	case query.RUNNING:
	case query.QUERY_UPDATE:
	case query.COMPLETE:
	case query.CANCELLED:
	case query.TIMEOUT:
	case query.ERROR:
	case query.QUERY_RESTART:
	}
}

func (q *QueryStateMultiplexer) errorAndClose(err error, chanIndex int) {
	q.output <- &QueryStateEnvelope{
		QueryStateChanData: &query.QueryStateChanData{
			StateName: query.ERROR,
			Error:     err,
		},
		channelIndex: chanIndex,
	}
	close(q.output)
}

// const (
// 	READY QueryState = iota + 1
// 	RUNNING
// 	QUERY_UPDATE // flush segment counts & aggs & records (if matched)
// 	COMPLETE
// 	CANCELLED
// 	TIMEOUT
// 	ERROR
// 	QUERY_RESTART
// )
