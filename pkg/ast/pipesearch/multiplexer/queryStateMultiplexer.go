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
	"github.com/siglens/siglens/pkg/segment/structs"
)

type channelIndex int

const (
	mainIndex channelIndex = iota
	timechartIndex
)

// If any channel gets an error/cancellation/timeout, the multiplexer will send
// that on the output channel and close it. If any channel gets a COMPLETE
// state, the multiplexer will change that to QUERY_UPDATE, unless all channels
// are COMPLETE.
type QueryStateMultiplexer struct {
	input           [2]*chanState
	output          chan *QueryStateEnvelope
	closedOutput    bool
	savedCompletion *structs.PipeSearchCompleteResponse // When 1 but not all channels are complete, info is saved here.
}

type chanState struct {
	channel    chan *query.QueryStateChanData
	isComplete bool
}

type QueryStateEnvelope struct {
	*query.QueryStateChanData
	channelIndex int
}

func NewQueryStateMultiplexer(mainQueryChan, timechartQueryChan chan *query.QueryStateChanData) *QueryStateMultiplexer {
	input := [2]*chanState{}
	input[mainIndex] = &chanState{channel: mainQueryChan}
	input[timechartIndex] = &chanState{channel: timechartQueryChan}

	return &QueryStateMultiplexer{
		input:  input,
		output: make(chan *QueryStateEnvelope),
	}
}

// This should only be called once per instance of QueryStateMultiplexer.
func (q *QueryStateMultiplexer) Multiplex() <-chan *QueryStateEnvelope {
	go func() {
		defer close(q.output)

		for {
			select {
			case data, ok := <-q.input[mainIndex].channel:
				q.handleMessage(data, ok, mainIndex)
			case data, ok := <-q.input[timechartIndex].channel:
				q.handleMessage(data, ok, timechartIndex)
			}

			if q.allChannelsAreComplete() || q.closedOutput {
				return
			}
		}
	}()

	return q.output
}

func (q *QueryStateMultiplexer) allChannelsAreComplete() bool {
	return q.input[0].isComplete && q.input[1].isComplete
}

func (q *QueryStateMultiplexer) handleMessage(data *query.QueryStateChanData, ok bool, chanIndex channelIndex) {
	state := q.input[chanIndex]
	if !ok {
		if state.isComplete {
			return
		}

		q.errorAndClose(fmt.Errorf("Channel closed unexpectedly"), chanIndex)
		return
	}

	q.handleData(data, chanIndex)
}

func (q *QueryStateMultiplexer) handleData(data *query.QueryStateChanData, chanIndex channelIndex) {
	switch data.StateName {
	case query.READY, query.RUNNING, query.QUERY_UPDATE, query.QUERY_RESTART:
		q.output <- &QueryStateEnvelope{
			QueryStateChanData: data,
			channelIndex:       int(chanIndex),
		}
	case query.COMPLETE:
		q.input[chanIndex].isComplete = true
		switch chanIndex {
		case mainIndex:
			savedResponse := data.CompleteWSResp
			if q.savedCompletion != nil {
				savedResponse.RelatedComplete = q.savedCompletion.RelatedComplete
			}

			q.savedCompletion = savedResponse
		case timechartIndex:
			savedResponse := q.savedCompletion
			if savedResponse == nil {
				savedResponse = &structs.PipeSearchCompleteResponse{}
			}

			savedResponse.RelatedComplete.Timechart = data.CompleteWSResp
			q.savedCompletion = savedResponse
		}

		if q.allChannelsAreComplete() {
			q.output <- &QueryStateEnvelope{
				QueryStateChanData: &query.QueryStateChanData{
					StateName:      query.COMPLETE,
					CompleteWSResp: q.savedCompletion,
				},
				channelIndex: int(mainIndex),
			}
		}
	case query.CANCELLED, query.TIMEOUT, query.ERROR:
		q.output <- &QueryStateEnvelope{
			QueryStateChanData: data,
			channelIndex:       int(mainIndex),
		}
		close(q.output)
		q.closedOutput = true
	}
}

func (q *QueryStateMultiplexer) errorAndClose(err error, chanIndex channelIndex) {
	q.output <- &QueryStateEnvelope{
		QueryStateChanData: &query.QueryStateChanData{
			StateName: query.ERROR,
			Error:     err,
		},
		channelIndex: int(chanIndex),
	}
	close(q.output)
	q.closedOutput = true
}
