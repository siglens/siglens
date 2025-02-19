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
	MainIndex channelIndex = iota
	TimechartIndex
)

// This reads from two channels and multiplexes the data into a single channel.
// If any channel gets an error/cancellation/timeout, the multiplexer will send
// that on the output channel and close it. If any channel gets a COMPLETE
// state, the multiplexer will save that info but not send anything; once all
// channels are COMPLETE, the multiplexer will send the COMPLETE state on the
// output channel with all the saved info. Other messages simply propagate.
type QueryStateMultiplexer struct {
	input           [2]*chanState
	output          chan *QueryStateEnvelope
	closedOutput    bool
	savedCompletion *structs.PipeSearchCompleteResponse // When channels COMPLETE, info is saved here.
	mainQid         uint64
}

type chanState struct {
	channel    chan *query.QueryStateChanData
	isComplete bool
}

type QueryStateEnvelope struct {
	*query.QueryStateChanData
	ChannelIndex channelIndex
}

func NewQueryStateMultiplexer(mainQueryChan, timechartQueryChan chan *query.QueryStateChanData) *QueryStateMultiplexer {
	input := [2]*chanState{}
	input[MainIndex] = &chanState{
		channel:    mainQueryChan,
		isComplete: mainQueryChan == nil,
	}
	input[TimechartIndex] = &chanState{
		channel:    timechartQueryChan,
		isComplete: timechartQueryChan == nil,
	}

	return &QueryStateMultiplexer{
		input:  input,
		output: make(chan *QueryStateEnvelope),
	}
}

// This should only be called once per instance of QueryStateMultiplexer.
func (q *QueryStateMultiplexer) Multiplex() <-chan *QueryStateEnvelope {
	go func() {
		defer func() {
			if !q.closedOutput {
				close(q.output)
				q.closedOutput = true
			}
		}()

		for {
			select {
			case data, ok := <-q.input[MainIndex].channel:
				q.handleMessage(data, ok, MainIndex)
			case data, ok := <-q.input[TimechartIndex].channel:
				q.handleMessage(data, ok, TimechartIndex)
			}

			if q.allChannelsAreComplete() {
				return
			}

			if q.closedOutput {
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

		q.errorAndClose(fmt.Errorf("Channel closed unexpectedly"))
		return
	}

	q.handleData(data, chanIndex)
}

func (q *QueryStateMultiplexer) handleData(data *query.QueryStateChanData, chanIndex channelIndex) {
	switch data.StateName {
	case query.READY, query.RUNNING, query.QUERY_RESTART:
		q.output <- &QueryStateEnvelope{
			QueryStateChanData: data,
			ChannelIndex:       chanIndex,
		}
	case query.QUERY_UPDATE:
		update := &structs.PipeSearchWSUpdateResponse{}
		switch chanIndex {
		case MainIndex:
			update = data.UpdateWSResp
		case TimechartIndex:
			update.TimechartUpdate = data.UpdateWSResp
		}

		data.UpdateWSResp = update
		q.output <- &QueryStateEnvelope{
			QueryStateChanData: data,
			ChannelIndex:       chanIndex,
		}

	case query.COMPLETE:
		q.input[chanIndex].isComplete = true
		switch chanIndex {
		case MainIndex:
			q.mainQid = data.Qid
			savedResponse := data.CompleteWSResp
			if q.savedCompletion != nil {
				savedResponse.TimechartComplete = q.savedCompletion.TimechartComplete
			}

			q.savedCompletion = savedResponse
		case TimechartIndex:
			if q.savedCompletion == nil {
				q.savedCompletion = &structs.PipeSearchCompleteResponse{}
			}

			q.savedCompletion.TimechartComplete = data.CompleteWSResp
		}

		if q.allChannelsAreComplete() {
			q.output <- &QueryStateEnvelope{
				QueryStateChanData: &query.QueryStateChanData{
					StateName:       query.COMPLETE,
					Qid:             q.mainQid,
					PercentComplete: 100,
					CompleteWSResp:  q.savedCompletion,
				},
				ChannelIndex: MainIndex,
			}
		}
	case query.CANCELLED, query.TIMEOUT, query.ERROR:
		q.output <- &QueryStateEnvelope{
			QueryStateChanData: data,
			ChannelIndex:       chanIndex,
		}
		close(q.output)
		q.closedOutput = true
	}
}

func (q *QueryStateMultiplexer) errorAndClose(err error) {
	q.output <- &QueryStateEnvelope{
		QueryStateChanData: &query.QueryStateChanData{
			StateName: query.ERROR,
			Error:     err,
		},
		ChannelIndex: MainIndex,
	}
	close(q.output)
	q.closedOutput = true
}
