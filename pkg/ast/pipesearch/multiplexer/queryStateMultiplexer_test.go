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
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func Test_Multiplexer_TimechartNil(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 2)
	multiplexer := NewQueryStateMultiplexer(mainChan, nil) // timechart channel is nil.
	outChan := multiplexer.Multiplex()
	const qid = uint64(42)

	// Send a READY message.
	readyData := &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       qid,
	}
	mainChan <- readyData

	// Send a COMPLETE message.
	completeData := &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            qid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{},
	}
	mainChan <- completeData

	messages := drainMessages(outChan, 1*time.Second)

	// Expect two messages: one for READY and one for COMPLETE.
	assert.Len(t, messages, 2)
	assert.Equal(t, query.READY, messages[0].StateName)
	assert.Equal(t, qid, messages[0].Qid)
	assert.Equal(t, query.COMPLETE, messages[1].StateName)
	assert.Equal(t, qid, messages[1].Qid)
}

func Test_Multiplexer_MainCompletesFirst(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 4)
	timechartChan := make(chan *query.QueryStateChanData, 4)
	multiplexer := NewQueryStateMultiplexer(mainChan, timechartChan)
	outChan := multiplexer.Multiplex()
	const mainQid = uint64(42)
	const timechartQid = uint64(100)

	mainChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       mainQid,
	}
	timechartChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       timechartQid,
	}
	mainChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            mainQid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{ColumnsOrder: []string{"m1", "m2"}},
	}

	// Verify the COMPLETE message is not sent, since timechart hasn't completed.
	assert.Never(t, func() bool {
		select {
		case msg := <-outChan:
			return msg.StateName == query.COMPLETE
		default:
			return false
		}
	}, 50*time.Millisecond, 10*time.Millisecond, "COMPLETE message should not be sent before timechart channel completes")

	// Now send the other COMPLETE.
	timechartChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            timechartQid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{ColumnsOrder: []string{"t1", "t2"}},
	}

	msg, ok := <-outChan
	assert.True(t, ok, "Expected one COMPLETE message")
	assert.Equal(t, query.COMPLETE, msg.StateName, "Message should be COMPLETE")
	assert.Equal(t, []string{"m1", "m2"}, msg.CompleteWSResp.ColumnsOrder)
	assert.NotNil(t, msg.CompleteWSResp.TimechartComplete)
	assert.Equal(t, []string{"t1", "t2"}, msg.CompleteWSResp.TimechartComplete.ColumnsOrder)

	_, ok = <-outChan
	assert.False(t, ok, "Expected no additional messages after COMPLETE")
}

func Test_Multiplexer_MainCompletesLast(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 4)
	timechartChan := make(chan *query.QueryStateChanData, 4)
	multiplexer := NewQueryStateMultiplexer(mainChan, timechartChan)
	outChan := multiplexer.Multiplex()
	const mainQid = uint64(42)
	const timechartQid = uint64(100)

	mainChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       mainQid,
	}
	timechartChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       timechartQid,
	}
	timechartChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            timechartQid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{ColumnsOrder: []string{"t1", "t2"}},
	}

	// Verify the COMPLETE message is not sent, since the main query hasn't completed.
	assert.Never(t, func() bool {
		select {
		case msg := <-outChan:
			return msg.StateName == query.COMPLETE
		default:
			return false
		}
	}, 50*time.Millisecond, 10*time.Millisecond, "COMPLETE message should not be sent before main channel completes")

	// Now send the other COMPLETE.
	mainChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            mainQid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{ColumnsOrder: []string{"m1", "m2"}},
	}

	msg, ok := <-outChan
	assert.True(t, ok, "Expected one COMPLETE message")
	assert.Equal(t, query.COMPLETE, msg.StateName, "Message should be COMPLETE")
	assert.Equal(t, []string{"m1", "m2"}, msg.CompleteWSResp.ColumnsOrder)
	assert.NotNil(t, msg.CompleteWSResp.TimechartComplete)
	assert.Equal(t, []string{"t1", "t2"}, msg.CompleteWSResp.TimechartComplete.ColumnsOrder)
	assert.Equal(t, query.COMPLETE.String(), msg.CompleteWSResp.State)

	_, ok = <-outChan
	assert.False(t, ok, "Expected no additional messages after COMPLETE")
}

func Test_Multiplexer_QueryUpdate(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 4)
	timechartChan := make(chan *query.QueryStateChanData, 4)
	multiplexer := NewQueryStateMultiplexer(mainChan, timechartChan)
	outChan := multiplexer.Multiplex()
	const mainQid = uint64(42)
	const timechartQid = uint64(100)

	mainChan <- &query.QueryStateChanData{
		StateName:    query.QUERY_UPDATE,
		Qid:          mainQid,
		UpdateWSResp: &structs.PipeSearchWSUpdateResponse{ColumnsOrder: []string{"m1", "m2"}},
	}
	timechartChan <- &query.QueryStateChanData{
		StateName:    query.QUERY_UPDATE,
		Qid:          timechartQid,
		UpdateWSResp: &structs.PipeSearchWSUpdateResponse{ColumnsOrder: []string{"t1", "t2"}},
	}

	messages := drainMessages(outChan, 1*time.Second)
	assert.Len(t, messages, 2)

	for _, msg := range messages {
		switch msg.ChannelIndex {
		case MainIndex:
			assert.Equal(t, query.QUERY_UPDATE, msg.StateName)
			assert.Equal(t, mainQid, msg.Qid)
			assert.Equal(t, []string{"m1", "m2"}, msg.UpdateWSResp.ColumnsOrder)
			assert.Nil(t, msg.UpdateWSResp.TimechartUpdate)
			assert.Equal(t, query.QUERY_UPDATE.String(), msg.UpdateWSResp.State)
		case TimechartIndex:
			assert.Equal(t, query.QUERY_UPDATE, msg.StateName)
			assert.Equal(t, timechartQid, msg.Qid)
			assert.Empty(t, msg.UpdateWSResp.ColumnsOrder)
			assert.NotNil(t, msg.UpdateWSResp.TimechartUpdate)
			assert.Equal(t, []string{"t1", "t2"}, msg.UpdateWSResp.TimechartUpdate.ColumnsOrder)
			assert.Equal(t, query.QUERY_UPDATE.String(), msg.UpdateWSResp.State)
		default:
			t.Fatalf("Unexpected channel index: %d", msg.ChannelIndex)
			t.FailNow()
		}
	}
}

func Test_Multiplexer_ErrorClosesOutput(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 4)
	timechartChan := make(chan *query.QueryStateChanData, 4)
	multiplexer := NewQueryStateMultiplexer(mainChan, timechartChan)
	outChan := multiplexer.Multiplex()
	const mainQid = uint64(42)
	const timechartQid = uint64(100)
	testErr := fmt.Errorf("test error")

	// Send an error from the main channel.
	mainChan <- &query.QueryStateChanData{
		StateName: query.ERROR,
		Qid:       mainQid,
		Error:     testErr,
	}

	// The multiplexer should output the error and close the channel.
	msg, ok := <-outChan
	assert.True(t, ok, "Expected an error message to be output")
	assert.Equal(t, query.ERROR, msg.StateName, "Expected state to be ERROR")
	assert.Contains(t, msg.Error.Error(), testErr.Error())

	// Even if the timechart channel sends a message, the multiplexer should not output it.
	timechartChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       timechartQid,
	}

	_, ok = <-outChan
	assert.False(t, ok, "Expected no additional messages after error")
}

func Test_Multiplexer_NonWebsocket(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 4)
	multiplexer := NewQueryStateMultiplexer(mainChan, nil) // timechart channel is nil.
	outChan := multiplexer.Multiplex()
	const qid = uint64(42)

	mainChan <- &query.QueryStateChanData{
		StateName: query.COMPLETE,
		Qid:       qid,
		HttpResponse: &structs.PipeSearchResponseOuter{
			Hits: structs.PipeSearchResponse{
				TotalMatched: 1,
				Hits:         []map[string]interface{}{{"foo": "bar"}},
			},
		},
	}

	msg, ok := <-outChan
	assert.True(t, ok, "Expected one message")
	assert.Equal(t, query.COMPLETE, msg.StateName)
	assert.Equal(t, qid, msg.Qid)
	assert.NotNil(t, msg.HttpResponse)
	assert.Equal(t, 1, msg.HttpResponse.Hits.TotalMatched)
	assert.Len(t, msg.HttpResponse.Hits.Hits, 1)
	assert.Equal(t, "bar", msg.HttpResponse.Hits.Hits[0]["foo"])
}

func drainMessages[T any](outChan <-chan T, timeout time.Duration) []T {
	var messages []T
	deadline := time.After(timeout)

loop:
	for {
		select {
		case msg, ok := <-outChan:
			if !ok {
				break loop
			}
			messages = append(messages, msg)
		case <-deadline:
			break loop
		}
	}

	return messages
}
