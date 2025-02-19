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
	"testing"
	"time"

	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

func TestMultiplexer_TimechartNil(t *testing.T) {
	// Create a buffered main query channel.
	mainChan := make(chan *query.QueryStateChanData, 2)
	// timechart channel is nil.
	multiplexer := NewQueryStateMultiplexer(mainChan, nil)
	outChan := multiplexer.Multiplex()

	// Send a READY message.
	const qid = uint64(42)
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

	// Close the main channel.
	close(mainChan)

	// Collect all output messages.
	var messages []*QueryStateEnvelope
	for msg := range outChan {
		messages = append(messages, msg)
	}

	// Expect two messages: one for READY and one for COMPLETE.
	assert.Len(t, messages, 2)
	assert.Equal(t, query.READY, messages[0].StateName)
	assert.Equal(t, qid, messages[0].Qid)
	assert.Equal(t, query.COMPLETE, messages[1].StateName)
	assert.Equal(t, qid, messages[1].Qid)
}

func TestMultiplexer_MainCompletesFirst(t *testing.T) {
	mainChan := make(chan *query.QueryStateChanData, 4)
	timechartChan := make(chan *query.QueryStateChanData, 4)
	multiplexer := NewQueryStateMultiplexer(mainChan, timechartChan)
	outChan := multiplexer.Multiplex()

	const qid = uint64(42)

	mainChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       qid,
	}
	timechartChan <- &query.QueryStateChanData{
		StateName: query.READY,
		Qid:       qid,
	}
	mainChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            qid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{ColumnsOrder: []string{"a", "b"}},
	}

	// Drain available output for a short period to verify no COMPLETE message is sent yet.
	completeFound := false
	timeout := time.After(50 * time.Millisecond)
DrainLoop:
	for {
		select {
		case msg := <-outChan:
			if msg.StateName == query.COMPLETE {
				completeFound = true
			}
		case <-timeout:
			break DrainLoop
		}
	}
	assert.False(t, completeFound, "COMPLETE message should not be sent before timechart channel completes")

	// Now send COMPLETE from the timechart channel.
	timechartChan <- &query.QueryStateChanData{
		StateName:      query.COMPLETE,
		Qid:            qid,
		CompleteWSResp: &structs.PipeSearchCompleteResponse{ColumnsOrder: []string{"y", "z"}},
	}

	// Close both channels.
	close(mainChan)
	close(timechartChan)

	// Collect all remaining messages.
	var messages []*QueryStateEnvelope
	for msg := range outChan {
		messages = append(messages, msg)
	}

	// Verify that exactly one COMPLETE message is emitted, and it is the final message.
	var completeCount int
	for i, msg := range messages {
		if msg.StateName == query.COMPLETE {
			completeCount++
			if i != len(messages)-1 {
				t.Errorf("COMPLETE message should be the last message, got index %d", i)
			}
		}
	}
	assert.Equal(t, 1, completeCount, "Expected exactly one COMPLETE message")
}
