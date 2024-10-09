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
	"testing"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/utils"
	"github.com/stretchr/testify/assert"
)

func Test_Getters(t *testing.T) {
	dp := &DataProcessor{
		inputOrderMatters: true,
		isPermutingCmd:    true,
		isBottleneckCmd:   true,
		isTwoPassCmd:      true,
	}

	assert.True(t, dp.DoesInputOrderMatter())
	assert.True(t, dp.IsPermutingCmd())
	assert.True(t, dp.IsBottleneckCmd())
	assert.True(t, dp.IsTwoPassCmd())

	dp = &DataProcessor{
		inputOrderMatters: false,
		isPermutingCmd:    false,
		isBottleneckCmd:   false,
		isTwoPassCmd:      false,
	}

	assert.False(t, dp.DoesInputOrderMatter())
	assert.False(t, dp.IsPermutingCmd())
	assert.False(t, dp.IsBottleneckCmd())
	assert.False(t, dp.IsTwoPassCmd())
}

type mockStreamer struct {
	allRecords map[string][]utils.CValueEnclosure
	numSent    int
	qid        uint64
}

func (ms *mockStreamer) Fetch() (*iqr.IQR, error) {
	if ms.numSent >= len(ms.allRecords["col1"]) {
		return nil, io.EOF
	}

	// Send one at a time.
	knownValues := map[string][]utils.CValueEnclosure{
		"col1": {ms.allRecords["col1"][ms.numSent]},
	}

	iqr := iqr.NewIQR(ms.qid)
	err := iqr.AppendKnownValues(knownValues)
	if err != nil {
		return nil, err
	}

	ms.numSent++
	return iqr, nil
}

func (ms *mockStreamer) Rewind() {
	ms.numSent = 0
}

type passThroughProcessor struct{}

func (ptp *passThroughProcessor) Process(input *iqr.IQR) (*iqr.IQR, error) {
	if input == nil {
		return nil, io.EOF
	}

	return input, nil
}

func (ptp *passThroughProcessor) Rewind() {}

func Test_Fetch_nonBottleneck(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: 0,
	}

	dp := &DataProcessor{
		streams:         []*cachedStream{{stream, nil, false}},
		processor:       &passThroughProcessor{},
		isBottleneckCmd: false,
	}

	for i := 0; i < 3; i++ {
		output, err := dp.Fetch()
		assert.NoError(t, err, "iteration %d", i)
		assert.NotNil(t, output, "iteration %d", i)
		assert.Equal(t, 1, output.NumberOfRecords(), "iteration %d", i)
	}

	_, err := dp.Fetch()
	assert.Equal(t, io.EOF, err)
}

type mockBottleneckProcessor struct {
	numSeen     int
	lastSeenIQR *iqr.IQR
}

func (mbp *mockBottleneckProcessor) Process(input *iqr.IQR) (*iqr.IQR, error) {
	defer func() { mbp.lastSeenIQR = input }()

	if input == nil {
		return mbp.lastSeenIQR, io.EOF
	}

	mbp.numSeen += input.NumberOfRecords()
	return input, nil
}

func (mbp *mockBottleneckProcessor) Rewind() {}

func Test_Fetch_bottleneck(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: 0,
	}

	dp := &DataProcessor{
		streams:         []*cachedStream{{stream, nil, false}},
		processor:       &mockBottleneckProcessor{},
		isBottleneckCmd: true,
		isTwoPassCmd:    false,
	}

	output, err := dp.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, output)
	assert.Equal(t, 3, dp.processor.(*mockBottleneckProcessor).numSeen)
}

func Test_Fetch_twoPass(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
		qid: 0,
	}

	dp := &DataProcessor{
		streams:           []*cachedStream{{stream, nil, false}},
		processor:         &mockBottleneckProcessor{},
		isBottleneckCmd:   true,
		isTwoPassCmd:      true,
		finishedFirstPass: false,
	}

	for i := 0; i < 3; i++ {
		output, err := dp.Fetch()
		assert.NoError(t, err, "iteration %d", i)
		assert.NotNil(t, output, "iteration %d", i)
		assert.Equal(t, 1, output.NumberOfRecords(), "iteration %d", i)
		assert.Equal(t, 3+i+1, dp.processor.(*mockBottleneckProcessor).numSeen, "iteration %d", i)
	}

	_, err := dp.Fetch()
	assert.Equal(t, io.EOF, err)
}

func Test_Fetch_multipleStreams(t *testing.T) {
	stream1 := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "f"},
			},
		},
		qid: 0,
	}

	stream2 := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "d"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "e"},
			},
		},
		qid: 0,
	}

	less := func(a, b *iqr.Record) bool {
		aVal, err := a.ReadColumn("col1")
		assert.NoError(t, err)

		bVal, err := b.ReadColumn("col1")
		assert.NoError(t, err)

		return aVal.CVal.(string) < bVal.CVal.(string)
	}

	dp := &DataProcessor{
		streams:         []*cachedStream{{stream1, nil, false}, {stream2, nil, false}},
		less:            less,
		processor:       &passThroughProcessor{},
		isBottleneckCmd: false,
	}

	for i := 0; i < 6; i++ {
		output, err := dp.Fetch()
		assert.NoError(t, err, "iteration %d", i)
		assert.NotNil(t, output, "iteration %d", i)
		assert.Equal(t, 1, output.NumberOfRecords(), "iteration %d", i)

		value, err := output.ReadColumn("col1")
		assert.NoError(t, err, "iteration %d", i)
		assert.Equal(t, 1, len(value), "iteration %d", i)
		expected := string('a' + rune(i))
		assert.Equal(t, expected, value[0].CVal.(string), "iteration %d", i)
	}

	_, err := dp.Fetch()
	assert.Equal(t, io.EOF, err)
}
