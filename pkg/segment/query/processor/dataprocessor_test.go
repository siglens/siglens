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
	"sync"
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
		processorLock:     &sync.Mutex{},
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
		processorLock:     &sync.Mutex{},
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

func (ms mockStreamer) String() string {
	return "<mock streamer>"
}

func (ms *mockStreamer) Fetch() (*iqr.IQR, error) {
	colName := "col1"
	for col := range ms.allRecords {
		colName = col
		break
	}

	if ms.numSent >= len(ms.allRecords[colName]) {
		return nil, io.EOF
	}

	// Send one at a time.
	knownValues := map[string][]utils.CValueEnclosure{}

	for col, values := range ms.allRecords {
		knownValues[col] = []utils.CValueEnclosure{values[ms.numSent]}
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

func (ms *mockStreamer) Cleanup() {}

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
		streams:         []*CachedStream{{stream, nil, false}},
		processor:       &passThroughProcessor{},
		isBottleneckCmd: false,
		processorLock:   &sync.Mutex{},
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
	numSeen          int
	lastSeenIQR      *iqr.IQR
	name             string // For debugging.
	finalResultExits bool
}

func (mbp *mockBottleneckProcessor) Process(input *iqr.IQR) (*iqr.IQR, error) {
	defer func() {
		if input != nil {
			mbp.lastSeenIQR = input
		}
	}()

	if input == nil {
		mbp.finalResultExits = true
		return mbp.lastSeenIQR, io.EOF
	}

	mbp.numSeen += input.NumberOfRecords()
	return nil, nil
}

func (mbp *mockBottleneckProcessor) Rewind()  {}
func (mbp *mockBottleneckProcessor) Cleanup() {}
func (mbp *mockBottleneckProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	if mbp.finalResultExits {
		return mbp.lastSeenIQR, true
	}
	return mbp.lastSeenIQR, false
}

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
		streams:         []*CachedStream{{stream, nil, false}},
		processor:       &mockBottleneckProcessor{},
		isBottleneckCmd: true,
		isTwoPassCmd:    false,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, output)
	assert.Equal(t, 3, dp.processor.(*mockBottleneckProcessor).numSeen)
}

type mockTwoPassProcessor struct {
	numSeen     int
	lastSeenIQR *iqr.IQR
	name        string // For debugging.
	secondPass  bool
}

func (mtp *mockTwoPassProcessor) Process(input *iqr.IQR) (*iqr.IQR, error) {
	defer func() { mtp.lastSeenIQR = input }()

	if input == nil {
		var output *iqr.IQR
		if mtp.secondPass {
			output = mtp.lastSeenIQR
		}
		return output, io.EOF
	}

	mtp.numSeen += input.NumberOfRecords()
	return input, nil
}

func (mtp *mockTwoPassProcessor) Rewind() {
	mtp.secondPass = true
}
func (mtp *mockTwoPassProcessor) Cleanup() {}
func (mtp *mockTwoPassProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
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
		streams:           []*CachedStream{{stream, nil, false}},
		processor:         &mockTwoPassProcessor{},
		isBottleneckCmd:   true,
		isTwoPassCmd:      true,
		finishedFirstPass: false,
		processorLock:     &sync.Mutex{},
	}

	for i := 0; i < 3; i++ {
		output, err := dp.Fetch()
		assert.NoError(t, err, "iteration %d", i)
		assert.NotNil(t, output, "iteration %d", i)
		assert.Equal(t, 1, output.NumberOfRecords(), "iteration %d", i)
		assert.Equal(t, 3+i+1, dp.processor.(*mockTwoPassProcessor).numSeen, "iteration %d", i)
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
		streams: []*CachedStream{{stream1, nil, false}, {stream2, nil, false}},
		mergeSettings: mergeSettings{
			less: less,
		},
		processor:       &passThroughProcessor{},
		isBottleneckCmd: false,
		processorLock:   &sync.Mutex{},
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

func Test_Fetch_multipleBottleneck(t *testing.T) {
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

	dp0 := &DataProcessor{
		streams:       []*CachedStream{{stream, nil, false}},
		processor:     &passThroughProcessor{},
		processorLock: &sync.Mutex{},
	}

	dp1 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp0)},
		processor:       &mockBottleneckProcessor{name: "dp1"},
		isBottleneckCmd: true,
		isTwoPassCmd:    false,
		processorLock:   &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockBottleneckProcessor{name: "dp2"},
		isBottleneckCmd: true,
		isTwoPassCmd:    false,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp2.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, output)
	assert.Equal(t, 1, dp2.processor.(*mockBottleneckProcessor).numSeen)
}

func Test_Fetch_multipleBottleneck_inputNil(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{},
		qid:        0,
	}

	dp1 := &DataProcessor{
		streams:       []*CachedStream{{stream, nil, false}},
		processor:     &passThroughProcessor{},
		processorLock: &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockBottleneckProcessor{name: "dp2"},
		isBottleneckCmd: true,
		isTwoPassCmd:    false,
		processorLock:   &sync.Mutex{},
	}

	dp3 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp2)},
		processor:       &mockBottleneckProcessor{name: "dp3"},
		isBottleneckCmd: true,
		isTwoPassCmd:    false,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp3.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, output)
	assert.Equal(t, 0, dp3.processor.(*mockBottleneckProcessor).numSeen)
}

func Test_Fetch_multipleBottleneck_twoPass(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{
			"col1": {
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "a"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "b"},
				utils.CValueEnclosure{Dtype: utils.SS_DT_STRING, CVal: "c"},
			},
		},
	}

	dp0 := &DataProcessor{
		streams:       []*CachedStream{{stream, nil, false}},
		processor:     &passThroughProcessor{},
		processorLock: &sync.Mutex{},
	}

	dp1 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp0)},
		processor:       &mockBottleneckProcessor{name: "dp1"},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockBottleneckProcessor{name: "dp2"},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp3 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp2)},
		processor:       &mockTwoPassProcessor{name: "dp3"},
		isTwoPassCmd:    true,
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	var output *iqr.IQR
	var err error
	for i := 0; i < 2; i++ {
		output, err = dp3.Fetch()
	}
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, output)
	// Two records because, one record in the first pass and one record in the second pass.
	assert.Equal(t, 2, dp3.processor.(*mockTwoPassProcessor).numSeen)
}

func Test_Fetch_multipleBottleneck_twoPass_inputNil(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{},
	}

	dp1 := &DataProcessor{
		streams:         []*CachedStream{{stream, nil, false}},
		processor:       &passThroughProcessor{},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockBottleneckProcessor{name: "dp2"},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp3 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp2)},
		processor:       &mockTwoPassProcessor{name: "dp3"},
		isTwoPassCmd:    true,
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp3.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, output)
	assert.Equal(t, 0, dp3.processor.(*mockTwoPassProcessor).numSeen)
}

func Test_Fetch_twoPass_Multiplebottleneck(t *testing.T) {
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

	dp0 := &DataProcessor{
		streams:       []*CachedStream{{stream, nil, false}},
		processor:     &passThroughProcessor{},
		processorLock: &sync.Mutex{},
	}

	dp1 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp0)},
		processor:       &mockTwoPassProcessor{name: "dp1"},
		isTwoPassCmd:    true,
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockBottleneckProcessor{name: "dp2"},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp3 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp2)},
		processor:       &mockBottleneckProcessor{name: "dp3"},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp3.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.NotNil(t, output)
	assert.Equal(t, 1, dp3.processor.(*mockBottleneckProcessor).numSeen)
}

func Test_Fetch_twoPass_Multiplebottleneck_inputNil(t *testing.T) {
	stream := &mockStreamer{
		allRecords: map[string][]utils.CValueEnclosure{},
		qid:        0,
	}

	dp1 := &DataProcessor{
		streams:       []*CachedStream{{stream, nil, false}},
		processor:     &passThroughProcessor{},
		processorLock: &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockTwoPassProcessor{name: "dp2"},
		isTwoPassCmd:    true,
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp3 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp2)},
		processor:       &mockBottleneckProcessor{name: "dp3"},
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp3.Fetch()
	assert.Equal(t, io.EOF, err)
	assert.Nil(t, output)
	assert.Equal(t, 0, dp3.processor.(*mockBottleneckProcessor).numSeen)
}

func Test_Fetch_Multiple_TwoPass(t *testing.T) {
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

	dp0 := &DataProcessor{
		streams:       []*CachedStream{{stream1, nil, false}},
		processor:     &passThroughProcessor{},
		processorLock: &sync.Mutex{},
	}

	dp1 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp0)},
		processor:       &mockTwoPassProcessor{name: "dp1"},
		isTwoPassCmd:    true,
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	dp2 := &DataProcessor{
		streams:         []*CachedStream{NewCachedStream(dp1)},
		processor:       &mockTwoPassProcessor{name: "dp2"},
		isTwoPassCmd:    true,
		isBottleneckCmd: true,
		processorLock:   &sync.Mutex{},
	}

	output, err := dp2.Fetch()
	assert.Equal(t, nil, err)
	assert.NotNil(t, output)
	// 5 because, 3 records in the first pass and 2 records in the second pass.
	assert.Equal(t, 5, dp2.processor.(*mockTwoPassProcessor).numSeen)
}
