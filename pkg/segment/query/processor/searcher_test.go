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
	"testing"

	"github.com/siglens/siglens/pkg/segment/structs"
)

type timeRange struct {
	high, low uint64
}

func makeBlocks(timeRanges []timeRange) []*block {
	blocks := make([]*block, len(timeRanges))
	for i, timeRange := range timeRanges {
		blocks[i] = &block{
			BlockSummary: structs.BlockSummary{
				HighTs: timeRange.high,
				LowTs:  timeRange.low,
			},
		}
	}

	return blocks
}

func Test_sortBlocks(t *testing.T) {
	highAndLowTimestamps := []timeRange{
		{high: 100, low: 50},
		{high: 200, low: 200},
		{high: 300, low: 205},
		{high: 220, low: 80},
		{high: 120, low: 30},
	}

	// Sort most recent first.
	blocks := makeBlocks(highAndLowTimestamps)
	sortBlocks(blocks, recentFirst)

	expectedBlocks := makeBlocks([]timeRange{
		{high: 300, low: 205},
		{high: 220, low: 80},
		{high: 200, low: 200},
		{high: 120, low: 30},
		{high: 100, low: 50},
	})

	for i, block := range blocks {
		if block.HighTs != expectedBlocks[i].HighTs || block.LowTs != expectedBlocks[i].LowTs {
			t.Errorf("Expected %v, got %v for iter %v", expectedBlocks[i], block, i)
		}
	}

	// Sort most recent last.
	blocks = makeBlocks(highAndLowTimestamps)
	sortBlocks(blocks, recentLast)

	expectedBlocks = makeBlocks([]timeRange{
		{high: 120, low: 30},
		{high: 100, low: 50},
		{high: 220, low: 80},
		{high: 200, low: 200},
		{high: 300, low: 205},
	})

	for i, block := range blocks {
		if block.HighTs != expectedBlocks[i].HighTs || block.LowTs != expectedBlocks[i].LowTs {
			t.Errorf("Expected %v, got %v for iter %v", expectedBlocks[i], block, i)
		}
	}
}
