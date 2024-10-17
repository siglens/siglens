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
	"fmt"
	"io"
	"time"

	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
	putils "github.com/siglens/siglens/pkg/utils"
)

type gentimesProcessor struct {
	options       *structs.GenTimes
	currStartTime uint64
}

func addGenTimeEvent(values map[string][]utils.CValueEnclosure, start time.Time, end time.Time) {
	values["starttime"] = append(values["starttime"], utils.CValueEnclosure{
		Dtype: utils.SS_DT_UNSIGNED_NUM,
		CVal:  uint64(start.UnixMilli()) / 1000,
	})

	values["endttime"] = append(values["endttime"], utils.CValueEnclosure{
		Dtype: utils.SS_DT_UNSIGNED_NUM,
		CVal:  uint64(end.UnixMilli()) / 1000,
	})

	values["starthuman"] = append(values["starthuman"], utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING,
		CVal:  putils.FormatToHumanReadableTime(start),
	})

	values["endhuman"] = append(values["endhuman"], utils.CValueEnclosure{
		Dtype: utils.SS_DT_STRING,
		CVal:  putils.FormatToHumanReadableTime(end),
	})
}

func (p *gentimesProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	if p.options == nil {
		return nil, fmt.Errorf("gentimesProcessor.Process: GenTimes is nil")
	}
	if p.options.Interval == nil {
		return nil, fmt.Errorf("gentimesProcessor.Process: options.Interval is nil")
	}

	curr := p.currStartTime
	currTime := time.UnixMilli(int64(p.currStartTime))
	knownValues := make(map[string][]utils.CValueEnclosure, 0)

	count := uint64(0)
	for curr < p.options.EndTime && count < utils.QUERY_EARLY_EXIT_LIMIT {
		endTime, err := utils.ApplyOffsetToTime(int64(p.options.Interval.Num), p.options.Interval.TimeScalr, currTime)
		if err != nil {
			return nil, fmt.Errorf("gentimesProcessor.Process: Error while calculating end time, err: %v", err)
		}
		intervalEndTime, err := utils.ApplyOffsetToTime(-1, utils.TMSecond, endTime)
		if err != nil {
			return nil, fmt.Errorf("gentimesProcessor.Process: Error while calculating interval end time, err: %v", err)
		}

		addGenTimeEvent(knownValues, currTime, intervalEndTime)

		currTime = endTime
		curr = uint64(currTime.UnixMilli())
	}

	p.currStartTime = curr
	err := iqr.AppendKnownValues(knownValues)

	if err != nil {
		return nil, fmt.Errorf("gentimesProcessor.Process: Error while appending known values, err: %v", err)
	}

	if curr >= p.options.EndTime {
		err = io.EOF
	}

	return iqr, err
}

func (p *gentimesProcessor) Rewind() {
	// If more than one pass is there we need to generate the records again as we are not storing it
	p.currStartTime = p.options.StartTime
}

func (p *gentimesProcessor) Cleanup() {
	// As there is no state to be stored, nothing to cleanup
}
