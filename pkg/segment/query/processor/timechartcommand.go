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
	"github.com/siglens/siglens/pkg/common/dtypeutils"
	"github.com/siglens/siglens/pkg/segment/aggregations"
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

type timechartOptions struct {
	timeBucket     *structs.TimeBucket
	timeChartExpr  *structs.TimechartExpr
	groupByRequest *structs.GroupByRequest
}

type timechartProcessor struct {
	options   *timechartOptions
	spanError error
}

func NewTimechartProcessor(aggs *structs.QueryAggregators, timeRange *dtypeutils.TimeRange, qid uint64) *timechartProcessor {
	if aggs.TimeHistogram == nil || aggs.TimeHistogram.Timechart == nil {
		return &timechartProcessor{options: nil}
	}

	if aggs.GroupByRequest != nil {
		aggs.GroupByRequest.BucketCount = int(utils.QUERY_MAX_BUCKETS)
	}

	processor := &timechartProcessor{}

	if aggs.TimeHistogram.Timechart.BinOptions != nil &&
		aggs.TimeHistogram.Timechart.BinOptions.SpanOptions != nil &&
		aggs.TimeHistogram.Timechart.BinOptions.SpanOptions.DefaultSettings {
		spanOptions, err := structs.GetDefaultTimechartSpanOptions(timeRange.StartEpochMs, timeRange.EndEpochMs, qid)
		if err != nil {
			processor.spanError = err
			return processor
		}
		aggs.TimeHistogram.Timechart.BinOptions.SpanOptions = spanOptions
		aggs.TimeHistogram.IntervalMillis = aggregations.GetIntervalInMillis(spanOptions.SpanLength.Num, spanOptions.SpanLength.TimeScalr)
	}

	processor.options = &timechartOptions{
		timeBucket:     aggs.TimeHistogram,
		timeChartExpr:  aggs.TimeHistogram.Timechart,
		groupByRequest: aggs.GroupByRequest,
	}

	return processor
}

func (p *timechartProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	panic("not implemented")
}

func (p *timechartProcessor) Rewind() {
	panic("not implemented")
}

func (p *timechartProcessor) Cleanup() {
	panic("not implemented")
}

func (p *timechartProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return nil, false
}
