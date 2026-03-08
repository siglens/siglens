// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
)

type topProcessor struct {
	options                *structs.StatisticExpr
	statisticExprProcessor *statisticExprProcessor
}

func NewTopProcessor(options *structs.QueryAggregators) *topProcessor {
	return &topProcessor{
		options:                options.StatisticExpr,
		statisticExprProcessor: NewStatisticExprProcessor(options),
	}
}

func (p *topProcessor) SetAsIqrStatsResults() {
	p.statisticExprProcessor.SetAsIqrStatsResults()
}

func (p *topProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	return p.statisticExprProcessor.Process(iqr)
}

func (p *topProcessor) Rewind() {
	p.statisticExprProcessor.Rewind()
}

func (p *topProcessor) Cleanup() {
	p.statisticExprProcessor.Cleanup()
}

func (p *topProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return p.statisticExprProcessor.GetFinalResultIfExists()
}
