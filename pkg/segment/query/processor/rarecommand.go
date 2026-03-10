// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

package processor

import (
	"github.com/siglens/siglens/pkg/segment/query/iqr"
	"github.com/siglens/siglens/pkg/segment/structs"
)

type rareProcessor struct {
	options                *structs.StatisticExpr
	statisticExprProcessor *statisticExprProcessor
}

func NewRareProcessor(options *structs.QueryAggregators) *rareProcessor {
	return &rareProcessor{
		options:                options.StatisticExpr,
		statisticExprProcessor: NewStatisticExprProcessor(options),
	}
}

func (p *rareProcessor) SetAsIqrStatsResults() {
	p.statisticExprProcessor.SetAsIqrStatsResults()
}

func (p *rareProcessor) Process(iqr *iqr.IQR) (*iqr.IQR, error) {
	return p.statisticExprProcessor.Process(iqr)
}

func (p *rareProcessor) Rewind() {
	p.statisticExprProcessor.Rewind()
}

func (p *rareProcessor) Cleanup() {
	p.statisticExprProcessor.Cleanup()
}

func (p *rareProcessor) GetFinalResultIfExists() (*iqr.IQR, bool) {
	return p.statisticExprProcessor.GetFinalResultIfExists()
}
