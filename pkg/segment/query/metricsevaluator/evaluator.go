// Copyright (c) 2021-2025 SigScalr, Inc.
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

package metricsevaluator

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/siglens/siglens/pkg/segment/query/summary"
	"github.com/siglens/siglens/pkg/segment/structs"
	log "github.com/sirupsen/logrus"
)

type Evaluator struct {
	reader DiskReader

	startEpochSec uint32
	endEpochSec   uint32
	step          uint32
	lookBackDelta time.Duration

	qid          uint64
	querySummary *summary.QuerySummary

	fetchTsidsForFullRange bool
}

type SeriesResult struct {
	Labels map[string]string
	Values []Sample
}

type SeriesId string

func (s SeriesId) Matches(other SeriesId) bool {
	// TODO
	return true
}

func NewEvaluator(reader DiskReader, startEpochSec, endEpochSec, step uint32, querySummary *summary.QuerySummary, qid uint64) *Evaluator {
	return &Evaluator{
		reader:        reader,
		startEpochSec: startEpochSec,
		endEpochSec:   endEpochSec,
		step:          step,
		lookBackDelta: structs.DEFAULT_LOOKBACK_FOR_INSTANT_VECTOR,

		qid:          qid,
		querySummary: querySummary,
	}
}

func generateLabelKey(labels map[string]string) SeriesId {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteString("=")
		b.WriteString(labels[k])
		b.WriteString(",")
	}
	return SeriesId(b.String())
}

func (e *Evaluator) EvalExpr(expr parser.Expr) (map[SeriesId]*SeriesResult, error) {
	if expr == nil {
		return nil, nil
	}

	seriesMap := make(map[SeriesId]*SeriesResult)

	for evalTs := e.startEpochSec; evalTs <= e.endEpochSec; evalTs += e.step {
		stream, err := e.evalStream(evalTs, expr)
		if err != nil {
			return nil, fmt.Errorf("Evaluator.EvalExpr: %w", err)
		}

		for {
			series, err := stream.Fetch()
			if series == nil {
				// No more series to fetch.
				break
			}
			if err != nil {
				log.Errorf("Evaluator.EvalExpr: failed to fetch; err=%v", err)
				continue
			}

			labelKey := generateLabelKey(series.Labels)
			if _, ok := seriesMap[labelKey]; !ok {
				seriesMap[labelKey] = &SeriesResult{
					Labels: series.Labels,
					Values: make([]Sample, 0),
				}
			}

			seriesMap[labelKey].Values = append(seriesMap[labelKey].Values, series.Values...)
		}
	}

	return seriesMap, nil
}

// Evaluator entry point.
func (e *Evaluator) evalStream(evalTs uint32, expr parser.Expr) (SeriesStream, error) {
	switch ex := expr.(type) {
	case *parser.VectorSelector:
		return NewVectorSelectorStream(evalTs, ex, e)

	case *parser.MatrixSelector:
		return NewMatrixSelectorStream(evalTs, ex, e)

	case *parser.Call:
		return NewCallExprStream(evalTs, ex, e)

	case *parser.AggregateExpr:
		return NewAggregateExprStream(evalTs, ex, e)

	case *parser.BinaryExpr:
		return NewBinaryExprStream(evalTs, ex, e)

	case *parser.SubqueryExpr:
		return NewSubqueryExprStream(evalTs, ex, e)

	default:
		return nil, fmt.Errorf("unsupported expression type: %T", expr)
	}
}
