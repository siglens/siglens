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
)

type Evaluator struct {
	startEpochSec uint32
	endEpochSec   uint32
	step          uint32
	lookBackDelta time.Duration

	qid          uint64
	querySummary *summary.QuerySummary

	mSearchReqs []*structs.MetricsSearchRequest

	fetchTsidsForFullRange bool
}

type SeriesResult struct {
	Labels map[string]string
	Values []Sample // Sample {Ts uint32, Value float64}
}

type SeriesId string

func NewEvaluator(startEpochSec, endEpochSec, step uint32, lookBackDelta time.Duration, querySummary *summary.QuerySummary, qid uint64,
	mSearchReqs []*structs.MetricsSearchRequest) *Evaluator {
	return &Evaluator{
		startEpochSec: startEpochSec,
		endEpochSec:   endEpochSec,
		step:          step,
		lookBackDelta: lookBackDelta,

		qid:          qid,
		querySummary: querySummary,

		mSearchReqs: mSearchReqs,
	}
}

func generateLabelKey(labels map[string]string) SeriesId {
	// e.g., job=api,instance=localhost:9090
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

	evalTs := e.startEpochSec
	for evalTs <= e.endEpochSec {
		stream, err := e.evalStream(evalTs, expr)
		if err != nil {
			return nil, fmt.Errorf("Evaluator.EvalExpr: %w", err)
		}

		for stream.Next() {
			err := stream.Fetch()
			if err != nil {
				// TODO: Handle error logging
				continue
			}

			samples, err := stream.At()
			if err != nil {
				continue
			}

			labels := stream.Labels()
			labelKey := generateLabelKey(labels)
			if _, ok := seriesMap[labelKey]; !ok {
				seriesMap[labelKey] = &SeriesResult{
					Labels: labels,
					Values: make([]Sample, 0),
				}
			}

			seriesMap[labelKey].Values = append(seriesMap[labelKey].Values, samples...)
		}
	}

	return seriesMap, nil
}

// func (e *Evaluator) evalAtTs(evalTs uint32, expr parser.Expr) (interface{}, error) {
// 	switch e := expr.(type) {
// 	case *parser.MatrixSelector:
// 		// TODO: Implement Range Vector
// 		return nil, nil
// 	case *parser.SubqueryExpr:
// 		// TODO: Implement Subquery
// 		return nil, nil
// 	default:
// 		return nil, nil
// 	}
// }

// func (e *Evaluator) evalInstantVector(evalTs uint32, expr parser.Expr) (interface{}, error) {
// 	return nil, nil
// }

// func (e *Evaluator) evalRangeVector(evalTs uint32, expr parser.Expr) (interface{}, error) {
// 	matrix, ok := expr.(*parser.MatrixSelector)
// 	if !ok {
// 		return nil, fmt.Errorf("expected MatrixSelector, got %T", expr)
// 	}

// }

// func (e *Evaluator) evalSubQueryExp(evalTs uint32, expr parser.Expr) (interface{}, error) {
// 	subquery, ok := expr.(*parser.SubqueryExpr)
// 	if !ok {
// 		return nil, fmt.Errorf("expected SubqueryExpr, got %T", expr)
// 	}

// 	rangeStart := evalTs - uint32(subquery.Range.Seconds())

// 	newEvaluator := NewEvaluator(
// 		rangeStart,
// 		evalTs,
// 		uint32(subquery.Step.Seconds()),
// 		e.lookBackDelta,
// 		e.querySummary,
// 		e.qid,
// 		e.mSearchReqs,
// 	)

// 	// Once the results are fetched, we need to evaluate the subquery expression
// 	_, err := newEvaluator.EvalExpr(subquery.Expr)

// 	return nil, err
// }

// func (e *Evaluator) fetchSamplesInRange(vs *parser.VectorSelector, startTs, endTs uint32) (interface{}, error) {
// 	return nil, nil
// }

// func (e *Evaluator) fetchInstantSample(vs *parser.VectorSelector, evalTs uint32) (interface{}, error) {
// 	return nil, nil
// }

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
