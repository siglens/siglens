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

	"github.com/prometheus/prometheus/promql/parser"
	"github.com/siglens/siglens/pkg/segment/structs"
)

// Sample represents a single sample in a time series.
type Sample struct {
	Ts    uint32
	Value float64
}

// SeriesStream is the streaming interface for per-series evaluation.
type SeriesStream interface {
	Fetch() (*SeriesResult, error)
	Close()
}

type BaseStream struct {
	curSeries SeriesResult

	evalTs    uint32
	expr      parser.Expr
	evaluator *Evaluator
}

type VectorSelectorStream struct {
	*BaseStream
	expr *parser.VectorSelector

	reader    DiskReader
	allSeries []SeriesResult // TODO: only store one series at a time.

	gotTSIDs        bool // indicates if we have already fetched the TSIDs for the current evalTs
	tsids           []uint64
	tsidToLabelsMap map[uint64]map[string]string
	currTsidIndex   int
}

type MatrixSelectorStream struct {
	*BaseStream
	expr *parser.MatrixSelector
}

type BinaryExprStream struct {
	*BaseStream
	expr      *parser.BinaryExpr
	lhsStream SeriesStream
	rhsStream SeriesStream
}

type CallExprStream struct {
	*BaseStream
	expr        *parser.Call
	inputStream SeriesStream

	gotAllInputs    bool
	sortedAllInputs bool
	allInputs       []*SeriesResult
	curIndex        int
}

type AggregateExprStream struct {
	*BaseStream
	expr        *parser.AggregateExpr
	inputStream SeriesStream
}

type SubqueryExprStream struct {
	*BaseStream
	expr        *parser.SubqueryExpr
	inputStream SeriesStream
}

type EvalExprStream struct {
	*BaseStream
	inputStream      SeriesStream
	currentTsidIndex int
}

func (bs *BaseStream) Fetch() (*SeriesResult, error) {
	if bs == nil {
		return nil, fmt.Errorf("stream is nil")
	}

	if len(bs.curSeries.Values) == 0 {
		return nil, nil
	}

	return &bs.curSeries, nil
}

func (bs *BaseStream) Close() {}

func newBaseStream(evalTs uint32, expr parser.Expr, evaluator *Evaluator) *BaseStream {
	return &BaseStream{
		evalTs:    evalTs,
		expr:      expr,
		evaluator: evaluator,
	}
}

func NewVectorSelectorStream(evalTs uint32, expr *parser.VectorSelector, evaluator *Evaluator) (SeriesStream, error) {
	return &VectorSelectorStream{
		BaseStream:      newBaseStream(evalTs, expr, evaluator),
		expr:            expr,
		reader:          evaluator.reader,
		tsids:           make([]uint64, 0),
		tsidToLabelsMap: make(map[uint64]map[string]string),
	}, nil
}

func NewMatrixSelectorStream(evalTs uint32, expr *parser.MatrixSelector, evaluator *Evaluator) (SeriesStream, error) {
	return &MatrixSelectorStream{
		BaseStream: newBaseStream(evalTs, expr, evaluator),
		expr:       expr,
	}, nil
}

func NewBinaryExprStream(evalTs uint32, expr *parser.BinaryExpr, evaluator *Evaluator) (SeriesStream, error) {
	baseStream := newBaseStream(evalTs, expr, evaluator)
	lhsStream, err := evaluator.evalStream(evalTs, expr.LHS)
	if err != nil {
		return nil, fmt.Errorf("NewBinaryExprStream: %w", err)
	}

	rhsStream, err := evaluator.evalStream(evalTs, expr.RHS)
	if err != nil {
		return nil, fmt.Errorf("NewBinaryExprStream: %w", err)
	}

	return &BinaryExprStream{
		BaseStream: baseStream,
		expr:       expr,
		lhsStream:  lhsStream,
		rhsStream:  rhsStream,
	}, nil
}

func NewCallExprStream(evalTs uint32, expr *parser.Call, evaluator *Evaluator) (SeriesStream, error) {
	baseStream := newBaseStream(evalTs, expr, evaluator)

	if len(expr.Args) == 0 {
		return nil, fmt.Errorf("NewCallExprStream: no arguments")
	}

	inputStream, err := evaluator.evalStream(evalTs, expr.Args[0])
	if err != nil {
		return nil, fmt.Errorf("NewCallExprStream: %w", err)
	}

	return &CallExprStream{
		BaseStream:  baseStream,
		expr:        expr,
		inputStream: inputStream,
	}, nil
}

func NewAggregateExprStream(evalTs uint32, expr *parser.AggregateExpr, evaluator *Evaluator) (SeriesStream, error) {
	inputStream, err := evaluator.evalStream(evalTs, expr.Expr)
	if err != nil {
		return nil, fmt.Errorf("NewAggregateExprStream: %w", err)
	}

	return &AggregateExprStream{
		BaseStream:  newBaseStream(evalTs, expr, evaluator),
		expr:        expr,
		inputStream: inputStream,
	}, nil
}

func NewEvalExprStream(evalTs uint32, expr parser.Expr, evaluator *Evaluator) (SeriesStream, error) {
	evaluator.fetchTsidsForFullRange = true // Fetches the TSIDs for the entire sub query eval range
	inputStream, err := evaluator.evalStream(evalTs, expr)
	if err != nil {
		return nil, fmt.Errorf("NewEvalExprStream: %w", err)
	}

	return &EvalExprStream{
		BaseStream:       newBaseStream(evalTs, expr, evaluator),
		currentTsidIndex: -1,
		inputStream:      inputStream,
	}, nil
}

func NewSubqueryExprStream(evalTs uint32, expr *parser.SubqueryExpr, evaluator *Evaluator) (SeriesStream, error) {
	rangeStart := evalTs - uint32(expr.Range.Seconds())

	newEvaluator := NewEvaluator(
		evaluator.reader,
		rangeStart,
		evalTs,
		uint32(expr.Step.Seconds()),
		evaluator.querySummary,
		evaluator.qid,
	)

	inputStream, err := NewEvalExprStream(evalTs, expr.Expr, newEvaluator)
	if err != nil {
		return nil, fmt.Errorf("NewSubqueryExprStream: %w", err)
	}

	return &SubqueryExprStream{
		BaseStream:  newBaseStream(evalTs, expr, evaluator),
		expr:        expr,
		inputStream: inputStream,
	}, nil
}

func (vss *VectorSelectorStream) Fetch() (*SeriesResult, error) {
	if vss == nil {
		return nil, fmt.Errorf("VectorSelectorStream: nil")
	}

	if !vss.gotTSIDs {
		vss.allSeries = vss.reader.Read(vss.expr.LabelMatchers, vss.evalTs, uint32(structs.PROMQL_LOOKBACK.Seconds()))
		vss.gotTSIDs = true
		vss.currTsidIndex = 0
	} else {
		vss.currTsidIndex++
	}

	if vss.currTsidIndex >= len(vss.allSeries) {
		return nil, nil
	}

	vss.curSeries = vss.allSeries[vss.currTsidIndex]
	samples := vss.curSeries.Values
	labels := vss.curSeries.Labels

	idx := sort.Search(len(samples), func(i int) bool {
		return vss.evalTs < samples[i].Ts
	})
	idx--

	if idx > -1 && idx < len(samples) && !isStale(samples[idx].Ts, vss.evalTs) {
		return &SeriesResult{
			Labels: labels,
			Values: []Sample{{Ts: vss.evalTs, Value: samples[idx].Value}},
		}, nil
	}

	// This series has no data, but there may still be others with data. So
	// return an empty series instead of a nil one.
	return &SeriesResult{}, nil
}

func isStale(ts, evalTs uint32) bool {
	return ts+uint32(structs.PROMQL_LOOKBACK.Seconds()) <= evalTs
}

func (ces *CallExprStream) Fetch() (*SeriesResult, error) {
	if ces == nil {
		return nil, fmt.Errorf("CallExprStream: nil")
	}

	if ces.inputStream == nil {
		return nil, fmt.Errorf("CallExprStream: input stream is nil")
	}

	switch ces.expr.Func.Name {
	case "sort":
		err := ces.sortAllInputs()
		if err != nil {
			return nil, fmt.Errorf("CallExprStream: cannot read all inputs: %v", err)
		}

		if ces.curIndex >= len(ces.allInputs) {
			return nil, nil
		}

		defer func() {
			ces.curIndex++
		}()
		return ces.allInputs[ces.curIndex], nil
	default:
		return nil, fmt.Errorf("unsupported function: %s", ces.expr.Func.Name)
	}
}

func (ces *CallExprStream) sortAllInputs() error {
	if ces.sortedAllInputs {
		return nil
	}

	if !ces.gotAllInputs {
		err := ces.readAllInputs()
		if err != nil {
			return err
		}
	}

	// Sort: https://prometheus.io/docs/prometheus/latest/querying/functions/#sort
	// This should only be called on instant vectors, so each series should
	// have only one value.
	sort.Slice(ces.allInputs, func(i, j int) bool {
		if len(ces.allInputs[i].Values) == 0 {
			return false
		} else if len(ces.allInputs[j].Values) == 0 {
			return true
		}

		return ces.allInputs[i].Values[0].Value < ces.allInputs[j].Values[0].Value
	})

	ces.sortedAllInputs = true
	ces.curIndex = 0

	return nil
}

func (ces *CallExprStream) readAllInputs() error {
	if ces.gotAllInputs {
		return nil
	}

	allInputs := make([]*SeriesResult, 0)
	for {
		input, err := ces.inputStream.Fetch()
		if err != nil {
			return err
		}
		if input == nil {
			break
		}
		allInputs = append(allInputs, input)
	}

	ces.allInputs = allInputs
	ces.gotAllInputs = true

	return nil
}
