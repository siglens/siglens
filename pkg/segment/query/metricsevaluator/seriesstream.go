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
	Next() bool            // Move to next TSIDs
	Fetch() error          // Fetch/evaluate for current TSID at given evalTs
	At() ([]Sample, error) // Returns samples for the current series
	Labels() map[string]string
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

	// should I define instantSampleStream here?
	// instantSampleStream will be used to fetch the samples for the current evalTs
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

func (bs *BaseStream) Next() bool {
	if bs == nil {
		return false
	}

	return len(bs.curSeries.Values) > 0
}

func (bs *BaseStream) At() ([]Sample, error) {
	if bs == nil {
		return nil, fmt.Errorf("stream is nil")
	}

	return bs.curSeries.Values, nil
}

func (bs *BaseStream) Fetch() error {
	if bs == nil {
		return fmt.Errorf("stream is nil")
	}

	return nil
}

func (bs *BaseStream) Labels() map[string]string {
	if bs == nil {
		return nil
	}

	return bs.curSeries.Labels
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

func (vss *VectorSelectorStream) Next() bool {
	if vss == nil {
		return false
	}

	// If the TSIDs are already fetched, move to the next TSID and set the labels
	// reset the current samples
	vss.currTsidIndex++

	if !vss.gotTSIDs {
		// TODO: Implement the logic to fetch all the TSIDs for the current evalTs => call a function similar to FIndTSIDs
		// Need to implement the logic to determine the Tags Trees need to be searched for the current evalTs
		// initiate the current labels and current samples with the first TSID
		// sets the currTsidIndex to 0
		// set the gotTSIDs to true
		// set the current Labels to the first TSID
		// set the current samples to the first TSID
		// return true

		seriesId := SeriesId(vss.expr.Name) // TODO: add labels
		vss.allSeries = vss.reader.Read(seriesId)
		vss.gotTSIDs = true
		vss.currTsidIndex = 0
	}

	if vss.currTsidIndex >= len(vss.allSeries) {
		return false
	}

	vss.curSeries = vss.allSeries[vss.currTsidIndex]

	return true
}

func (vss *VectorSelectorStream) Fetch() error {
	return nil
}

func (vss *VectorSelectorStream) At() ([]Sample, error) {
	if vss == nil {
		return nil, fmt.Errorf("VectorSelectorStream: nil")
	}

	if vss.currTsidIndex < 0 || vss.currTsidIndex >= len(vss.allSeries) {
		return nil, fmt.Errorf("VectorSelectorStream: no TSIDs")
	}

	samples := vss.allSeries[vss.currTsidIndex].Values
	idx := sort.Search(len(samples), func(i int) bool {
		return vss.evalTs < samples[i].Ts
	})
	idx--

	if idx > -1 && idx < len(samples) && !isStale(samples[idx].Ts, vss.evalTs) {
		return []Sample{{Ts: vss.evalTs, Value: samples[idx].Value}}, nil
	}

	return nil, nil
}

func isStale(ts, evalTs uint32) bool {
	return ts+uint32(structs.DEFAULT_LOOKBACK_FOR_INSTANT_VECTOR.Seconds()) <= evalTs
}
