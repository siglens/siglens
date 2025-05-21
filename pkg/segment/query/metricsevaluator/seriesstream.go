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
	"time"

	"github.com/prometheus/prometheus/promql/parser"
)

const DEFAULT_SUB_QUERY_EXPR_CHUNK_SIZE time.Duration = 4 * time.Hour

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
	currentSamples []Sample
	currentLabels  map[string]string

	evalTs    uint32
	expr      parser.Expr
	evaluator *Evaluator
}

type VectorSelectorStream struct {
	*BaseStream
	expr *parser.VectorSelector

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

	bothStreamsExhausted bool
	seriesResult         []*SeriesResult
	currSeriesIndex      int
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
	expr              *parser.SubqueryExpr
	step              uint32 // Subquery step in seconds (e.g., 300 for 5m)
	chunkSize         uint32 // Base chunk duration in seconds (e.g., 7200 for 2h)
	rangeStart        uint32 // Start of the entire subquery window
	rangeEnd          uint32 // End of the subquery window
	currentChunkStart uint32 // Start of current chunk
	currentChunkEnd   uint32 // End of current chunk (aligned to step)
	seriesMap         map[string]*SeriesResult
}

func (bs *BaseStream) Next() bool {
	if bs == nil {
		return false
	}

	return len(bs.currentSamples) > 0
}

func (bs *BaseStream) At() ([]Sample, error) {
	if bs == nil {
		return nil, fmt.Errorf("stream is nil")
	}

	return bs.currentSamples, nil
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

	return bs.currentLabels
}

func (bs *BaseStream) Close() {}

func newBaseStream(evalTs uint32, expr parser.Expr, evaluator *Evaluator) *BaseStream {
	return &BaseStream{
		currentSamples: []Sample{},
		currentLabels:  make(map[string]string),
		evalTs:         evalTs,
		expr:           expr,
		evaluator:      evaluator,
	}
}

func NewVectorSelectorStream(evalTs uint32, expr *parser.VectorSelector, evaluator *Evaluator) (SeriesStream, error) {
	return &VectorSelectorStream{
		BaseStream: newBaseStream(evalTs, expr, evaluator),
		expr:       expr,
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

func NewSubqueryExprStream(evalTs uint32, expr *parser.SubqueryExpr, evaluator *Evaluator) (SeriesStream, error) {
	rangeStart := evalTs - uint32(expr.Range.Seconds())
	step := uint32(expr.Step.Seconds())
	baseChunkSize := uint32(DEFAULT_SUB_QUERY_EXPR_CHUNK_SIZE.Seconds())

	// Align chunk size to be a multiple of step
	alignedChunkSize := (baseChunkSize / step) * step
	chunkEnd := rangeStart + alignedChunkSize
	if chunkEnd > evalTs {
		chunkEnd = evalTs
	}

	return &SubqueryExprStream{
		BaseStream:        newBaseStream(evalTs, expr, evaluator),
		expr:              expr,
		step:              step,
		chunkSize:         alignedChunkSize,
		rangeStart:        rangeStart,
		rangeEnd:          evalTs,
		currentChunkStart: rangeStart,
		currentChunkEnd:   chunkEnd,
		seriesMap:         make(map[string]*SeriesResult),
	}, nil
}

func (vss *VectorSelectorStream) Next() bool {
	if vss == nil {
		return false
	}

	if !vss.gotTSIDs {
		// TODO: Implement the logic to fetch all the TSIDs for the current evalTs => call a function similar to FIndTSIDs
		// Need to implement the logic to determine the Tags Trees need to be searched for the current evalTs
		// initiate the current labels and current samples with the first TSID
		// sets the currTsidIndex to 0
		// set the gotTSIDs to true
		// set the current Labels to the first TSID
		// set the current samples to the first TSID
		// return true

		vss.gotTSIDs = true
		vss.currTsidIndex = 0
		vss.currentLabels = vss.tsidToLabelsMap[vss.tsids[vss.currTsidIndex]]
		vss.currentSamples = vss.currentSamples[:0]

		return true
	}

	// If the TSIDs are already fetched, move to the next TSID and set the labels
	// reset the current samples
	vss.currTsidIndex++

	vss.currentSamples = vss.currentSamples[:0]
	if vss.currTsidIndex >= len(vss.tsids) {
		return false
	}
	vss.currentLabels = vss.tsidToLabelsMap[vss.tsids[vss.currTsidIndex]]

	return true
}

func (vss *VectorSelectorStream) Fetch() error {
	// Fetch the instant samples for the current evalTs
	if vss == nil {
		return fmt.Errorf("VectorSelectorStream: nil")
	}

	if !vss.gotTSIDs || vss.currTsidIndex >= len(vss.tsids) {
		return fmt.Errorf("VectorSelectorStream: no TSIDs")
	}

	// TODO: Determine the segments to be searched for the current evalTs
	// Call function fetchInstantSample(evalTs, tsid)
	// set the current samples to the fetched samples

	return nil
}

func (mss *MatrixSelectorStream) Next() bool {
	// This function is similar to the VectorSelectorStream Next function
	// But this will be used to fetch the range vector
	// may be we can refactor this to a common function

	// for now, we will just return false
	return false
}

func (mss *MatrixSelectorStream) Fetch() error {
	// This function is similar to the VectorSelectorStream Fetch function
	// But this will be used to fetch the range vector
	// may be we can refactor this to a common function

	// for now, we will just return nil
	return nil
}

func (bes *BinaryExprStream) Next() bool {
	if bes == nil {
		return false
	}

	if bes.bothStreamsExhausted {
		return bes.currSeriesIndex < len(bes.seriesResult)
	}

	// If both streams are exhausted, return if there are any series results
	if !bes.lhsStream.Next() && !bes.rhsStream.Next() {
		bes.bothStreamsExhausted = true
		return len(bes.seriesResult) > 0
	}

	return true
}

func (bes *BinaryExprStream) Fetch() error {
	// Binary Expr is not suited for streaming
	// We need to fetch all the series at the evalTs for both streams
	// apply the binary operation
	// and set the result to the series result

	if bes == nil {
		return fmt.Errorf("BinaryExprStream: nil")
	}

	if bes.bothStreamsExhausted {
		bes.currentLabels = bes.seriesResult[bes.currSeriesIndex].Labels
		bes.currentSamples = bes.seriesResult[bes.currSeriesIndex].Values
		bes.currSeriesIndex++
		return nil
	}

	lhsSeries := make([]*SeriesResult, 0)
	rhsSeries := make([]*SeriesResult, 0)

	for bes.lhsStream.Next() {
		err := bes.lhsStream.Fetch()
		if err != nil {
			// TODO: Handle error logging
			continue
		}
		lhsSamples, err := bes.lhsStream.At()
		if err != nil {
			continue
		}
		lhsLabels := bes.lhsStream.Labels()
		lhsSeries = append(lhsSeries, &SeriesResult{
			Labels: lhsLabels,
			Values: lhsSamples,
		})
	}

	for bes.rhsStream.Next() {
		err := bes.rhsStream.Fetch()
		if err != nil {
			// TODO: Handle error logging
			continue
		}

		rhsSamples, err := bes.rhsStream.At()
		if err != nil {
			continue
		}
		rhsLabels := bes.rhsStream.Labels()
		rhsSeries = append(rhsSeries, &SeriesResult{
			Labels: rhsLabels,
			Values: rhsSamples,
		})
	}

	// TODO: Apply the binary operation on the lhs and rhs series
	// set the result to the series result
	// set the current labels and current samples to the first series result
	// set the currSeriesIndex to 0
	// set the bothStreamsExhausted to true, since we have already fetched all the series

	return nil
}

func (sqes *SubqueryExprStream) Next() bool {
	if sqes == nil {
		return false
	}

	for {
		// Still have series left in current chunk
		if len(sqes.seriesMap) > 0 {
			return true
		}

		// No more chunks left to evaluate
		if sqes.currentChunkEnd >= sqes.rangeEnd {
			return false
		}

		// Advance to next chunk
		sqes.currentChunkStart = sqes.currentChunkEnd + 1
		sqes.currentChunkEnd = sqes.currentChunkStart + sqes.chunkSize
		if sqes.currentChunkEnd > sqes.rangeEnd {
			sqes.currentChunkEnd = sqes.rangeEnd
		}

		err := sqes.fetchChunk()
		if err != nil {
			// TODO: handle error logging or sending back to the caller
			continue // fetching the next chunk
		}
	}
}

func (sqes *SubqueryExprStream) fetchChunk() error {
	// Evaluate this chunk
	eval := NewEvaluator(
		sqes.currentChunkStart,
		sqes.currentChunkEnd,
		sqes.step,
		sqes.evaluator.lookBackDelta,
		sqes.evaluator.querySummary,
		sqes.evaluator.qid,
		sqes.evaluator.mSearchReqs,
	)

	seriesMap, err := eval.EvalExpr(sqes.expr.Expr)
	if err != nil {
		return fmt.Errorf("SubqueryExprStream.fetchchunk: %v", err)
	}

	// Found non-empty chunk, assign seriesMap
	sqes.seriesMap = seriesMap
	return nil
}

func (sqes *SubqueryExprStream) Fetch() error {
	if sqes == nil {
		return fmt.Errorf("SubqueryExprStream: nil")
	}

	for key, seriesResult := range sqes.seriesMap {
		sqes.currentSamples = seriesResult.Values
		sqes.currentLabels = seriesResult.Labels

		delete(sqes.seriesMap, key)

		return nil
	}

	return fmt.Errorf("SubqueryExprStream.Fetch: no more series in current chunk")
}

func (ce *CallExprStream) Fetch() error {
	/**
	TODO: Function evaluation for CallExprStream

	This Fetch function supports evaluating call expressions like:
	- agg_over_time functions: max_over_time, avg_over_time, etc.
	- rate functions, scalar math functions, etc.

	Flow (only for agg_over_time functions):
	- These need to aggregate over a range of data points, possibly spread across multiple chunks (e.g., SubqueryExprStream).
	- For these, we need to:
	  1. Loop: while inputStream.Next() → inputStream.Fetch() → inputStream.At()
	  2. Accumulate all samples into a slice
	  3. Apply the aggregation function to the complete slice
	  4. Return a single result sample in ce.currentSamples

	For most other functions (like rate, abs, delta):
	- A single call to inputStream.Next() and inputStream.Fetch() is sufficient
	- The function can be evaluated over that one set of samples
	- No internal accumulation is needed
	*/

	return nil
}
