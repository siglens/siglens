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

package ast

import (
	"fmt"
	"strings"

	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/siglens/siglens/pkg/segment/utils"
)

type QueryStruct struct {
	SearchFilter *Node
	PipeCommands *structs.QueryAggregators
}

// NodeType represents the type of a node in the parse tree
type NodeType int

// Node Types
const (
	_ NodeType = iota
	NodeNot
	NodeAnd
	NodeOr
	NodeTerminal
	TimeModifierNode
)

// Node is a node in the query parse tree
type Node struct {
	NodeType      NodeType
	Comparison    Comparison
	Left          *Node
	Right         *Node
	TimeModifiers *TimeModifiers
}

func (n *Node) isMatchAll() bool {
	if n == nil {
		return false
	}

	return n.NodeType == NodeTerminal && n.Comparison.isMatchAll()
}

// Potentially change the structure of the tree, but not the meaning.
func (n *Node) Simplify() {
	if n == nil {
		return
	}

	n.Left.Simplify()
	n.Right.Simplify()

	if n.NodeType == NodeAnd {
		if n.Left.isMatchAll() {
			*n = *n.Right
			n.Simplify()
			return
		}

		if n.Right.isMatchAll() {
			*n = *n.Left
			n.Simplify()
			return
		}
	}
}

func JoinNodes(nodes []*Node, operation NodeType) *Node {
	if len(nodes) == 0 {
		return nil
	}

	if len(nodes) == 1 {
		return nodes[0]
	}

	if len(nodes) == 2 {
		return &Node{
			NodeType: operation,
			Left:     nodes[0],
			Right:    nodes[1],
		}
	}

	return &Node{
		NodeType: operation,
		Left:     nodes[0],
		Right:    JoinNodes(nodes[1:], operation),
	}
}

// TimeModifiers is a struct that holds the time modifiers for a query
type TimeModifiers struct {
	StartEpoch uint64
	EndEpoch   uint64
}

// Comparison is an individual comparison operation on a terminal node
type Comparison struct {
	Op           string
	Field        string
	Values       interface{}
	ValueIsRegex bool // True if Values is a regex string. False if Values is a wildcarded string or anything else.
}

func (c *Comparison) isMatchAll() bool {
	if c == nil {
		return false
	}

	return c.Op == "=" && c.Field == "*" && c.Values == `"*"`
}

type GrepValue struct {
	Field string
}

// ParseError is the exported error type for parsing errors with detailed information as to where they occurred
type ParseError struct {
	Inner    error    `json:"inner"`
	Line     int      `json:"line"`
	Column   int      `json:"column"`
	Offset   int      `json:"offset"`
	Prefix   string   `json:"prefix"`
	Expected []string `json:"expected"`
}

type TimeModifier struct {
	RelativeTime   RelativeTimeModifier
	AbsoluteTime   string
	ChainedOffsets []RelativeTimeOffset
	ChainedSnaps   []string
}

type RelativeTimeModifier struct {
	RelativeTimeOffset RelativeTimeOffset
	Snap               string
}

type RelativeTimeOffset struct {
	Offset   int64
	TimeUnit utils.TimeUnit
}

func MakeValue(val interface{}) (interface{}, error) {
	return val, nil
}

func StringFromChars(chars interface{}) string {
	str := ""
	r := chars.([]interface{})
	for _, i := range r {
		j := i.([]uint8)
		str += string(j[0])
	}
	return str
}

func (p *ParseError) Error() string {
	return p.Prefix + ": " + p.Inner.Error()
}

func OpNameToString(label interface{}) (string, error) {
	var sb strings.Builder
	value := label.([]interface{})
	for _, i := range value {
		if i == nil {
			continue
		}
		switch b := i.(type) {
		case []byte:
			sb.WriteByte(b[0])
		case string:
			sb.WriteString(b)
		case []interface{}:
			s, err := OpNameToString(i)
			if err != nil {
				return "", err
			}
			sb.WriteString(s)
		default:
			return "", fmt.Errorf("OpNameToString: unexpected type [%T] found in label interfaces: %+v", i, i)
		}
	}
	return sb.String(), nil
}

func toIfaceSlice(v interface{}) []interface{} {
	if v == nil {
		return nil
	}
	return v.([]interface{})
}

// helper method to get individual tokens from their rule index
func GetTokens(first, rest interface{}, idx int) []string {
	out := []string{first.(string)}
	restSl := toIfaceSlice(rest)
	for _, v := range restSl {
		expr := toIfaceSlice(v)
		out = append(out, expr[idx].(string))
	}
	return out
}

// helper method to get individual tokens from their rule index
func GetMeasureAggsTokens(first, rest interface{}, idx int) *structs.QueryAggregators {
	aggNode := &structs.QueryAggregators{}
	aggNode.PipeCommandType = structs.MeasureAggsType
	aggNode.MeasureOperations = make([]*structs.MeasureAggregator, 0)
	aggNode.MeasureOperations = append(aggNode.MeasureOperations, first.(*structs.MeasureAggregator))

	restSl := toIfaceSlice(rest)
	for _, v := range restSl {
		expr := toIfaceSlice(v)
		aggNode.MeasureOperations = append(aggNode.MeasureOperations, expr[idx].(*structs.MeasureAggregator))
	}
	return aggNode
}

// helper method to get individual tokens from their rule index
func GetGroupByTokens(cols, first, rest interface{}, idx int, limit int) *structs.QueryAggregators {
	aggNode := &structs.QueryAggregators{}
	aggNode.PipeCommandType = structs.GroupByType
	aggNode.GroupByRequest = &structs.GroupByRequest{}
	aggNode.GroupByRequest.MeasureOperations = make([]*structs.MeasureAggregator, 0)
	aggNode.GroupByRequest.MeasureOperations = append(aggNode.GroupByRequest.MeasureOperations, first.(*structs.MeasureAggregator))
	aggNode.BucketLimit = limit

	if cols != nil {
		aggNode.GroupByRequest.GroupByColumns = cols.([]string)
	}
	restSl := toIfaceSlice(rest)
	for _, v := range restSl {
		expr := toIfaceSlice(v)
		aggNode.GroupByRequest.MeasureOperations = append(aggNode.GroupByRequest.MeasureOperations, expr[idx].(*structs.MeasureAggregator))
	}
	return aggNode
}

func AggTypeToAggregateFunction(aggType string) (utils.AggregateFunctions, error) {
	var aggFunc utils.AggregateFunctions

	if aggType == "avg" {
		aggFunc = utils.Avg
	} else if aggType == "min" {
		aggFunc = utils.Min
	} else if aggType == "max" {
		aggFunc = utils.Max
	} else if aggType == "sum" {
		aggFunc = utils.Sum
	} else if aggType == "count" {
		aggFunc = utils.Count
	} else if aggType == "cardinality" {
		aggFunc = utils.Cardinality
	} else {
		return aggFunc, fmt.Errorf("AggTypeToAggregateFunction: unsupported statistic aggregation type %v", aggType)
	}
	return aggFunc, nil
}
