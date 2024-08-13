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
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_JoinNodes(t *testing.T) {
	node1 := &Node{Comparison: Comparison{Field: "node1Field"}}
	node2 := &Node{Comparison: Comparison{Field: "node2Field"}}
	node3 := &Node{Comparison: Comparison{Field: "node3Field"}}

	nodes := []*Node{node1, node2, node3}
	operation := NodeAnd

	// JoinNodes() happens to be right-associative.
	resultNode := JoinNodes(nodes, operation)
	assert.Equal(t, NodeAnd, resultNode.NodeType)
	assert.Equal(t, "node1Field", resultNode.Left.Comparison.Field)
	assert.Equal(t, NodeAnd, resultNode.Right.NodeType)
	assert.Equal(t, "node2Field", resultNode.Right.Left.Comparison.Field)
	assert.Equal(t, "node3Field", resultNode.Right.Right.Comparison.Field)

	// Test 0 nodes.
	resultNode = JoinNodes([]*Node{}, operation)
	assert.Nil(t, resultNode)

	// Test 1 node.
	resultNode = JoinNodes([]*Node{node1}, operation)
	assert.Equal(t, node1, resultNode)
}
