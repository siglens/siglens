// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

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
