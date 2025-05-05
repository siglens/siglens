// Copyright (c) 2021-2025 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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

package utils

import (
	"container/heap"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ImplementsHeap(t *testing.T) {
	var _ heap.Interface = NewHeap(func(a, b int) bool { return a < b })
}

func Test_HeapBasicOperations(t *testing.T) {
	// Max heap for integers
	h := NewHeap(func(a, b int) bool { return a > b })

	// Test empty heap
	if h.Len() != 0 {
		t.Errorf("Expected empty heap, got %d elements", h.Len())
	}

	// Add elements
	heap.Init(h)
	heap.Push(h, &HeapItem[int]{Value: 5})
	heap.Push(h, &HeapItem[int]{Value: 3})
	heap.Push(h, &HeapItem[int]{Value: 7})

	// Check size
	if h.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", h.Len())
	}

	// Verify max element
	max := heap.Pop(h).(*HeapItem[int])
	if max.Value != 7 {
		t.Errorf("Expected max value 7, got %d", max.Value)
	}

	// Verify remaining elements
	secondMax := heap.Pop(h).(*HeapItem[int])
	if secondMax.Value != 5 {
		t.Errorf("Expected second max value 5, got %d", secondMax.Value)
	}

	thirdMax := heap.Pop(h).(*HeapItem[int])
	if thirdMax.Value != 3 {
		t.Errorf("Expected third max value 3, got %d", thirdMax.Value)
	}

	// Verify empty again
	if h.Len() != 0 {
		t.Errorf("Expected empty heap after popping all elements, got %d", h.Len())
	}
}

func Test_HeapConvenienceMethods(t *testing.T) {
	// Min heap for strings
	h := NewHeap(func(a, b string) bool { return a < b })
	heap.Init(h)

	// Use convenience methods
	h.PushValue("banana")
	h.PushValue("apple")
	h.PushValue("cherry")

	if h.Len() != 3 {
		t.Errorf("Expected 3 elements, got %d", h.Len())
	}

	assert.Equal(t, "apple", h.PopValue())
	assert.Equal(t, "banana", h.PopValue())
	assert.Equal(t, "cherry", h.PopValue())
}

func Test_TopN(t *testing.T) {
	data := []int{3, 1, 5, 9, 2, 6, 5, 3, 5, 8, 9, 1, 4, 7, 9}

	top5 := GetTopN(5, data, func(a, b int) bool {
		return a > b
	})

	assert.Equal(t, 5, len(top5))
	assert.Equal(t, []int{9, 9, 9, 8, 7}, top5)
}
