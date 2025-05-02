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

import "container/heap"

type HeapItem[T any] struct {
	Value T
	index int // Position in the heap, maintained by heap operations
}

type lessFunc[T any] func(a, b T) bool

// Heap implements heap.Interface for a generic collection of items.
type Heap[T any] struct {
	items []*HeapItem[T]
	less  lessFunc[T]
}

func NewHeap[T any](less lessFunc[T]) *Heap[T] {
	if less == nil {
		return nil
	}

	return &Heap[T]{
		items: []*HeapItem[T]{},
		less:  less,
	}
}

func (h *Heap[T]) Len() int {
	return len(h.items)
}

func (h *Heap[T]) Less(i, j int) bool {
	return h.less(h.items[i].Value, h.items[j].Value)
}

func (h *Heap[T]) Swap(i, j int) {
	h.items[i], h.items[j] = h.items[j], h.items[i]
	h.items[i].index = i
	h.items[j].index = j
}

func (h *Heap[T]) Push(x any) {
	n := len(h.items)
	item := x.(*HeapItem[T])
	item.index = n
	h.items = append(h.items, item)
}

// Remove and return the last item.
func (h *Heap[T]) Pop() any {
	old := h.items
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	item.index = -1
	h.items = old[0 : n-1]

	return item
}

func (h *Heap[T]) PushValue(v T) *HeapItem[T] {
	item := &HeapItem[T]{Value: v}
	heap.Push(h, item)

	return item
}

func (h *Heap[T]) PopValue() T {
	return heap.Pop(h).(*HeapItem[T]).Value
}

func GetTopN[T any](N int, items []T, less lessFunc[T]) []T {
	// Create min-heap (smaller items at root)
	h := NewHeap(func(a, b T) bool {
		return less(b, a) // Reverse the order for min-heap
	})

	// Process all items
	for _, item := range items {
		h.PushValue(item)

		// If we have more than N items, remove the smallest
		if h.Len() > N {
			h.PopValue()
		}
	}

	// Extract results in reverse order
	result := make([]T, 0, h.Len())
	for h.Len() > 0 {
		result = append(result, h.PopValue())
	}

	// Reverse to get correct order
	for i, j := 0, len(result)-1; i < j; i, j = i+1, j-1 {
		result[i], result[j] = result[j], result[i]
	}

	return result
}
