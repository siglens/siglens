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

package memorypool

import (
	"sync"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/assert"
	"golang.org/x/exp/rand"
)

func Test_newItem(t *testing.T) {
	bufferCapcity := 100
	inUse := true
	item := newItem(uint64(bufferCapcity), inUse)
	assert.NotNil(t, item)
	assert.Equal(t, bufferCapcity, cap(item.buffer))
	assert.Equal(t, 0, len(item.buffer))
	assert.Equal(t, inUse, item.inUse)

	bufferCapcity = 20
	inUse = false
	item = newItem(uint64(bufferCapcity), inUse)
	assert.NotNil(t, item)
	assert.Equal(t, bufferCapcity, cap(item.buffer))
	assert.Equal(t, 0, len(item.buffer))
	assert.Equal(t, inUse, item.inUse)
}

func Test_pointerOfSlice(t *testing.T) {
	buffer := make([]byte, 100)
	ptr := pointerOfSlice(buffer)
	assert.NotNil(t, ptr)
	assert.Equal(t, unsafe.Pointer(&buffer[0]), ptr)

	subBuffer := buffer[10:]
	subBufferPtr := pointerOfSlice(subBuffer)
	assert.NotNil(t, subBufferPtr)
	assert.Equal(t, unsafe.Pointer(&subBuffer[0]), subBufferPtr)
	assert.Equal(t, unsafe.Pointer(uintptr(ptr)+uintptr(10)), subBufferPtr)

	nilPtr := pointerOfSlice(nil)
	assert.Nil(t, nilPtr)
}

func Test_NewMemoryPool(t *testing.T) {
	numInitialItems := 10
	defaultBufferCapacity := 100

	pool := NewMemoryPool(numInitialItems, uint64(defaultBufferCapacity))
	assert.NotNil(t, pool)
	assert.Equal(t, numInitialItems, len(pool.items))
	assert.Equal(t, defaultBufferCapacity, int(pool.defaultBufferCapacity))

	for i := 0; i < numInitialItems; i++ {
		item := pool.items[i]
		assert.NotNil(t, item)
		assert.Equal(t, defaultBufferCapacity, cap(item.buffer))
		assert.Equal(t, 0, len(item.buffer))
		assert.False(t, item.inUse)
	}
}

func numItemsInUse(pool *MemoryPool) int {
	count := 0
	for _, item := range pool.items {
		if item.inUse {
			count++
		}
	}
	return count
}

func Test_GetFromMemoryPool(t *testing.T) {
	numInitialItems := 2
	defaultBufferCapacity := 100

	pool := NewMemoryPool(numInitialItems, uint64(defaultBufferCapacity))
	assert.NotNil(t, pool)

	buffer1 := pool.Get(50)
	assert.NotNil(t, buffer1)
	assert.Equal(t, 0, len(buffer1))
	assert.True(t, cap(buffer1) >= 50)
	assert.Equal(t, 1, numItemsInUse(pool))
	assert.Equal(t, 2, len(pool.items))
	buffer1 = buffer1[:1]
	buffer1[0] = 42

	// Get a buffer larger than the default buffer capacity
	buffer2 := pool.Get(150)
	assert.NotNil(t, buffer2)
	assert.Equal(t, 0, len(buffer2))
	assert.True(t, cap(buffer2) >= 150)
	assert.Equal(t, 2, numItemsInUse(pool))
	assert.Equal(t, 2, len(pool.items))
	buffer2 = buffer2[:1]
	buffer2[0] = 42 * 2

	// Get another item, forcing the pool to grow.
	buffer3 := pool.Get(50)
	assert.NotNil(t, buffer3)
	assert.Equal(t, 0, len(buffer3))
	assert.True(t, cap(buffer3) >= 50)
	assert.Equal(t, 3, numItemsInUse(pool))
	assert.Equal(t, 3, len(pool.items))
	buffer3 = buffer3[:1]
	buffer3[0] = 42 * 3

	assert.Equal(t, buffer1[0], byte(42))
	assert.Equal(t, buffer2[0], byte(42*2))
	assert.Equal(t, buffer3[0], byte(42*3))
}

func Test_MemoryPoolConcurrency(t *testing.T) {
	numInitialItems := 4
	defaultBufferCapacity := 100

	pool := NewMemoryPool(numInitialItems, uint64(defaultBufferCapacity))
	assert.NotNil(t, pool)

	numGoroutines := 10
	numBuffersPerGoroutine := 100
	var waitGroup sync.WaitGroup

	for i := 0; i < numGoroutines; i++ {
		waitGroup.Add(1)
		go func() {
			for k := 0; k < numBuffersPerGoroutine; k++ {
				bufferCapacity := 1 + rand.Intn(2*defaultBufferCapacity)
				buffer := pool.Get(uint64(bufferCapacity))
				assert.NotNil(t, buffer)
				assert.Equal(t, 0, len(buffer))
				assert.True(t, cap(buffer) >= bufferCapacity)

				// Write to the buffer to check if anyone else is writing to it.
				testValue := byte(rand.Intn(256))
				buffer = buffer[:1]
				buffer[0] = testValue
				time.Sleep(time.Duration(1+rand.Intn(10)) * time.Millisecond)
				assert.Equal(t, testValue, buffer[0])

				// Return the buffer to the pool.
				err := pool.Put(buffer)
				assert.Nil(t, err)
			}

			waitGroup.Done()
		}()
	}

	waitGroup.Wait()

	assert.Equal(t, numGoroutines, len(pool.items))
}
