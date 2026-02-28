package memorypool

import (
	"fmt"
	"sync"
	"unsafe"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

// A thread-safe memory pool for []byte buffers.
//
// Note: After getting a buffer from the pool, the caller should not increase
// the buffer's capacity (e.g., by doing append() past the buffer's initial
// capcity specified to Get()) because that can cause the buffer to be moved
// and then the Put() method on the new buffer will not be able to find which
// items is being returned.

type MemoryPool struct {
	items                 []poolItem
	mutex                 sync.Mutex
	defaultBufferCapacity uint64
}

type poolItem struct {
	buffer  []byte
	inUse   bool
	pointer unsafe.Pointer
}

func NewMemoryPool(numInitialItems int, defaultBufferCapacity uint64) *MemoryPool {
	pool := &MemoryPool{
		items:                 make([]poolItem, 0, numInitialItems),
		mutex:                 sync.Mutex{},
		defaultBufferCapacity: defaultBufferCapacity,
	}

	for i := 0; i < numInitialItems; i++ {
		pool.items = append(pool.items, newItem(defaultBufferCapacity, false))
	}

	return pool
}

func newItem(bufferCapacity uint64, inUse bool) poolItem {
	buffer := make([]byte, bufferCapacity)

	return poolItem{
		buffer:  buffer[:0],
		inUse:   inUse,
		pointer: unsafe.Pointer(&buffer[0]),
	}
}

func pointerOfSlice(buffer []byte) unsafe.Pointer {
	if cap(buffer) == 0 {
		return nil
	}

	if len(buffer) == 0 {
		buffer = buffer[:1]
		defer func() { buffer = buffer[:0] }()
	}

	return unsafe.Pointer(&buffer[0])
}

// Returns a zero-length buffer with a capacity of at least minCapacity.
func (self *MemoryPool) Get(minCapacity uint64) []byte {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	for i := range self.items {
		if !self.items[i].inUse {
			self.items[i].expandBufferToMinCapacity(minCapacity)
			self.items[i].buffer = self.items[i].buffer[:0]
			self.items[i].inUse = true

			return self.items[i].buffer
		}
	}

	// All items are in use, so make a new item.
	bufferCapacity := utils.Max(minCapacity, self.defaultBufferCapacity)
	item := newItem(bufferCapacity, true)
	self.items = append(self.items, item)

	return item.buffer
}

func (self *poolItem) expandBufferToMinCapacity(minCapacity uint64) {
	if self.inUse {
		panic("Cannot expand buffer that is in use")
	}

	if cap(self.buffer) < int(minCapacity) {
		self.buffer = make([]byte, 0, minCapacity)
		self.pointer = pointerOfSlice(self.buffer)
	}
}

// Put back a buffer from the pool. Returns error if the buffer was not
// obtained from the pool.
func (self *MemoryPool) Put(buffer []byte) error {
	self.mutex.Lock()
	defer self.mutex.Unlock()

	if len(buffer) == 0 {
		buffer = buffer[:1]
	}

	bufferPointer := pointerOfSlice(buffer)
	for i := range self.items {
		if self.items[i].pointer == bufferPointer {
			self.items[i].inUse = false
			return nil
		}
	}

	// We should not get here. The returned buffer is not in the pool.
	allBufferPointers := make([]string, 0)
	for i := range self.items {
		allBufferPointers = append(allBufferPointers, fmt.Sprintf("%p", self.items[i].pointer))
	}
	log.Errorf("customPool.Put: Buffer at %p not found in the pool; expected one of: %+v", bufferPointer, allBufferPointers)

	return fmt.Errorf("Buffer not found in the pool")
}
