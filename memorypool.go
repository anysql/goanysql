package goanysql

import (
	"os"
	"sync"
	"unsafe"
)

const memAllocSize = 64

var osPageSize = os.Getpagesize()

type memoryPool struct {
	slots [4096]*sync.Pool
}

func newMemoryPool() *memoryPool {
	mp := &memoryPool{}
	for i := range mp.slots {
		mp.slots[i] = &sync.Pool{
			New: func() any {
				b := make([]byte, i*memAllocSize)
				return unsafe.SliceData(b)
			},
		}
	}
	return mp
}

var memPool *memoryPool = newMemoryPool()

func MemAlloc(size int) []byte {
	if size <= 0 {
		return nil
	}
	buckets := size / memAllocSize
	if size&(memAllocSize-1) != 0 {
		buckets++
	}
	if buckets < cap(memPool.slots) {
		return unsafe.Slice(memPool.slots[buckets].Get().(*byte), buckets*memAllocSize)[:size]
	}
	return make([]byte, size)
}

func MemFree(buf []byte) {
	if buf == nil {
		return
	}
	size := cap(buf)
	if size&(memAllocSize-1) == 0 {
		buckets := size / memAllocSize
		if buckets < cap(memPool.slots) {
			memPool.slots[buckets].Put(unsafe.SliceData(buf))
		}
	}
}

var pagePool *memoryPool = newMemoryPool()

func PageAlloc(pages int) []byte {
	if pages <= 0 {
		return nil
	}
	if pages < cap(memPool.slots) {
		return unsafe.Slice(pagePool.slots[pages].Get().(*byte), pages*osPageSize)[:pages*osPageSize]
	}
	return make([]byte, osPageSize*pages)
}

func PageFree(buf []byte) {
	if buf == nil {
		return
	}
	size := cap(buf)
	if size&(osPageSize-1) == 0 {
		size = size / osPageSize
		if size < cap(pagePool.slots) {
			pagePool.slots[size].Put(unsafe.SliceData(buf))
		}
	}
}
