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

type ByteArray struct {
	data []byte
}

var byteArrPool = &sync.Pool{New: func() any { return &ByteArray{} }}

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

func MemAlloc(size int) *ByteArray {
	buf := byteArrPool.Get().(*ByteArray)
	if size <= 0 {
		return nil
	}
	buckets := size / memAllocSize
	if size&(memAllocSize-1) != 0 {
		buckets++
	}
	if buckets < cap(memPool.slots) {
		buf.data = unsafe.Slice(memPool.slots[buckets].Get().(*byte), buckets*memAllocSize)[:size]
	} else {
		buf.data = make([]byte, size)
	}
	return buf
}

func MemFree(buf *ByteArray) {
	if buf == nil || buf.data == nil {
		if buf != nil {
			byteArrPool.Put(buf)
		}
		return
	}
	size := cap(buf.data)
	if size&(memAllocSize-1) == 0 {
		buckets := size / memAllocSize
		if buckets < cap(memPool.slots) {
			memPool.slots[buckets].Put(unsafe.SliceData(buf.data))
		}
	}
	buf.data = nil
	byteArrPool.Put(buf)
}

var pagePool *memoryPool = newMemoryPool()

func PageAlloc(pages int) *ByteArray {
	buf := byteArrPool.Get().(*ByteArray)
	if pages <= 0 {
		return buf
	}
	if pages < cap(memPool.slots) {
		buf.data = unsafe.Slice(pagePool.slots[pages].Get().(*byte), pages*osPageSize)[:pages*osPageSize]
	} else {
		buf.data = make([]byte, osPageSize*pages)
	}
	return buf
}

func PageFree(buf *ByteArray) {
	if buf == nil || buf.data == nil {
		if buf != nil {
			byteArrPool.Put(buf)
		}
		return
	}
	size := cap(buf.data)
	if size&(osPageSize-1) == 0 {
		size = size / osPageSize
		if size < cap(pagePool.slots) {
			pagePool.slots[size].Put(unsafe.SliceData(buf.data))
		}
	}
	buf.data = nil
	byteArrPool.Put(buf)
}
