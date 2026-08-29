package goanysql

import (
	"math/bits"
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"
)

const maxRetries = 3

// AtomicQueue is a lock-free fixed-size single-producer,
// multi-consumer queue. The single producer can both push and pop
// from the head, and consumers can pop from the tail.
//
// It has the added feature that it nils out unused slots to avoid
// unnecessary retention of objects. This is important for sync.Pool,
// but not typically a property considered in the literature.
type poolQueue[T any] struct {
	// headTail packs together a 32-bit head index and a 32-bit
	// tail index. Both are indexes into vals modulo len(vals)-1.
	//
	// tail = index of oldest data in queue
	// head = index of next slot to fill
	//
	// Slots in the range [tail, head) are owned by consumers.
	// A consumer continues to own a slot outside this range until
	// it nils the slot, at which point ownership passes to the
	// producer.
	//
	// The head index is stored in the most-significant bits so
	// that we can atomically add to it and the overflow is
	// harmless.
	headTail atomic.Uint64

	// vals is a ring buffer of interface{} values stored in this
	// dequeue. The size of this must be a power of 2.
	//
	// vals[i].typ is nil if the slot is empty and non-nil
	// otherwise. A slot is still in use until *both* the tail
	// index has moved beyond it and typ has been set to nil. This
	// is set to nil atomically by the consumer and read
	// atomically by the producer.
	vals []unsafe.Pointer
}

const dequeueBits = 32

// dequeueNil is used in AtomicQueue to represent interface{}(nil).
// Since we use nil to represent empty slots, we need a sentinel value
// to represent nil.
var dequeueNil unsafe.Pointer = unsafe.Pointer(uintptr(1))

func (d *poolQueue[T]) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (d *poolQueue[T]) pack(head, tail uint32) uint64 {
	const mask = 1<<dequeueBits - 1
	return (uint64(head) << dequeueBits) |
		uint64(tail&mask)
}

// pushHead adds val at the head of the queue. It returns false if the
// queue is full. It must only be called by a single producer.
func (d *poolQueue[T]) pushHead(val *T) bool {
	var retrycnt int
	var backoff int = 1
	ptrs := d.headTail.Load()
	head, tail := d.unpack(ptrs)
	if (tail+uint32(len(d.vals)))&(1<<dequeueBits-1) == head {
		// Queue is full.
		return false
	}
	slot := &d.vals[head&uint32(len(d.vals)-1)]
	// The head slot is free, so we own it.
	if val == nil {
		val = (*T)(dequeueNil)
	}
retry:
	if atomic.LoadPointer(slot) != nil || !atomic.CompareAndSwapPointer(slot, nil, unsafe.Pointer(val)) {
		// Another goroutine is still cleaning up the tail, so
		// the queue is actually still full.
		if backoff < 64 {
			backoff <<= 1
		}
		if retrycnt < maxRetries {
			retrycnt++
			for j := 0; j < backoff; j++ {
				// runtime.Gosched() is too heavy here.
				// In Go 1.24+, you can use clear spin loops or simple empty loops
				// that the compiler optimizes for hardware pause instructions.
				_ = j
			}
			goto retry
		}
		return false
	}

	// Increment head. This passes ownership of slot to popTail
	// and acts as a store barrier for writing the slot.
	d.headTail.Add(1 << dequeueBits)
	return true
}

// popTail removes and returns the element at the tail of the queue.
// It returns false if the queue is empty. It may be called by any
// number of consumers.
func (d *poolQueue[T]) popTail() (*T, bool) {
	var slot *unsafe.Pointer
	for {
		ptrs := d.headTail.Load()
		head, tail := d.unpack(ptrs)
		if tail == head {
			// Queue is empty.
			return nil, false
		}

		// Confirm head and tail (for our speculative check
		// above) and increment tail. If this succeeds, then
		// we own the slot at tail.
		ptrs2 := d.pack(head, tail+1)
		if d.headTail.CompareAndSwap(ptrs, ptrs2) {
			// Success.
			slot = &d.vals[tail&uint32(len(d.vals)-1)]
			break
		}
	}

	// We now own slot.
	val := atomic.SwapPointer(slot, nil)
	if val == dequeueNil {
		val = nil
	}

	return (*T)(val), true
}

func (d *poolQueue[T]) empty() bool {
	ptrs := d.headTail.Load()
	head, tail := d.unpack(ptrs)
	return tail == head
}

// poolChain is a dynamically-sized version of poolDequeue.
//
// This is implemented as a doubly-linked list queue of poolDequeues
// where each dequeue is double the size of the previous one. Once a
// dequeue fills up, this allocates a new one and only ever pushes to
// the latest dequeue. Pops happen from the other end of the list and
// once a dequeue is exhausted, it gets removed from the list.
type poolChain[T any] struct {
	// head is the poolDequeue to push to. This is only accessed
	// by the producer, so doesn't need to be synchronized.
	head *poolChainElt[T]

	// tail is the poolDequeue to popTail from. This is accessed
	// by consumers, so reads and writes must be atomic.
	tail atomic.Pointer[poolChainElt[T]]

	// sync.Pool
	pool sync.Pool

	// bucket size
	buckets uint
}

type poolChainElt[T any] struct {
	poolQueue[T]

	// next and prev link to the adjacent poolChainElts in this
	// poolChain.
	//
	// next is written atomically by the producer and read
	// atomically by the consumer. It only transitions from nil to
	// non-nil.
	//
	// prev is written atomically by the consumer and read
	// atomically by the producer. It only transitions from
	// non-nil to nil.
	next, prev atomic.Pointer[poolChainElt[T]]
}

func (c *poolChain[T]) newPoolChainElt() *poolChainElt[T] {
	var d *poolChainElt[T]
	if d_ := c.pool.Get(); d_ != nil {
		d = d_.(*poolChainElt[T])
		d.next.Store(nil)
		d.prev.Store(nil)
	} else {
		d = new(poolChainElt[T])
		d.vals = make([]unsafe.Pointer, c.buckets)
	}
	return d
}

func (c *poolChain[T]) pushHead(val *T) {
	d := c.head
	if d == nil {
		// Initialize the chain.
		d = c.newPoolChainElt()
		c.head = d
		c.tail.Store(d)
	}

	if d.pushHead(val) {
		return
	}

	// The current dequeue is full. Allocate a new one
	d2 := c.newPoolChainElt()
	d2.prev.Store(d)
	c.head = d2
	d.next.Store(d2)
	d2.pushHead(val)
}

func (c *poolChain[T]) popTail() (*T, bool) {
	d := c.tail.Load()
	if d == nil {
		return nil, false
	}

	for {
		// It's important that we load the next pointer
		// *before* popping the tail. In general, d may be
		// transiently empty, but if next is non-nil before
		// the pop and the pop fails, then d is permanently
		// empty, which is the only condition under which it's
		// safe to drop d from the chain.
		d2 := d.next.Load()

		if val, ok := d.popTail(); ok {
			return val, ok
		}

		if d2 == nil {
			// This is the only dequeue. It's empty right
			// now, but could be pushed to in the future.
			return nil, false
		}

		// The tail of the chain has been drained, so move on
		// to the next dequeue. Try to drop it from the chain
		// so the next pop doesn't have to look at the empty
		// dequeue again.
		if c.tail.CompareAndSwap(d, d2) {
			// We won the race. Clear the prev pointer so
			// the garbage collector can collect the empty
			// dequeue and so popHead doesn't back up
			// further than necessary.
			d2.prev.Store(nil)

			// push to pool for reuse
			d.prev.Store(nil)
			d.next.Store(nil)
			c.pool.Put(d)
		}

		d = d2
	}
}

func (c *poolChain[T]) empty() bool {
	d := c.tail.Load()
	if d == nil {
		return true
	}
	return d.empty()
}

type AtomicQueue[T any] struct {
	poolQueue[T]
	wlock atomic.Bool
}

func (fq *AtomicQueue[T]) Push(m *T) {
	var backoff int = 1
	for !fq.TryPush(m) {
		if backoff < 64 {
			backoff <<= 1
		}
		for range maxRetries {
			for j := 0; j < backoff; j++ {
				// runtime.Gosched() is too heavy here.
				// In Go 1.24+, you can use clear spin loops or simple empty loops
				// that the compiler optimizes for hardware pause instructions.
				_ = j
			}
		}
	}
}

func (fq *AtomicQueue[T]) TryPush(m *T) bool {
	var retrycnt int
	var backoff int = 1
retry:
	if !fq.wlock.Load() && fq.wlock.CompareAndSwap(false, true) {
		ret := fq.poolQueue.pushHead(m)
		fq.wlock.Store(false)
		return ret
	}
	if backoff < 64 {
		backoff <<= 1
	}
	if retrycnt < maxRetries {
		retrycnt++
		for j := 0; j < backoff; j++ {
			// runtime.Gosched() is too heavy here.
			// In Go 1.24+, you can use clear spin loops or simple empty loops
			// that the compiler optimizes for hardware pause instructions.
			_ = j
		}
		goto retry
	}
	return false
}

func (fq *AtomicQueue[T]) Pop() (*T, bool) {
	return fq.poolQueue.popTail()
}

func (fq *AtomicQueue[T]) IsEmpty() bool {
	return fq.poolQueue.empty()
}

func NewAtomicQueue[T any](size uint) *AtomicQueue[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicQueue[T]{
		poolQueue: poolQueue[T]{
			vals: make([]unsafe.Pointer, size),
		},
	}
	return q
}

type AtomicPoolQueue[T any] struct {
	AtomicQueue[T]
	wait atomic.Uint32
	cond chan struct{}
}

func NewAtomicPoolQueue[T any](size uint) *AtomicPoolQueue[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicPoolQueue[T]{
		AtomicQueue: AtomicQueue[T]{
			poolQueue: poolQueue[T]{
				vals: make([]unsafe.Pointer, size),
			},
		},
		cond: make(chan struct{}, 1),
	}
	return q
}

func (fq *AtomicPoolQueue[T]) Push(m *T) {
	fq.AtomicQueue.Push(m)
	if fq.wait.Load() == 1 {
		if fq.wait.CompareAndSwap(1, 0) {
			select {
			case fq.cond <- struct{}{}:
			default:
			}
		}
	}
}

type PoolQueueFunc[T any] func(param *T)

func (fq *AtomicPoolQueue[T]) Consume(f PoolQueueFunc[T]) {
	for {
		if fq.IsEmpty() {
			if fq.wait.CompareAndSwap(0, 1) {
				<-fq.cond
			}
		}
		for v, ok := fq.Pop(); ok; v, ok = fq.Pop() {
			if v == nil {
				return
			} else {
				f(v)
			}
		}
		for range maxRetries {
			runtime.Gosched()
		}
	}
}

type AtomicChain[T any] struct {
	poolChain[T]
	wlock atomic.Bool
}

func (fq *AtomicChain[T]) Push(m *T) {
	var backoff int = 1
	for !fq.TryPush(m) {
		if backoff < 64 {
			backoff <<= 1
		}
		for range maxRetries {
			for j := 0; j < backoff; j++ {
				// runtime.Gosched() is too heavy here.
				// In Go 1.24+, you can use clear spin loops or simple empty loops
				// that the compiler optimizes for hardware pause instructions.
				_ = j
			}
		}
	}
}

func (fq *AtomicChain[T]) TryPush(m *T) bool {
	var retrycnt int
	var backoff int = 1
retry:
	if !fq.wlock.Load() && fq.wlock.CompareAndSwap(false, true) {
		fq.poolChain.pushHead(m)
		fq.wlock.Store(false)
		return true
	}
	if backoff < 64 {
		backoff <<= 1
	}
	if retrycnt < maxRetries {
		retrycnt++
		for j := 0; j < backoff; j++ {
			// runtime.Gosched() is too heavy here.
			// In Go 1.24+, you can use clear spin loops or simple empty loops
			// that the compiler optimizes for hardware pause instructions.
			_ = j
		}
		goto retry
	}
	return false
}

func (fq *AtomicChain[T]) Pop() (*T, bool) {
	return fq.poolChain.popTail()
}

func (fq *AtomicChain[T]) IsEmpty() bool {
	return fq.poolChain.empty()
}

func NewAtomicChain[T any](size uint) *AtomicChain[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicChain[T]{
		poolChain: poolChain[T]{buckets: size},
	}
	return q
}

type AtomicPoolChain[T any] struct {
	AtomicChain[T]
	wait atomic.Uint32
	cond chan struct{}
}

func NewAtomicPoolChain[T any](size uint) *AtomicPoolChain[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicPoolChain[T]{
		AtomicChain: AtomicChain[T]{
			poolChain: poolChain[T]{buckets: size},
		},
		cond: make(chan struct{}, 1),
	}
	return q
}

func (fq *AtomicPoolChain[T]) Push(m *T) {
	fq.AtomicChain.Push(m)
	if fq.wait.Load() == 1 {
		if fq.wait.CompareAndSwap(1, 0) {
			select {
			case fq.cond <- struct{}{}:
			default:
			}
		}
	}
}

func (fq *AtomicPoolChain[T]) Consume(f PoolQueueFunc[T]) {
	for {
		if fq.IsEmpty() {
			if fq.wait.CompareAndSwap(0, 1) {
				<-fq.cond
			}
		}
		for v, ok := fq.Pop(); ok; v, ok = fq.Pop() {
			if v == nil {
				return
			} else {
				f(v)
			}
		}
		for range maxRetries {
			runtime.Gosched()
		}
	}
}

type AtomicPriorityChain[T any] struct {
	qhigh *AtomicChain[T]
	qlow  *AtomicChain[T]
	wait  atomic.Uint32
	cond  chan struct{}
}

func NewAtomicPriorityChain[T any](size uint) *AtomicPriorityChain[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicPriorityChain[T]{
		qhigh: &AtomicChain[T]{
			poolChain: poolChain[T]{buckets: size},
		},
		qlow: &AtomicChain[T]{
			poolChain: poolChain[T]{buckets: size},
		},
		cond: make(chan struct{}, 1),
	}
	return q
}

func (fq *AtomicPriorityChain[T]) Push(m *T, hpri bool) {
	if hpri {
		fq.qhigh.Push(m)
	} else {
		fq.qlow.Push(m)
	}
	if fq.wait.Load() == 1 {
		if fq.wait.CompareAndSwap(1, 0) {
			select {
			case fq.cond <- struct{}{}:
			default:
			}
		}
	}
}

func (fq *AtomicPriorityChain[T]) PopH() (*T, bool) {
	return fq.qhigh.popTail()
}

func (fq *AtomicPriorityChain[T]) PopL() (*T, bool) {
	return fq.qlow.popTail()
}

func (fq *AtomicPriorityChain[T]) IsEmpty(hpri bool) bool {
	if hpri {
		return fq.qhigh.empty()
	} else {
		return fq.qlow.empty()
	}
}

func (fq *AtomicPriorityChain[T]) Wait() {
	if fq.IsEmpty(true) && fq.IsEmpty(false) {
		if fq.wait.CompareAndSwap(0, 1) {
			<-fq.cond
		}
	}
}

func (fq *AtomicPriorityChain[T]) Consume(fh PoolQueueFunc[T], fl PoolQueueFunc[T]) {
	for {
		if fq.IsEmpty(true) && fq.IsEmpty(false) {
			if fq.wait.CompareAndSwap(0, 1) {
				<-fq.cond
			}
		}
	retry:
		for v, ok := fq.PopH(); ok; v, ok = fq.PopH() {
			if v == nil {
				return
			} else {
				fh(v)
			}
		}
		if v, ok := fq.PopL(); ok {
			if v == nil {
				return
			} else {
				fl(v)
			}
			goto retry
		}
		for range maxRetries {
			runtime.Gosched()
		}
	}
}
