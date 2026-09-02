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
type poolQueue struct {
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

func (d *poolQueue) unpack(ptrs uint64) (head, tail uint32) {
	const mask = 1<<dequeueBits - 1
	head = uint32((ptrs >> dequeueBits) & mask)
	tail = uint32(ptrs & mask)
	return
}

func (d *poolQueue) pack(head, tail uint32) uint64 {
	const mask = 1<<dequeueBits - 1
	return (uint64(head) << dequeueBits) |
		uint64(tail&mask)
}

//go:linkname procyield runtime.procyield
func procyield(cycles uint32)

func pause(backoff uint32) {
	procyield(backoff)
}

// pushHead adds val at the head of the queue. It returns false if the
// queue is full. It must only be called by a single producer.
func (d *poolQueue) pushHead(val unsafe.Pointer) bool {
	var retrycnt int
	var backoff uint32 = 8
	ptrs := d.headTail.Load()
	head, tail := d.unpack(ptrs)
	if (tail+uint32(len(d.vals)))&(1<<dequeueBits-1) == head {
		// Queue is full.
		return false
	}
	slot := &d.vals[head&uint32(len(d.vals)-1)]
	// The head slot is free, so we own it.
	if val == nil {
		val = dequeueNil
	}
retry:
	if atomic.LoadPointer(slot) != nil {
		// Another goroutine is still cleaning up the tail, so
		// the queue is actually still full.
		if backoff < 64 {
			backoff += 16
		}
		if retrycnt < maxRetries {
			retrycnt++
			pause(backoff)
			goto retry
		}
		return false
	}

	// Store the val
	atomic.StorePointer(slot, unsafe.Pointer(val))

	// Increment head. This passes ownership of slot to popTail
	// and acts as a store barrier for writing the slot.
	d.headTail.Add(1 << dequeueBits)
	return true
}

// popTail removes and returns the element at the tail of the queue.
// It returns false if the queue is empty. It may be called by any
// number of consumers.
func (d *poolQueue) popTail() (unsafe.Pointer, bool) {
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

	return val, true
}

func (d *poolQueue) empty() bool {
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
type poolChain struct {
	// head is the poolDequeue to push to. This is only accessed
	// by the producer, so doesn't need to be synchronized.
	head *poolChainElt

	// bucket size
	buckets uint

	// tail is the poolDequeue to popTail from. This is accessed
	// by consumers, so reads and writes must be atomic.
	tail atomic.Pointer[poolChainElt]

	// sync.Pool
	pool sync.Pool
}

type poolChainElt struct {
	poolQueue

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
	next, prev atomic.Pointer[poolChainElt]
}

func (c *poolChain) newPoolChainElt() *poolChainElt {
	var d *poolChainElt
	if d_ := c.pool.Get(); d_ != nil {
		d = d_.(*poolChainElt)
		clear(d.vals)
		d.headTail.Store(0)
		d.next.Store(nil)
		d.prev.Store(nil)
	} else {
		d = new(poolChainElt)
		d.vals = make([]unsafe.Pointer, c.buckets)
	}
	return d
}

func (c *poolChain) pushHead(val unsafe.Pointer) {
	if c.head != nil && c.head.pushHead(val) {
		return
	}
	c.pushHeadSlow(val)
}

func (c *poolChain) pushHeadSlow(val unsafe.Pointer) {
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

	// Fast-path setup for the new element before linking it to the chain.
	// We can push to d2 non-atomically or safely here because it isn't visible to consumers yet.
	d2.pushHead(val)

	d2.prev.Store(d)
	c.head = d2

	// Store release barrier ensures d2 is fully initialized before consumers can traverse to it via d.next.
	d.next.Store(d2)
}

func (c *poolChain) popTail() (unsafe.Pointer, bool) {
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
			return val, true
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

func (c *poolChain) empty() bool {
	d := c.tail.Load()
	if d == nil {
		return true
	}
	return d.empty()
}

type queueWait struct {
	wait atomic.Bool
	cond chan struct{}
}

func (qw *queueWait) waitEvent() {
	qw.wait.Store(true)
	<-qw.cond
}

func (qw *queueWait) signal() {
	if qw.wait.Load() && qw.wait.Swap(false) {
		select {
		case qw.cond <- struct{}{}:
		default:
		}
	}
}

type QueueFunc[T any] func(param *T)

type AtomicQueue[T any] struct {
	wlock atomic.Bool
	poolChain
	qw queueWait
}

func (fq *AtomicQueue[T]) Push(m *T) {
	var backoff uint32 = 8
	for fq.wlock.Swap(true) {
		if backoff < 64 {
			pause(backoff)
			backoff += 16
		} else {
			runtime.Gosched()
		}
	}
	fq.poolChain.pushHead(unsafe.Pointer(m))
	fq.wlock.Store(false)
}

func (fq *AtomicQueue[T]) BPush(m *T) {
	fq.Push(m)
	fq.qw.signal()
}

func (fq *AtomicQueue[T]) Pop() (*T, bool) {
	v, ok := fq.poolChain.popTail()
	return (*T)(v), ok
}

func (fq *AtomicQueue[T]) IsEmpty() bool {
	return fq.poolChain.empty()
}

func NewAtomicQueue[T any](size uint) *AtomicQueue[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicQueue[T]{
		poolChain: poolChain{buckets: size},
		qw: queueWait{
			cond: make(chan struct{}, 1),
		},
	}
	return q
}

func (fq *AtomicQueue[T]) Consume(f QueueFunc[T]) {
	for {
		if fq.IsEmpty() {
			fq.qw.waitEvent()
		}
		for v, ok := fq.Pop(); ok; v, ok = fq.Pop() {
			if v == nil {
				return
			} else {
				f(v)
			}
		}
		runtime.Gosched()
	}
}

type AtomicPriorityQueue[T any] struct {
	qw    queueWait
	qhigh *AtomicQueue[T]
	qlow  *AtomicQueue[T]
}

func NewAtomicPriorityQueue[T any](size uint) *AtomicPriorityQueue[T] {
	if bits.OnesCount(size) != 1 {
		size = (1 << bits.Len(size))
	}
	q := &AtomicPriorityQueue[T]{
		qhigh: &AtomicQueue[T]{
			poolChain: poolChain{buckets: size},
		},
		qlow: &AtomicQueue[T]{
			poolChain: poolChain{buckets: size},
		},
		qw: queueWait{
			cond: make(chan struct{}, 1),
		},
	}
	return q
}

func (fq *AtomicPriorityQueue[T]) Push(m *T, hpri bool) {
	if hpri {
		fq.qhigh.Push(m)
	} else {
		fq.qlow.Push(m)
	}
	fq.qw.signal()
}

func (fq *AtomicPriorityQueue[T]) PopH() (*T, bool) {
	v, ok := fq.qhigh.popTail()
	return (*T)(v), ok
}

func (fq *AtomicPriorityQueue[T]) PopL() (*T, bool) {
	v, ok := fq.qlow.popTail()
	return (*T)(v), ok
}

func (fq *AtomicPriorityQueue[T]) IsEmpty(hpri bool) bool {
	if hpri {
		return fq.qhigh.empty()
	} else {
		return fq.qlow.empty()
	}
}

func (fq *AtomicPriorityQueue[T]) Wait() {
	if fq.IsEmpty(true) && fq.IsEmpty(false) {
		fq.qw.waitEvent()
	}
}

func (fq *AtomicPriorityQueue[T]) Consume(fh QueueFunc[T], fl QueueFunc[T]) {
	for {
		if fq.IsEmpty(true) && fq.IsEmpty(false) {
			fq.qw.waitEvent()
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
		runtime.Gosched()
	}
}
