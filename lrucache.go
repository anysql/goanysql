/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package lru implements an LRU cache.
package goanysql

import (
	"container/list"
	"sync"
)

// cacheShard is an LRU cache. It is not safe for concurrent access.
type cacheShard[T1 any, T2 any] struct {
	sync.RWMutex

	// MaxEntries is the maximum number of cache entries before
	// an item is evicted. Zero means no limit.
	MaxEntries int

	// OnEvicted optionally specifies a callback function to be
	// executed when an entry is purged from the cache.
	OnEvicted func(key T1, value T2)

	ll    *list.List
	cache map[interface{}]*list.Element

	entryPool *sync.Pool
}

// A Key may be any value that is comparable. See http://golang.org/ref/spec#Comparison_operators
//type Key interface{}

type CacheEntry[T1 any, T2 any] struct {
	Key   T1
	Value T2
	Hit   bool
	Pin   bool
}

func (kv *CacheEntry[T1, T2]) reset() {
	kv.Hit = false
	kv.Pin = false
}

// New creates a new cacheShard.
// If maxEntries is zero, the cache has no limit and it's assumed
// that eviction is done by the caller.
func newShard[T1 any, T2 any](maxEntries int) *cacheShard[T1, T2] {
	return &cacheShard[T1, T2]{
		MaxEntries: maxEntries,
		ll:         list.New(),
		cache:      make(map[interface{}]*list.Element),
		entryPool:  &sync.Pool{New: func() any { return &CacheEntry[T1, T2]{Hit: false, Pin: false} }},
	}
}

// Add adds a value to the cache.
func (c *cacheShard[T1, T2]) Add(key T1, value T2) {
	if c.cache == nil {
		c.cache = make(map[interface{}]*list.Element)
		c.ll = list.New()
		c.entryPool = &sync.Pool{New: func() any { return &CacheEntry[T1, T2]{Hit: false, Pin: false} }}
	}
	if ee, ok := c.cache[key]; ok {
		if c.OnEvicted != nil {
			c.removeElement(ee)
		} else {
			kv := ee.Value.(*CacheEntry[T1, T2])
			kv.Hit = true
			kv.Value = value
			return
		}
	}
	val := c.entryPool.Get().(*CacheEntry[T1, T2])
	val.Key = key
	val.Value = value
	val.Hit = false
	val.Pin = false
	ele := c.ll.PushFront(val)
	c.cache[key] = ele
	if c.MaxEntries != 0 && c.ll.Len() > c.MaxEntries {
		c.RemoveOldest()
	}
}

// Get looks up a key's value from the cache.
func (c *cacheShard[T1, T2]) Get(key T1) (value T2, ok bool) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		kv := ele.Value.(*CacheEntry[T1, T2])
		kv.Hit = true
		return kv.Value, true
	}
	return
}

// Pinn a key's value in the cache.
func (c *cacheShard[T1, T2]) Pin(key T1) bool {
	if c.cache == nil {
		return false
	}
	if ele, hit := c.cache[key]; hit {
		ele.Value.(*CacheEntry[T1, T2]).Pin = true
		return true
	}
	return false
}

// Remove removes the provided key from the cache.
func (c *cacheShard[T1, T2]) Remove(key T1) {
	if c.cache == nil {
		return
	}
	if ele, hit := c.cache[key]; hit {
		c.removeElement(ele)
	}
}

// RemoveOldest removes the oldest item from the cache.
func (c *cacheShard[T1, T2]) RemoveOldest() {
	if c.cache == nil {
		return
	}
	var retry int
again:
	ele := c.ll.Back()
	if ele != nil {
		kv := ele.Value.(*CacheEntry[T1, T2])
		if retry < 2 && kv.Hit {
			c.ll.MoveToFront(ele)
			kv.Hit = false
			retry++
			goto again
		} else {
			if kv.Pin {
				c.ll.MoveToFront(ele)
			} else {
				c.removeElement(ele)
			}
		}
	}
}

func (c *cacheShard[T1, T2]) Cleanup(limit int) {
	if c.cache == nil {
		return
	}
	var retry int
again:
	ele := c.ll.Back()
	if ele != nil {
		if retry < limit {
			retry++
			kv := ele.Value.(*CacheEntry[T1, T2])
			if !kv.Hit && !kv.Pin {
				c.removeElement(ele)
			} else {
				c.ll.MoveToFront(ele)
				kv.Hit = false
			}
			goto again
		}
	}
}

func (c *cacheShard[T1, T2]) removeElement(e *list.Element) {
	c.ll.Remove(e)
	kv := e.Value.(*CacheEntry[T1, T2])
	delete(c.cache, kv.Key)
	if c.OnEvicted != nil {
		c.OnEvicted(kv.Key, kv.Value)
	}
	kv.reset()
	c.entryPool.Put(kv)
}

// Len returns the number of items in the cache.
func (c *cacheShard[T1, T2]) Len() int {
	if c.cache == nil {
		return 0
	}
	return c.ll.Len()
}

// Clear purges all stored items from the cache.
func (c *cacheShard[T1, T2]) Clear() {
	if c.OnEvicted != nil {
		for _, e := range c.cache {
			kv := e.Value.(*CacheEntry[T1, T2])
			c.OnEvicted(kv.Key, kv.Value)
		}
	}
	c.ll = nil
	c.cache = nil
}

func (c *cacheShard[T1, T2]) setOnEvicted(f func(key T1, value T2)) {
	c.OnEvicted = f
}

type LRUCache[T1 any, T2 any] struct {
	shards     []*cacheShard[T1, T2]
	KeyShard   func(key T1) uint32
	OnEvicted  func(key T1, value T2)
	MaxEntries int
}

func (c *LRUCache[T1, T2]) SetOnEvicted(f func(key T1, value T2)) {
	for _, shard := range c.shards {
		shard.setOnEvicted(f)
	}
}

func NewLRUCache[T1 any, T2 any](shards int, maxEntries int, keyf func(key T1) uint32) *LRUCache[T1, T2] {
	cache := &LRUCache[T1, T2]{
		shards:   make([]*cacheShard[T1, T2], shards),
		KeyShard: keyf,
	}
	for i, _ := range cache.shards {
		cache.shards[i] = newShard[T1, T2](maxEntries / len(cache.shards))
	}
	return cache
}

func (c *LRUCache[T1, T2]) getShard(key T1) *cacheShard[T1, T2] {
	if len(c.shards) == 1 {
		return c.shards[0]
	}
	return c.shards[c.KeyShard(key)%uint32(len(c.shards))]
}

func (c *LRUCache[T1, T2]) Add(key T1, value T2) {
	shard := c.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.Add(key, value)
}

func (c *LRUCache[T1, T2]) Get(key T1) (value T2, ok bool) {
	shard := c.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	return shard.Get(key)
}

func (c *LRUCache[T1, T2]) Pin(key T1) bool {
	shard := c.getShard(key)
	shard.RLock()
	defer shard.RUnlock()
	return shard.Pin(key)
}

// Remove removes the provided key from the cache.
func (c *LRUCache[T1, T2]) Remove(key T1) {
	shard := c.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.Remove(key)
}

// RemoveOldest removes the oldest item from the cache.
func (c *LRUCache[T1, T2]) RemoveOldest() {
	for _, shard := range c.shards {
		shard.Lock()
		shard.RemoveOldest()
		shard.Unlock()
	}
}

func (c *LRUCache[T1, T2]) Cleanup(limit int) {
	for _, shard := range c.shards {
		shard.Lock()
		shard.Cleanup(limit/len(c.shards) + 1)
		shard.Unlock()
	}
}

// Len returns the number of items in the cache.
func (c *LRUCache[T1, T2]) Len() int {
	var total int
	for _, shard := range c.shards {
		shard.RLock()
		total += shard.Len()
		shard.RUnlock()
	}
	return total
}

// Clear purges all stored items from the cache.
func (c *LRUCache[T1, T2]) Clear() {
	for _, shard := range c.shards {
		shard.Lock()
		shard.Clear()
		shard.Unlock()
	}
}
