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

package goanysql

import (
	"testing"
)

type simpleStruct struct {
	int
	string
}

type complexStruct struct {
	int
	simpleStruct
}

var getTests = []struct {
	name       string
	keyToAdd   string
	keyToGet   string
	expectedOk bool
}{
	{"string_hit", "myKey", "myKey", true},
	{"string_miss", "myKey", "nonsense", false},
}

func TestLRUCacheGet(t *testing.T) {
	for _, tt := range getTests {
		lru := NewLRUCache[string, string](1, 0, nil)
		lru.Add(tt.keyToAdd, "1234")
		val, ok := lru.Get(tt.keyToGet)
		if ok != tt.expectedOk {
			t.Fatalf("%s: cache hit = %v; want %v", tt.name, ok, !ok)
		} else if ok && val != "1234" {
			t.Fatalf("%s expected get to return 1234 but got %v", tt.name, val)
		}
		lru.ForAll(func(entry *CacheEntry[string, string]) bool {
			return false
		})
		if lru.Len() != 0 {
			t.Fatal("Incorrect LRUCache length")
		}
	}
}

func TestLRUCacheRemove(t *testing.T) {
	lru := NewLRUCache[string, string](1, 0, nil)
	lru.Add("myKey", "1234")
	if val, ok := lru.Get("myKey"); !ok {
		t.Fatal("TestRemove returned no match")
	} else if val != "1234" {
		t.Fatalf("TestRemove failed.  Expected %d, got %v", 1234, val)
	}

	lru.Remove("myKey")
	if _, ok := lru.Get("myKey"); ok {
		t.Fatal("TestRemove returned a removed entry")
	}
	if lru.Len() != 0 {
		t.Fatal("Incorrect LRUCache length")
	}
}
