package packages

import (
	"fmt"
	"hash/fnv"
	"sync"
)

func Test() {
	fmt.Printf("test")
}

type KeyValue struct {
	Key   string
	Value interface{}
}

type HashTable struct {
	sync.RWMutex
	size  int
	table map[int][]KeyValue
}

func NewHashTable(size int) *HashTable {
	return &HashTable{
		size:  size,
		table: make(map[int][]KeyValue),
	}
}

func (ht *HashTable) hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % ht.size
}

func (ht *HashTable) Insert(key string, value interface{}) {
	ht.Lock()
	defer ht.Unlock()

	index := ht.hash(key)
	ht.table[index] = append(ht.table[index], KeyValue{Key: key, Value: value})
}

func (ht *HashTable) Get(key string) (interface{}, bool) {
	ht.Lock()
	defer ht.Unlock()

	index := ht.hash(key)
	for _, kv := range ht.table[index] {
		if kv.Key == key {
			return kv.Value, true
		}
	}

	return nil, false
}

func (ht *HashTable) Delete(key string) {
	ht.Lock()
	defer ht.Unlock()

	index := ht.hash(key)
	for i, kv := range ht.table[index] {
		if kv.Key == key {
			ht.table[index] = append(ht.table[index][:i], ht.table[index][i+1:]...)
			return
		}
	}
}
