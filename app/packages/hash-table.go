package packages

import (
	"hash/fnv"
	"strings"
	"sync"
	"time"
)

type KeyValue struct {
	Key        string
	Value      interface{}
	Expiration *int
	SetTime    time.Time
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

func (ht *HashTable) Insert(key string, value interface{}, expiration *int, setTime time.Time) {
	ht.Lock()
	defer ht.Unlock()

	index := ht.hash(key)
	if expiration != nil {
		ht.table[index] = append(ht.table[index], KeyValue{Key: key, Value: value, Expiration: expiration, SetTime: setTime})
	} else {
		ht.table[index] = append(ht.table[index], KeyValue{Key: key, Value: value, SetTime: setTime})
	}
}

func (ht *HashTable) Get(key string) (interface{}, bool) {
	ht.Lock()
	defer ht.Unlock()

	index := ht.hash(key)
	for _, kv := range ht.table[index] {
		if kv.Key == key && kv.Expiration == nil {
			return kv.Value, true
		} else if kv.Key == key && kv.Expiration != nil {
			if time.Now().Sub(kv.SetTime).Milliseconds() < int64(*kv.Expiration) {
				return kv.Value, true
			} else {
				println(time.Now().Sub(kv.SetTime).Milliseconds())
				return nil, false
			}
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

func (ht *HashTable) HashLength() int {
	ht.Lock()
	defer ht.Unlock()

	count := 0
	for _, bucket := range ht.table {
		for _, kv := range bucket {
			if !strings.HasPrefix(kv.Key, "config_") {
				count++
			}
		}
	}

	return count
}

func (ht *HashTable) GetKeyValues() map[int][]KeyValue {
	ht.Lock()
	defer ht.Unlock()

	tempKeyValues := make(map[int][]KeyValue)
	for hash, bucket := range ht.table {
		for _, kv := range bucket {
			if !strings.HasPrefix(kv.Key, "config_") {
				tempKeyValues[hash] = append(tempKeyValues[hash], kv)
			}
		}
	}

	return tempKeyValues
}
