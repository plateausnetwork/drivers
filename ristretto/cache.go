/*
	This package implements all functions of drivers pkg.
	Ristretto is a fast, concurrent cache library using a TinyLFU admission policy and Sampled LFU eviction policy.
*/

package ristretto

import (
	"fmt"
	"sync"
	"time"

	r "github.com/dgraph-io/ristretto"
	"github.com/rhizomplatform/drivers/dbtx"
)

// Cache memory-bound
// keys is the current list
// this list helps in ForEach() with business rules
// because the ristretto has only get by one key
type Cache struct {
	name   string
	tp     int
	opened bool
	db     *r.Cache
	keys   map[string]int
	sync.RWMutex
}

// Open returns the cache in memory
func Open(tp int, name string) (*Cache, error) {
	// default parameters
	cacheDB, err := r.NewCache(&r.Config{
		NumCounters: 1000000 * 10,
		MaxCost:     1000000,
		BufferItems: 64,
	})
	cache := &Cache{
		name:   name,
		tp:     tp,
		opened: true,
		db:     cacheDB,
		keys:   make(map[string]int),
	}
	return cache, err
}

func (c *Cache) waitForKey(key []byte, available bool) {
	for {
		if _, ok := c.db.Get(key); ok == available {
			break
		}

		time.Sleep(20 * time.Millisecond)
	}
}

// Open returns true if the cache is open
func (c *Cache) Open() bool {
	return c.opened
}

// Clean all data
func (c *Cache) Clean() {
	c.keys = make(map[string]int)
}

// Get key value from cache
func (c *Cache) Get(key []byte) ([]byte, error) {
	value, ok := c.db.Get(key)
	if !ok {
		return nil, fmt.Errorf("key not found: %v", key)
	}
	return value.([]byte), nil
}

// Upsert in cache
func (c *Cache) Upsert(key, value []byte) error {
	cost := int64(len(value))
	if !c.db.Set(key, value, cost) {
		return fmt.Errorf("error on set key value bytes in ristretto cache")
	}
	c.add(key)
	c.waitForKey(key, true)
	return nil
}

func (c *Cache) add(key []byte) {
	c.Lock()
	defer c.Unlock()
	c.keys[string(key)] = 0
}

func (c *Cache) delete(key []byte) {
	c.Lock()
	defer c.Unlock()

	delete(c.keys, string(key))
}

// Keys return all current keys in cache memory
func (c *Cache) Keys() map[string]int {
	c.Lock()
	defer c.Unlock()
	return c.keys
}

// ForEach get many
func (c *Cache) ForEach(query func([]byte) error) error {
	for key := range c.Keys() {
		value, err := c.Get([]byte(key))
		if err != nil {
			return err
		}

		if err := query(value); err != nil {
			return err
		}
	}

	return nil
}

// KeyIterator in current keys cached
func (c *Cache) KeyIterator(query func([]byte) error) error {
	for key := range c.Keys() {
		if err := query([]byte(key)); err != nil {
			return err
		}
	}
	return nil
}

// Delete the key/value
func (c *Cache) Delete(key []byte) error {
	c.delete(key)
	c.db.Del(key)
	c.waitForKey(key, false)

	return nil
}

// Update updates all database executions inside one transaction, in ristreto nothing change
func (c *Cache) Update(execute dbtx.Execute) error {
	return execute(dbtx.BucketImp{ // actual implementation of bucket
		PutImp: func(key []byte, val []byte) error {
			return c.Upsert(key, val)
		},
		DeleteImp: func(key []byte) error {
			return c.Delete(key)
		},
	})
}

// Close the database
func (c *Cache) Close() error {
	c.opened = false
	c.db.Close()
	return nil
}

// Length amount of keys in database
func (c *Cache) Length() int {
	return len(c.keys)
}

// Type of database
func (c *Cache) Type() int {
	return c.tp
}

// Size of database in bytes
func (c *Cache) Size() (int64, error) {
	return 0, nil
}

// Path of the database
func (c *Cache) Path() string {
	return c.name
}

// CreateBuckets ristretto don't have buckets
func (c *Cache) CreateBuckets(buckets ...[]byte) error {
	return nil
}

// DeleteBuckets ristretto don't have buckets
func (c *Cache) DeleteBuckets(buckets ...[]byte) error {
	return nil
}
