package ristretto_test

import (
	r "github.com/plateausnetwork/drivers/ristretto"

	"bytes"
	"fmt"
	"testing"
	"time"
)

var key = []byte("key")
var value = []byte("value")

func withCache(handler func(*r.Cache)) {
	cache, err := r.Open(2, "foo")
	if err != nil {
		panic(err)
	}

	if err := cache.Upsert(key, value); err != nil {
		panic(err)
	}

	defer cache.Close()
	handler(cache)
}

func TestGet(t *testing.T) {
	withCache(func(cache *r.Cache) {
		vl, err := cache.Get(key)
		if err != nil {
			t.Error(err)
		}

		if !bytes.Equal(value, vl) {
			t.Error("inserted value not exists")
		}
	})
}

func TestDelete(t *testing.T) {
	withCache(func(cache *r.Cache) {
		// wait for the Sets to be processed so that all values are in the cache
		time.Sleep(time.Duration(5) * time.Millisecond)

		if err := cache.Delete(key); err != nil {
			t.Error(err)
		}

		if _, err := cache.Get(key); err == nil {
			t.Error(err)
		}
	})
}

// TestQueries for Iterator and ForEach results
func TestQueries(t *testing.T) {
	withCache(func(cache *r.Cache) {
		if err := cache.Upsert([]byte("k2"), []byte("v2")); err != nil {
			t.Error(err)
		}

		// wait for the Sets to be processed so that all values are in the cache
		time.Sleep(time.Duration(20) * time.Millisecond)

		values := make([][]byte, 0)
		queryV := func(v []byte) error {
			values = append(values, v)
			return nil
		}

		if err := cache.ForEach(queryV); err != nil {
			t.Error(err)
		}

		keys := make([][]byte, 0)
		query := func(k []byte) error {
			keys = append(keys, k)
			return nil
		}

		if err := cache.KeyIterator(query); err != nil {
			t.Error(err)
			return
		}

		if len(keys) != len(values) {
			t.Error("number of iterator result is different from values result")
		}
	})
}

func TestErrorsHandle(t *testing.T) {
	withCache(func(cache *r.Cache) {
		// wait for the Sets to be processed so that all values are in the cache
		time.Sleep(time.Duration(20) * time.Millisecond)

		// error coverage of iterators
		queryError := func(v []byte) error {
			return fmt.Errorf("error")
		}

		if err := cache.ForEach(queryError); err == nil {
			t.Error("must handle the error from query result")
		}

		if err := cache.KeyIterator(queryError); err == nil {
			t.Error("must handle the error from query result")
		}
	})
}

func TestDBInformations(t *testing.T) {
	withCache(func(cache *r.Cache) {
		// wait for the Sets to be processed so that all values are in the cache
		time.Sleep(time.Duration(20) * time.Millisecond)

		values := make([][]byte, 0)
		query := func(v []byte) error {
			values = append(values, v)
			return nil
		}

		if err := cache.ForEach(query); err != nil {
			t.Error(err)
		}

		if _, err := cache.Size(); err != nil {
			t.Error(err)
		}

		length := cache.Length()

		if length != len(values) {
			t.Error("the size must be equal to result from get all")
		}
	})
}
