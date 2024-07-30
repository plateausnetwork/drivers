package bolt_test

import (
	"bytes"
	"testing"

	"github.com/plateausnetwork/drivers/bolt"
	"github.com/plateausnetwork/drivers/runners"
	"github.com/plateausnetwork/fs"
)

var key = []byte("key")
var value = []byte("value")
var testBucket = []byte("tbucket")
var tp = 0

func withBolt(handler func(*bolt.Bolt)) {
	runners.WithTempDir(func(dir string) {
		db, err := bolt.Open(tp, fs.Path(dir).Join("test.db").String(), testBucket)
		if err != nil {
			panic(err)
		}

		defer db.Close()
		handler(db)
	})
}

func TestOpenError(t *testing.T) {
	if _, err := bolt.Open(tp, "", testBucket); err == nil {
		t.Error("wrong path must return an error")
	}
}

func TestUpSertData(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		if err := db.Upsert(key, value); err != nil {
			t.Error(err)
			return
		}

		encValue, err := db.Get(key)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(encValue, value) {
			t.Error("inserted value is different than expected")
		}

		if err := db.Delete(key); err != nil {
			t.Error(err)
			return
		}

	})
}

func TestGet(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		if err := db.Upsert(key, value); err != nil {
			t.Error(err)
			return
		}

		getValue, err := db.Get(key)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(getValue, value) {
			t.Error("inserted value is different than expected")
			return
		}
	})
}

func TestDelete(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		if err := db.Upsert(key, value); err != nil {
			t.Error(err)
			return
		}

		if err := db.Delete(key); err != nil {
			t.Error(err)
			return
		}

		if _, err := db.Get(key); err == nil {
			t.Errorf("Error expected, but none received")
			return
		}
	})
}

func TestGetDBInformations(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		if err := db.Upsert(key, value); err != nil {
			t.Error(err)
			return
		}

		if db.Path() == "" {
			t.Error("The db path cannot be empty")
		}

		size, err := db.Size()
		if err != nil {
			t.Error(err)
			return
		}

		if db.Length() == 0 && size == 0 {
			t.Error("length and size must be greater than zero")
			return
		}
	})
}

func TestIterator(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		if err := db.Upsert(key, value); err != nil {
			t.Error(err)
			return
		}

		keys := make([][]byte, 0)

		query := func(k []byte) error {
			keys = append(keys, k)
			return nil
		}

		if err := db.KeyIterator(query); err != nil {
			t.Error(err)
			return
		}

		if len(keys) != 1 {
			t.Error("the expected result is len=1")
		}
	})
}

func TestForEach(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		if err := db.Upsert(key, value); err != nil {
			t.Error(err)
			return
		}

		var values = make([][]byte, 0)

		query := func(v []byte) error {
			values = append(values, v)
			return nil
		}

		if err := db.ForEach(query); err != nil {
			t.Error(err)
			return
		}

		if len(values) != 1 {
			t.Error("the result must be len=1")
			return
		}
	})
}

func TestBuckets(t *testing.T) {
	withBolt(func(db *bolt.Bolt) {
		newbucket1 := []byte("newbucket1")
		newbucket2 := []byte("newbucket2")

		if err := db.CreateBuckets(newbucket1, newbucket2); err != nil {
			t.Error(err)
			return
		}

		if err := db.DeleteBuckets(newbucket1, newbucket2); err != nil {
			t.Error(err)
			return
		}

		// testing the return of error
		if err := db.DeleteBuckets(newbucket1); err == nil {
			t.Error("must be a error if bucket not exists")
		}
	})
}
