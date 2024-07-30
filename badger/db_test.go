package badger_test

import (
	"bytes"
	"fmt"
	"testing"

	b "github.com/plateausnetwork/drivers/badger"
	"github.com/plateausnetwork/drivers/runners"
)

var key = []byte("key")
var value = []byte("value")
var tp = 0

func withBadger(handler func(*b.Badger)) {
	runners.WithTempDir(func(dir string) {
		db, err := b.Open(tp, dir)
		if err != nil {
			panic(err)
		}
		defer db.Close()

		if err := db.Upsert(key, value); err != nil {
			panic(err)
		}

		handler(db)
	})
}

func TestOpenErr(t *testing.T) {
	if _, err := b.Open(tp, ""); err == nil {
		t.Error("wrong path must return an error")
	}
}

func TestGet(t *testing.T) {
	withBadger(func(db *b.Badger) {
		getValue, err := db.Get(key)
		if err != nil {
			t.Error(err)
			return
		}

		if !bytes.Equal(getValue, value) {
			t.Error("inserted value is different tham expected")
			return
		}
	})
}

func TestDelete(t *testing.T) {
	withBadger(func(db *b.Badger) {
		if err := db.Delete(key); err != nil {
			t.Error(err)
			return
		}

		// test GET return error
		if _, err := db.Get(key); err == nil {
			t.Error(err)
			return
		}
	})
}

func TestGetDBInformations(t *testing.T) {
	withBadger(func(db *b.Badger) {
		if _, err := db.Size(); err != nil {
			t.Error(err)
			return
		}
	})
}

func TestIterator(t *testing.T) {
	withBadger(func(db *b.Badger) {
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
	withBadger(func(db *b.Badger) {
		var values = make([][]byte, 0)

		// coverage of a right query
		query := func(v []byte) error {
			values = append(values, v)
			return nil
		}

		if err := db.ForEach(query); err != nil {
			t.Error(err)
			return
		}

		// testing the result of ForEach
		if len(values) != 1 {
			t.Error("the result must be len=1")
			return
		}

		// coverage of return errors in queries
		queryWithError := func(v []byte) error {
			return fmt.Errorf("test error")
		}

		if err := db.ForEach(queryWithError); err == nil {
			t.Error("must return an error if the query was wrong")
		}
	})
}
