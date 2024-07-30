/*
	This package implements all functions of drivers pkg.
	Bolt is a pure Go key/value store inspired by Howard Chu's LMDB project.
	The goal of Bolt is to provide a simple, fast, and reliable database.
*/

package bolt

import (
	"bytes"
	"fmt"

	b "go.etcd.io/bbolt"
	"github.com/plateausnetwork/drivers/dbtx"
)

// Bolt with locked file with key/values
// Path: of the main file.db which contains the key/values
// The BoltDB work with focus in disk
type Bolt struct {
	db     *b.DB // database client
	tp     int
	opened bool
	path   string // database path inside the path
	Bucket []byte // used for default or current bucket
}

// Open open file boltDB
func Open(tp int, filepath string, bucket []byte) (*Bolt, error) {
	db, err := b.Open(filepath, 0600, nil)
	if err != nil {
		return nil, fmt.Errorf("on opening boltdb : %s", err.Error())
	}

	boltdb := &Bolt{
		db:     db,
		tp:     tp,
		opened: true,
		path:   filepath,
	}

	return boltdb, boltdb.CreateBuckets(bucket)
}

// Open returns true if the db is oppen
func (blt *Bolt) Open() bool {
	return blt.opened
}

// Type setted by caller
func (blt *Bolt) Type() int {
	return blt.tp
}

// CreateBuckets if not exists in database
// Buckets are schemas/collections/tables of boltdb
// One connection can access all buckets
func (blt *Bolt) CreateBuckets(buckets ...[]byte) error {
	var err error
	// ensures that don't create bucket if is nil
	for _, bkt := range buckets {
		// ignores the empty buckets
		if bytes.Equal(bkt, []byte("")) {
			continue
		}
		blt.Bucket = bkt // the last is the current
		err = blt.db.Update(func(tx *b.Tx) error {
			_, err := tx.CreateBucketIfNotExists(blt.Bucket)
			return err
		})
	}
	return err
}

// DeleteBuckets from database
func (blt Bolt) DeleteBuckets(buckets ...[]byte) error {
	var err error
	var bucket []byte
	for _, bkt := range buckets {
		bucket = bkt
		err = blt.db.Update(func(tx *b.Tx) error {
			err := tx.DeleteBucket(bucket)
			if err != nil {
				return err
			}
			return nil
		})
	}
	return err
}

// Size of database
func (blt Bolt) Size() (int64, error) {
	size := int64(0)
	err := blt.db.View(func(tx *b.Tx) error {
		size = tx.Size()
		return nil
	})
	return size, err
}

// Clean bucket
func (blt Bolt) Clean() {
	blt.db.Update(func(tx *b.Tx) error { //nolint:errcheck
		return tx.DeleteBucket(blt.Bucket)
	})
}

// Length amount of keys in database
func (blt Bolt) Length() int {
	var len int
	blt.db.View(func(tx *b.Tx) error { //nolint:errcheck
		len = tx.Bucket(blt.Bucket).Stats().KeyN
		return nil
	})
	return len
}

// Path returns the full path
func (blt Bolt) Path() string {
	return blt.path
}

// Get from boltd
func (blt Bolt) Get(key []byte) ([]byte, error) {
	var value []byte
	err := blt.db.View(func(tx *b.Tx) error {
		b := tx.Bucket(blt.Bucket)
		value = b.Get(key)
		if value == nil {
			return fmt.Errorf("inexistent value")
		}
		return nil
	})
	return value, err
}

// Upsert update or insert into boltdb
func (blt Bolt) Upsert(key, value []byte) error {
	return blt.db.Update(func(tx *b.Tx) error {
		return tx.Bucket(blt.Bucket).Put(key, value)
	})
}

// Update updates all database executions inside one transaction
func (blt Bolt) Update(execute dbtx.Execute) error {
	return blt.db.Update(func(tx *b.Tx) error {
		return execute(tx.Bucket(blt.Bucket))
	})
}

// ForEach values from boltdb
func (blt Bolt) ForEach(query func([]byte) error) error {
	// its necessary for bolt queries
	boltQuery := func(k, v []byte) error {
		return query(v)
	}
	return blt.db.View(func(tx *b.Tx) error {
		return tx.Bucket(blt.Bucket).ForEach(boltQuery)
	})
}

// Delete the key/value
func (blt Bolt) Delete(key []byte) error {
	return blt.db.Update(func(tx *b.Tx) error {
		return tx.Bucket(blt.Bucket).Delete(key)
	})
}

// Close and unlock the database
// the db file or path are lock
func (blt *Bolt) Close() error {
	blt.opened = false
	return blt.db.Close()
}

// KeyIterator iterates only in keys
func (blt Bolt) KeyIterator(query func([]byte) error) error {
	boltQuery := func(k, v []byte) error {
		return query(k)
	}
	return blt.db.View(func(tx *b.Tx) error {
		return tx.Bucket(blt.Bucket).ForEach(boltQuery)
	})
}
