/*
	This package implements all functions of drivers pkg.
	Badger is stable and is being used to serve data sets worth hundreds of terabytes.
	Supports concurrent ACID transactions with serializable snapshot isolation (SSI) guarantees.
*/

package badger

import (
	"fmt"

	b "github.com/dgraph-io/badger"
	"github.com/rhizomplatform/drivers/dbtx"
)

// Badger with locked instance of badgerdb
// path: main path of the database, the badgerdb has no main file
// the Badger work with memory and disk
type Badger struct {
	DB          *b.DB
	opened      bool
	tp          int
	path        string
	IteratorOpt b.IteratorOptions
}

// Open client by given path
func Open(tp int, filepath string) (*Badger, error) {
	if filepath == "" {
		return nil, fmt.Errorf("empty path")
	}

	// set default iterator options for Badger
	iteratorOpt := b.IteratorOptions{
		PrefetchValues: false,
		PrefetchSize:   1000,
		Reverse:        false,
		AllVersions:    false,
	}

	db, err := b.Open(b.DefaultOptions(filepath))
	return &Badger{
		DB:          db,
		opened:      true,
		tp:          tp,
		path:        filepath,
		IteratorOpt: iteratorOpt,
	}, err
}

// Open returns true if the database is open
func (bdger *Badger) Open() bool {
	return bdger.opened
}

// Clean buckets
func (bdger *Badger) Clean() {
	bdger.DB.Update(func(txn *b.Txn) error { //nolint:errcheck
		it := txn.NewIterator(bdger.IteratorOpt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := txn.Delete(item.Key())
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// Close the database
func (bdger *Badger) Close() error {
	bdger.opened = false
	return bdger.DB.Close()
}

func getValue(key []byte, txn *b.Txn) ([]byte, error) {
	item, err := txn.Get(key)
	if err != nil {
		return nil, fmt.Errorf("Inexistent key")
	}
	var encodedValue []byte
	return encodedValue, item.Value(func(val []byte) error {
		encodedValue = val
		return nil
	})
}

// Type setted by caller
func (bdger Badger) Type() int {
	return bdger.tp
}

// Size of database
func (bdger Badger) Size() (int64, error) {
	return 0, nil // TODO: implement this
}

// Length amount of keys
func (bdger Badger) Length() int {
	return 0 // TODO: implement this
}

// Path returns the full path
func (bdger Badger) Path() string {
	return bdger.path
}

// Get value by given key
func (bdger Badger) Get(key []byte) ([]byte, error) {
	var v []byte
	var err error
	return v, bdger.DB.View(func(txn *b.Txn) error {
		// if that is no error from view will check the get
		v, err = getValue(key, txn)
		return err
	})
}

// ForEach enables filters and append to arrays
// the query contains all rules and will be executed for each key/value
func (bdger Badger) ForEach(query func([]byte) error) error {
	return bdger.DB.View(func(txn *b.Txn) error {
		it := txn.NewIterator(bdger.IteratorOpt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			err := item.Value(query)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

// KeyIterator iterates only in keys
func (bdger Badger) KeyIterator(query func([]byte) error) error {
	var err error
	return bdger.DB.View(func(txn *b.Txn) error {
		it := txn.NewIterator(bdger.IteratorOpt)
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			err = query(k)
		}
		return err
	})
}

// Upsert update or insert the key/value
func (bdger Badger) Upsert(k, v []byte) error {
	return bdger.DB.Update(func(txn *b.Txn) error {
		return txn.Set(k, v)
	})
}

// Update updates all database executions inside one transaction
func (bdger Badger) Update(execute dbtx.Execute) error {
	return bdger.DB.Update(func(txn *b.Txn) error {
		return execute(dbtx.BucketImp{ // actual implementation of bucket
			PutImp: func(key []byte, val []byte) error {
				return txn.Set(key, val)
			},
			DeleteImp: func(key []byte) error {
				return txn.Delete(key)
			},
		})
	})
}

// Delete key/value from database
func (bdger Badger) Delete(key []byte) error {
	return bdger.DB.Update(func(txn *b.Txn) error {
		return txn.Delete(key)
	})
}

// CreateBuckets Badger don't support buckets, collections or tables
func (bdger Badger) CreateBuckets(...[]byte) error {
	return nil
}

// DeleteBuckets Badger don't support buckets, collections or tables
func (bdger Badger) DeleteBuckets(...[]byte) error {
	return nil
}
