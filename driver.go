/*
	Package drivers has all interfaces that a database need to be used in blockchain project.
*/

package drivers

import (
	"fmt"

	"github.com/plateausnetwork/drivers/badger"
	"github.com/plateausnetwork/drivers/bolt"
	"github.com/plateausnetwork/drivers/dbtx"
	"github.com/plateausnetwork/drivers/ristretto"
)

var (
	// DefaultOptions for open a database connection
	DefaultOptions = Options{Bucket: []byte("rhz")}
)

// KeyValueDB driver signature
type KeyValueDB interface {
	Database
	Reader
	Writer
}

// Database has the methods for management
type Database interface {
	Type() int
	Path() string
	Clean()
	Open() bool                    // returns true if the database is open
	Size() (int64, error)          // size of database in bytes
	Length() int                   // amount of key/values
	Close() error                  // close the database and unlock the key/value path
	CreateBuckets(...[]byte) error // create the N buckets
	DeleteBuckets(...[]byte) error // delete N buckets
}

// Reader all methods to read the database
// Get: searches for a specific key/value
// KeyIterator: iterates only in keys' tree
// ForEach: apply rules with values from database
// the queries must be in same scope, example:
// var list [][]byte
// query := func(v []byte) error {list=append(list,v)}
type Reader interface {
	Get([]byte) ([]byte, error)
	ForEach(func([]byte) error) error
	KeyIterator(func([]byte) error) error
}

// Writer all methods to write in database
// Upsert will update the value if key exists
type Writer interface {
	Upsert([]byte, []byte) error // update or insert
	Delete([]byte) error         // delete the key/value
	Update(dbtx.Execute) error
}

// OptionsNil helps if the database has default values
var OptionsNil = Options{}

// Options has all options to connect with any available driver
type Options struct {
	Bucket  []byte
	Size    int64
	Timeout int64
}

// DriverOptions set specific or normal options for key/value databases
func DriverOptions() Options {
	return Options{}
}

// AddBucket as a option for the database
func (op *Options) AddBucket(bucket []byte) {
	op.Bucket = bucket
}

// database types list
// available drivers
const (
	Boltdb DriverType = iota
	Badgerdb
	Ristretto
)

// DriverType ensures that the range of drivers will be respected
type DriverType int

// check the list of available drivers
// database list condition:
// range firs_const <-> last_const
func (dtp DriverType) isValid() bool {
	return dtp >= Boltdb && dtp <= Ristretto
}

// ToDriverType returns a valid driver type
func ToDriverType(tp int) DriverType {
	drtp := DriverType(tp)
	if drtp.isValid() {
		return drtp
	}
	return 0
}

// Open returns the key/value database
// dbpath: full path with the file+extension if needed
func Open(dbtype DriverType, dbpath string, options Options) (KeyValueDB, error) {
	switch dbtype {
	case Boltdb:
		return bolt.Open(int(Boltdb), dbpath, options.Bucket)
	case Badgerdb:
		return badger.Open(int(Badgerdb), dbpath)
	case Ristretto:
		// the path is the name of the cache
		return ristretto.Open(int(Ristretto), dbpath)
	}
	return nil, fmt.Errorf("inexistent database type " + string(dbtype))
}

// Int returns the DriverType as int
func (dtp DriverType) Int() int {
	return int(dtp)
}
