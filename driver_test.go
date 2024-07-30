package drivers_test

import (
	"testing"

	dr "github.com/plateausnetwork/drivers"
	"github.com/plateausnetwork/drivers/runners"
)

var key = []byte("key")
var value = []byte("value")

var testBucket = []byte("tbucket")

func openKeyValueDB(dbType dr.DriverType) error {
	var result error

	runners.WithTempDir(func(dir string) {
		opts := dr.DriverOptions()
		opts.AddBucket(testBucket)
		db, err := dr.Open(dbType, dir+"/test.db", opts)

		if err == nil {
			db.Close()
		}
		result = err
	})

	return result
}

func TestBolt(t *testing.T) {
	if err := openKeyValueDB(dr.Boltdb); err != nil {
		t.Error(err)
	}
}

func TestRistretto(t *testing.T) {
	if err := openKeyValueDB(dr.Ristretto); err != nil {
		t.Error(err)
	}
}

func TestBadger(t *testing.T) {
	if err := openKeyValueDB(dr.Badgerdb); err != nil {
		t.Error(err)
	}
}

// coverage of errors when open the driver
func TestInvalidDriver(t *testing.T) {
	if err := openKeyValueDB(dr.DriverType(99)); err == nil {
		t.Errorf("Invalid driver type should return error.")
	}
}

func benchmarkMiseEnPlace(handler func(map[string]dr.KeyValueDB)) {
	runners.WithTempSubDirs(3, func(dirs []string) {
		// open the ristretto in memory
		cache, err := dr.Open(dr.Ristretto, dirs[0], dr.Options{})
		if err != nil {
			panic(err)
		}

		// open badger database
		badgerdb, err := dr.Open(dr.Badgerdb, dirs[1]+"/test.db", dr.Options{})
		if err != nil {
			panic(err)
		}

		// open bolt database with default bucket
		options := dr.DriverOptions()
		options.AddBucket(testBucket)
		boltdb, err := dr.Open(dr.Boltdb, dirs[2]+"/test.db", options)
		if err != nil {
			panic(err)
		}

		drivers := make(map[string]dr.KeyValueDB)
		drivers["ristretto"] = cache
		drivers["badger"] = badgerdb
		drivers["bolt"] = boltdb

		handler(drivers)

		for _, driver := range drivers {
			driver.Close()
		}
	})
}

func BenchmarkDatabases(b *testing.B) {
	benchmarkMiseEnPlace(func(drivers map[string]dr.KeyValueDB) {
		for name, driver := range drivers {
			b.Run("Insert/"+name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					driver.Upsert(key, value) // nolint
				}
			})
			b.Run("Get/"+name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					driver.Get(key) // nolint
				}
			})
			b.Run("Delete/"+name, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					driver.Delete(key) // nolint
				}
			})
		}
	})
}
