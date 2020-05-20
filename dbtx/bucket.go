package dbtx

// Bucket basic methods of an transaction on key-value db
type Bucket interface {
	Put([]byte, []byte) error // Update or Insert
	Delete([]byte) error      // Delete
}

// Execute function receive an bucket and if an error occours, it rollback the transaction
type Execute func(Bucket) error

// BucketImp Bucket Interface implementation
type BucketImp struct {
	PutImp    func([]byte, []byte) error
	DeleteImp func([]byte) error
}

// Put translate the implementation of dbtx.Bucket.Put
func (mb BucketImp) Put(key []byte, val []byte) error {
	return mb.PutImp(key, val)
}

// Delete translate the implementation of dbtx.Bucket.Delete
func (mb BucketImp) Delete(key []byte) error {
	return mb.DeleteImp(key)
}
