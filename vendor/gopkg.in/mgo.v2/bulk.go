package mgo

// Bulk represents an operation that can be prepared with several
// orthogonal changes before being delivered to the server.
//
// WARNING: This API is still experimental.
//
// Relevant documentation:
//
//   http://blog.mongodb.org/post/84922794768/mongodbs-new-bulk-api
//
type Bulk struct {
	c       *Collection
	ordered bool
	inserts []interface{}
}

// BulkError holds an error returned from running a Bulk operation.
//
// TODO: This is private for the moment, until we understand exactly how
//       to report these multi-errors in a useful and convenient way.
type bulkError struct {
	err error
}

// BulkResult holds the results for a bulk operation.
type BulkResult struct {
	// Be conservative while we understand exactly how to report these
	// results in a useful and convenient way, and also how to emulate
	// them with prior servers.
	private bool
}

func (e *bulkError) Error() string {
	return e.err.Error()
}

// Bulk returns a value to prepare the execution of a bulk operation.
//
// WARNING: This API is still experimental.
//
func (c *Collection) Bulk() *Bulk {
	return &Bulk{c: c, ordered: true}
}

// Unordered puts the bulk operation in unordered mode.
//
// In unordered mode the indvidual operations may be sent
// out of order, which means latter operations may proceed
// even if prior ones have failed.
func (b *Bulk) Unordered() {
	b.ordered = false
}

// Insert queues up the provided documents for insertion.
func (b *Bulk) Insert(docs ...interface{}) {
	b.inserts = append(b.inserts, docs...)
}

// Run runs all the operations queued up.
func (b *Bulk) Run() (*BulkResult, error) {
	op := &insertOp{b.c.FullName, b.inserts, 0}
	if !b.ordered {
		op.flags = 1 // ContinueOnError
	}
	_, err := b.c.writeQuery(op)
	if err != nil {
		return nil, &bulkError{err}
	}
	return &BulkResult{}, nil
}
