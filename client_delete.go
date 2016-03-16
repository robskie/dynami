package dynami

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
)

// Delete removes an item from a table. item must be a
// map[string]interface{}, struct, or a pointer to any
// of the two with nonempty primary key.
func (c *Client) Delete(tableName string, item interface{}) error {
	err := checkType(item, reflect.Struct, map[string]interface{}{})
	if err != nil {
		return err
	}

	key, err := getKey(item)
	if err != nil {
		return err
	}

	cdb := c.db
	_, err = cdb.DeleteItem(&db.DeleteItemInput{
		Key:       key.value,
		TableName: aws.String(tableName),
	})
	if err != nil {
		return fmt.Errorf("dynami: cannot delete item (%v)", err)
	}

	return nil
}

// BatchDelete can delete multiple items from one or more tables.
type BatchDelete struct {
	db *db.DynamoDB
	op *batchOp

	err    error
	tables map[string]bool
}

// BatchDelete queues a batch delete operation. items must be a
// []T or []*T where T is a map[string]interface{} or a struct.
func (c *Client) BatchDelete(tableName string, items interface{}) *BatchDelete {
	b := &BatchDelete{
		db:     c.db,
		op:     newBatchOp(),
		tables: map[string]bool{},
	}
	b.tables[tableName] = true

	err := checkSliceType(items, reflect.Struct, map[string]interface{}{})
	if err != nil {
		b.err = err
		return b
	}

	keysOnly := true
	b.op.addItems(tableName, items, keysOnly)
	return b
}

// Delete chains another batch delete operation. This can be
// called multiple times as long as tableName is unique for each call.
func (b *BatchDelete) Delete(tableName string, items interface{}) *BatchDelete {
	if b.err != nil {
		return b
	} else if b.tables[tableName] {
		b.err = fmt.Errorf("dynami: only one BatchDelete operation per table is allowed")
		return b
	} else if err := checkSliceType(items, reflect.Struct, map[string]interface{}{}); err != nil {
		b.err = err
		return b
	}
	b.tables[tableName] = true

	keysOnly := true
	b.op.addItems(tableName, items, keysOnly)
	return b
}

// Run executes all delete operations in
// this batch. This may return a BatchError.
func (b *BatchDelete) Run() error {
	const maxDelsPerOp = 25

	if b.err != nil {
		return b.err
	}

	op := b.op
	citems := map[string][]dbitem{}
	for !op.isEmpty() || len(citems) > 0 {
		citems = op.collectItems(maxDelsPerOp, citems)

		// Add processed items to the request
		reqItems := map[string][]*db.WriteRequest{}
		for table, items := range citems {
			writeReqs := make([]*db.WriteRequest, len(items))
			for i, item := range items {
				delReq := &db.DeleteRequest{Key: item}
				writeReq := &db.WriteRequest{DeleteRequest: delReq}
				writeReqs[i] = writeReq
			}
			reqItems[table] = writeReqs
		}

		// Delete items from database
		input := &db.BatchWriteItemInput{
			RequestItems: reqItems,
		}
		resp, err := b.db.BatchWriteItem(input)

		if err != nil {
			return fmt.Errorf("dynami: BatchDelete failed (%v)", err)
		}

		unproc := op.unwrap(resp.UnprocessedItems)
		op.processItems(citems, unproc, nil)
		citems = unproc
	}

	return op.errors()
}
