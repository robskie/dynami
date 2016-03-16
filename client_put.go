package dynami

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Put adds an item to the database. item must be a
// map[string]interface{}, struct, or a pointer to any
// of those with nonempty primary key.
func (c *Client) Put(tableName string, item interface{}) error {
	err := checkType(item, reflect.Struct, map[string]interface{}{})
	if err != nil {
		return err
	}

	item = reflect.Indirect(reflect.ValueOf(item)).Interface()
	mitem, err := dbattribute.ConvertToMap(item)
	if err != nil {
		return fmt.Errorf("dynami: invalid item (%v)", err)
	}
	mitem = removeEmptyAttr(mitem)

	cdb := c.db
	_, err = cdb.PutItem(&db.PutItemInput{
		Item:      mitem,
		TableName: aws.String(tableName),
	})

	if err != nil {
		return fmt.Errorf("dynami: cannot put item (%v)", err)
	}

	return nil
}

// BatchPut represents a batch put operation. It
// can put multiple items in multiple tables with
// one DynamoDB operation.
type BatchPut struct {
	db *db.DynamoDB
	op *batchOp

	err    error
	tables map[string]bool
}

// BatchPut queues a batch put operation. items must
// satisfy the same conditions as that in BatchDelete.
func (c *Client) BatchPut(tableName string, items interface{}) *BatchPut {
	b := &BatchPut{
		db:     c.db,
		op:     newBatchOp(),
		tables: map[string]bool{},
	}

	err := checkSliceType(items, reflect.Struct, map[string]interface{}{})
	if err != nil {
		b.err = err
		return b
	}

	b.tables[tableName] = true
	b.op.addItems(tableName, items)
	return b
}

// Put chains another batch put operation.
// This can be called one or more times as
// long as tableName is unique for each call.
func (b *BatchPut) Put(tableName string, items interface{}) *BatchPut {
	if b.err != nil {
		return b
	} else if b.tables[tableName] {
		b.err = fmt.Errorf("dynami: only one BatchPut operation per table is allowed")
		return b
	} else if err := checkSliceType(items, reflect.Struct, map[string]interface{}{}); err != nil {
		b.err = err
		return b
	}

	b.tables[tableName] = true
	b.op.addItems(tableName, items)
	return b
}

// Run executes every put operation in
// this batch. This may return a BatchError.
func (b *BatchPut) Run() error {
	const maxPutsPerOp = 25

	if b.err != nil {
		return b.err
	}

	op := b.op
	citems := map[string][]dbitem{}
	for !op.isEmpty() || len(citems) > 0 {
		citems = op.collectItems(maxPutsPerOp, citems)

		// Add processed items to the request
		reqItems := map[string][]*db.WriteRequest{}
		for table, items := range citems {
			writeReqs := make([]*db.WriteRequest, len(items))
			for i, item := range items {
				putReq := &db.PutRequest{Item: item}
				writeReq := &db.WriteRequest{PutRequest: putReq}
				writeReqs[i] = writeReq
			}
			reqItems[table] = writeReqs
		}

		// Put items into database
		input := &db.BatchWriteItemInput{
			RequestItems: reqItems,
		}
		resp, err := b.db.BatchWriteItem(input)

		if err != nil {
			return fmt.Errorf("dynami: BatchPut failed (%v)", err)
		}

		unproc := op.unwrap(resp.UnprocessedItems)
		op.processItems(citems, unproc, nil)
		citems = unproc
	}

	return op.errors()
}
