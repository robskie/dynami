package dynamini

import (
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Get fetches an item from the database. item must
// be a pointer to a map[string]interface{} or pointer
// to a struct. In addition to retrieving items by its
// primary key, it can also get items using its local or
// global secondary index key, whichever is not empty. This
// only applies if item is a struct pointer.
func (c *Client) Get(
	tableName string,
	item interface{}, consistent ...bool) error {

	err := checkPtrType(item, reflect.Struct, map[string]interface{}{})
	if err != nil {
		return err
	}

	key, err := getKey(item)
	if err != nil {
		return err
	}

	var consistentRead *bool
	if len(consistent) > 0 && key.indexType != globalIndexType {
		consistentRead = aws.Bool(consistent[0])
	}

	// Fetch using primary key
	cdb := c.db
	if key.indexName == "" {
		resp, err := cdb.GetItem(&db.GetItemInput{
			Key:            key.value,
			TableName:      aws.String(tableName),
			ConsistentRead: consistentRead,
		})

		if err != nil {
			return fmt.Errorf("dynamini: cannot get item (%v)", err)
		}
		if len(resp.Item) == 0 {
			return ErrNoSuchItem
		}

		err = dbattribute.ConvertFromMap(resp.Item, item)
		if err != nil {
			return fmt.Errorf("dynamini: cannot get item (%v)", err)
		}

		return nil
	}

	// Fetch using index key
	placeHolders := [][2]string{
		{"#N1", ":v1"},
		{"#N2", ":v2"},
	}

	keyExpression := "#N1 = :v1"
	if len(key.value) > 1 {
		keyExpression += " AND #N2 = :v2"
	}

	i := 0
	attributeNames := map[string]*string{}
	attributeValues := map[string]*db.AttributeValue{}
	for name, value := range key.value {
		attributeNames[placeHolders[i][0]] = aws.String(name)
		attributeValues[placeHolders[i][1]] = value
		i++
	}

	queryInput := &db.QueryInput{
		TableName:                 aws.String(tableName),
		IndexName:                 aws.String(key.indexName),
		KeyConditionExpression:    aws.String(keyExpression),
		ExpressionAttributeNames:  attributeNames,
		ExpressionAttributeValues: attributeValues,
		ConsistentRead:            consistentRead,
		Limit:                     aws.Int64(1),
	}

	resp, err := cdb.Query(queryInput)
	if err != nil {
		return fmt.Errorf("dynamini: cannot get item (%v)", err)
	}
	if len(resp.Items) == 0 {
		return ErrNoSuchItem
	}

	err = dbattribute.ConvertFromMap(resp.Items[0], item)
	if err != nil {
		return fmt.Errorf("dynamini: invalid item (%v)", err)
	}

	return nil
}

// BatchGet represents a batch get operation.
// It allows fetching of multiple items from
// multiple tables.
type BatchGet struct {
	db *db.DynamoDB
	op *batchOp

	err        error
	items      map[string]reflect.Value
	consistent map[string]bool
}

// BatchGet queues a batch get operation. items must
// have the same conditions as that in BatchDelete.
func (c *Client) BatchGet(
	tableName string,
	items interface{},
	consistent ...bool) *BatchGet {

	b := &BatchGet{
		db:         c.db,
		op:         newBatchOp(),
		items:      map[string]reflect.Value{},
		consistent: map[string]bool{},
	}

	if err := checkSliceType(items, reflect.Struct, map[string]interface{}{}); err != nil {
		b.err = err
		return b
	}

	b.items[tableName] = reflect.ValueOf(items)
	if len(consistent) > 0 {
		b.consistent[tableName] = consistent[0]
	}

	keysOnly := true
	b.op.addItems(tableName, items, keysOnly)
	return b
}

// Get adds another batch get operation. This can be called multiple
// times with the constraint that a unique tableName is used for each call.
func (b *BatchGet) Get(
	tableName string,
	items interface{},
	consistent ...bool) *BatchGet {

	if b.err != nil {
		return b
	} else if _, ok := b.items[tableName]; ok {
		b.err = fmt.Errorf("dynamini: only one BatchGet operation per table is allowed")
		return b
	} else if err := checkSliceType(items, reflect.Struct, map[string]interface{}{}); err != nil {
		b.err = err
		return b
	}

	b.items[tableName] = reflect.ValueOf(items)
	if len(consistent) > 0 {
		b.consistent[tableName] = consistent[0]
	}

	keysOnly := true
	b.op.addItems(tableName, items, keysOnly)
	return b
}

// Run fetches all the items in this
// batch. This may return a BatchError.
func (b *BatchGet) Run() error {
	const maxGetsPerOp = 100

	if b.err != nil {
		return b.err
	}

	op := b.op
	citems := map[string][]dbitem{}
	for !op.isEmpty() || len(citems) > 0 {
		citems = op.collectItems(maxGetsPerOp, citems)

		// Add processed items to the request
		reqItems := map[string]*db.KeysAndAttributes{}
		for table, items := range citems {
			keys := make([]map[string]*db.AttributeValue, len(items))
			for i, item := range items {
				keys[i] = item
			}

			reqItems[table] = &db.KeysAndAttributes{
				Keys:           keys,
				ConsistentRead: aws.Bool(b.consistent[table]),
			}
		}

		// Get items from database
		input := &db.BatchGetItemInput{
			RequestItems: reqItems,
		}
		resp, err := b.db.BatchGetItem(input)

		if err != nil {
			return fmt.Errorf("dynamini: BatchGet failed (%v)", err)
		}

		proc := op.unwrap(resp.Responses)
		unproc := op.unwrap(resp.UnprocessedKeys)

		op.processItems(
			proc,
			unproc,
			func(table string, idx int, item dbitem) error {
				vslice := b.items[table]
				vitem := vslice.Index(idx)
				if vitem.Kind() != reflect.Ptr {
					vitem = vitem.Addr()
				}

				err := dbattribute.ConvertFromMap(item, vitem.Interface())
				return err
			})
		citems = unproc
	}

	op.flushUnproc(ErrNoSuchItem)
	return op.errors()
}
