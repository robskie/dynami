package dynami

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

type tItem struct {
	Key   string `dbkey:"hash"`
	Value string
}

func createStreamTable(
	dbc *db.DynamoDB,
	tableName string,
	streamViewType string) error {

	createTableInput := &db.CreateTableInput{
		AttributeDefinitions: []*db.AttributeDefinition{
			{
				AttributeName: aws.String("Key"),
				AttributeType: aws.String(db.ScalarAttributeTypeS),
			},
		},

		KeySchema: []*db.KeySchemaElement{
			{
				AttributeName: aws.String("Key"),
				KeyType:       aws.String(db.KeyTypeHash),
			},
		},

		ProvisionedThroughput: &db.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(2),
			WriteCapacityUnits: aws.Int64(2),
		},

		TableName: aws.String(tableName),

		StreamSpecification: &db.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(streamViewType),
		},
	}

	_, err := dbc.CreateTable(createTableInput)
	if err != nil {
		return err
	}
	err = dbc.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return err
	}

	return nil
}

func (suite *DatabaseTestSuite) TestGetStream() {
	assert := suite.Assert()
	require := suite.Require()

	type tRecord struct {
		RecordType RecordType
		Value      tItem
	}

	nitems := 10
	items := make([]tItem, nitems)
	for i := range items {
		items[i] = tItem{
			Key:   randString(20),
			Value: randString(10),
		}
	}

	err := createStreamTable(
		suite.db,
		"StreamTable",
		db.StreamViewTypeNewAndOldImages,
	)
	require.Nil(err)

	// Add new items
	sdb := suite.db
	for _, q := range items {
		item, err := dbattribute.MarshalMap(q)
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("StreamTable"),
		})
		require.Nil(err)
	}

	c := suite.client
	it, err := c.GetStream("StreamTable")
	require.Nil(err)

	records := []tRecord{}
	for it.HasNext() {
		var q tItem
		rt, err := it.Next(&q)
		require.Nil(err)

		records = append(records, tRecord{rt, q})
	}

	require.Len(records, nitems)
	for i, r := range records {
		assert.Equal(AddedRecord, r.RecordType)
		assert.Equal(items[i], r.Value)
	}

	// Update items
	for i := range items {
		items[i].Value = randString(10)
		item, err := dbattribute.MarshalMap(items[i])
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("StreamTable"),
		})
		require.Nil(err)
	}

	it, err = c.GetStream("StreamTable")
	require.Nil(err)

	records = []tRecord{}
	for it.HasNext() {
		var q tItem
		rt, err := it.Next(&q)
		require.Nil(err)

		records = append(records, tRecord{rt, q})
	}

	require.Len(records, 2*nitems)
	for i, r := range records[nitems:] {
		assert.Equal(UpdatedRecord, r.RecordType)
		assert.Equal(items[i], r.Value)
	}

	// Delete items
	for _, q := range items {
		item, err := dbattribute.MarshalMap(q)
		require.Nil(err)

		// Remove non-key attributes
		delete(item, "Value")

		_, err = sdb.DeleteItem(&db.DeleteItemInput{
			Key:       item,
			TableName: aws.String("StreamTable"),
		})
		require.Nil(err)
	}

	it, err = c.GetStream("StreamTable")
	require.Nil(err)

	records = []tRecord{}
	for it.HasNext() {
		var q tItem
		rt, err := it.Next(&q)
		assert.Nil(err)

		records = append(records, tRecord{rt, q})
	}

	require.Len(records, 3*nitems)
	for i, r := range records[2*nitems:] {
		assert.Equal(DeletedRecord, r.RecordType)
		assert.Equal(items[i], r.Value)
	}
}

func (suite *DatabaseTestSuite) TestGetStreamLive() {
	assert := suite.Assert()
	require := suite.Require()

	items := make([]tItem, 10)
	for i := range items {
		items[i] = tItem{
			Key:   randString(20),
			Value: randString(10),
		}
	}

	err := createStreamTable(
		suite.db,
		"LiveStreamTable",
		db.StreamViewTypeNewAndOldImages,
	)
	require.Nil(err)

	// Start putting items to table
	go func() {
		sdb := suite.db
		for _, q := range items {
			time.Sleep(200 * time.Millisecond)

			item, err := dbattribute.MarshalMap(q)
			require.Nil(err)

			_, err = sdb.PutItem(&db.PutItemInput{
				Item:      item,
				TableName: aws.String("LiveStreamTable"),
			})
			require.Nil(err)
		}

		// Disable stream
		_, err := sdb.UpdateTable(&db.UpdateTableInput{
			TableName: aws.String("LiveStreamTable"),
			StreamSpecification: &db.StreamSpecification{
				StreamEnabled: aws.Bool(false),
			},
		})
		require.Nil(err)
	}()

	// Get live stream records
	done := make(chan bool)
	fetchedItems := []tItem{}
	go func() {
		c := suite.client
		it, err := c.GetStream("LiveStreamTable")
		require.Nil(err)

		for it.WaitNext() {
			var q tItem
			_, err = it.Next(&q)
			require.Nil(err)

			fetchedItems = append(fetchedItems, q)
		}
		done <- true

	}()

	<-done
	assert.Equal(items, fetchedItems)
}

func (suite *DatabaseTestSuite) TestGetStreamBackCompat() {
	if testing.Short() {
		suite.T().SkipNow()
	}

	assert := suite.Assert()
	require := suite.Require()

	// Create keys only stream
	err := createStreamTable(
		suite.db,
		"KeysOnlyTable",
		db.StreamViewTypeKeysOnly,
	)
	require.Nil(err)

	item := tItem{
		Key:   "key1",
		Value: "value1",
	}
	dbitem, err := dbattribute.MarshalMap(item)
	require.Nil(err)

	sdb := suite.db
	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      dbitem,
		TableName: aws.String("KeysOnlyTable"),
	})
	require.Nil(err)

	c := suite.client
	it, err := c.GetStream("KeysOnlyTable")
	require.Nil(err)

	var fetched tItem
	require.True(it.HasNext())
	_, err = it.Next(&fetched)
	require.Nil(err)
	assert.Equal("key1", fetched.Key)

	// Create new image stream
	err = createStreamTable(
		suite.db,
		"NewImageTable",
		db.StreamViewTypeNewImage,
	)
	require.Nil(err)

	item = tItem{
		Key:   "key2",
		Value: "value2",
	}
	dbitem, err = dbattribute.MarshalMap(item)
	require.Nil(err)

	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      dbitem,
		TableName: aws.String("NewImageTable"),
	})
	require.Nil(err)

	it, err = c.GetStream("NewImageTable")
	require.Nil(err)

	require.True(it.HasNext())
	_, err = it.Next(&fetched)
	require.Nil(err)
	assert.Equal(item, fetched)

	// Create old image stream
	err = createStreamTable(
		suite.db,
		"OldImageTable",
		db.StreamViewTypeOldImage,
	)
	require.Nil(err)

	item = tItem{
		Key:   "key3",
		Value: "value3",
	}
	dbitem, err = dbattribute.MarshalMap(item)
	require.Nil(err)

	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      dbitem,
		TableName: aws.String("OldImageTable"),
	})
	require.Nil(err)

	delete(dbitem, "Value")
	_, err = sdb.DeleteItem(&db.DeleteItemInput{
		Key:       dbitem,
		TableName: aws.String("OldImageTable"),
	})

	it, err = c.GetStream("OldImageTable")
	require.Nil(err)

	require.True(it.HasNext())
	rt, err := it.Next(&fetched)
	require.Nil(err)
	assert.Equal(AddedRecord, rt)
	assert.Equal("key3", fetched.Key)

	require.True(it.HasNext())
	rt, err = it.Next(&fetched)
	require.Nil(err)
	assert.Equal(DeletedRecord, rt)
	assert.Equal(item, fetched)
}
