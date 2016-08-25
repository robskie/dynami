package dynami

import (
	"strconv"
	"testing"

	sc "github.com/robskie/dynami/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func assertEqualIndices(
	t *testing.T,
	expected []sc.SecondaryIndex,
	actual []sc.SecondaryIndex) {

	require.Len(t, actual, len(expected))
	aidxs := make(map[string]*sc.SecondaryIndex, len(actual))
	for i, idx := range actual {
		aidxs[idx.Name] = &actual[i]
	}

	for _, idx := range expected {
		aidx := aidxs[idx.Name]
		require.NotNil(t, aidx)

		assert.Equal(t, idx.Key, aidx.Key)
		assert.Equal(t, idx.Throughput, aidx.Throughput)

		assert.Equal(t, idx.Size(), aidx.Size())
		assert.Equal(t, idx.ItemCount(), aidx.ItemCount())
		assert.Equal(t, idx.Status(), aidx.Status())

		assert.Equal(t, idx.Projection.Type, aidx.Projection.Type)
		for _, inc := range idx.Projection.Include {
			assert.Contains(t, aidx.Projection.Include, inc)
		}
	}
}

func (suite *DatabaseTestSuite) TestCreateTable() {
	assert := suite.Assert()
	require := suite.Require()

	table := &sc.Table{
		Name: "TestTable",
		Throughput: sc.Throughput{
			Read:  1,
			Write: 1,
		},
		Attributes: []sc.Attribute{
			{"Hash", sc.StringType},
			{"Range", sc.NumberType},
			{"AnotherRange", sc.StringType},
			{"GlobalHash", sc.StringType},
		},
		Key: []sc.Key{
			{"Hash", sc.HashKey},
			{"Range", sc.RangeKey},
		},
		LocalSecondaryIndexes: []sc.SecondaryIndex{
			{
				Name: "LocalIndex",
				Key: []sc.Key{
					{"Hash", sc.HashKey},
					{"AnotherRange", sc.RangeKey},
				},
				Projection: sc.Projection{
					Type: sc.ProjectKeysOnly,
				},
			},
		},
		GlobalSecondaryIndexes: []sc.SecondaryIndex{
			{
				Name: "GlobalIndex",
				Key: []sc.Key{
					{
						Name: "GlobalHash",
						Type: sc.HashKey,
					},
				},
				Throughput: sc.Throughput{
					Read:  1,
					Write: 1,
				},
				Projection: sc.Projection{
					Type: sc.ProjectInclude,
					Include: []string{
						"GlobalHash",
						"Hash",
						"Projected",
						"Range",
					},
				},
			},
		},

		StreamEnabled: true,
	}
	table.GlobalSecondaryIndexes[0].PStatus = sc.ActiveStatus

	c := suite.client
	err := c.CreateTable(table)
	require.Nil(err)

	sdb := suite.db
	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	require.Nil(err)

	desc := resp.Table
	assert.Equal(aws.String(db.TableStatusActive), desc.TableStatus)

	// Check returned table
	assert.Equal(table.Name, *desc.TableName)
	assert.Equal(table.Throughput, throughput(desc.ProvisionedThroughput))
	assert.Equal(table.Key, keySchema(desc.KeySchema))

	attributeDefs := attributeDefinitions(desc.AttributeDefinitions)
	for _, attr := range table.Attributes {
		assert.Contains(attributeDefs, attr)
	}

	assertEqualIndices(
		suite.T(),
		table.LocalSecondaryIndexes,
		secondaryIndexes(desc.LocalSecondaryIndexes),
	)

	assertEqualIndices(
		suite.T(),
		table.GlobalSecondaryIndexes,
		secondaryIndexes(desc.GlobalSecondaryIndexes),
	)

	assert.Equal(
		table.StreamEnabled,
		streamEnabled(desc.StreamSpecification),
	)
}

func (suite *DatabaseTestSuite) TestDeleteTable() {
	assert := suite.Assert()
	require := suite.Require()

	sdb := suite.db
	_, err := sdb.CreateTable(&db.CreateTableInput{
		TableName: aws.String("TestTable"),
		AttributeDefinitions: []*db.AttributeDefinition{
			{
				AttributeType: aws.String(db.ScalarAttributeTypeS),
				AttributeName: aws.String("Hash"),
			},
		},
		ProvisionedThroughput: &db.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		KeySchema: []*db.KeySchemaElement{
			&db.KeySchemaElement{
				KeyType:       aws.String(db.KeyTypeHash),
				AttributeName: aws.String("Hash"),
			},
		},
	})
	require.Nil(err)

	err = sdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	require.Nil(err)

	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	require.Nil(err)
	assert.Equal(aws.String(db.TableStatusActive), resp.Table.TableStatus)

	c := suite.client
	_, err = c.DeleteTable("TestTable")
	require.Nil(err)

	_, err = sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	assert.Error(err)
}

func (suite *DatabaseTestSuite) TestClearTable() {
	assert := suite.Assert()
	require := suite.Require()

	// Create table
	sdb := suite.db
	_, err := sdb.CreateTable(&db.CreateTableInput{
		TableName: aws.String("TestTable"),
		AttributeDefinitions: []*db.AttributeDefinition{
			{
				AttributeType: aws.String(db.ScalarAttributeTypeS),
				AttributeName: aws.String("Hash"),
			},
		},
		ProvisionedThroughput: &db.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(3),
		},
		KeySchema: []*db.KeySchemaElement{
			&db.KeySchemaElement{
				KeyType:       aws.String(db.KeyTypeHash),
				AttributeName: aws.String("Hash"),
			},
		},
	})
	require.Nil(err)

	err = sdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	require.Nil(err)

	// Add items
	for i := 0; i < 10; i++ {
		item, err := dbattribute.MarshalMap(map[string]interface{}{
			"Hash": randString(15),
		})
		assert.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			TableName: aws.String("TestTable"),
			Item:      item,
		})
		require.Nil(err)
	}

	// Clear table
	c := suite.client
	err = c.ClearTable("TestTable")
	require.Nil(err)

	// Check if table is cleared
	resp, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("TestTable"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Len(resp.Items, 0)
}

func (suite *DatabaseTestSuite) TestDescribeTable() {
	assert := suite.Assert()
	require := suite.Require()

	// Create table
	sdb := suite.db
	resp, err := sdb.CreateTable(&db.CreateTableInput{
		TableName: aws.String("TestTable"),
		AttributeDefinitions: []*db.AttributeDefinition{
			{
				AttributeType: aws.String(db.ScalarAttributeTypeS),
				AttributeName: aws.String("Hash"),
			},
			{
				AttributeType: aws.String(db.ScalarAttributeTypeN),
				AttributeName: aws.String("Range"),
			},
			{
				AttributeType: aws.String(db.ScalarAttributeTypeS),
				AttributeName: aws.String("AnotherRange"),
			},
			{
				AttributeType: aws.String(db.ScalarAttributeTypeS),
				AttributeName: aws.String("GlobalHash"),
			},
		},
		ProvisionedThroughput: &db.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},
		KeySchema: []*db.KeySchemaElement{
			&db.KeySchemaElement{
				KeyType:       aws.String(db.KeyTypeHash),
				AttributeName: aws.String("Hash"),
			},
			&db.KeySchemaElement{
				KeyType:       aws.String(db.KeyTypeRange),
				AttributeName: aws.String("Range"),
			},
		},
		LocalSecondaryIndexes: []*db.LocalSecondaryIndex{
			&db.LocalSecondaryIndex{
				IndexName: aws.String("LocalIndex"),
				KeySchema: []*db.KeySchemaElement{
					&db.KeySchemaElement{
						KeyType:       aws.String(db.KeyTypeHash),
						AttributeName: aws.String("Hash"),
					},
					&db.KeySchemaElement{
						KeyType:       aws.String(db.KeyTypeRange),
						AttributeName: aws.String("AnotherRange"),
					},
				},
				Projection: &db.Projection{
					ProjectionType: aws.String(db.ProjectionTypeKeysOnly),
				},
			},
		},
		GlobalSecondaryIndexes: []*db.GlobalSecondaryIndex{
			&db.GlobalSecondaryIndex{
				IndexName: aws.String("GlobalIndex"),
				KeySchema: []*db.KeySchemaElement{
					&db.KeySchemaElement{
						KeyType:       aws.String(db.KeyTypeHash),
						AttributeName: aws.String("GlobalHash"),
					},
				},
				Projection: &db.Projection{
					ProjectionType: aws.String(db.ProjectionTypeInclude),
					NonKeyAttributes: []*string{
						aws.String("GlobalHash"),
						aws.String("Hash"),
						aws.String("Projected"),
						aws.String("Range"),
					},
				},
				ProvisionedThroughput: &db.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(2),
					WriteCapacityUnits: aws.Int64(2),
				},
			},
		},
		StreamSpecification: &db.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(db.StreamViewTypeNewAndOldImages),
		},
	})
	require.Nil(err)
	desc := resp.TableDescription
	desc.TableStatus = aws.String(string(sc.ActiveStatus))
	desc.GlobalSecondaryIndexes[0].IndexStatus = aws.String(string(sc.ActiveStatus))

	err = sdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	require.Nil(err)

	// Describe table
	c := suite.client
	table, err := c.DescribeTable("TestTable")
	require.Nil(err)

	// Check returned table
	assert.Equal(*desc.TableName, table.Name)
	assert.Equal(throughput(desc.ProvisionedThroughput), table.Throughput)
	assert.Equal(keySchema(desc.KeySchema), table.Key)

	attributeDefs := attributeDefinitions(desc.AttributeDefinitions)
	for _, attr := range attributeDefs {
		assert.Contains(table.Attributes, attr)
	}

	assertEqualIndices(
		suite.T(),
		secondaryIndexes(desc.LocalSecondaryIndexes),
		table.LocalSecondaryIndexes,
	)

	assertEqualIndices(
		suite.T(),
		secondaryIndexes(desc.GlobalSecondaryIndexes),
		table.GlobalSecondaryIndexes,
	)

	assert.Equal(
		streamEnabled(desc.StreamSpecification),
		table.StreamEnabled,
	)

	assert.EqualValues(*desc.TableSizeBytes, table.Size())
	assert.EqualValues(*desc.ItemCount, table.ItemCount())
	assert.EqualValues(*desc.TableStatus, table.Status())
}

func (suite *DatabaseTestSuite) TestListTable() {
	assert := suite.Assert()
	require := suite.Require()

	tables := make([]string, 5)
	for i := range tables {
		tables[i] = "TestTable" + strconv.Itoa(i+1)
	}

	sdb := suite.db
	for _, tableName := range tables {
		_, err := sdb.CreateTable(&db.CreateTableInput{
			TableName: aws.String(tableName),
			AttributeDefinitions: []*db.AttributeDefinition{
				{
					AttributeType: aws.String(db.ScalarAttributeTypeS),
					AttributeName: aws.String("Hash"),
				},
			},
			ProvisionedThroughput: &db.ProvisionedThroughput{
				ReadCapacityUnits:  aws.Int64(1),
				WriteCapacityUnits: aws.Int64(1),
			},
			KeySchema: []*db.KeySchemaElement{
				&db.KeySchemaElement{
					KeyType:       aws.String(db.KeyTypeHash),
					AttributeName: aws.String("Hash"),
				},
			},
		})
		require.Nil(err)

		err = sdb.WaitUntilTableExists(&db.DescribeTableInput{
			TableName: aws.String(tableName),
		})
		require.Nil(err)
	}

	c := suite.client
	actualTables, err := c.ListTables()
	require.Nil(err)

	for _, t := range tables {
		assert.Contains(actualTables, t)
	}
}

func (suite *DatabaseTestSuite) TestUpdateTable() {
	if testing.Short() {
		suite.T().SkipNow()
	}

	assert := suite.Assert()
	require := suite.Require()

	table := &sc.Table{
		Name: "TestTable",
		Attributes: []sc.Attribute{
			{
				Name: "Hash",
				Type: sc.StringType,
			},
			{
				Name: "Range",
				Type: sc.NumberType,
			},
			{
				Name: "GlobalHashA",
				Type: sc.StringType,
			},
			{
				Name: "GlobalHashB",
				Type: sc.StringType,
			},
		},
		Throughput: sc.Throughput{
			Read:  1,
			Write: 1,
		},
		Key: []sc.Key{
			{
				Name: "Hash",
				Type: sc.HashKey,
			},
			{
				Name: "Range",
				Type: sc.RangeKey,
			},
		},
		GlobalSecondaryIndexes: []sc.SecondaryIndex{
			{
				Name: "GlobalIndexA",
				Key: []sc.Key{
					{
						Name: "GlobalHashA",
						Type: sc.HashKey,
					},
				},
				Projection: sc.Projection{
					Type: sc.ProjectKeysOnly,
				},
				Throughput: sc.Throughput{
					Read:  2,
					Write: 2,
				},
			},
			{
				Name: "GlobalIndexB",
				Key: []sc.Key{
					{
						Name: "GlobalHashB",
						Type: sc.HashKey,
					},
				},
				Projection: sc.Projection{
					Type: sc.ProjectKeysOnly,
				},
				Throughput: sc.Throughput{
					Read:  2,
					Write: 2,
				},
			},
		},
	}

	// Create table
	sdb := suite.db
	_, err := sdb.CreateTable(&db.CreateTableInput{
		TableName:              aws.String("TestTable"),
		AttributeDefinitions:   dbAttributeDefinitions(table.Attributes),
		ProvisionedThroughput:  dbProvisionedThroughput(table.Throughput),
		KeySchema:              dbKeySchema(table.Key),
		GlobalSecondaryIndexes: dbGlobalSecondaryIndexes(table.GlobalSecondaryIndexes),
	})
	require.Nil(err)

	err = sdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	require.Nil(err)

	// Enable stream
	table.StreamEnabled = true

	// Remove GlobalIndexA
	table.RemoveGlobalSecondaryIndex("GlobalIndexA")

	// Update GlobalIndexB
	idxB, err := table.GetGlobalSecondaryIndex("GlobalIndexB")
	require.Nil(err)
	idxB.Throughput = sc.Throughput{
		Read:  1,
		Write: 1,
	}
	idxB.PStatus = sc.ActiveStatus
	table.AddGlobalSecondaryIndex(idxB)

	// Add GlobalIndexC
	idxC := sc.SecondaryIndex{
		Name: "GlobalIndexC",
		Key: []sc.Key{
			{
				Name: "GlobalHashC",
				Type: sc.HashKey,
			},
		},
		Projection: sc.Projection{
			Type: sc.ProjectAll,
		},
		Throughput: sc.Throughput{
			Read:  1,
			Write: 1,
		},
	}
	idxC.PStatus = sc.ActiveStatus
	table.AddGlobalSecondaryIndex(idxC)
	table.AddAttributes([]sc.Attribute{
		{
			Name: "GlobalHashC",
			Type: sc.NumberType,
		},
	})

	// Update table throughput
	table.Throughput = sc.Throughput{
		Read:  2,
		Write: 2,
	}

	// Perform update
	c := suite.client
	err = c.UpdateTable(table)
	require.Nil(err)

	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	require.Nil(err)

	// Check if table is active and updated
	desc := resp.Table
	assert.Equal(table.Throughput, throughput(desc.ProvisionedThroughput))
	assert.Equal(string(sc.ActiveStatus), *desc.TableStatus)

	// Check if stream is enabled
	assert.Equal(table.StreamEnabled, streamEnabled(desc.StreamSpecification))

	// Check if added and updated gsi's are active
	globalIndices := map[string]sc.SecondaryIndex{}
	for _, idx := range secondaryIndexes(desc.GlobalSecondaryIndexes) {
		assert.Equal(sc.ActiveStatus, idx.Status())
		assert.Contains(table.GlobalSecondaryIndexes, idx)

		globalIndices[idx.Name] = idx
	}

	// Check if deleted gsi is actually removed
	_, ok := globalIndices["GlobalIndexA"]
	assert.False(ok)
}
