package dynamini

import (
	"sort"

	sc "github.com/robskie/dynamini/schema"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestNewTable() {
	assert := suite.Assert()

	type tStruct struct {
		Hash  string `dbkey:"hash"`
		Range int    `dbkey:"range"`

		AnotherRange string `dbindex:"range,LocalIndex"`
		GlobalHash   string `dbindex:"hash,GlobalIndex"`

		Projected int `dbindex:"project,GlobalIndex"`
	}

	table := NewTable("TestTable", tStruct{}, map[string]*sc.Throughput{
		"TestTable":   &sc.Throughput{1, 2},
		"GlobalIndex": &sc.Throughput{3, 4},
	})

	assert.Equal("TestTable", table.Name)
	assert.Equal(&sc.Throughput{1, 2}, table.Throughput)

	expectedKeySchema := []sc.KeySchema{
		{"Hash", sc.HashKey},
		{"Range", sc.RangeKey},
	}
	assert.Equal(expectedKeySchema, table.KeySchema)

	expectedAttrs := []sc.AttributeDefinition{
		{"Hash", sc.StringType},
		{"Range", sc.NumberType},
		{"AnotherRange", sc.StringType},
		{"GlobalHash", sc.StringType},
	}
	for _, attr := range table.Attributes {
		assert.Contains(expectedAttrs, attr)
	}
	assert.Len(table.Attributes, len(expectedAttrs))

	expectedLocalIdx := sc.SecondaryIndex{
		Name: "LocalIndex",
		KeySchema: []sc.KeySchema{
			{"Hash", sc.HashKey},
			{"AnotherRange", sc.RangeKey},
		},
		Projection: &sc.Projection{
			Type: sc.ProjectInclude,
			Include: []string{
				"AnotherRange",
				"Hash",
				"Range",
			},
		},
	}
	assert.Len(table.LocalSecondaryIndexes, 1)
	sort.Strings(table.LocalSecondaryIndexes[0].Projection.Include)
	assert.Equal(expectedLocalIdx, table.LocalSecondaryIndexes[0])

	expectedGlobalIdx := sc.SecondaryIndex{
		Name: "GlobalIndex",
		KeySchema: []sc.KeySchema{
			{"GlobalHash", sc.HashKey},
		},
		Projection: &sc.Projection{
			Type: sc.ProjectInclude,
			Include: []string{
				"GlobalHash",
				"Hash",
				"Projected",
				"Range",
			},
		},
		Throughput: &sc.Throughput{3, 4},
	}
	assert.Len(table.GlobalSecondaryIndexes, 1)
	sort.Strings(table.GlobalSecondaryIndexes[0].Projection.Include)
	assert.Equal(expectedGlobalIdx, table.GlobalSecondaryIndexes[0])
}

func (suite *DatabaseTestSuite) TestCreateTable() {
	assert := suite.Assert()

	table := &sc.Table{
		Name:       "TestTable",
		Throughput: &sc.Throughput{1, 2},
		Attributes: []sc.AttributeDefinition{
			{"Hash", sc.StringType},
			{"Range", sc.NumberType},
			{"AnotherRange", sc.StringType},
			{"GlobalHash", sc.StringType},
		},
		KeySchema: []sc.KeySchema{
			{"Hash", sc.HashKey},
			{"Range", sc.RangeKey},
		},
		LocalSecondaryIndexes: []sc.SecondaryIndex{
			{
				Name: "LocalIndex",
				KeySchema: []sc.KeySchema{
					{"Hash", sc.HashKey},
					{"AnotherRange", sc.RangeKey},
				},
				Projection: &sc.Projection{
					Type: sc.ProjectKeysOnly,
				},
			},
		},
		GlobalSecondaryIndexes: []sc.SecondaryIndex{
			{
				Name: "GlobalIndex",
				KeySchema: []sc.KeySchema{
					{"GlobalHash", sc.HashKey},
				},
				Throughput: &sc.Throughput{3, 4},
				Projection: &sc.Projection{
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
	}

	c := suite.client
	err := c.CreateTable(table)
	assert.Nil(err)

	sdb := suite.db
	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	assert.Nil(err)
	assert.Equal(aws.String(db.TableStatusActive), resp.Table.TableStatus)
}

func (suite *DatabaseTestSuite) TestDeleteTable() {
	assert := suite.Assert()

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
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		KeySchema: []*db.KeySchemaElement{
			&db.KeySchemaElement{
				KeyType:       aws.String(db.KeyTypeHash),
				AttributeName: aws.String("Hash"),
			},
		},
	})
	assert.Nil(err)

	err = sdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	assert.Nil(err)

	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	assert.Nil(err)
	assert.Equal(aws.String(db.TableStatusActive), resp.Table.TableStatus)

	c := suite.client
	_, err = c.DeleteTable("TestTable")
	assert.Nil(err)

	_, err = sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String("TestTable"),
	})
	assert.Error(err)
}

func (suite *DatabaseTestSuite) TestClearTable() {
	assert := suite.Assert()

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
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},
		KeySchema: []*db.KeySchemaElement{
			&db.KeySchemaElement{
				KeyType:       aws.String(db.KeyTypeHash),
				AttributeName: aws.String("Hash"),
			},
		},
	})
	assert.Nil(err)

	// Add items
	for i := 0; i < 30; i++ {
		item, err := dbattribute.ConvertToMap(map[string]interface{}{
			"Hash": randString(15),
		})
		assert.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			TableName: aws.String("TestTable"),
			Item:      item,
		})
		assert.Nil(err)
	}

	// Clear table
	c := suite.client
	err = c.ClearTable("TestTable")
	assert.Nil(err)

	// Check if table is cleared
	resp, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("TestTable"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Len(resp.Items, 0)
}

func (suite *DatabaseTestSuite) TestDescribeTable() {
	assert := suite.Assert()

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
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(20),
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
					ReadCapacityUnits:  aws.Int64(30),
					WriteCapacityUnits: aws.Int64(40),
				},
			},
		},
	})
	assert.Nil(err)
	desc := resp.TableDescription

	// Describe table
	c := suite.client
	table, err := c.DescribeTable("TestTable")
	assert.Nil(err)

	// Check returned table
	assert.Equal(*desc.TableName, table.Name)
	assert.Equal(throughput(desc.ProvisionedThroughput), table.Throughput)
	assert.Equal(keySchema(desc.KeySchema), table.KeySchema)

	assert.Equal(
		attributeDefinitions(desc.AttributeDefinitions),
		table.Attributes,
	)

	assert.Equal(
		secondaryIndexes(desc.LocalSecondaryIndexes),
		table.LocalSecondaryIndexes,
	)

	assert.Equal(
		secondaryIndexes(desc.GlobalSecondaryIndexes),
		table.GlobalSecondaryIndexes,
	)
}

func (suite *DatabaseTestSuite) TestListTable() {
	assert := suite.Assert()

	// Clear tables first
	suite.deleteTables()

	tables := make([]string, 101)
	for i := range tables {
		tables[i] = randString(15)
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
				ReadCapacityUnits:  aws.Int64(10),
				WriteCapacityUnits: aws.Int64(10),
			},
			KeySchema: []*db.KeySchemaElement{
				&db.KeySchemaElement{
					KeyType:       aws.String(db.KeyTypeHash),
					AttributeName: aws.String("Hash"),
				},
			},
		})
		assert.Nil(err)
	}

	c := suite.client
	actualTables, err := c.ListTables()
	assert.Nil(err)

	assert.Len(actualTables, len(tables))
	for _, t := range actualTables {
		assert.Contains(tables, t)
	}
}
