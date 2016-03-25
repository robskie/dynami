package dynami

import (
	sc "github.com/robskie/dynami/schema"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestCreateTable() {
	assert := suite.Assert()

	table := &sc.Table{
		Name: "TestTable",
		Throughput: &sc.Throughput{
			Read:  1,
			Write: 2,
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
				Projection: &sc.Projection{
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
				Throughput: &sc.Throughput{
					Read:  3,
					Write: 4,
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
			},
		},

		StreamEnabled: true,
	}
	table.GlobalSecondaryIndexes[0].PStatus = sc.ActiveStatus

	c := suite.client
	err := c.CreateTable(table)
	assert.Nil(err)

	sdb := suite.db
	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	assert.Nil(err)

	desc := resp.Table
	assert.Equal(aws.String(db.TableStatusActive), desc.TableStatus)

	// Check returned table
	assert.Equal(table.Name, *desc.TableName)
	assert.Equal(table.Throughput, throughput(desc.ProvisionedThroughput))
	assert.Equal(table.Key, keySchema(desc.KeySchema))

	assert.Equal(
		table.Attributes,
		attributeDefinitions(desc.AttributeDefinitions),
	)

	assert.Equal(
		table.LocalSecondaryIndexes,
		secondaryIndexes(desc.LocalSecondaryIndexes),
	)

	assert.Equal(
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
	assert.Equal(keySchema(desc.KeySchema), table.Key)

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

	assert.Equal(
		streamEnabled(desc.StreamSpecification),
		table.StreamEnabled,
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

func (suite *DatabaseTestSuite) TestUpdateTable() {
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
			{
				Name: "GlobalHashC",
				Type: sc.NumberType,
			},
		},
		Throughput: &sc.Throughput{
			Read:  10,
			Write: 20,
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
				Projection: &sc.Projection{
					Type: sc.ProjectKeysOnly,
				},
				Throughput: &sc.Throughput{
					Read:  20,
					Write: 30,
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
				Projection: &sc.Projection{
					Type: sc.ProjectKeysOnly,
				},
				Throughput: &sc.Throughput{
					Read:  30,
					Write: 40,
				},
			},
			{
				Name: "GlobalIndexC",
				Key: []sc.Key{
					{
						Name: "GlobalHashC",
						Type: sc.HashKey,
					},
				},
				Projection: &sc.Projection{
					Type: sc.ProjectAll,
				},
				Throughput: &sc.Throughput{
					Read:  40,
					Write: 50,
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

	// Enable stream
	table.StreamEnabled = true

	// Update table throughput
	table.Throughput = &sc.Throughput{
		Read:  42,
		Write: 52,
	}

	// Remove GlobalIndexA
	table.RemoveGlobalSecondaryIndex("GlobalIndexA")

	// Update GlobalIndexB
	idxB := table.GetGlobalSecondaryIndex("GlobalIndexB")
	idxB.Throughput = &sc.Throughput{
		Read:  50,
		Write: 60,
	}
	idxB.PStatus = sc.ActiveStatus
	table.AddGlobalSecondaryIndex(idxB)

	// Update GlobalIndexC
	idxC := table.GetGlobalSecondaryIndex("GlobalIndexC")
	idxC.Throughput = &sc.Throughput{
		Read:  60,
		Write: 70,
	}
	idxC.PStatus = sc.ActiveStatus
	table.AddGlobalSecondaryIndex(idxC)

	// Add GlobalIndexD
	idxD := &sc.SecondaryIndex{
		Name: "GlobalIndexD",
		Key: []sc.Key{
			{
				Name: "GlobalHashD",
				Type: sc.HashKey,
			},
		},
		Projection: &sc.Projection{
			Type: sc.ProjectAll,
		},
		Throughput: &sc.Throughput{
			Read:  70,
			Write: 80,
		},
	}
	idxD.PStatus = sc.ActiveStatus
	table.AddGlobalSecondaryIndex(idxD)
	table.AddAttributes([]sc.Attribute{
		{
			Name: "GlobalHashD",
			Type: sc.NumberType,
		},
	})

	// Perform update
	c := suite.client
	err = c.UpdateTable(table)
	assert.Nil(err)

	resp, err := sdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	assert.Nil(err)

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
