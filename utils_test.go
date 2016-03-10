package dynamini

import (
	"sort"

	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestGetKey() {
	assert := suite.Assert()

	// Test primary key
	bookA := tBook{
		Title:  "1984",
		Author: "George Orwell",
	}

	item, err := dbattribute.ConvertToMap(bookA)
	assert.Nil(err)

	key, err := getKey(bookA)
	assert.Nil(err)
	assert.Equal("", key.indexName)
	assert.Equal(key.value["Title"], item["Title"])
	assert.Equal(key.value["Author"], item["Author"])

	// Test secondary index
	bookB := tBook{
		Genre: "War",
		Title: "Catch-22",
	}

	item, err = dbattribute.ConvertToMap(bookB)
	assert.Nil(err)

	key, err = getKey(bookB)
	assert.Nil(err)
	assert.Equal("GenreIndex", key.indexName)
	assert.Equal(key.value["Genre"], item["Genre"])
	assert.Equal(key.value["Title"], item["Title"])
}

func (suite *DatabaseTestSuite) TestFieldTags() {
	assert := suite.Assert()

	type tStruct struct {
		Hash  string `dbkey:"hash"`
		Range int    `dbkey:"range"`

		AnotherRange string `dbindex:"range,SecondaryIndex"`
		GlobalHash   string `dbindex:"hash,GlobalIndex"`

		Projected int `dbindex:"project,GlobalIndex"`
	}

	expectedKey := []kelement{
		{
			name:     "Hash",
			keyType:  HashKey,
			attrType: StringType,
		},
		{
			name:     "Range",
			keyType:  RangeKey,
			attrType: NumberType,
		},
	}

	expectedIndices := []ischema{
		{
			name: "SecondaryIndex",
			key: []kelement{
				{
					name:     expectedKey[0].name,
					keyType:  expectedKey[0].keyType,
					attrType: expectedKey[0].attrType,
				},
				{
					name:     "AnotherRange",
					keyType:  RangeKey,
					attrType: StringType,
				},
			},
			projections: []string{
				"AnotherRange",
				"Hash",
				"Range",
			},
			indexType: localIndexType,
		},
		{
			name: "GlobalIndex",
			key: []kelement{
				{
					name:     "GlobalHash",
					keyType:  HashKey,
					attrType: StringType,
				},
			},
			projections: []string{
				"GlobalHash",
				"Hash",
				"Projected",
				"Range",
			},
			indexType: globalIndexType,
		},
	}

	s := getSchema(tStruct{})
	assert.Equal(expectedKey, s.key)
	for _, idx := range s.indices {
		sort.Strings(idx.projections)
		assert.Contains(expectedIndices, idx)
	}
}
