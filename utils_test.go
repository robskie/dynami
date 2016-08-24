package dynami

import dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

func (suite *DatabaseTestSuite) TestGetKey() {
	assert := suite.Assert()
	require := suite.Require()

	// Test primary key
	bookA := tBook{
		Title:  "1984",
		Author: "George Orwell",
	}

	item, err := dbattribute.MarshalMap(bookA)
	require.Nil(err)

	key, err := getKey(bookA)
	require.Nil(err)
	assert.Equal("", key.indexName)
	assert.Equal(key.value["Title"], item["Title"])
	assert.Equal(key.value["Author"], item["Author"])

	// Test secondary index
	bookB := tBook{
		Genre: "War",
		Title: "Catch-22",
	}

	item, err = dbattribute.MarshalMap(bookB)
	require.Nil(err)

	key, err = getKey(bookB)
	require.Nil(err)
	assert.Equal("GenreIndex", key.indexName)
	assert.Equal(key.value["Genre"], item["Genre"])
	assert.Equal(key.value["Title"], item["Title"])
}
