package dynami

import dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"

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
