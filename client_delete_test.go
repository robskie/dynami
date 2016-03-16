package dynami

import (
	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestDelete() {
	assert := suite.Assert()

	quote := tQuote{
		Text:   "So many books, so little time.",
		Author: "Frank Zappa",
	}

	item, err := dbattribute.ConvertToMap(quote)
	assert.Nil(err)
	item = removeEmptyAttr(item)

	sdb := suite.db
	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Quote"),
	})
	assert.Nil(err)

	c := suite.client
	err = c.Delete("Quote", quote)
	assert.Nil(err)

	key := item
	delete(key, "Date")
	out, err := sdb.GetItem(&db.GetItemInput{
		Key:            key,
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Empty(out.Item)
}

func (suite *DatabaseTestSuite) TestDeleteMap() {
	assert := suite.Assert()

	quote := map[string]interface{}{
		"Text":   "So many books, so little time.",
		"Author": "Frank Zappa",
	}

	item, err := dbattribute.ConvertToMap(quote)
	assert.Nil(err)

	sdb := suite.db
	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Quote"),
	})
	assert.Nil(err)

	c := suite.client
	err = c.Delete("Quote", quote)
	assert.Nil(err)

	key := item
	out, err := sdb.GetItem(&db.GetItemInput{
		Key:            key,
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Empty(out.Item)
}

func (suite *DatabaseTestSuite) TestBatchDelete() {
	assert := suite.Assert()

	quotes := []tQuote{
		{
			Text:   "Be the change that you wish to see in the world.",
			Author: "Mahatma Gandhi",
		},
		{
			Text:   "Without music, life would be a mistake.",
			Author: "Friedrich Nietzsche",
		},
		{
			Text:   "A day without sunshine is like, you know, night.",
			Author: "Steve Martin",
		},
	}

	sdb := suite.db
	for _, q := range quotes {
		item, err := dbattribute.ConvertToMap(q)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	// Add duplicate key
	quotes = append(quotes, quotes[0])

	c := suite.client
	err := c.BatchDelete("Quote", quotes).Run()
	assert.Nil(err)

	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Empty(out.Items)
}

func (suite *DatabaseTestSuite) TestBatchDeleteMap() {
	assert := suite.Assert()

	quotes := []map[string]interface{}{
		{
			"Text":   "Be the change that you wish to see in the world.",
			"Author": "Mahatma Gandhi",
		},
		{
			"Text":   "Without music, life would be a mistake.",
			"Author": "Friedrich Nietzsche",
		},
		{
			"Text":   "A day without sunshine is like, you know, night.",
			"Author": "Steve Martin",
		},
	}

	sdb := suite.db
	for _, q := range quotes {
		item, err := dbattribute.ConvertToMap(q)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	c := suite.client
	err := c.BatchDelete("Quote", quotes).Run()
	assert.Nil(err)

	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Empty(out.Items)
}

func (suite *DatabaseTestSuite) TestBatchDeleteMultiTable() {
	assert := suite.Assert()

	randBooks := make([]tBook, 100)
	for i := range randBooks {
		randBooks[i] = tBook{
			Title:  randString(20),
			Author: randString(15),
		}
	}

	sdb := suite.db
	for _, b := range randBooks {
		item, err := dbattribute.ConvertToMap(b)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		assert.Nil(err)
	}

	randQuotes := make([]tQuote, 30)
	for i := range randQuotes {
		randQuotes[i] = tQuote{
			Author: randString(15),
			Text:   randString(100),
		}
	}

	for _, q := range randQuotes {
		item, err := dbattribute.ConvertToMap(q)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	randBooks = append(randBooks, tBook{
		Title:  randString(20),
		Author: randString(15),
	})

	c := suite.client
	err := c.BatchDelete("Book", randBooks).
		Delete("Quote", randQuotes).
		Run()
	assert.Nil(err)

	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Empty(out.Items)

	out, err = sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)
	assert.Empty(out.Items)
}
