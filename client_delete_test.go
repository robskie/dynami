package dynami

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestDelete() {
	assert := suite.Assert()
	require := suite.Require()

	quote := tQuote{
		Text:   "So many books, so little time.",
		Author: "Frank Zappa",
	}

	item, err := dbattribute.MarshalMap(quote)
	require.Nil(err)

	sdb := suite.db
	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Quote"),
	})
	require.Nil(err)

	c := suite.client
	err = c.DeleteItem("Quote", quote)
	require.Nil(err)

	key := item
	delete(key, "Topic")
	delete(key, "Date")

	out, err := sdb.GetItem(&db.GetItemInput{
		Key:            key,
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Empty(out.Item)
}

func (suite *DatabaseTestSuite) TestDeleteMap() {
	assert := suite.Assert()
	require := suite.Require()

	quote := map[string]interface{}{
		"Text":   "So many books, so little time.",
		"Author": "Frank Zappa",
	}

	item, err := dbattribute.MarshalMap(quote)
	require.Nil(err)

	sdb := suite.db
	_, err = sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Quote"),
	})
	require.Nil(err)

	c := suite.client
	err = c.DeleteItem("Quote", quote)
	require.Nil(err)

	out, err := sdb.GetItem(&db.GetItemInput{
		Key:            item,
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Empty(out.Item)
}

func (suite *DatabaseTestSuite) TestBatchDelete() {
	assert := suite.Assert()
	require := suite.Require()

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
		item, err := dbattribute.MarshalMap(q)
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		require.Nil(err)
	}

	// Add duplicate key
	quotes = append(quotes, quotes[0])

	c := suite.client
	err := c.BatchDelete("Quote", quotes).Run()
	require.Nil(err)

	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Empty(out.Items)
}

func (suite *DatabaseTestSuite) TestBatchDeleteMap() {
	assert := suite.Assert()
	require := suite.Require()

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
		item, err := dbattribute.MarshalMap(q)
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		require.Nil(err)
	}

	c := suite.client
	err := c.BatchDelete("Quote", quotes).Run()
	require.Nil(err)

	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Empty(out.Items)
}

func (suite *DatabaseTestSuite) TestBatchDeleteMultiTable() {
	if testing.Short() {
		suite.T().SkipNow()
	}

	assert := suite.Assert()
	require := suite.Require()

	randBooks := make([]tBook, 50)
	for i := range randBooks {
		randBooks[i] = tBook{
			Title:  randString(20),
			Author: randString(15),
			Genre:  randString(15),
		}
	}

	randQuotes := make([]tQuote, 150)
	for i := range randQuotes {
		randQuotes[i] = tQuote{
			Author: randString(15),
			Text:   randString(100),
		}
	}

	count := 0
	nitems := len(randBooks) + len(randQuotes)

	sdb := suite.db
	for _, b := range randBooks {
		item, err := dbattribute.MarshalMap(b)
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		require.Nil(err)

		count++
		fmt.Printf("\rAdding items (%d/%d)", count, nitems)
	}

	for _, q := range randQuotes {
		item, err := dbattribute.MarshalMap(q)
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		require.Nil(err)

		count++
		fmt.Printf("\rAdding items (%d/%d)", count, nitems)
	}

	randBooks = append(randBooks, tBook{
		Title:  randString(20),
		Author: randString(15),
	})

	fmt.Printf("\rDeleting items...      ")

	c := suite.client
	err := c.BatchDelete("Book", randBooks).
		Delete("Quote", randQuotes).
		Run()
	require.Nil(err)

	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Empty(out.Items)

	out, err = sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	require.Nil(err)
	assert.Empty(out.Items)

	fmt.Println("\rTest finished. Cleaning up...")
}
