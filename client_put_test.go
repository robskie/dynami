package dynami

import (
	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestPut() {
	assert := suite.Assert()

	origBook := tBook{
		Title:  "The Pillars of the Earth",
		Author: "Ken Follett",
		Genre:  "Fiction",
	}

	c := suite.client
	err := c.PutItem("Book", origBook)
	assert.Nil(err)

	actualBook := tBook{
		Title:  origBook.Title,
		Author: origBook.Author,
	}

	attrs, err := dbattribute.ConvertToMap(actualBook)
	assert.Nil(err)

	sdb := suite.db
	out, err := sdb.GetItem(&db.GetItemInput{
		Key: map[string]*db.AttributeValue{
			"Title":  attrs["Title"],
			"Author": attrs["Author"],
		},
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)

	err = dbattribute.ConvertFromMap(out.Item, &actualBook)
	assert.Nil(err)
	assert.Equal(origBook, actualBook)
}

func (suite *DatabaseTestSuite) TestPutMap() {
	assert := suite.Assert()

	origBook := map[string]interface{}{
		"Title":  "The Pillars of the Earth",
		"Author": "Ken Follett",
		"Genre":  "Fiction",
	}

	c := suite.client
	err := c.PutItem("Book", origBook)
	assert.Nil(err)

	actualBook := map[string]interface{}{
		"Title":  origBook["Title"],
		"Author": origBook["Author"],
	}

	attrs, err := dbattribute.ConvertToMap(actualBook)
	assert.Nil(err)

	sdb := suite.db
	out, err := sdb.GetItem(&db.GetItemInput{
		Key: map[string]*db.AttributeValue{
			"Title":  attrs["Title"],
			"Author": attrs["Author"],
		},
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)

	err = dbattribute.ConvertFromMap(out.Item, &actualBook)
	assert.Nil(err)
	assert.Equal(origBook, actualBook)
}

func (suite *DatabaseTestSuite) TestBatchPut() {
	assert := suite.Assert()

	books := []tBook{
		{
			Title:  "To Kill a Mockingbird",
			Author: "Harper Lee",
			Genre:  "Classics",
		},
		{
			Title:  "Animal Farm",
			Author: "George Orwell",
			Genre:  "Fiction",
		},
		{
			Title:  "A Clockwork Orange",
			Author: "Anthony Burgess",
			Genre:  "Science Fiction",
		},
	}

	// Add duplicate item
	books = append(books, tBook{
		Title:  "To Kill a Mockingbird",
		Author: "Harper Lee",
		Genre:  "Fiction",
	})

	c := suite.client
	err := c.BatchPut("Book", books).Run()
	assert.Nil(err)

	expected := []tBook{
		books[1],
		books[2],
		books[3],
	}

	sdb := suite.db
	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)

	assert.Len(out.Items, len(expected))
	for _, itemAttr := range out.Items {
		var b tBook
		err = dbattribute.ConvertFromMap(itemAttr, &b)
		assert.Contains(expected, b)
	}

	comics := []tBook{
		{
			Title:  "DMZ: On the Ground",
			Author: "Brian Wood",
		},
		{
			Title:  "The Walking Dead",
			Author: "Robert Kirkman",
		},
		{
			Title:  "Fables",
			Author: "Bill Willingham",
		},
	}

	err = c.BatchPut("Comics", comics).Run()
	assert.NotNil(err)
}

func (suite *DatabaseTestSuite) TestBatchPutMap() {
	assert := suite.Assert()

	books := []map[string]interface{}{
		{
			"Title":  "To Kill a Mockingbird",
			"Author": "Harper Lee",
			"Genre":  "Classics",
		},
		{
			"Title":  "Animal Farm",
			"Author": "George Orwell",
			"Genre":  "Fiction",
		},
		{
			"Title":  "A Clockwork Orange",
			"Author": "Anthony Burgess",
			"Genre":  "Science Fiction",
		},
	}

	c := suite.client
	err := c.BatchPut("Book", books).Run()
	assert.Nil(err)

	expected := []map[string]interface{}{
		books[0],
		books[1],
		books[2],
	}

	sdb := suite.db
	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)

	assert.Len(out.Items, len(expected))
	for _, itemAttr := range out.Items {
		var b map[string]interface{}
		err = dbattribute.ConvertFromMap(itemAttr, &b)
		assert.Contains(expected, b)
	}
}

func (suite *DatabaseTestSuite) TestBatchPutMultiTable() {
	assert := suite.Assert()

	randBooks := make([]tBook, 100)
	for i := range randBooks {
		randBooks[i] = tBook{
			Title:  randString(20),
			Author: randString(15),
		}
	}

	randQuotes := make([]tQuote, 30)
	for i := range randQuotes {
		randQuotes[i] = tQuote{
			Author: randString(15),
			Text:   randString(100),
		}
	}

	c := suite.client
	err := c.BatchPut("Book", randBooks).
		Put("Quote", randQuotes).
		Run()
	assert.Nil(err)

	sdb := suite.db
	out, err := sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Book"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)

	assert.Len(out.Items, len(randBooks))
	for _, itemAttr := range out.Items {
		var b tBook
		err = dbattribute.ConvertFromMap(itemAttr, &b)
		assert.Contains(randBooks, b)
	}

	out, err = sdb.Scan(&db.ScanInput{
		TableName:      aws.String("Quote"),
		ConsistentRead: aws.Bool(true),
	})
	assert.Nil(err)

	assert.Len(out.Items, len(randQuotes))
	for _, itemAttr := range out.Items {
		var q tQuote
		err = dbattribute.ConvertFromMap(itemAttr, &q)
		assert.Contains(randQuotes, q)
	}
}
