package dynami

import (
	"math/rand"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestGet() {
	assert := suite.Assert()

	book := tBook{
		Title:  "All the King’s Men",
		Author: "Robert Penn Warren",
		Genre:  "Fiction",
	}
	item, err := dbattribute.MarshalMap(book)
	assert.Nil(err)
	item = removeEmptyAttr(item)

	sdb := suite.db
	sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Book"),
	})

	// Fetch by primary key
	bookA := tBook{
		Title:  book.Title,
		Author: book.Author,
	}

	c := suite.client
	consistent := true
	err = c.GetItem("Book", &bookA, consistent)
	assert.Nil(err)
	assert.Equal(book, bookA)

	// Fetch by secondary key
	bookB := tBook{
		Genre: book.Genre,
		Title: book.Title,
	}

	err = c.GetItem("Book", &bookB, consistent)
	assert.Nil(err)
	assert.Equal(book, bookB)
}

func (suite *DatabaseTestSuite) TestGetMap() {
	assert := suite.Assert()

	book := tBook{
		Title:  "All the King’s Men",
		Author: "Robert Penn Warren",
		Genre:  "Fiction",
	}
	item, err := dbattribute.MarshalMap(book)
	assert.Nil(err)
	item = removeEmptyAttr(item)

	sdb := suite.db
	sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Book"),
	})

	// Fetch using map
	bookC := map[string]interface{}{
		"Title":  book.Title,
		"Author": book.Author,
	}

	c := suite.client
	consistent := true
	err = c.GetItem("Book", &bookC, consistent)
	assert.Nil(err)
	assert.Equal(book.Genre, bookC["Genre"])
}

func (suite *DatabaseTestSuite) TestBatchGet() {
	assert := suite.Assert()

	origBooks := []tBook{
		{
			Title:  "The Hitchhiker's Guide to the Galaxy",
			Author: "Douglas Adams",
			Genre:  "Science Fiction",
		},
		{
			Title:  "The Hobbit",
			Author: "J.R.R. Tolkien",
			Genre:  "Adventure",
		},
		{
			Title:  "The Lord of the Rings",
			Author: "J.R.R. Tolkien",
			Genre:  "Adventure",
		},
	}

	sdb := suite.db
	for _, b := range origBooks {
		item, err := dbattribute.MarshalMap(b)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		assert.Nil(err)
	}

	fetchedBooks := make([]tBook, len(origBooks))
	for i, b := range origBooks {
		fetchedBooks[i] = tBook{
			Title:  b.Title,
			Author: b.Author,
		}
	}

	// Add duplicate key
	fetchedBooks = append(fetchedBooks, fetchedBooks[0])

	c := suite.client
	consistent := true
	err := c.BatchGet("Book", fetchedBooks, consistent).Run()
	assert.Nil(err)

	for _, b := range fetchedBooks {
		assert.Contains(origBooks, b)
	}

	// Add unknown key
	fetchedBooks = append(fetchedBooks, tBook{
		Title:  randString(20),
		Author: randString(15),
	})
	err = c.BatchGet("Book", fetchedBooks, consistent).Run()
	assert.NotNil(err)

	batchErr, ok := err.(BatchError)
	assert.True(ok)
	if ok {
		berr := batchErr["Book"][len(fetchedBooks)-1]
		assert.Equal(ErrNoSuchItem, berr)
	}

}

func (suite *DatabaseTestSuite) TestBatchGetMap() {
	assert := suite.Assert()

	origBooks := []tBook{
		{
			Title:  "The Hitchhiker's Guide to the Galaxy",
			Author: "Douglas Adams",
			Genre:  "Science Fiction",
		},
		{
			Title:  "The Hobbit",
			Author: "J.R.R. Tolkien",
			Genre:  "Adventure",
		},
		{
			Title:  "The Lord of the Rings",
			Author: "J.R.R. Tolkien",
			Genre:  "Adventure",
		},
	}

	sdb := suite.db
	for _, b := range origBooks {
		item, err := dbattribute.MarshalMap(b)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		assert.Nil(err)
	}

	// Fetch using map keys
	fetchedBooks := make([]map[string]interface{}, len(origBooks))
	for i, b := range origBooks {
		fetchedBooks[i] = map[string]interface{}{
			"Title":  b.Title,
			"Author": b.Author,
		}
	}

	c := suite.client
	consistent := true
	err := c.BatchGet("Book", fetchedBooks, consistent).Run()
	assert.Nil(err)

	for i, b := range origBooks {
		assert.Equal(b.Genre, fetchedBooks[i]["Genre"])
	}
}

func (suite *DatabaseTestSuite) TestBatchGetMultiTable() {
	assert := suite.Assert()

	randBooks := make([]tBook, 200)
	fetchedBooks := make([]tBook, len(randBooks))
	for i := range randBooks {
		b := tBook{
			Title:  randString(20),
			Author: randString(15),
			Genre:  randString(10),
		}

		randBooks[i] = b
		fetchedBooks[i] = tBook{
			Title:  b.Title,
			Author: b.Author,
		}
	}

	sdb := suite.db
	for _, b := range randBooks {
		item, err := dbattribute.MarshalMap(b)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		assert.Nil(err)
	}

	randQuotes := make([]tQuote, 150)
	fetchedQuotes := make([]tQuote, len(randQuotes))
	for i := range randQuotes {
		q := tQuote{
			Author: randString(15),
			Text:   randString(100),
			Date:   rand.Int63(),
		}

		randQuotes[i] = q
		fetchedQuotes[i] = tQuote{
			Author: q.Author,
			Text:   q.Text,
		}
	}

	for _, q := range randQuotes {
		item, err := dbattribute.MarshalMap(q)
		assert.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	c := suite.client
	consistent := true
	err := c.BatchGet("Book", fetchedBooks, consistent).
		Get("Quote", fetchedQuotes, consistent).
		Run()
	assert.Nil(err)
	assert.Equal(randBooks, fetchedBooks)
	assert.Equal(randQuotes, fetchedQuotes)
}

func (suite *DatabaseTestSuite) TestBatchGetBig() {
	assert := suite.Assert()
	require := suite.Require()

	nitems := 150
	itemSize := 300 << 10
	bigText := randString(itemSize)

	books := make([]tBook, nitems)
	for i := range books {
		books[i] = tBook{
			Title:  randString(20),
			Author: randString(15),
			Genre:  randString(10),
			Info: tInfo{
				Publisher: bigText,
			},
		}
	}

	// Create write requests
	wreqs := make([]*db.WriteRequest, nitems)
	for i, b := range books {
		dbitem, err := dbattribute.MarshalMap(b)
		require.Nil(err)

		wreqs[i] = &db.WriteRequest{
			PutRequest: &db.PutRequest{
				Item: dbitem,
			},
		}
	}

	// Write items
	nputsPerOp := 25
	sdb := suite.db
	unprocs := wreqs
	for len(unprocs) > 0 {
		_, err := sdb.BatchWriteItem(&db.BatchWriteItemInput{
			RequestItems: map[string][]*db.WriteRequest{
				"Book": unprocs[:min(nputsPerOp, len(unprocs))],
			},
		})
		require.Nil(err)

		unprocs = unprocs[min(nputsPerOp, len(unprocs)):]
	}

	fetchedBooks := make([]tBook, nitems)
	for i, b := range books {
		fetchedBooks[i] = tBook{
			Title:  b.Title,
			Author: b.Author,
		}
	}

	c := suite.client
	consistent := true
	err := c.BatchGet("Book", fetchedBooks, consistent).Run()
	assert.Nil(err)
	assert.Equal(books, fetchedBooks)
}
