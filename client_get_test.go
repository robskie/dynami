package dynami

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestGet() {
	assert := suite.Assert()
	require := suite.Require()

	book := tBook{
		Title:  "All the King’s Men",
		Author: "Robert Penn Warren",
		Genre:  "Fiction",
	}
	item, err := dbattribute.MarshalMap(book)
	require.Nil(err)

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
	require.Nil(err)
	assert.Equal(book, bookA)

	// Fetch by secondary key
	bookB := tBook{
		Genre: book.Genre,
		Title: book.Title,
	}

	err = c.GetItem("Book", &bookB, consistent)
	require.Nil(err)
	assert.Equal(book, bookB)
}

func (suite *DatabaseTestSuite) TestGetMap() {
	assert := suite.Assert()
	require := suite.Require()

	book := tBook{
		Title:  "All the King’s Men",
		Author: "Robert Penn Warren",
		Genre:  "Fiction",
	}
	item, err := dbattribute.MarshalMap(book)
	require.Nil(err)

	sdb := suite.db
	sdb.PutItem(&db.PutItemInput{
		Item:      item,
		TableName: aws.String("Book"),
	})

	// Fetch using map
	fbook := map[string]interface{}{
		"Title":  book.Title,
		"Author": book.Author,
	}

	c := suite.client
	consistent := true
	err = c.GetItem("Book", &fbook, consistent)
	require.Nil(err)
	assert.Equal(book.Genre, fbook["Genre"])
}

func (suite *DatabaseTestSuite) TestBatchGet() {
	assert := suite.Assert()
	require := suite.Require()

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
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		require.Nil(err)
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
	require.Nil(err)

	for _, b := range fetchedBooks {
		assert.Contains(origBooks, b)
	}

	// Add unknown key
	fetchedBooks = append(fetchedBooks, tBook{
		Title:  randString(20),
		Author: randString(15),
	})
	err = c.BatchGet("Book", fetchedBooks, consistent).Run()
	require.NotNil(err)

	batchErr, ok := err.(BatchError)
	require.True(ok)
	berr := batchErr["Book"][len(fetchedBooks)-1]
	assert.Equal(ErrNoSuchItem, berr)
}

func (suite *DatabaseTestSuite) TestBatchGetMap() {
	assert := suite.Assert()
	require := suite.Require()

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
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		require.Nil(err)
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
	require.Nil(err)

	for i, b := range origBooks {
		assert.Equal(b.Genre, fetchedBooks[i]["Genre"])
	}
}

func (suite *DatabaseTestSuite) TestBatchGetMultiTable() {
	if testing.Short() {
		suite.T().SkipNow()
	}

	assert := suite.Assert()
	require := suite.Require()

	randBooks := make([]tBook, 50)
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

	fmt.Printf("\rFetching items...      ")

	c := suite.client
	consistent := true
	err := c.BatchGet("Book", fetchedBooks, consistent).
		Get("Quote", fetchedQuotes, consistent).
		Run()
	require.Nil(err)
	assert.Equal(randBooks, fetchedBooks)
	assert.Equal(randQuotes, fetchedQuotes)

	fmt.Println("\rTest finished. Cleaning up...")
}

func (suite *DatabaseTestSuite) TestBatchGetBig() {
	if testing.Short() {
		suite.T().SkipNow()
	}

	assert := suite.Assert()
	require := suite.Require()

	nitems := 10
	itemSize := 300 << 10
	bigText := randString(itemSize)

	quotes := make([]tQuote, nitems)
	for i := range quotes {
		quotes[i] = tQuote{
			Author: randString(15),
			Text:   randString(100),
			Topic:  bigText,
			Date:   rand.Int63(),
		}
	}

	count := 0
	sdb := suite.db
	for _, q := range quotes {
		item, err := dbattribute.MarshalMap(q)
		require.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		require.Nil(err)

		count++
		fmt.Printf("\rAdding items (%d/%d)", count, len(quotes))
	}

	fetchedQuotes := make([]tQuote, nitems)
	for i, q := range quotes {
		fetchedQuotes[i] = tQuote{
			Author: q.Author,
			Text:   q.Text,
		}
	}

	fmt.Printf("\rFetching items...      ")

	c := suite.client
	consistent := true
	err := c.BatchGet("Quote", fetchedQuotes, consistent).Run()
	require.Nil(err)
	assert.Equal(quotes, fetchedQuotes)

	fmt.Println("\rTest finished. Cleaning up...")
}
