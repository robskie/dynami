package dynami

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestQuery() {
	assert := suite.Assert()
	require := suite.Require()

	books := []tBook{
		{
			Title:  "Harry Potter and the Goblet of Fire",
			Author: "J.K. Rowling",
			Genre:  "Fantasy",
			Info: tInfo{
				Publisher:     "Scholastic",
				DatePublished: 2002,
			},
		},
		{
			Title:  "Harry Potter and the Sorcerer's Stone",
			Author: "J.K. Rowling",
			Genre:  "Fantasy",
			Info: tInfo{
				Publisher:     "Scholastic",
				DatePublished: 2003,
			},
		},
		{
			Title:  "Life After Life",
			Author: "Jill McCorkle",
			Genre:  "Fiction",
			Info: tInfo{
				Publisher:     "A Shannon Ravenel Book",
				DatePublished: 2013,
			},
		},
		{
			Title:  "Life After Life",
			Author: "Kate Atkinson",
			Genre:  "Science Fiction",
			Info: tInfo{
				Publisher:     "Reagan Arthur Books",
				DatePublished: 2013,
			},
		},
		{
			Title:  "Lord of the Flies",
			Author: "William Golding",
			Genre:  "Adventure",
			Info: tInfo{
				DatePublished: 1954,
			},
		},
		{
			Title:  "The Alchemist",
			Author: "Paulo Coelho",
			Genre:  "Fiction",
			Info: tInfo{
				Publisher:     "HarperCollins",
				DatePublished: 1993,
			},
		},
		{
			Title:  "The Hitchhiker's Guide to the Galaxy",
			Author: "Douglas Adams",
			Genre:  "Science Fiction",
			Info: tInfo{
				DatePublished: 1979,
			},
		},
		{
			Title:  "The Hobbit",
			Author: "J.R.R. Tolkien",
			Genre:  "Adventure",
			Info: tInfo{
				DatePublished: 1937,
				Characters: []string{
					"Bilbo",
					"Gandalf",
					"Smaug",
				},
			},
		},
		{
			Title:  "The Lord of the Rings",
			Author: "J.R.R. Tolkien",
			Genre:  "Adventure",
			Info: tInfo{
				DatePublished: 1966,
				Characters: []string{
					"Frodo",
					"Bilbo",
					"Gandalf",
					"Gollum",
				},
			},
		},
		{
			Title:  "The Stand",
			Author: "Stephen King",
			Genre:  "Horror",
			Info: tInfo{
				DatePublished: 1978,
			},
		},
	}

	sdb := suite.db
	for _, b := range books {
		item, err := dbattribute.MarshalMap(b)
		require.Nil(err)
		item = removeEmptyAttr(item)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Book"),
		})
		require.Nil(err)
	}

	tests := []struct {
		index           string
		hashName        string
		hashValue       interface{}
		rangeFilterExpr string
		rangeFilterVals []interface{}
		filterExpr      string
		filterVals      []interface{}
		desc            bool

		expected []tBook
	}{
		{
			hashName:        "Title",
			hashValue:       "Life After Life",
			rangeFilterExpr: "Author = :val",
			rangeFilterVals: []interface{}{"Kate Atkinson"},

			expected: []tBook{books[3]},
		},
		{
			index:     "AuthorIndex",
			hashName:  "Author",
			hashValue: "J.K. Rowling",

			filterExpr: "Info.DatePublished > :val1 AND Info.DatePublished <= :val2",
			filterVals: []interface{}{2001, 2003},

			expected: []tBook{books[0], books[1]},
		},
		{
			index:     "AuthorIndex",
			hashName:  "Author",
			hashValue: "J.R.R. Tolkien",

			filterExpr: "Info.DatePublished >= :val1 AND Info.DatePublished < :val2",
			filterVals: []interface{}{1937, 1970},

			expected: []tBook{books[7], books[8]},
		},
		{
			index:     "GenreIndex",
			hashName:  "Genre",
			hashValue: "Adventure",

			filterExpr: "Info.DatePublished BETWEEN :val1 AND :val2",
			filterVals: []interface{}{1900, 2000},

			expected: []tBook{books[4], books[7], books[8]},
		},
		{
			index:     "GenreIndex",
			hashName:  "Genre",
			hashValue: "Adventure",

			filterExpr: "attribute_exists(Info.Characters)",

			expected: []tBook{books[7], books[8]},
		},
		{
			index:     "GenreIndex",
			hashName:  "Genre",
			hashValue: "Adventure",

			filterExpr: "attribute_not_exists(Info.Characters)",

			expected: []tBook{books[4]},
		},
		{
			filterExpr: "attribute_type(Info.Publisher, :val)",
			filterVals: []interface{}{"S"},

			expected: []tBook{
				books[0],
				books[1],
				books[2],
				books[3],
				books[5],
			},
		},
		{
			filterExpr: "begins_with(Title, :val1) OR begins_with(Title, :val2)",
			filterVals: []interface{}{"Harry Potter", "Life"},

			expected: []tBook{
				books[0],
				books[1],
				books[2],
				books[3],
			},
		},
		{
			index:      "GenreIndex",
			filterExpr: "contains(Genre, :val)",
			filterVals: []interface{}{"Fiction"},

			expected: []tBook{
				books[2],
				books[3],
				books[5],
				books[6],
			},
		},
		{
			index:      "GenreIndex",
			hashName:   "Genre",
			hashValue:  "Adventure",
			filterExpr: "contains(Info.Characters, :val)",
			filterVals: []interface{}{"Frodo"},

			expected: []tBook{books[8]},
		},
		{
			filterExpr: "size(Title) = :val",
			filterVals: []interface{}{len(books[9].Title)},

			expected: []tBook{books[9]},
		},
		{
			index:      "GenreIndex",
			hashName:   "Genre",
			hashValue:  "Science Fiction",
			filterExpr: "NOT Info.DatePublished > :val",
			filterVals: []interface{}{2000},

			expected: []tBook{books[6]},
		},
	}

	c := suite.client
	for _, tc := range tests {
		q := c.Query("Book").
			Index(tc.index).
			HashFilter(tc.hashName, tc.hashValue).
			RangeFilter(tc.rangeFilterExpr, tc.rangeFilterVals...).
			Filter(tc.filterExpr, tc.filterVals...)

		if tc.desc {
			q.Desc()
		}

		nout := 0
		it := q.Run()
		for it.HasNext() {
			var book tBook
			err := it.Next(&book)
			require.Nil(err)
			assert.Contains(tc.expected, book)

			nout++
		}
		assert.Equal(len(tc.expected), nout)
	}
}

func (suite *DatabaseTestSuite) TestQueryBig() {
	if testing.Short() {
		suite.T().SkipNow()
	}

	assert := suite.Assert()
	require := suite.Require()

	itemSize := 10 << 10
	nitems := (1 << 20) / itemSize

	bigText := randString(itemSize)
	quotes := make([]tQuote, nitems)
	for i := range quotes {
		quotes[i] = tQuote{
			Author: "some famous author",
			Text:   randString(30),
			Topic:  bigText,
			Date:   rand.Int63(),
		}
	}

	sdb := suite.db
	count := 0
	for _, q := range quotes {
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

	fmt.Printf("\rQuerying items...      ")

	// Test query
	c := suite.client
	it := c.Query("Quote").
		HashFilter("Author", "some famous author").
		Consistent().
		Run()

	qout := []tQuote{}
	for it.HasNext() {
		var q tQuote
		err := it.Next(&q)
		require.Nil(err)

		qout = append(qout, q)
	}
	assert.Len(qout, len(quotes))
	for _, q := range quotes {
		require.Contains(qout, q)
	}

	fmt.Printf("\rScanning items...")

	// Test scan
	it = c.Query("Quote").
		Consistent().
		Run()

	sout := []tQuote{}
	for it.HasNext() {
		var q tQuote
		err := it.Next(&q)
		require.Nil(err)

		sout = append(sout, q)
	}
	assert.Len(sout, len(quotes))
	for _, q := range quotes {
		require.Contains(sout, q)
	}

	fmt.Println("\rTest finished. Cleaning up...")
}
