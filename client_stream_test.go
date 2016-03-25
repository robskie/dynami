package dynami

import (
	"math/rand"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

func (suite *DatabaseTestSuite) TestGetStream() {
	assert := suite.Assert()
	type tRecord struct {
		RecordType RecordType
		Value      tQuote
	}

	nitems := 10
	randQuotes := make([]tQuote, nitems)
	for i := range randQuotes {
		randQuotes[i] = tQuote{
			Author: randString(15),
			Text:   randString(100),
			Date:   rand.Int63(),
		}
	}

	// Add new items
	sdb := suite.db
	for _, q := range randQuotes {
		item, err := dbattribute.ConvertToMap(q)
		assert.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	c := suite.client
	it, err := c.GetStream("Quote")
	assert.Nil(err)

	records := []tRecord{}
	for it.HasNext() {
		var q tQuote
		rt, err := it.Next(&q)
		assert.Nil(err)

		records = append(records, tRecord{rt, q})
	}

	for i, r := range records {
		assert.Equal(AddedRecord, r.RecordType)
		assert.Equal(randQuotes[i], r.Value)
	}

	// Update items
	for i := range randQuotes {
		randQuotes[i].Date = rand.Int63()
		item, err := dbattribute.ConvertToMap(randQuotes[i])
		assert.Nil(err)

		_, err = sdb.PutItem(&db.PutItemInput{
			Item:      item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	it, err = c.GetStream("Quote")
	assert.Nil(err)

	records = []tRecord{}
	for it.HasNext() {
		var q tQuote
		rt, err := it.Next(&q)
		assert.Nil(err)

		records = append(records, tRecord{rt, q})
	}

	for i, r := range records[nitems:] {
		assert.Equal(UpdatedRecord, r.RecordType)
		assert.Equal(randQuotes[i], r.Value)
	}

	// Delete items
	for _, q := range randQuotes {
		item, err := dbattribute.ConvertToMap(q)
		assert.Nil(err)
		delete(item, "Date")

		_, err = sdb.DeleteItem(&db.DeleteItemInput{
			Key:       item,
			TableName: aws.String("Quote"),
		})
		assert.Nil(err)
	}

	it, err = c.GetStream("Quote")
	assert.Nil(err)

	records = []tRecord{}
	for it.HasNext() {
		var q tQuote
		rt, err := it.Next(&q)
		assert.Nil(err)

		records = append(records, tRecord{rt, q})
	}

	for i, r := range records[2*nitems:] {
		assert.Equal(DeletedRecord, r.RecordType)
		assert.Equal(randQuotes[i], r.Value)
	}
}

func (suite *DatabaseTestSuite) TestGetStreamLive() {
	assert := suite.Assert()

	randQuotes := make([]tQuote, 10)
	for i := range randQuotes {
		randQuotes[i] = tQuote{
			Author: randString(15),
			Text:   randString(100),
			Date:   rand.Int63(),
		}
	}

	// Start putting items to table
	go func() {
		sdb := suite.db
		for _, q := range randQuotes {
			time.Sleep(200 * time.Millisecond)

			item, err := dbattribute.ConvertToMap(q)
			assert.Nil(err)

			_, err = sdb.PutItem(&db.PutItemInput{
				Item:      item,
				TableName: aws.String("Quote"),
			})
			assert.Nil(err)
		}

		// Disable stream
		_, err := sdb.UpdateTable(&db.UpdateTableInput{
			TableName: aws.String("Quote"),
			StreamSpecification: &db.StreamSpecification{
				StreamEnabled: aws.Bool(false),
			},
		})
		assert.Nil(err)
	}()

	// Get live stream records
	done := make(chan bool)
	fetchedQuotes := []tQuote{}
	go func() {
		c := suite.client
		it, err := c.GetStream("Quote")
		assert.Nil(err)

		for it.WaitNext() {
			var q tQuote
			_, err = it.Next(&q)
			assert.Nil(err)

			fetchedQuotes = append(fetchedQuotes, q)
		}
		done <- true

	}()

	<-done
	assert.Equal(randQuotes, fetchedQuotes)
}