package dynami

import (
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbs "github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

var (
	// ErrNoSuchItem is returned when no item is found for the given key.
	ErrNoSuchItem = errors.New("dynami: no such item")
)

// Region defines where DynamoDB services are located.
type Region struct {
	Name string

	// DynamoDB and DynamoDB Streams endpoint
	// URLs (hostname only or fully qualified URI)
	DynamoDBEndpoint        string
	DynamoDBStreamsEndpoint string
}

// These are the list of all supported AWS regions.
var (
	USEast1 = &Region{
		"us-east-1",
		"dynamodb.us-east-1.amazonaws.com",
		"streams.dynamodb.us-east-1.amazonaws.com",
	}

	USWest1 = &Region{
		"us-west-1",
		"dynamodb.us-west-1.amazonaws.com",
		"streams.dynamodb.us-west-1.amazonaws.com",
	}

	USWest2 = &Region{
		"us-west-2",
		"dynamodb.us-west-2.amazonaws.com",
		"streams.dynamodb.us-west-2.amazonaws.com",
	}

	EUWest1 = &Region{
		"eu-west-1",
		"dynamodb.eu-west-1.amazonaws.com",
		"streams.dynamodb.eu-west-1.amazonaws.com",
	}

	EUCentral1 = &Region{
		"eu-central-1",
		"dynamodb.eu-central-1.amazonaws.com",
		"streams.dynamodb.eu-central-1.amazonaws.com",
	}

	APNortheast1 = &Region{
		"ap-northeast-1",
		"dynamodb.ap-northeast-1.amazonaws.com",
		"streams.dynamodb.ap-northeast-1.amazonaws.com",
	}

	APNortheast2 = &Region{
		"ap-northeast-2",
		"dynamodb.ap-northeast-2.amazonaws.com",
		"streams.dynamodb.ap-northeast-2.amazonaws.com",
	}

	APSoutheast1 = &Region{
		"ap-southeast-1",
		"dynamodb.ap-southeast-1.amazonaws.com",
		"streams.dynamodb.ap-southeast-1.amazonaws.com",
	}

	APSoutheast2 = &Region{
		"ap-southeast-2",
		"dynamodb.ap-southeast-2.amazonaws.com",
		"streams.dynamodb.ap-southeast-2.amazonaws.com",
	}

	SAEast1 = &Region{
		"sa-east-1",
		"dynamodb.sa-east-1.amazonaws.com",
		"streams.dynamodb.sa-east-1.amazonaws.com",
	}
)

var regions = map[string]*Region{
	"us-east-1":      USEast1,
	"us-west-1":      USWest1,
	"us-west-2":      USWest2,
	"eu-west-1":      EUWest1,
	"eu-central-1":   EUCentral1,
	"ap-northeast-1": APNortheast1,
	"ap-northeast-2": APNortheast2,
	"ap-southeast-1": APSoutheast1,
	"ap-southeast-2": APSoutheast2,
	"sa-east-1":      SAEast1,
}

// GetRegion returns a new Region object
// given a valid AWS region, eg. "us-east-1".
func GetRegion(name string) *Region {
	reg, ok := regions[name]
	if !ok {
		panic(fmt.Errorf("dynami: unknown region (%v)", name))
	}

	return reg
}

// Client represents a DynamoDB client.
type Client struct {
	db  *db.DynamoDB
	dbs *dbs.DynamoDBStreams
}

// NewClient creates a new client from the given credentials.
func NewClient(region *Region, accessKeyID string, secretAccessKey string) *Client {
	dbSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(region.Name),
		Endpoint:    aws.String(region.DynamoDBEndpoint),
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		panic(fmt.Errorf("dynami: cannot create new client (%v)", err))
	}
	db := db.New(dbSession)

	dbsSession, err := session.NewSession(&aws.Config{
		Region:      aws.String(region.Name),
		Endpoint:    aws.String(region.DynamoDBStreamsEndpoint),
		Credentials: credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
	})
	if err != nil {
		panic(fmt.Errorf("dynami: cannot create new client (%v)", err))
	}
	dbs := dbs.New(dbsSession)

	return &Client{db: db, dbs: dbs}
}
