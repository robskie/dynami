package dynami

import (
	"errors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
)

var (
	// ErrNoSuchItem is returned when no item is found for the given key.
	ErrNoSuchItem = errors.New("dynami: no such item")
)

// Client represents a DynamoDB client.
type Client struct {
	db      *db.DynamoDB
	session *session.Session
}

// NewClient creates a new client from the given credentials.
func NewClient(region string, id string, key string) *Client {
	session := session.New(&aws.Config{
		Region:      aws.String(region),
		Credentials: credentials.NewStaticCredentials(id, key, ""),
	})

	db := db.New(session)
	return &Client{db: db, session: session}
}
