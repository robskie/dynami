package dynami

import (
	"log"
	"os"
	"os/exec"
	"os/user"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/awstesting/unit"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
)

type tInfo struct {
	Publisher     string
	DatePublished int

	Characters []string
}

type tBook struct {
	Title  string `dbkey:"hash" dbindex:"range,AuthorIndex,range,GenreIndex"`
	Author string `dbkey:"range" dbindex:"hash,AuthorIndex"`
	Genre  string `dbindex:"hash,GenreIndex,project,AuthorIndex"`
	Info   tInfo  `dbindex:"project,AuthorIndex,project,GenreIndex"`
}

type tQuote struct {
	Author string `dbkey:"hash"`
	Text   string `dbkey:"range"`
	Date   int64
}

type DatabaseTestSuite struct {
	suite.Suite

	dbProc *os.Process

	db     *db.DynamoDB
	client *Client

	tableName string
}

func (suite *DatabaseTestSuite) deleteTables() {
	sdb := suite.db
	resp, err := sdb.ListTables(&db.ListTablesInput{})
	if err != nil {
		log.Fatal(err)
	}

	for _, tableName := range resp.TableNames {
		_, err = sdb.DeleteTable(&db.DeleteTableInput{
			TableName: tableName,
		})
		if err != nil {
			log.Fatal(err)
		}

		err = sdb.WaitUntilTableNotExists(&db.DescribeTableInput{
			TableName: tableName,
		})
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (suite *DatabaseTestSuite) createQuoteTable() {
	createTableInput := &db.CreateTableInput{
		AttributeDefinitions: []*db.AttributeDefinition{
			{
				AttributeName: aws.String("Author"),
				AttributeType: aws.String(db.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Text"),
				AttributeType: aws.String(db.ScalarAttributeTypeS),
			},
		},

		KeySchema: []*db.KeySchemaElement{
			{
				AttributeName: aws.String("Author"),
				KeyType:       aws.String(db.KeyTypeHash),
			},
			{
				AttributeName: aws.String("Text"),
				KeyType:       aws.String(db.KeyTypeRange),
			},
		},

		ProvisionedThroughput: &db.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},

		TableName: aws.String("Quote"),

		StreamSpecification: &db.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(db.StreamViewTypeNewAndOldImages),
		},
	}

	_, err := suite.db.CreateTable(createTableInput)
	if err != nil {
		log.Fatal(err)
	}
}

func (suite *DatabaseTestSuite) createBookTable() {
	createTableInput := &db.CreateTableInput{
		AttributeDefinitions: []*db.AttributeDefinition{
			{
				AttributeName: aws.String("Title"),
				AttributeType: aws.String(db.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Author"),
				AttributeType: aws.String(db.ScalarAttributeTypeS),
			},
			{
				AttributeName: aws.String("Genre"),
				AttributeType: aws.String(db.ScalarAttributeTypeS),
			},
		},

		KeySchema: []*db.KeySchemaElement{
			{
				AttributeName: aws.String("Title"),
				KeyType:       aws.String(db.KeyTypeHash),
			},
			{
				AttributeName: aws.String("Author"),
				KeyType:       aws.String(db.KeyTypeRange),
			},
		},

		GlobalSecondaryIndexes: []*db.GlobalSecondaryIndex{
			&db.GlobalSecondaryIndex{
				ProvisionedThroughput: &db.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},

				Projection: &db.Projection{
					ProjectionType: aws.String(db.ProjectionTypeAll),
				},
				IndexName: aws.String("AuthorIndex"),
				KeySchema: []*db.KeySchemaElement{
					{
						AttributeName: aws.String("Author"),
						KeyType:       aws.String(db.KeyTypeHash),
					},
					{
						AttributeName: aws.String("Title"),
						KeyType:       aws.String(db.KeyTypeRange),
					},
				},
			},

			&db.GlobalSecondaryIndex{
				ProvisionedThroughput: &db.ProvisionedThroughput{
					ReadCapacityUnits:  aws.Int64(1),
					WriteCapacityUnits: aws.Int64(1),
				},

				Projection: &db.Projection{
					ProjectionType: aws.String(db.ProjectionTypeAll),
				},
				IndexName: aws.String("GenreIndex"),
				KeySchema: []*db.KeySchemaElement{
					{
						AttributeName: aws.String("Genre"),
						KeyType:       aws.String(db.KeyTypeHash),
					},
					{
						AttributeName: aws.String("Title"),
						KeyType:       aws.String(db.KeyTypeRange),
					},
				},
			},
		},

		ProvisionedThroughput: &db.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(1),
			WriteCapacityUnits: aws.Int64(1),
		},

		TableName: aws.String("Book"),
	}

	_, err := suite.db.CreateTable(createTableInput)
	if err != nil {
		log.Fatal(err)
	}
}

func (suite *DatabaseTestSuite) SetupSuite() {
	// Start local dynamoDB instance
	usr, _ := user.Current()
	cmd := &exec.Cmd{
		Path: "/usr/bin/java",
		Args: []string{
			"-Djava.library.path=./DynamoDBLocal_lib",
			"-jar",
			"DynamoDBLocal.jar",
			"-sharedDb",
			"-inMemory",
		},
		Dir: usr.HomeDir + "/DynamoDBLocal",
	}
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}
	suite.dbProc = cmd.Process

	// Create client
	session := unit.Session.Copy(
		&aws.Config{
			Endpoint: aws.String("http://localhost:8000"),
		},
	)
	suite.db = db.New(session)

	testRegion := &Region{
		"test-region",
		"http://localhost:8000",
		"http://localhost:8000",
	}
	suite.client = NewClient(testRegion, "test-id", "test-key")
}

func (suite *DatabaseTestSuite) TearDownSuite() {
	if suite.dbProc != nil {
		suite.dbProc.Kill()
	}
}

func (suite *DatabaseTestSuite) SetupTest() {
	suite.createBookTable()
	suite.createQuoteTable()
}

func (suite *DatabaseTestSuite) TearDownTest() {
	suite.deleteTables()
}

func TestDatabaseTestSuite(t *testing.T) {
	suite.Run(t, new(DatabaseTestSuite))
}
