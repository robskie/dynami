package dynami

import (
	"flag"
	"log"
	"os"
	"os/exec"
	"os/user"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
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
	Topic  string
	Date   int64
}

type DatabaseTestSuite struct {
	suite.Suite

	proc *os.Process

	db     *db.DynamoDB
	client *Client
}

func (suite *DatabaseTestSuite) deleteAllTablesExcept(except ...string) {
	exceptions := map[string]bool{}
	for _, table := range except {
		exceptions[table] = true
	}

	sdb := suite.db
	resp, err := sdb.ListTables(&db.ListTablesInput{})
	if err != nil {
		log.Fatal(err)
	}

	for _, tableName := range resp.TableNames {
		if exceptions[*tableName] {
			continue
		}

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

func (suite *DatabaseTestSuite) deleteAllTables() {
	suite.deleteAllTablesExcept()
}

func (suite *DatabaseTestSuite) clearTable(name string) {
	var attributes []*string
	if name == "Book" {
		attributes = []*string{
			aws.String("Title"),
			aws.String("Author"),
		}
	} else if name == "Quote" {
		attributes = []*string{
			aws.String("Author"),
			aws.String("Text"),
		}
	} else {
		log.Fatal("cannot clear unknown table")
	}

	for {
		var lastKey map[string]*db.AttributeValue
		resp, err := suite.db.Scan(&db.ScanInput{
			TableName:         aws.String(name),
			ConsistentRead:    aws.Bool(true),
			ExclusiveStartKey: lastKey,
			AttributesToGet:   attributes,
		})
		if err != nil {
			log.Fatal(err)
		}

		for _, item := range resp.Items {
			_, err = suite.db.DeleteItem(&db.DeleteItemInput{
				Key:       item,
				TableName: aws.String(name),
			})
			if err != nil {
				log.Fatal(err)
			}
		}

		lastKey = resp.LastEvaluatedKey
		if len(lastKey) == 0 {
			break
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
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(10),
		},

		TableName: aws.String("Quote"),
	}

	_, err := suite.db.CreateTable(createTableInput)
	if err != nil {
		log.Fatal(err)
	}
	err = suite.db.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("Quote"),
	})
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
					ReadCapacityUnits:  aws.Int64(3),
					WriteCapacityUnits: aws.Int64(3),
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
					ReadCapacityUnits:  aws.Int64(3),
					WriteCapacityUnits: aws.Int64(3),
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
			ReadCapacityUnits:  aws.Int64(3),
			WriteCapacityUnits: aws.Int64(3),
		},

		TableName: aws.String("Book"),
	}

	_, err := suite.db.CreateTable(createTableInput)
	if err != nil {
		log.Fatal(err)
	}
	err = suite.db.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String("Book"),
	})
	if err != nil {
		log.Fatal(err)
	}
}

func (suite *DatabaseTestSuite) SetupSuite() {
	if *onlineFlag == false {
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
		suite.proc = cmd.Process

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
	} else {
		testRegion := os.Getenv("DYNAMI_TEST_REGION")
		if testRegion == "" {
			testRegion = "us-east-1"
		}

		session, err := session.NewSession(&aws.Config{
			Region: aws.String(testRegion),
		})
		if err != nil {
			log.Fatal(err)
		}
		suite.db = db.New(session)

		creds, err := session.Config.Credentials.Get()
		if err != nil {
			log.Fatal(err)
		}
		suite.client = NewClient(
			GetRegion(testRegion),
			creds.AccessKeyID,
			creds.SecretAccessKey)
	}

	suite.createBookTable()
	suite.createQuoteTable()
}

func (suite *DatabaseTestSuite) TearDownSuite() {
	suite.deleteAllTables()
	if suite.proc != nil {
		suite.proc.Kill()
	}
}

func (suite *DatabaseTestSuite) TearDownTest() {
	suite.clearTable("Quote")
	suite.clearTable("Book")
	suite.deleteAllTablesExcept("Quote", "Book")
}

var onlineFlag = flag.Bool("online", false, "runs the tests on a remote database")

func TestDatabaseTestSuite(t *testing.T) {
	flag.Parse()
	suite.Run(t, new(DatabaseTestSuite))
}
