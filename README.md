# Dynami

Dynami provides a simple wrapper over the official [Go DynamoDB SDK][1].

In order to use this package effectively, an understanding of the underlying
DynamoDB operations is recommended. For an introduction, click [here][2].


[1]:https://docs.aws.amazon.com/sdk-for-go/api/service/dynamodb
[2]:https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html


## Installation
```sh
go get -u github.com/robskie/dynami
```

## Examples

Basic item operations:

```go
type Item struct {
	Key   string `dbkey:"hash"`
	Value string
}

item := Item{"key", "somevalue"}
client := dynami.NewClient(dynami.USEast1, "id", "key")
client.Put("ItemTable", item)

// After some time...

fetched := Item{Key: "key"}
client.Get("ItemTable", &fetched)

// Do something with the fetched item

client.Delete("ItemTable", fetched)
```

Query example:

```go
type Item struct {
	Hash  string `dbkey:"hash"`
	Range int    `dbkey:"range"`
	Value int
}

client := dynami.NewClient(dynami.USEast1, "id", "key")
it := client.Query("ItemTable").
	HashFilter("Hash", "somehashvalue").
	RangeFilter("Range BETWEEN :rval1 AND :rval2", 1, 10).
	Filter("Value = :fval", 42).
	Run()

for it.HasNext() {
	var item Item
	err := it.Next(&item)
	if err != nil {
		// Do something with item
	}
}
```

## API Reference

Godoc documentation can be found [here][3].

[3]: https://godoc.org/github.com/robskie/dynami

## Tests

Before running the tests, install all the test dependencies first.

```sh
go get -t github.com/robskie/dynami
```

Tests can be run locally or online. To run the tests locally, DynamoDB Local 
must be installed in your home directory. You can download and install DynamoDB 
Local through these commands:

```sh
cd ~
curl -O -L http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz
mkdir DynamoDBLocal
tar -xzf dynamodb_local_latest.tar.gz -C DynamoDBLocal
rm dynamodb_local_latest.tar.gz
```
Now you can execute the tests locally via this command:

```sh
go test -v github.com/robskie/dynami
```

Or you can run them online by adding an `online` flag as shown in the following 
command. This will create test tables in the region set in `DYNAMI_TEST_REGION` 
environment variable. Note that these tests can take more than 30 minutes to 
finish.

```sh
DYNAMI_TEST_REGION=us-west-1 go test github.com/robskie/dynami -v -online -timeout 1h
```