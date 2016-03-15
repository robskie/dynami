/*
Package dynamini provides a simple wrapper over the official Go DynamoDB SDK.

In order to use this package effectively, an understanding of the underlying
DynamoDB operations is recommended. For an introduction, please visit
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Introduction.html


Items

Items can be a map[string]interface{}, a struct, or a pointer to any of those.
For an item to be valid, it must contain a nonempty key attribute. Key
attributes are restricted to strings, numbers, and binary data. For non-key
attributes, the following data types are allowed:

  * Strings
  * Numbers (int, uint, float32, etc)
  * Binary data ([]byte up to 400KB)
  * Maps (map[string]interface{})
  * Structs of any of the above
  * Slices of any of the above


Field Tags

This package uses field tags to specify an item's primary key and secondary
index properties. To designate a struct field as the item's primary key use
"dbkey:keytype". "keytype" can be "hash" or "range" for hash and range
attributes respectively. For secondary index properties, use the tag
"dbindex:type,IndexName". If the tagged property is a secondary index key, set
"type" to "hash" or "range". If it is a projected attribute, change "type" to
"project". In addition to the said tags, JSON struct tags are also respected.
This is useful for optional and ignored attributes.

Example code:

  // A and B constitutes the primary key.
  // C is the range attribute of LocalIndex.
  // D is the hash attribute of GlobalIndex.
  // A is the range attribute of GlobalIndex.
  // E is projected to both LocalIndex and GlobalIndex.
  // F is ignored and not stored.
  // G is an optional attribute.
  type TaggedStruct struct {
    A string `dbkey:"hash" dbindex:"range,GlobalIndex"`
    B string `dbkey:"range"`
    C string `dbindex:"range,LocalIndex"`
    D string `dbindex:"hash,GlobalIndex"`
    E string `dbindex:"project,LocalIndex,project,GlobalIndex"`
    F string `json:"-"`
    G string `json:",omitempty"`
  }

Note that for local secondary indices, only the range attribute is tagged as
shown in struct field C.

Item Operations

There are three basic item operations: Put, Get, and Delete. Each of these
operations accepts an item with nonempty primary key except Get which also
accepts local and global secondary index keys.

Example code:

  type Item struct {
    Key   string `dbkey:"hash"`
    Value string
  }

  item := Item{"key", "somevalue"}
  client := dynamini.NewClient("region", "id", "key")
  client.Put("ItemTable", item)

  // After some time...

  fetched := Item{Key: "key"}
  client.Get("ItemTable", &fetched)

  // Do something with the fetched item

  client.Delete("ItemTable", fetched)


Batch Operations

Each of the basic item operations also has a batch version: BatchPut, BatchGet
and BatchDelete. Each of these operation returns a corresponding batch operation
structure which allows method chaining so that multiple items from different
tables can be processed at once. Unlike the official SDK, there are no limits on
how many items each batch operation can process.

Example code:

  type ItemA struct {
    Key   string `dbkey:"hash"`
    Value string
  }

  type ItemB struct {
    Key   string `dbkey:"hash"`
    Value string
  }

  // Create items
  limitless := 200
  itemsA := make([]ItemA, limitless)
  itemsB := make([]ItemB, limitless)
  for i := 0; i < limitless; i++ {
    itemsA[i] = ItemA{strconv.Itoa(i), "somevalue"}
    itemsB[i] = ItemB{strconv.Itoa(i), "anothervalue"}
  }

  // Add items
  client := dynamini.NewClient("region", "id", "key")
  client.BatchPut("ItemTableA", itemsA).
    Put("ItemTableB", itemsB).
    Run()

  // Some time later...

  fetchedA := make([]ItemA, limitless)
  fetchedB := make([]ItemB, limitless)
  for i := 0; i < limitless; i++ {
    fetchedA[i] = ItemA{Key: strconv.Itoa(i)}
    fetchedB[i] = ItemB{Key: strconv.Itoa(i)}
  }
  client.BatchGet("ItemTableA", fetchedA).
    Get("ItemTableB", fetchedB).
    Run()

  // Process the items

  client.BatchDelete("ItemTableA", fetchedA).
    Delete("ItemTableB", fetchedB).
    Run()


Queries

Queries are built by chaining filters and conditions. Running a query yields a
result iterator. Unlike the official SDK, there is no size limit for each query
operation.

Queries retrieve items that satisfies a given set of filters. There are three
types of filter: a hash filter, a range filter, and a post filter. If a query
has a hash filter, running it will perform a DynamoDB query operation,
otherwise, a scan operation is performed. The range filter and the post filter
accepts a filter expression and a set of values. A simple filter expression
might look like this:

  AttributeName > :value

A filter expression is composed of an attribute name, a condition, and a value
placeholder. Value placeholders always start with a colon (:) and will be
replaced with their corresponding filter values when the query is run. For all
valid filter expressions see
https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Expressions.SpecifyingConditions.html#ConditionExpressionReference
Note that a range filter only accepts comparator, BETWEEN, and begins_with
filter expressions.

Example code:

  type Item struct {
    Hash  string `dbkey:"hash"`
    Range int    `dbkey:"range"`
    Value int
  }

  client := dynamini.NewClient("region", "id", "key")
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

*/
package dynamini // import "github.com/robskie/dynamini"
