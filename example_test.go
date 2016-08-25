package dynami_test

import (
	"github.com/robskie/dynami"
	"github.com/robskie/dynami/schema"
)

func ExampleClient_GetItem(client *dynami.Client) {
	type TestItem struct {
		Hash       string `dbkey:"hash"`
		Range      string `dbkey:"range"`
		GlobalHash string `dbindex:"hash,GlobalIndex"`

		Value int `dbindex:"project,GlobalIndex"`
	}

	// Fetch using primary key
	itemA := TestItem{
		Hash:  "somehash",
		Range: "somerange",
	}
	client.GetItem("TestTable", &itemA)

	// Fetch using secondary index key
	itemB := TestItem{
		GlobalHash: "anotherhash",
	}
	client.GetItem("TestTable", &itemB)
}

func ExampleClient_GetStream(client *dynami.Client) {
	type TestItem struct {
		Hash  string `dbkey:"hash"`
		Value int
	}

	it, _ := client.GetStream("TestTable")

	// Process each record as it arrives.
	// Note that this loop doesn't terminate
	// as long as the stream is enabled.
	var item TestItem
	for it.WaitNext() {
		recordType, _ := it.Next(&item)
		switch recordType {
		case dynami.AddedRecord:
			// Process added item
		case dynami.UpdatedRecord:
			// Process updated item
		case dynami.DeletedRecord:
			// Process deleted item
		}
	}
}

func ExampleClient_CreateTable(client *dynami.Client) {
	type TestItem struct {
		Hash       string `dbkey:"hash"`
		Range      string `dbkey:"range"`
		GlobalHash string `dbindex:"hash,GlobalIndex"`

		BigValue   []byte
		SmallValue string `dbindex:"project,GlobalIndex"`
	}

	// Create table schema from TestItem
	table := schema.NewTable(
		"TestTable",
		TestItem{},
		map[string]schema.Throughput{
			"TestTable": schema.Throughput{
				Read:  10,
				Write: 20,
			},
			"GlobalIndex": schema.Throughput{
				Read:  30,
				Write: 40,
			},
		},
	)

	// Perform table creation
	client.CreateTable(table)
}

func ExampleClient_UpdateTable(client *dynami.Client) {
	// Get table schema
	table, _ := client.DescribeTable("TestTable")

	// Update table throughput
	table.Throughput = schema.Throughput{
		Read:  10,
		Write: 20,
	}

	// Remove GlobalIndexA
	table.RemoveGlobalSecondaryIndex("GlobalIndexA")

	// Add GlobalIndexB
	table.AddGlobalSecondaryIndex(schema.SecondaryIndex{
		Name: "GlobalIndexB",
		Throughput: schema.Throughput{
			Read:  30,
			Write: 40,
		},
		Key: []schema.Key{
			{
				Name: "GlobalHashB",
				Type: schema.HashKey,
			},
		},
	})
	table.AddAttributes([]schema.Attribute{
		{
			Name: "GlobalHashB",
			Type: schema.StringType,
		},
	})

	// Update GlobalIndexC
	idx, _ := table.GetGlobalSecondaryIndex("GlobalIndexC")
	idx.Throughput = schema.Throughput{
		Read:  50,
		Write: 60,
	}
	table.AddGlobalSecondaryIndex(idx)

	// Perform update
	client.UpdateTable(table)
}
