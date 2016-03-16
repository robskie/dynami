package dynamini_test

import (
	"github.com/robskie/dynamini"
	"github.com/robskie/dynamini/schema"
)

func ExampleClient_Get(client *dynamini.Client) {
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
	client.Get("TestTable", &itemA)

	// Fetch using secondary index key
	itemB := TestItem{
		GlobalHash: "anotherhash",
	}
	client.Get("TestTable", &itemB)
}

func ExampleClient_CreateTable(client *dynamini.Client) {
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
		map[string]*schema.Throughput{
			"TestTable": &schema.Throughput{
				Read:  10,
				Write: 20,
			},
			"GlobalIndex": &schema.Throughput{
				Read:  30,
				Write: 40,
			},
		},
	)

	// Perform table creation
	client.CreateTable(table)
}

func ExampleClient_UpdateTable(client *dynamini.Client) {
	// Get table schema
	table, _ := client.DescribeTable("TestTable")

	// Update table throughput
	table.Throughput = &schema.Throughput{
		Read:  10,
		Write: 20,
	}

	// Remove GlobalIndexA
	table.RemoveGlobalSecondaryIndex("GlobalIndexA")

	// Add GlobalIndexB
	table.AddGlobalSecondaryIndex(&schema.SecondaryIndex{
		Name: "GlobalIndexB",
		Throughput: &schema.Throughput{
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
	idx := table.GetGlobalSecondaryIndex("GlobalIndexC")
	idx.Throughput = &schema.Throughput{
		Read:  50,
		Write: 60,
	}
	table.AddGlobalSecondaryIndex(idx)

	// Perform update
	client.UpdateTable(table)
}
