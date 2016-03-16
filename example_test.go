package dynamini_test

import (
	"github.com/robskie/dynamini"
	"github.com/robskie/dynamini/schema"
)

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
