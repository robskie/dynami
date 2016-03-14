package schema

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewTable(t *testing.T) {
	type tStruct struct {
		Hash  string `dbkey:"hash"`
		Range int    `dbkey:"range"`

		AnotherRange string `dbindex:"range,LocalIndex"`
		GlobalHash   string `dbindex:"hash,GlobalIndex"`

		Projected int `dbindex:"project,GlobalIndex"`
	}

	table := NewTable("TestTable", tStruct{}, map[string]*Throughput{
		"TestTable":   &Throughput{1, 2},
		"GlobalIndex": &Throughput{3, 4},
	})

	assert.Equal(t, "TestTable", table.Name)
	assert.Equal(t, &Throughput{1, 2}, table.Throughput)

	expectedKeySchema := []Key{
		{"Hash", HashKey},
		{"Range", RangeKey},
	}
	assert.Equal(t, expectedKeySchema, table.Key)

	expectedAttrs := []Attribute{
		{"Hash", StringType},
		{"Range", NumberType},
		{"AnotherRange", StringType},
		{"GlobalHash", StringType},
	}
	assert.Len(t, table.Attributes, len(expectedAttrs))
	for _, attr := range expectedAttrs {
		assert.Contains(t, table.Attributes, attr)
	}

	expectedLocalIdx := []SecondaryIndex{
		{
			Name: "LocalIndex",
			Key: []Key{
				{"Hash", HashKey},
				{"AnotherRange", RangeKey},
			},
			Projection: &Projection{
				Type: ProjectInclude,
				Include: []string{
					"AnotherRange",
					"Hash",
					"Range",
				},
			},
		},
	}
	require.Len(t, table.LocalSecondaryIndexes, 1)
	require.NotNil(t, table.LocalSecondaryIndexes[0].Projection)
	sort.Strings(table.LocalSecondaryIndexes[0].Projection.Include)
	assert.Equal(t, expectedLocalIdx, table.LocalSecondaryIndexes)

	expectedGlobalIdx := []SecondaryIndex{
		{
			Name: "GlobalIndex",
			Key: []Key{
				{"GlobalHash", HashKey},
			},
			Projection: &Projection{
				Type: ProjectInclude,
				Include: []string{
					"GlobalHash",
					"Hash",
					"Projected",
					"Range",
				},
			},
			Throughput: &Throughput{3, 4},
		},
	}
	require.Len(t, table.GlobalSecondaryIndexes, 1)
	require.NotNil(t, table.GlobalSecondaryIndexes[0].Projection)
	sort.Strings(table.GlobalSecondaryIndexes[0].Projection.Include)
	assert.Equal(t, expectedGlobalIdx, table.GlobalSecondaryIndexes)
}
