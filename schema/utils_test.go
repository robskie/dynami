package schema

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldTags(t *testing.T) {
	type tStruct struct {
		Hash  string `dbkey:"hash"`
		Range int    `dbkey:"range"`

		AnotherRange string `dbindex:"range,SecondaryIndex"`
		GlobalHash   string `dbindex:"hash,GlobalIndex"`

		Projected int `dbindex:"project,GlobalIndex"`
	}

	s := GetSchema(tStruct{})

	expectedAttrs := []AttributeDefinition{
		{"Hash", StringType},
		{"Range", NumberType},
		{"AnotherRange", StringType},
		{"GlobalHash", StringType},
	}
	assert.Len(t, expectedAttrs, len(s.Attributes))
	for _, attr := range expectedAttrs {
		assert.Contains(t, s.Attributes, attr)
	}

	expectedKey := []KeySchema{
		{"Hash", HashKey},
		{"Range", RangeKey},
	}
	assert.Equal(t, expectedKey, s.KeySchema)

	expectedLocalIdx := []SecondaryIndex{
		{
			Name: "SecondaryIndex",
			KeySchema: []KeySchema{
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
	require.Len(t, s.LocalSecondaryIndexes, 1)
	sort.Strings(s.LocalSecondaryIndexes[0].Projection.Include)
	assert.Equal(t, expectedLocalIdx, s.LocalSecondaryIndexes)

	expectedGlobalIdx := []SecondaryIndex{
		{
			Name: "GlobalIndex",
			KeySchema: []KeySchema{
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
		},
	}
	require.Len(t, s.GlobalSecondaryIndexes, 1)
	sort.Strings(s.GlobalSecondaryIndexes[0].Projection.Include)
	assert.Equal(t, expectedGlobalIdx, s.GlobalSecondaryIndexes)
}
