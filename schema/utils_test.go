package schema

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldTags(t *testing.T) {
	type tStruct struct {
		Hash  string `dbkey:"hash" dbindex:"hash,GlobalIndexA"`
		Range int    `dbkey:"range"`

		AnotherRange string `dbindex:"range,LocalIndex"`
		GlobalHash   string `dbindex:"hash,GlobalIndexB"`

		Projected int `dbindex:"project,GlobalIndexA,project,GlobalIndexB"`
	}

	s := GetSchema(tStruct{})

	expectedAttrs := []Attribute{
		{"Hash", StringType},
		{"Range", NumberType},
		{"AnotherRange", StringType},
		{"GlobalHash", StringType},
	}
	assert.Len(t, s.Attributes, len(expectedAttrs))
	for _, attr := range expectedAttrs {
		assert.Contains(t, s.Attributes, attr)
	}

	expectedKey := []Key{
		{"Hash", HashKey},
		{"Range", RangeKey},
	}
	assert.Equal(t, expectedKey, s.Key)

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
	require.Len(t, s.LocalSecondaryIndexes, 1)
	require.NotNil(t, s.LocalSecondaryIndexes[0].Projection)
	sort.Strings(s.LocalSecondaryIndexes[0].Projection.Include)
	assert.Equal(t, expectedLocalIdx, s.LocalSecondaryIndexes)

	expectedGlobalIdxs := []SecondaryIndex{
		{
			Name: "GlobalIndexA",
			Key: []Key{
				{"Hash", HashKey},
			},
			Projection: &Projection{
				Type: ProjectInclude,
				Include: []string{
					"Hash",
					"Projected",
					"Range",
				},
			},
		},
		{
			Name: "GlobalIndexB",
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
		},
	}
	require.Len(t, s.GlobalSecondaryIndexes, 2)
	for _, actualIdx := range s.GlobalSecondaryIndexes {
		require.NotNil(t, actualIdx.Projection)
		sort.Strings(actualIdx.Projection.Include)
		assert.Contains(t, expectedGlobalIdxs, actualIdx)
	}
}
