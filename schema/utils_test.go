package schema

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFieldTags(t *testing.T) {
	type tStruct struct {
		Hash  string `dbkey:"hash" dbindex:"hash,GlobalIndexA" dynamodbav:"hash"`
		Range int    `dbkey:"range" dynamodbav:"range"`

		AnotherRange string `dbindex:"range,LocalIndex" json:"another_range"`
		GlobalHash   string `dbindex:"hash,GlobalIndexB" json:"global_hash"`

		Projected int `dbindex:"project,GlobalIndexA,project,GlobalIndexB" json:"projected"`
	}

	s := GetSchema(tStruct{})

	expectedAttrs := []Attribute{
		{"hash", StringType},
		{"range", NumberType},
		{"another_range", StringType},
		{"global_hash", StringType},
	}
	assert.Len(t, s.Attributes, len(expectedAttrs))
	for _, attr := range expectedAttrs {
		assert.Contains(t, s.Attributes, attr)
	}

	expectedKey := []Key{
		{"hash", HashKey},
		{"range", RangeKey},
	}
	assert.Equal(t, expectedKey, s.Key)

	expectedLocalIdx := []SecondaryIndex{
		{
			Name: "LocalIndex",
			Key: []Key{
				{"hash", HashKey},
				{"another_range", RangeKey},
			},
			Projection: &Projection{
				Type: ProjectInclude,
				Include: []string{
					"another_range",
					"hash",
					"range",
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
				{"hash", HashKey},
			},
			Projection: &Projection{
				Type: ProjectInclude,
				Include: []string{
					"hash",
					"projected",
					"range",
				},
			},
		},
		{
			Name: "GlobalIndexB",
			Key: []Key{
				{"global_hash", HashKey},
			},
			Projection: &Projection{
				Type: ProjectInclude,
				Include: []string{
					"global_hash",
					"hash",
					"projected",
					"range",
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
