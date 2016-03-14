// Package schema contains the structs that describe the database table and its
// fields.
package schema

import (
	"fmt"
	"time"

	db "github.com/aws/aws-sdk-go/service/dynamodb"
)

// Throughput contains the provisioned throughput
// for a given table or global secondary index.
type Throughput struct {
	Read  int
	Write int
}

// AttributeType is the data
// type for key attributes.
type AttributeType string

// A key attribute can only be a
// number, string, or binary data.
const (
	BinaryType AttributeType = "B"
	NumberType AttributeType = "N"
	StringType AttributeType = "S"
)

// AttributeDefinition describes
// an attribute used for key schemas.
type AttributeDefinition struct {
	Name string
	Type AttributeType
}

// ProjectionType specifies which set
// of attributes are projected into the index.
type ProjectionType string

// ProjectKeysOnly is the default projection type. It
// only projects the primary key and the secondary index
// key. ProjectInclude projects only the specified attributes.
// The names of the projected attributes are in Projection.Include
// field. ProjectAll projects all attributes from the table to the
// index.
const (
	ProjectAll      ProjectionType = db.ProjectionTypeAll
	ProjectInclude  ProjectionType = db.ProjectionTypeInclude
	ProjectKeysOnly ProjectionType = db.ProjectionTypeKeysOnly
)

// Projection specifies which attributes
// are projected from a table into an index.
type Projection struct {
	Type    ProjectionType
	Include []string
}

// KeyType is the key type of an attribute.
type KeyType string

// A key attribute can only be a
// hash (partition) or range (sort) key.
const (
	HashKey  KeyType = db.KeyTypeHash
	RangeKey KeyType = db.KeyTypeRange
)

// KeySchema describes the key
// attributes of a table or index.
type KeySchema struct {
	AttributeName string
	KeyType       KeyType
}

// Status represents the current state
// of a table or a global secondary index.
type Status string

// A table or a global secondary index can only be used
// if its current status is active.
const (
	ActiveStatus   Status = "ACTIVE"
	CreatingStatus Status = "CREATING"
	DeletingStatus Status = "DELETING"
	UpdatingStatus Status = "UPDATING"
	UnknownStatus  Status = "UNKNOWN"
)

type iprivate struct {
	PSize      int
	PItemCount int
	PStatus    Status
}

// SecondaryIndex contains the properties of an index.
type SecondaryIndex struct {
	Name       string
	KeySchema  []KeySchema
	Projection *Projection
	Throughput *Throughput

	// private read-only fields
	iprivate
}

// Size returns the index size in bytes.
func (idx *SecondaryIndex) Size() int {
	return idx.PSize
}

// ItemCount returns the number of items in this index.
func (idx *SecondaryIndex) ItemCount() int {
	return idx.PItemCount
}

// Status returns the current state of the index.
func (idx *SecondaryIndex) Status() Status {
	return idx.PStatus
}

type tprivate struct {
	// Read-only fields
	PSize         int
	PItemCount    int
	PStatus       Status
	PCreationDate time.Time

	PStreamSpec *db.StreamSpecification
}

// Table contains the properties of a table.
type Table struct {
	Name                   string
	Attributes             []AttributeDefinition
	Throughput             *Throughput
	KeySchema              []KeySchema
	LocalSecondaryIndexes  []SecondaryIndex
	GlobalSecondaryIndexes []SecondaryIndex

	// private read-only fields
	tprivate
}

// ItemCount returns the number of items in this table.
func (t *Table) ItemCount() int {
	return t.PItemCount
}

// Size returns the table size in bytes.
func (t *Table) Size() int {
	return t.PSize
}

// Status return the status of this table.
func (t *Table) Status() Status {
	return t.PStatus
}

// CreationDate returns the date and time
// the table was created in unix epoch time.
func (t *Table) CreationDate() time.Time {
	return t.PCreationDate
}

// NewTable returns a new table from an item. item must be
// a struct or a pointer to struct with properly tagged fields.
// throughput should contain the provisioned throughput for the
// given table and any additional secondary global indices.
func NewTable(
	tableName string,
	item interface{},
	throughput map[string]*Throughput) *Table {

	if tableName == "" {
		panic("dynamini: table name must not be empty")
	} else if throughput[tableName] == nil {
		panic("dynamini: no provisioned throughput for table")
	}

	table := &Table{
		Name:       tableName,
		Throughput: throughput[tableName],
	}
	table.PSize = -1
	table.PItemCount = -1
	table.PStatus = UnknownStatus

	// Get incomplete table schema
	sc := GetSchema(item)

	// Begin copying fields...
	table.KeySchema = make(
		[]KeySchema,
		len(sc.KeySchema),
	)
	copy(table.KeySchema, sc.KeySchema)

	table.Attributes = make(
		[]AttributeDefinition,
		len(sc.Attributes),
	)
	copy(table.Attributes, sc.Attributes)

	table.LocalSecondaryIndexes = make(
		[]SecondaryIndex,
		len(sc.LocalSecondaryIndexes),
	)
	copy(table.LocalSecondaryIndexes, sc.LocalSecondaryIndexes)

	table.GlobalSecondaryIndexes = make(
		[]SecondaryIndex,
		len(sc.GlobalSecondaryIndexes),
	)
	copy(table.GlobalSecondaryIndexes, sc.GlobalSecondaryIndexes)

	// Add provisioned throughput for all global secondary indices
	for i, idx := range table.GlobalSecondaryIndexes {
		tp := throughput[idx.Name]
		if tp == nil {
			panic(fmt.Errorf(
				"dynamini: no provisioned throughput for global index (%s)",
				idx.Name,
			))
		}

		table.GlobalSecondaryIndexes[i].Throughput = tp
	}

	return table
}
