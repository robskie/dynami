package dynamini

import (
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go/aws"
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

// SecondaryIndex contains the properties of an index.
type SecondaryIndex struct {
	Name       string
	KeySchema  []KeySchema
	Projection *Projection
	Throughput *Throughput

	// Read only fields
	size      int
	itemCount int
	status    Status
}

// Size returns the index size in bytes.
func (idx *SecondaryIndex) Size() int {
	return idx.size
}

// ItemCount returns the number of items in this index.
func (idx *SecondaryIndex) ItemCount() int {
	return idx.itemCount
}

// Status returns the current state of the index.
func (idx *SecondaryIndex) Status() Status {
	return idx.status
}

// Table contains the properties of a table.
type Table struct {
	Name                   string
	Attributes             []AttributeDefinition
	Throughput             *Throughput
	KeySchema              []KeySchema
	LocalSecondaryIndexes  []SecondaryIndex
	GlobalSecondaryIndexes []SecondaryIndex

	// Read only fields
	itemCount    int
	size         int
	status       Status
	creationDate time.Time

	streamSpec *db.StreamSpecification
}

// ItemCount returns the number of items in this table.
func (t *Table) ItemCount() int {
	return t.itemCount
}

// Size returns the table size in bytes.
func (t *Table) Size() int {
	return t.size
}

// Status return the status of this table.
func (t *Table) Status() Status {
	return t.status
}

// CreationDate returns the date and time
// the table was created in unix epoch time.
func (t *Table) CreationDate() time.Time {
	return t.creationDate
}

func dbKeySchema(ks []KeySchema) []*db.KeySchemaElement {
	keySchema := make([]*db.KeySchemaElement, len(ks))
	for i, ke := range ks {
		keySchema[i] = &db.KeySchemaElement{
			AttributeName: aws.String(ke.AttributeName),
			KeyType:       aws.String(string(ke.KeyType)),
		}
	}

	return keySchema
}

func dbAttributeDefinitions(attrs []AttributeDefinition) []*db.AttributeDefinition {
	defs := make([]*db.AttributeDefinition, len(attrs))
	for i, attr := range attrs {
		defs[i] = &db.AttributeDefinition{
			AttributeType: aws.String(string(attr.Type)),
			AttributeName: aws.String(attr.Name),
		}
	}

	return defs
}

func dbProjection(proj *Projection) *db.Projection {
	projection := &db.Projection{
		ProjectionType: aws.String(string(proj.Type)),
	}

	var attrs []*string
	for _, inc := range proj.Include {
		attrs = append(attrs, aws.String(inc))
	}
	projection.NonKeyAttributes = attrs

	return projection
}

func dbLocalSecondaryIndex(idx SecondaryIndex) *db.LocalSecondaryIndex {
	return &db.LocalSecondaryIndex{
		IndexName:  aws.String(idx.Name),
		Projection: dbProjection(idx.Projection),
		KeySchema:  dbKeySchema(idx.KeySchema),
	}
}

func dbLocalSecondaryIndexes(idxs []SecondaryIndex) []*db.LocalSecondaryIndex {
	var localIdxs []*db.LocalSecondaryIndex
	for _, idx := range idxs {
		sidx := dbLocalSecondaryIndex(idx)
		localIdxs = append(localIdxs, sidx)
	}

	return localIdxs
}

func dbProvisionedThroughput(tp *Throughput) *db.ProvisionedThroughput {
	return &db.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(int64(tp.Read)),
		WriteCapacityUnits: aws.Int64(int64(tp.Write)),
	}
}

func dbGlobalSecondaryIndex(idx SecondaryIndex) *db.GlobalSecondaryIndex {
	return &db.GlobalSecondaryIndex{
		IndexName:             aws.String(idx.Name),
		Projection:            dbProjection(idx.Projection),
		KeySchema:             dbKeySchema(idx.KeySchema),
		ProvisionedThroughput: dbProvisionedThroughput(idx.Throughput),
	}
}

func dbGlobalSecondaryIndexes(idxs []SecondaryIndex) []*db.GlobalSecondaryIndex {
	var globalIdxs []*db.GlobalSecondaryIndex
	for _, idx := range idxs {
		gidx := dbGlobalSecondaryIndex(idx)
		globalIdxs = append(globalIdxs, gidx)
	}

	return globalIdxs
}

func attributeDefinitions(dbAttrs []*db.AttributeDefinition) []AttributeDefinition {
	attrs := make([]AttributeDefinition, len(dbAttrs))
	for i, attrDef := range dbAttrs {
		attrs[i] = AttributeDefinition{
			Name: *attrDef.AttributeName,
			Type: AttributeType(*attrDef.AttributeType),
		}
	}

	return attrs
}

func keySchema(dbKeySchema []*db.KeySchemaElement) []KeySchema {
	keySchema := make([]KeySchema, len(dbKeySchema))
	for i, ke := range dbKeySchema {
		keySchema[i] = KeySchema{
			AttributeName: *ke.AttributeName,
			KeyType:       KeyType(*ke.KeyType),
		}
	}

	return keySchema
}

func projection(dbProj *db.Projection) *Projection {
	proj := &Projection{
		Type: ProjectionType(*dbProj.ProjectionType),
	}

	var include []string
	for _, attr := range dbProj.NonKeyAttributes {
		include = append(include, *attr)
	}
	proj.Include = include

	return proj
}

func throughput(dbThroughput interface{}) *Throughput {
	v := reflect.ValueOf(dbThroughput).Elem()
	return &Throughput{
		Read:  int(v.FieldByName("ReadCapacityUnits").Elem().Int()),
		Write: int(v.FieldByName("WriteCapacityUnits").Elem().Int()),
	}
}

func secondaryIndex(dbSecondaryIdx interface{}) SecondaryIndex {
	v := reflect.ValueOf(dbSecondaryIdx).Elem()
	vidxName := v.FieldByName("IndexName")
	vkeySchema := v.FieldByName("KeySchema")
	vprojection := v.FieldByName("Projection")
	vidxSize := v.FieldByName("IndexSizeBytes")
	vitemCount := v.FieldByName("ItemCount")
	vthroughput := v.FieldByName("ProvisionedThroughput")
	vstatus := v.FieldByName("IndexStatus")

	index := SecondaryIndex{
		Name:       vidxName.Elem().Interface().(string),
		Projection: projection(vprojection.Interface().(*db.Projection)),
		KeySchema:  keySchema(vkeySchema.Interface().([]*db.KeySchemaElement)),
	}

	if vthroughput.Kind() != reflect.Invalid {
		index.Throughput = throughput(vthroughput.Interface())
	}

	if vstatus.Kind() != reflect.Invalid {
		index.status = Status(vstatus.Elem().Interface().(string))
	}

	if vidxSize.Kind() != reflect.Invalid {
		index.size = int(vidxSize.Elem().Int())
	}

	if vitemCount.Kind() != reflect.Invalid {
		index.itemCount = int(vitemCount.Elem().Int())
	}

	return index
}

func secondaryIndexes(dbSecondaryIdxs interface{}) []SecondaryIndex {
	v := reflect.ValueOf(dbSecondaryIdxs)
	idxs := make([]SecondaryIndex, v.Len())
	for i := range idxs {
		idxs[i] = secondaryIndex(v.Index(i).Interface())
	}

	return idxs
}
