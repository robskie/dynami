package dynamini

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
)

// NewTable returns a new table from an item. item must be
// a struct or a pointer to struct with properly tagged fields.
// throughput should contain the provisioned throughput for the
// given table name and any additional secondary global indices.
func NewTable(
	tableName string,
	item interface{},
	throughput map[string]*Throughput) *Table {

	table := &Table{
		Name:       tableName,
		Throughput: throughput[tableName],

		itemCount: -1,
		size:      -1,
		status:    UnknownStatus,
	}

	schema := getSchema(item)
	attrs := map[string]AttributeType{}

	// Get primary key
	pk := schema.key
	keySchema := make([]KeySchema, len(pk))
	for i, ke := range pk {
		keySchema[i] = KeySchema{
			AttributeName: ke.name,
			KeyType:       ke.keyType,
		}
		attrs[ke.name] = ke.attrType
	}
	table.KeySchema = keySchema

	// Get secondary indices
	localIdxs := []SecondaryIndex{}
	globalIdxs := []SecondaryIndex{}
	pkhash := keySchema[0].AttributeName
	for _, idx := range schema.indices {
		sidx := SecondaryIndex{
			Name:       idx.name,
			Throughput: throughput[idx.name],
		}

		if len(idx.projections) == 0 {
			sidx.Projection = &Projection{
				Type: ProjectKeysOnly,
			}
		} else {
			sidx.Projection = &Projection{
				Type:    ProjectInclude,
				Include: idx.projections,
			}
		}

		ik := idx.key
		ks := make([]KeySchema, len(ik))
		for i, ke := range ik {
			ks[i] = KeySchema{
				AttributeName: ke.name,
				KeyType:       ke.keyType,
			}
			attrs[ke.name] = ke.attrType
		}
		sidx.KeySchema = ks

		if ks[0].AttributeName == pkhash {
			localIdxs = append(localIdxs, sidx)
		} else {
			globalIdxs = append(globalIdxs, sidx)
			if sidx.Throughput == nil {
				panic(fmt.Errorf(
					"dynamini: no provisioned throughput for global index (%s)",
					sidx.Name,
				))
			}
		}
	}
	table.LocalSecondaryIndexes = localIdxs
	table.GlobalSecondaryIndexes = globalIdxs

	// Add attributes
	attrDefs := make([]AttributeDefinition, 0, len(attrs))
	for attrName, attrType := range attrs {
		attrDefs = append(attrDefs, AttributeDefinition{
			Name: attrName,
			Type: attrType,
		})
	}
	table.Attributes = attrDefs

	return table
}

// CreateTable adds a new table to the current account.
// This waits for the table to become useable or active
// before returning.
func (c *Client) CreateTable(table *Table) error {
	input := &db.CreateTableInput{
		TableName:              aws.String(table.Name),
		AttributeDefinitions:   dbAttributeDefinitions(table.Attributes),
		ProvisionedThroughput:  dbProvisionedThroughput(table.Throughput),
		KeySchema:              dbKeySchema(table.KeySchema),
		LocalSecondaryIndexes:  dbLocalSecondaryIndexes(table.LocalSecondaryIndexes),
		GlobalSecondaryIndexes: dbGlobalSecondaryIndexes(table.GlobalSecondaryIndexes),
		StreamSpecification:    table.streamSpec,
	}

	cdb := c.db
	_, err := cdb.CreateTable(input)
	if err != nil {
		return fmt.Errorf("dynamini: cannot create table (%v)", err)
	}

	err = cdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	if err != nil {
		return fmt.Errorf("dynamini: failed waiting for table creation (%v)", err)
	}
	return err
}

// DescribeTable provides additional information about the given table.
// This includes the table's creation date, size in bytes, and the number
// of items it contains.
func (c *Client) DescribeTable(tableName string) (*Table, error) {
	cdb := c.db
	resp, err := cdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamini: cannot describe table (%v)", err)
	}

	desc := resp.Table
	table := &Table{
		Name:                   tableName,
		Attributes:             attributeDefinitions(desc.AttributeDefinitions),
		Throughput:             throughput(desc.ProvisionedThroughput),
		KeySchema:              keySchema(desc.KeySchema),
		LocalSecondaryIndexes:  secondaryIndexes(desc.LocalSecondaryIndexes),
		GlobalSecondaryIndexes: secondaryIndexes(desc.GlobalSecondaryIndexes),

		itemCount:    int(*desc.ItemCount),
		size:         int(*desc.TableSizeBytes),
		status:       Status(*desc.TableStatus),
		creationDate: *desc.CreationDateTime,

		streamSpec: desc.StreamSpecification,
	}

	return table, nil
}

// DeleteTable removes a table from the current account.
// This blocks until the table no longer exists.
func (c *Client) DeleteTable(tableName string) (*Table, error) {
	cdb := c.db
	resp, err := cdb.DeleteTable(&db.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("dynamini: cannot delete table (%v)", err)
	}

	desc := resp.TableDescription
	t := &Table{
		Name:                   tableName,
		Attributes:             attributeDefinitions(desc.AttributeDefinitions),
		Throughput:             throughput(desc.ProvisionedThroughput),
		KeySchema:              keySchema(desc.KeySchema),
		LocalSecondaryIndexes:  secondaryIndexes(desc.LocalSecondaryIndexes),
		GlobalSecondaryIndexes: secondaryIndexes(desc.GlobalSecondaryIndexes),

		itemCount:    int(*desc.ItemCount),
		size:         int(*desc.TableSizeBytes),
		status:       Status(*desc.TableStatus),
		creationDate: *desc.CreationDateTime,

		streamSpec: desc.StreamSpecification,
	}

	err = cdb.WaitUntilTableNotExists(&db.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return t, fmt.Errorf("dynamini: failed waiting for table deletion (%v)", err)
	}
	return t, nil
}

// ClearTable removes all items from a table.
// This is achieved by deleting items by batch.
func (c *Client) ClearTable(tableName string) error {
	desc, err := c.DescribeTable(tableName)
	if err != nil {
		return fmt.Errorf("dynamini: cannot clear table")
	}

	const keysPerBatch = 25
	keySchema := desc.KeySchema
	it := c.Query(tableName).Consistent().Run()
	keys := make([]map[string]interface{}, 0, keysPerBatch)
	for it.HasNext() {
		var item map[string]interface{}
		it.Next(&item)

		key := map[string]interface{}{}
		for _, k := range keySchema {
			key[k.AttributeName] = item[k.AttributeName]
		}
		keys = append(keys, key)

		if len(keys) == keysPerBatch {
			err = c.BatchDelete(tableName, keys).Run()
			if err != nil {
				return fmt.Errorf("dynamini: cannot clear table (%v)", err)
			}
			keys = keys[0:]
		}
	}

	if len(keys) > 0 {
		err = c.BatchDelete(tableName, keys).Run()
		if err != nil {
			return fmt.Errorf("dynamini: cannot clear table (%v)", err)
		}
	}

	return nil
}

// ListTables returns all table names
// associated with the current account.
func (c *Client) ListTables() ([]string, error) {
	cdb := c.db
	tables := []string{}

	inp := &db.ListTablesInput{}
	resp, err := cdb.ListTables(inp)
	for _, t := range resp.TableNames {
		tables = append(tables, *t)
	}

	for err == nil && resp.LastEvaluatedTableName != nil {
		inp.ExclusiveStartTableName = resp.LastEvaluatedTableName
		resp, err = cdb.ListTables(inp)
		for _, t := range resp.TableNames {
			tables = append(tables, *t)
		}
	}

	return tables, err
}
