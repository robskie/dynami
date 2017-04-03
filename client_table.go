package dynami

import (
	"fmt"
	"reflect"

	"github.com/robskie/dynami/schema"

	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
)

// CreateTable adds a new table to the current account.
// This waits for the table to become useable or active
// before returning.
func (c *Client) CreateTable(table *schema.Table) error {
	input := &db.CreateTableInput{
		TableName:              aws.String(table.Name),
		AttributeDefinitions:   dbAttributeDefinitions(table.Attributes),
		ProvisionedThroughput:  dbProvisionedThroughput(table.Throughput),
		KeySchema:              dbKeySchema(table.Key),
		LocalSecondaryIndexes:  dbLocalSecondaryIndexes(table.LocalSecondaryIndexes),
		GlobalSecondaryIndexes: dbGlobalSecondaryIndexes(table.GlobalSecondaryIndexes),
		StreamSpecification:    table.PStreamSpec,
	}

	// If stream is enabled, create a new stream spec if there isn't any
	if table.StreamEnabled && input.StreamSpecification == nil {
		input.StreamSpecification = &db.StreamSpecification{
			StreamEnabled:  aws.Bool(true),
			StreamViewType: aws.String(db.StreamViewTypeNewAndOldImages),
		}
	}

	cdb := c.db
	_, err := cdb.CreateTable(input)
	if err != nil {
		return fmt.Errorf("dynami: cannot create table (%v)", err)
	}

	err = cdb.WaitUntilTableExists(&db.DescribeTableInput{
		TableName: aws.String(table.Name),
	})
	if err != nil {
		return fmt.Errorf("dynami: failed waiting for table creation (%v)", err)
	}
	return err
}

// UpdateTable modifies the table's throughput or global secondary
// indices. It can create and delete global secondary indices or update
// their throughputs. This method waits until all updates are finished.
func (c *Client) UpdateTable(table *schema.Table) error {
	// Get unmodified table schema
	origt, err := c.DescribeTable(table.Name)
	if err != nil {
		return fmt.Errorf("dynami: cannot update table (%v)", err)
	}

	// Update table stream
	cdb := c.db
	if table.StreamEnabled != origt.StreamEnabled {
		dbStreamSpec := &db.StreamSpecification{
			StreamEnabled:  &table.StreamEnabled,
			StreamViewType: aws.String(db.StreamViewTypeNewAndOldImages),
		}
		if !table.StreamEnabled {
			dbStreamSpec.StreamViewType = nil
		}

		_, err := cdb.UpdateTable(&db.UpdateTableInput{
			TableName:           aws.String(table.Name),
			StreamSpecification: dbStreamSpec,
		})
		if err != nil {
			return fmt.Errorf("dynami: cannot update stream (%v)", err)
		}

		// Wait until table is finished updating
		err = cdb.WaitUntilTableExists(&db.DescribeTableInput{
			TableName: aws.String(table.Name),
		})
		if err != nil {
			return fmt.Errorf("dynami: failed waiting for successful table update (%v)", err)
		}
	}

	// Update table's provisioned throughput
	if table.Throughput != origt.Throughput {
		_, err := cdb.UpdateTable(&db.UpdateTableInput{
			TableName:             aws.String(table.Name),
			ProvisionedThroughput: dbProvisionedThroughput(table.Throughput),
		})
		if err != nil {
			return fmt.Errorf("dynami: cannot update table (%v)", err)
		}

		// Wait until table is finished updating
		err = cdb.WaitUntilTableExists(&db.DescribeTableInput{
			TableName: aws.String(table.Name),
		})
		if err != nil {
			return fmt.Errorf("dynami: failed waiting for successful table update (%v)", err)
		}
	}

	// Create attribute map
	attrs := map[string]schema.Attribute{}
	for _, attr := range table.Attributes {
		attrs[attr.Name] = attr
	}

	// Create global secondary index maps
	gsi := map[string]schema.SecondaryIndex{}
	ogsi := map[string]schema.SecondaryIndex{}
	for _, idx := range table.GlobalSecondaryIndexes {
		gsi[idx.Name] = idx
	}
	for _, idx := range origt.GlobalSecondaryIndexes {
		ogsi[idx.Name] = idx
	}

	// Remove global indices
	for name := range ogsi {
		if _, ok := gsi[name]; !ok {
			deleteAction := &db.DeleteGlobalSecondaryIndexAction{
				IndexName: aws.String(name),
			}

			_, err := cdb.UpdateTable(&db.UpdateTableInput{
				TableName: aws.String(table.Name),
				GlobalSecondaryIndexUpdates: []*db.GlobalSecondaryIndexUpdate{
					{
						Delete: deleteAction,
					},
				},
			})
			if err != nil {
				return fmt.Errorf("dynami: cannot delete global secondary index (%v)", err)
			}

			// Wait until all gsi's are active
			err = waitUntilIndicesAreActive(cdb, table.Name)
			if err != nil {
				return fmt.Errorf("dynami: failed waiting for successful index update (%v)", err)
			}
		}
	}

	// Perform create and queue update actions
	var gsiUpdateActs []*db.GlobalSecondaryIndexUpdate
	for name, idx := range gsi {
		oidx, ok := ogsi[name]

		// Create GSI
		if !ok {
			createAction := &db.CreateGlobalSecondaryIndexAction{
				IndexName:             aws.String(name),
				ProvisionedThroughput: dbProvisionedThroughput(idx.Throughput),
				Projection:            dbProjection(idx.Projection),
				KeySchema:             dbKeySchema(idx.Key),
			}

			var attrDefs []*db.AttributeDefinition
			for _, k := range idx.Key {
				attr, ok := attrs[k.Name]
				if !ok {
					return fmt.Errorf("dynami: missing attribute definition")
				}

				attrDefs = append(attrDefs, &db.AttributeDefinition{
					AttributeName: aws.String(k.Name),
					AttributeType: aws.String(string(attr.Type)),
				})
			}

			_, err := cdb.UpdateTable(&db.UpdateTableInput{
				TableName:            aws.String(table.Name),
				AttributeDefinitions: attrDefs,
				GlobalSecondaryIndexUpdates: []*db.GlobalSecondaryIndexUpdate{
					{
						Create: createAction,
					},
				},
			})
			if err != nil {
				return fmt.Errorf("dynami: cannot create global secondary index (%v)", err)
			}

			// Wait until all gsi's are active
			err = waitUntilIndicesAreActive(cdb, table.Name)
			if err != nil {
				return fmt.Errorf("dynami: failed waiting for successful index update (%v)", err)
			}
		} else if idx.Throughput != oidx.Throughput { // Update GSI
			updateAction := &db.UpdateGlobalSecondaryIndexAction{
				IndexName:             aws.String(name),
				ProvisionedThroughput: dbProvisionedThroughput(idx.Throughput),
			}

			gsiUpdateActs = append(gsiUpdateActs, &db.GlobalSecondaryIndexUpdate{
				Update: updateAction,
			})
		}
	}

	// Perform update actions
	if len(gsiUpdateActs) > 0 {
		_, err := cdb.UpdateTable(&db.UpdateTableInput{
			TableName:                   aws.String(table.Name),
			GlobalSecondaryIndexUpdates: gsiUpdateActs,
		})
		if err != nil {
			return fmt.Errorf("dynami: cannot update global secondary index (%v)", err)
		}

		// Wait until all gsi's are active
		err = waitUntilIndicesAreActive(cdb, table.Name)
		if err != nil {
			return fmt.Errorf("dynami: failed waiting for successful index update (%v)", err)
		}
	}

	return nil
}

// DescribeTable provides additional information about the given table.
// This includes the table's creation date, size in bytes, and the number
// of items it contains.
func (c *Client) DescribeTable(tableName string) (*schema.Table, error) {
	cdb := c.db
	resp, err := cdb.DescribeTable(&db.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("dynami: cannot describe table (%v)", err)
	}

	desc := resp.Table
	table := &schema.Table{
		Name:       tableName,
		Attributes: attributeDefinitions(desc.AttributeDefinitions),
		Throughput: throughput(desc.ProvisionedThroughput),
		Key:        keySchema(desc.KeySchema),
		LocalSecondaryIndexes:  secondaryIndexes(desc.LocalSecondaryIndexes),
		GlobalSecondaryIndexes: secondaryIndexes(desc.GlobalSecondaryIndexes),
		StreamEnabled:          streamEnabled(desc.StreamSpecification),
	}
	table.PSize = int(*desc.TableSizeBytes)
	table.PItemCount = int(*desc.ItemCount)
	table.PStatus = schema.Status(*desc.TableStatus)
	table.PCreationDate = *desc.CreationDateTime
	table.PStreamSpec = desc.StreamSpecification
	if desc.LatestStreamArn != nil {
		table.PStreamARN = *desc.LatestStreamArn
	}

	return table, nil
}

// DeleteTable removes a table from the current account.
// This blocks until the table no longer exists.
func (c *Client) DeleteTable(tableName string) (*schema.Table, error) {
	cdb := c.db
	resp, err := cdb.DeleteTable(&db.DeleteTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, fmt.Errorf("dynami: cannot delete table (%v)", err)
	}

	desc := resp.TableDescription
	table := &schema.Table{
		Name:       tableName,
		Attributes: attributeDefinitions(desc.AttributeDefinitions),
		Throughput: throughput(desc.ProvisionedThroughput),
		Key:        keySchema(desc.KeySchema),
		LocalSecondaryIndexes:  secondaryIndexes(desc.LocalSecondaryIndexes),
		GlobalSecondaryIndexes: secondaryIndexes(desc.GlobalSecondaryIndexes),
		StreamEnabled:          streamEnabled(desc.StreamSpecification),
	}
	table.PSize = int(*desc.TableSizeBytes)
	table.PItemCount = int(*desc.ItemCount)
	table.PStatus = schema.Status(*desc.TableStatus)
	if desc.CreationDateTime != nil {
		table.PCreationDate = *desc.CreationDateTime
	}
	table.PStreamSpec = desc.StreamSpecification
	if desc.LatestStreamArn != nil {
		table.PStreamARN = *desc.LatestStreamArn
	}

	err = cdb.WaitUntilTableNotExists(&db.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return table, fmt.Errorf("dynami: failed waiting for table deletion (%v)", err)
	}
	return table, nil
}

// ClearTable removes all items from a table.
// This is achieved by deleting items by batch.
func (c *Client) ClearTable(tableName string) error {
	desc, err := c.DescribeTable(tableName)
	if err != nil {
		return fmt.Errorf("dynami: cannot clear table")
	}

	const keysPerBatch = 25
	keySchema := desc.Key
	it := c.Query(tableName).Consistent().Run()
	keys := make([]map[string]interface{}, 0, keysPerBatch)
	for it.HasNext() {
		var item map[string]interface{}
		it.Next(&item)

		key := map[string]interface{}{}
		for _, k := range keySchema {
			key[k.Name] = item[k.Name]
		}
		keys = append(keys, key)

		if len(keys) == keysPerBatch {
			err = c.BatchDelete(tableName, keys).Run()
			if err != nil {
				return fmt.Errorf("dynami: cannot clear table (%v)", err)
			}
			keys = keys[0:]
		}
	}

	if len(keys) > 0 {
		err = c.BatchDelete(tableName, keys).Run()
		if err != nil {
			return fmt.Errorf("dynami: cannot clear table (%v)", err)
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

func dbKeySchema(ks []schema.Key) []*db.KeySchemaElement {
	keySchema := make([]*db.KeySchemaElement, len(ks))
	for i, ke := range ks {
		keySchema[i] = &db.KeySchemaElement{
			AttributeName: aws.String(ke.Name),
			KeyType:       aws.String(string(ke.Type)),
		}
	}

	return keySchema
}

func dbAttributeDefinitions(attrs []schema.Attribute) []*db.AttributeDefinition {
	defs := make([]*db.AttributeDefinition, len(attrs))
	for i, attr := range attrs {
		defs[i] = &db.AttributeDefinition{
			AttributeType: aws.String(string(attr.Type)),
			AttributeName: aws.String(attr.Name),
		}
	}

	return defs
}

func dbProjection(proj schema.Projection) *db.Projection {
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

func dbLocalSecondaryIndex(idx schema.SecondaryIndex) *db.LocalSecondaryIndex {
	return &db.LocalSecondaryIndex{
		IndexName:  aws.String(idx.Name),
		Projection: dbProjection(idx.Projection),
		KeySchema:  dbKeySchema(idx.Key),
	}
}

func dbLocalSecondaryIndexes(idxs []schema.SecondaryIndex) []*db.LocalSecondaryIndex {
	var localIdxs []*db.LocalSecondaryIndex
	for _, idx := range idxs {
		sidx := dbLocalSecondaryIndex(idx)
		localIdxs = append(localIdxs, sidx)
	}

	return localIdxs
}

func dbProvisionedThroughput(tp schema.Throughput) *db.ProvisionedThroughput {
	return &db.ProvisionedThroughput{
		ReadCapacityUnits:  aws.Int64(int64(tp.Read)),
		WriteCapacityUnits: aws.Int64(int64(tp.Write)),
	}
}

func dbGlobalSecondaryIndex(idx schema.SecondaryIndex) *db.GlobalSecondaryIndex {
	return &db.GlobalSecondaryIndex{
		IndexName:             aws.String(idx.Name),
		Projection:            dbProjection(idx.Projection),
		KeySchema:             dbKeySchema(idx.Key),
		ProvisionedThroughput: dbProvisionedThroughput(idx.Throughput),
	}
}

func dbGlobalSecondaryIndexes(idxs []schema.SecondaryIndex) []*db.GlobalSecondaryIndex {
	var globalIdxs []*db.GlobalSecondaryIndex
	for _, idx := range idxs {
		gidx := dbGlobalSecondaryIndex(idx)
		globalIdxs = append(globalIdxs, gidx)
	}

	return globalIdxs
}

func attributeDefinitions(dbAttrs []*db.AttributeDefinition) []schema.Attribute {
	attrs := make([]schema.Attribute, len(dbAttrs))
	for i, attrDef := range dbAttrs {
		attrs[i] = schema.Attribute{
			Name: *attrDef.AttributeName,
			Type: schema.AttributeType(*attrDef.AttributeType),
		}
	}

	return attrs
}

func keySchema(dbKeySchema []*db.KeySchemaElement) []schema.Key {
	keySchema := make([]schema.Key, len(dbKeySchema))
	for i, ke := range dbKeySchema {
		keySchema[i] = schema.Key{
			Name: *ke.AttributeName,
			Type: schema.KeyType(*ke.KeyType),
		}
	}

	return keySchema
}

func projection(dbProj *db.Projection) schema.Projection {
	proj := schema.Projection{
		Type: schema.ProjectionType(*dbProj.ProjectionType),
	}

	var include []string
	for _, attr := range dbProj.NonKeyAttributes {
		include = append(include, *attr)
	}
	proj.Include = include

	return proj
}

func throughput(dbThroughput interface{}) schema.Throughput {
	v := reflect.ValueOf(dbThroughput).Elem()
	return schema.Throughput{
		Read:  int(v.FieldByName("ReadCapacityUnits").Elem().Int()),
		Write: int(v.FieldByName("WriteCapacityUnits").Elem().Int()),
	}
}

func secondaryIndex(dbSecondaryIdx interface{}) schema.SecondaryIndex {
	v := reflect.ValueOf(dbSecondaryIdx).Elem()
	vidxName := v.FieldByName("IndexName")
	vkeySchema := v.FieldByName("KeySchema")
	vprojection := v.FieldByName("Projection")
	vidxSize := v.FieldByName("IndexSizeBytes")
	vitemCount := v.FieldByName("ItemCount")
	vthroughput := v.FieldByName("ProvisionedThroughput")
	vstatus := v.FieldByName("IndexStatus")

	index := schema.SecondaryIndex{
		Name:       vidxName.Elem().Interface().(string),
		Projection: projection(vprojection.Interface().(*db.Projection)),
		Key:        keySchema(vkeySchema.Interface().([]*db.KeySchemaElement)),
	}

	if vthroughput.Kind() != reflect.Invalid {
		index.Throughput = throughput(vthroughput.Interface())
	}

	if vstatus.Kind() != reflect.Invalid {
		index.PStatus = schema.Status(vstatus.Elem().Interface().(string))
	}

	if vidxSize.Kind() != reflect.Invalid {
		index.PSize = int(vidxSize.Elem().Int())
	}

	if vitemCount.Kind() != reflect.Invalid {
		index.PItemCount = int(vitemCount.Elem().Int())
	}

	return index
}

func secondaryIndexes(dbSecondaryIdxs interface{}) []schema.SecondaryIndex {
	v := reflect.ValueOf(dbSecondaryIdxs)
	idxs := make([]schema.SecondaryIndex, v.Len())
	for i := range idxs {
		idxs[i] = secondaryIndex(v.Index(i).Interface())
	}

	return idxs
}

func streamEnabled(dbStreamSpec *db.StreamSpecification) bool {
	if dbStreamSpec == nil {
		return false
	}

	return *dbStreamSpec.StreamEnabled
}

func waitUntilIndicesAreActive(c *db.DynamoDB, tableName string) error {
	w := request.Waiter{
		Name:        "WaitUntilIndicesAreActive",
		MaxAttempts: 25,
		Delay:       request.ConstantWaiterDelay(20 * time.Second),
		Acceptors: []request.WaiterAcceptor{
			{
				State:    request.SuccessWaiterState,
				Matcher:  request.PathWaiterMatch,
				Argument: "Table.GlobalSecondaryIndexes[*].IndexStatus",
				Expected: string(schema.ActiveStatus),
			},
		},
		Logger: c.Config.Logger,
		NewRequest: func(opts []request.Option) (*request.Request, error) {
			req, _ := c.DescribeTableRequest(&db.DescribeTableInput{
				TableName: aws.String(tableName),
			})
			return req, nil
		},
	}

	return w.WaitWithContext(aws.BackgroundContext())
}
