package dynamini

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"

	sc "github.com/robskie/dynamini/schema"

	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// BatchError represents a batch operation error.
// The outer map specifies the table where the error
// occurred and the inner map gives the index of the
// input element that caused the error.
type BatchError map[string]map[int]error

func (e BatchError) Error() string {
	return "dynamini: an error occurred in one of the items"
}

// ikey contains the table and the key
// values for an item. It is used as a
// map key to get the item's input index.
type ikey struct {
	tableName string
	keys      []interface{}
}

func (ik *ikey) toStr() string {
	buf := &bytes.Buffer{}
	enc := gob.NewEncoder(buf)

	enc.Encode(ik.tableName)
	enc.Encode(ik.keys)

	return buf.String()
}

func ikeyFromStr(str string) *ikey {
	k := &ikey{}

	dec := gob.NewDecoder(bytes.NewReader([]byte(str)))
	dec.Decode(&k.tableName)
	dec.Decode(&k.keys)

	return k
}

// ekey is used as a map key to get
// error values. It contains the table
// and the index of the erroneous item.
type ekey struct {
	tableName string
	index     int
}

// getIndexKey returns the string
// representation of ikey for a given
// dynamodb item.
func getIndexKey(tableName string, keySchema []sc.KeySchema, item dbitem) string {
	ik := &ikey{tableName: tableName}

	mkey := map[string]interface{}{}
	dbattribute.ConvertFromMap(item, &mkey)
	for _, k := range keySchema {
		ik.keys = append(ik.keys, mkey[k.AttributeName])
	}

	return ik.toStr()
}

// batchOp represents a batch operation.
type batchOp struct {
	// itemIdxs maps an item's
	// string ikey to its input index.
	itemIdxs map[string][]int

	unproc     map[string][]dbitem
	unprocIdxs map[string][]int
	unpCount   int

	schemas map[string][]sc.KeySchema

	errs map[ekey]error
}

func newBatchOp() *batchOp {
	return &batchOp{
		itemIdxs:   map[string][]int{},
		unproc:     map[string][]dbitem{},
		unprocIdxs: map[string][]int{},
		schemas:    map[string][]sc.KeySchema{},
		errs:       map[ekey]error{},
	}
}

// isEmpty returns true if there
// are no more unprocessed items.
func (b *batchOp) isEmpty() bool {
	for table, items := range b.unproc {
		if len(items) == 0 {
			delete(b.unproc, table)
		} else {
			return false
		}
	}

	return true
}

func (b *batchOp) addItems(
	tableName string,
	items interface{},
	keysOnly ...bool) {

	v := reflect.ValueOf(items)
	if v.Kind() != reflect.Slice {
		panic("dynamini: items must be a slice")
	}

	if v.Len() == 0 {
		return
	}
	b.unpCount += v.Len()

	schema := sc.GetSchema(v.Index(0).Interface())
	kschema := schema.KeySchema
	b.schemas[tableName] = kschema

	dups := map[string][]int{}
	unpItems := make([]dbitem, v.Len())
	for i := range unpItems {
		item := reflect.Indirect(v.Index(i)).Interface()
		k, err := getPrimaryKey(item)
		if err != nil {
			b.errs[ekey{tableName, i}] = err
			continue
		}

		ik := getIndexKey(tableName, kschema, k.value)
		b.itemIdxs[ik] = append(b.itemIdxs[ik], i)
		b.unprocIdxs[ik] = append(b.unprocIdxs[ik], i)
		dups[ik] = append(dups[ik], i)

		dbitem := k.value
		if len(keysOnly) == 0 || keysOnly[0] == false {
			dbitem, err = dbattribute.ConvertToMap(item)
			if err != nil {
				err = fmt.Errorf("dynamini: invalid item (%v)", err)
				b.errs[ekey{tableName, i}] = err
				continue
			}
			dbitem = removeEmptyAttr(dbitem)
		}

		unpItems[i] = dbitem
	}

	// Process duplicates
	for _, idxs := range dups {
		// Set duplicates to nil except for the last one
		var lastIdx int
		var lastItem dbitem
		for _, lastIdx = range idxs {
			lastItem = unpItems[lastIdx]
			unpItems[lastIdx] = nil
		}
		unpItems[lastIdx] = lastItem
	}

	b.unproc[tableName] = unpItems
}

// collectItems adds items to unproc until there are
// no more items to collect or until its size reaches
// batchSize.
func (b *batchOp) collectItems(
	batchSize int,
	unproc map[string][]dbitem) map[string][]dbitem {

	if b.unpCount == 0 {
		return unproc
	}

	// Reduce batch size if there
	// are unprocessed items
	for _, unps := range unproc {
		batchSize -= len(unps)
	}

	nitems := 0
	itemsPerTable := max(batchSize/len(b.unproc), 1)

Collect:
	for {
		for table, unprocs := range b.unproc {
			var i int
			var item dbitem
			for i, item = range unprocs {
				// Process item
				b.unpCount--
				if item != nil {
					unproc[table] = append(unproc[table], item)
					nitems++
				}

				if nitems == batchSize || b.unpCount == 0 {
					// Remove processed items
					b.unproc[table] = b.unproc[table][i+1:]
					break Collect
				} else if i+1 >= itemsPerTable {
					break
				}
			}

			if len(b.unproc[table]) > 0 {
				// Remove processed items
				b.unproc[table] = b.unproc[table][i+1:]
			}
		}
	}

	return unproc
}

func (b *batchOp) processItems(
	collected map[string][]dbitem,
	unprocessed map[string][]dbitem,
	fproc func(table string, idx int, item dbitem) error) {

	// Create unprocessed map. This is used
	// to identify processed items in the
	// collected items.
	unproc := map[string]bool{}
	for table, unprocs := range unprocessed {
		kschema := b.schemas[table]
		for _, unp := range unprocs {
			ik := getIndexKey(table, kschema, unp)
			unproc[ik] = true
		}
	}

	for table, procs := range collected {
		kschema := b.schemas[table]
		for _, item := range procs {
			ik := getIndexKey(table, kschema, item)
			if !unproc[ik] {
				// Remove processed items
				delete(b.unprocIdxs, ik)
				if fproc == nil {
					continue
				}

				for _, idx := range b.itemIdxs[ik] {
					err := fproc(table, idx, item)
					if err != nil {
						b.errs[ekey{table, idx}] = err
					}
				}
			}
		}
	}
}

// flushUnproc marks all unprocessed
// items' indices as erroneous.
func (b *batchOp) flushUnproc(err error) {
	for ik, idxs := range b.unprocIdxs {
		tableName := ikeyFromStr(ik).tableName
		for _, idx := range idxs {
			ek := ekey{tableName, idx}
			b.errs[ek] = err
		}
	}
}

// errors returns all accumulated
// errors as a BatchError.
func (b *batchOp) errors() error {
	if len(b.errs) == 0 {
		return nil
	}

	berr := BatchError{}
	for ek, err := range b.errs {
		if _, ok := berr[ek.tableName]; !ok {
			berr[ek.tableName] = map[int]error{}
		}

		berr[ek.tableName][ek.index] = err
	}

	return berr
}

// unwrap converts a dynamodb batch
// operation response items into dbitems.
func (b *batchOp) unwrap(items interface{}) map[string][]dbitem {
	unwrapped := map[string][]dbitem{}

	switch w := items.(type) {
	case map[string][]*db.WriteRequest:
		for table, reqs := range w {
			unwitems := make([]dbitem, len(reqs))
			for i, w := range reqs {
				if w.DeleteRequest != nil {
					unwitems[i] = w.DeleteRequest.Key
				} else { // w.PutRequest != nil
					unwitems[i] = w.PutRequest.Item
				}
			}
			unwrapped[table] = unwitems
		}
	case map[string]*db.KeysAndAttributes:
		for table, keysAndAttr := range w {
			unwitems := make([]dbitem, len(keysAndAttr.Keys))
			for i, w := range keysAndAttr.Keys {
				unwitems[i] = w
			}
			unwrapped[table] = unwitems
		}

	case map[string][]map[string]*db.AttributeValue:
		for table, witems := range w {
			unwitems := make([]dbitem, len(witems))
			for i, w := range witems {
				unwitems[i] = w
			}
			unwrapped[table] = unwitems
		}
	}

	return unwrapped
}
