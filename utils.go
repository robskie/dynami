package dynamini

import (
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"strings"
	"sync"

	sc "github.com/robskie/dynamini/schema"

	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

const (
	tagHashAttr      = "hash"
	tagRangeAttr     = "range"
	tagProjectedAttr = "project"
)

type indexType string

const (
	globalIndexType indexType = "GLOBAL"
	localIndexType  indexType = "LOCAL"
)

type (
	// Table schema
	tschema struct {
		// key's first element is
		// always a hash attribute
		key     []kelement
		indices []ischema
	}

	kelement struct {
		name     string
		keyType  sc.KeyType
		attrType sc.AttributeType
	}

	// Secondary index schema
	ischema struct {
		name        string
		key         []kelement
		projections []string

		indexType indexType
	}

	dbkey struct {
		value dbitem

		// indexName is the index name for this key.
		// If this is empty, then this key represents
		// the primary key.
		indexName string
		indexType indexType
	}

	dbitem map[string]*db.AttributeValue
)

type sregister struct {
	mutex   *sync.RWMutex
	schemas map[reflect.Type]*tschema
}

var sreg = sregister{
	mutex:   &sync.RWMutex{},
	schemas: map[reflect.Type]*tschema{},
}

func getSchema(item interface{}) *tschema {
	v := reflect.Indirect(reflect.ValueOf(item))

	sreg.mutex.RLock()
	s, ok := sreg.schemas[v.Type()]
	sreg.mutex.RUnlock()

	if !ok {
		// Process map schema
		if v.Kind() == reflect.Map {
			m := v.Interface().(map[string]interface{})
			mk := make([]string, 0, len(m))
			for k := range m {
				mk = append(mk, k)
			}
			sort.Strings(mk)

			key := make([]kelement, len(mk))
			for i, k := range mk {
				key[i] = kelement{name: k}
			}

			return &tschema{key: key}
		}

		// Process struct schema

		// map key is the index name for
		// the key element. Empty key means
		// it is a primary key.
		hashes := map[string]*kelement{}
		ranges := map[string]*kelement{}
		projections := map[string]map[string]bool{}

		indices := map[string]bool{}

		t := v.Type()
		nf := t.NumField()
		for i := 0; i < nf; i++ {
			f := t.Field(i)

			// Consider only exported fields
			if f.PkgPath != "" {
				continue
			}

			keyTag := f.Tag.Get("dbkey")
			if keyTag != "" {
				elem := &kelement{
					name:     f.Name,
					attrType: getAttrType(f.Type),
				}

				if keyTag == tagHashAttr {
					elem.keyType = sc.HashKey
					hashes[""] = elem
				} else if keyTag == tagRangeAttr {
					elem.keyType = sc.RangeKey
					ranges[""] = elem
				} else {
					panic("dynamini: invalid dbkey field tag")
				}
			}

			indexTag := f.Tag.Get("dbindex")
			if indexTag != "" {
				parts := strings.Split(indexTag, ",")
				if len(parts)&1 != 0 {
					panic("dynamini: invalid dbindex field tag")
				}

				for i := 0; i < len(parts); i += 2 {
					indices[parts[i+1]] = true
					if _, ok := projections[parts[i+1]]; !ok {
						projections[parts[i+1]] = map[string]bool{}
					}

					if parts[i] == tagHashAttr {
						hashes[parts[i+1]] = &kelement{
							name:     f.Name,
							keyType:  sc.HashKey,
							attrType: getAttrType(f.Type),
						}
						projections[parts[i+1]][f.Name] = true
					} else if parts[i] == tagRangeAttr {
						ranges[parts[i+1]] = &kelement{
							name:     f.Name,
							keyType:  sc.RangeKey,
							attrType: getAttrType(f.Type),
						}
						projections[parts[i+1]][f.Name] = true
					} else if parts[i] == tagProjectedAttr {
						projections[parts[i+1]][f.Name] = true
					} else {
						panic("dynamini: invalid dbindex field tag")
					}
				}
			}
		}

		// Create primary key schema
		pkey := []kelement{}
		pkhash, ok := hashes[""]
		if !ok {
			panic("dynamini: primary key is not tagged")
		}
		pkey = append(pkey, *pkhash)
		pkrange, ok := ranges[""]
		if ok {
			pkey = append(pkey, *pkrange)
		}

		// Project primary key to all indices
		for _, projs := range projections {
			for _, pk := range pkey {
				projs[pk.name] = true
			}
		}

		// Create index schema
		ischemas := []ischema{}
		for iname := range indices {
			hk, ok := hashes[iname]
			if !ok {
				// No hash provided. Get the primary
				// hash key. This is the case for local
				// secondary indices.
				hk = hashes[""]
			}

			sk := []kelement{}
			sk = append(sk, *hk)
			if rk, ok := ranges[iname]; ok {
				sk = append(sk, *rk)
			}

			idxType := globalIndexType
			if hk.name == pkhash.name {
				idxType = localIndexType
			}

			idxSchema := ischema{
				name:        iname,
				key:         sk,
				projections: mapToSlice(projections[iname]),
				indexType:   idxType,
			}

			ischemas = append(ischemas, idxSchema)
		}

		s = &tschema{pkey, ischemas}

		sreg.mutex.Lock()
		sreg.schemas[v.Type()] = s
		sreg.mutex.Unlock()
	}

	return s
}

func mapToSlice(m map[string]bool) []string {
	res := make([]string, 0, len(m))
	for k := range m {
		res = append(res, k)
	}

	return res
}

func getAttrType(t reflect.Type) sc.AttributeType {
	switch t.Kind() {
	case reflect.String:
		return sc.StringType
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint,
		reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Float32, reflect.Float64:

		return sc.NumberType
	case reflect.Array, reflect.Slice:
		et := t.Elem().Kind()
		if et != reflect.Uint8 {
			panic("dynamini: field must be a byte slice, number or string")
		}
		return sc.StringType
	default:
		panic("dynamini: field must be a byte slice, number or string")
	}
}

// getKey returns a key from a value. This is done by
// first checking the primary key in val and returns it
// if it's nonempty. If it's empty, it will return the
// first nonempty secondary key.
func getKey(item interface{}) (*dbkey, error) {
	k, err := getPrimaryKey(item)
	if err != nil {
		k, err = getSecondaryKey(item)
	}

	return k, err
}

func getPrimaryKey(item interface{}) (*dbkey, error) {
	val := reflect.Indirect(reflect.ValueOf(item))
	item = val.Interface()

	kv, err := dbattribute.ConvertToMap(item)
	if err != nil {
		return nil, fmt.Errorf("dynamini: invalid item (%v)", err)
	}

	schema := getSchema(item)
	key := &dbkey{value: dbitem{}}
	for _, k := range schema.key {
		v := valueByName(val, k.name)
		if isZeroValue(v) {
			return nil, fmt.Errorf("dynamini: incomplete primary key")
		}
		key.value[k.name] = kv[k.name]
	}

	if len(key.value) == 0 {
		return nil, fmt.Errorf("dynamini: no valid key")
	}

	return key, nil
}

func getSecondaryKey(item interface{}) (*dbkey, error) {
	val := reflect.Indirect(reflect.ValueOf(item))
	item = val.Interface()

	kv, err := dbattribute.ConvertToMap(item)
	if err != nil {
		return nil, fmt.Errorf("dynamini: invalid item (%v)", err)
	}

	key := &dbkey{value: dbitem{}}
	schema := getSchema(item)

Indices:
	for _, idx := range schema.indices {
		for _, k := range idx.key {
			v := val.FieldByName(k.name)
			if isZeroValue(v) {
				key.value = dbitem{}
				continue Indices
			}

			key.value[k.name] = kv[k.name]
		}

		key.indexName = idx.name
		key.indexType = idx.indexType
		break
	}

	if len(key.value) == 0 {
		return nil, fmt.Errorf("dynamini: no valid key")
	}

	return key, nil
}

func removeEmptyAttr(item dbitem) dbitem {
	for attrName, attrValue := range item {
		if attrValue.S != nil && *attrValue.S == "" {
			delete(item, attrName)
		} else if attrValue.NULL != nil && *attrValue.NULL == true {
			delete(item, attrName)
		} else if attrValue.M != nil {
			removeEmptyAttr(attrValue.M)
		}
	}

	return item
}

func toPtr(v interface{}) interface{} {
	switch vv := v.(type) {
	case string:
		if vv != "" {
			return &vv
		}
		return (*string)(nil)

	case bool:
		return &vv

	case uint, uint8, uint16, uint32, uint64,
		int, int8, int16, int32, int64:

		rv := reflect.ValueOf(vv)
		ret := reflect.New(reflect.PtrTo(rv.Type())).Elem()
		if rv.Int() >= 0 {
			pv := reflect.New(rv.Type())
			pv.Elem().Set(rv)
			ret.Set(pv)
		}
		return ret.Interface()
	default:
		panic("dynamini: cannot convert type to pointer")
	}
}

func isZeroValue(val reflect.Value) bool {
	return val.Interface() == reflect.Zero(val.Type()).Interface()
}

func valueByName(val reflect.Value, name string) reflect.Value {
	if val.Kind() == reflect.Struct {
		return val.FieldByName(name)
	} else if val.Kind() == reflect.Map {
		return val.MapIndex(reflect.ValueOf(name))
	}

	return reflect.Zero(val.Type())
}

func checkType(item interface{}, types ...interface{}) error {
	t := reflect.TypeOf(item)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for _, validType := range types {
		switch vt := validType.(type) {
		case reflect.Kind:
			if t.Kind() == vt {
				return nil
			}
		default:
			if t == reflect.TypeOf(validType) {
				return nil
			}
		}
	}

	return fmt.Errorf("dynamini: invalid type (%v)", t)
}

func checkPtrType(item interface{}, types ...interface{}) error {
	t := reflect.TypeOf(item)
	if t.Kind() != reflect.Ptr {
		return fmt.Errorf("dynamini: invalid type (%v)", t)
	}

	t = t.Elem()
	for _, validType := range types {
		switch vt := validType.(type) {
		case reflect.Kind:
			if t.Kind() == vt {
				return nil
			}
		default:
			if t == reflect.TypeOf(validType) {
				return nil
			}
		}
	}

	return fmt.Errorf("dynamini: invalid type (%v)", reflect.TypeOf(item))
}

func checkSliceType(item interface{}, types ...interface{}) error {
	t := reflect.TypeOf(item)
	if t.Kind() != reflect.Slice {
		return fmt.Errorf("dynamini: invalid type (%v)", t)
	}

	t = t.Elem()
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	for _, validType := range types {
		switch vt := validType.(type) {
		case reflect.Kind:
			if t.Kind() == vt {
				return nil
			}
		default:
			if t == reflect.TypeOf(validType) {
				return nil
			}
		}
	}

	return fmt.Errorf("dynamini: invalid type (%v)", reflect.TypeOf(item))
}

func max(a, b int) int {
	if a > b {
		return a
	}

	return b
}

func randString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

	b := make([]byte, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
