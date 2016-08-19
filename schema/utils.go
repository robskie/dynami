package schema

import (
	"reflect"
	"sort"
	"strings"
	"sync"
)

const (
	tagHashAttr      = "hash"
	tagRangeAttr     = "range"
	tagProjectedAttr = "project"
)

var register = struct {
	mutex   *sync.RWMutex
	schemas map[reflect.Type]*Table
}{
	&sync.RWMutex{},
	map[reflect.Type]*Table{},
}

// GetSchema is a utility function that returns an
// incomplete table schema from the given item. If
// a complete table schema is needed, use NewTable
// instead.
func GetSchema(item interface{}) *Table {
	v := reflect.Indirect(reflect.ValueOf(item))

	register.mutex.RLock()
	s, ok := register.schemas[v.Type()]
	register.mutex.RUnlock()

	if !ok {
		// Process map schema
		if v.Kind() == reflect.Map {
			m := v.Interface().(map[string]interface{})
			mk := make([]string, 0, len(m))
			for k := range m {
				mk = append(mk, k)
			}
			sort.Strings(mk)

			ks := make([]Key, len(mk))
			for i, k := range mk {
				ks[i] = Key{Name: k}
			}

			return &Table{Key: ks}
		}

		// Process struct schema

		// map key is the index name for
		// the Key element. Empty map key
		// means it is a primary key.
		hashes := map[string]*Key{}
		ranges := map[string]*Key{}
		projs := map[string]map[string]bool{}

		indices := map[string]bool{}
		defs := map[string]*Attribute{}

		// Extract table schema from field tags
		t := v.Type()
		nf := t.NumField()
		for i := 0; i < nf; i++ {
			f := t.Field(i)

			// Consider only exported fields
			if f.PkgPath != "" {
				continue
			}

			// Get name from dynamodbav or json tag
			nameTag := f.Tag.Get("dynamodbav")
			if nameTag == "" {
				nameTag = f.Tag.Get("json")
			}
			tags := strings.Split(nameTag, ",")
			if len(tags) > 0 && tags[0] != "" {
				f.Name = tags[0]
			}

			keyTag := f.Tag.Get("dbkey")
			if keyTag != "" {
				ks := &Key{Name: f.Name}
				defs[f.Name] = &Attribute{
					Name: f.Name,
					Type: getAttrType(f.Type),
				}

				if keyTag == tagHashAttr {
					ks.Type = HashKey
					hashes[""] = ks
				} else if keyTag == tagRangeAttr {
					ks.Type = RangeKey
					ranges[""] = ks
				} else {
					panic("dynami: invalid dbkey field tag")
				}
			}

			indexTag := f.Tag.Get("dbindex")
			if indexTag != "" {
				parts := strings.Split(indexTag, ",")
				if len(parts)&1 != 0 {
					panic("dynami: invalid dbindex field tag")
				}

				for i := 0; i < len(parts); i += 2 {
					indices[parts[i+1]] = true
					proj, ok := projs[parts[i+1]]
					if !ok {
						proj = map[string]bool{}
						projs[parts[i+1]] = proj
					}

					if parts[i] == tagHashAttr {
						defs[f.Name] = &Attribute{
							Name: f.Name,
							Type: getAttrType(f.Type),
						}
						hashes[parts[i+1]] = &Key{
							Name: f.Name,
							Type: HashKey,
						}
						proj[f.Name] = true
					} else if parts[i] == tagRangeAttr {
						defs[f.Name] = &Attribute{
							Name: f.Name,
							Type: getAttrType(f.Type),
						}
						ranges[parts[i+1]] = &Key{
							Name: f.Name,
							Type: RangeKey,
						}
						proj[f.Name] = true
					} else if parts[i] == tagProjectedAttr {
						proj[f.Name] = true
					} else {
						panic("dynami: invalid dbindex field tag")
					}
				}
			}
		}

		// Create primary key schema
		pkey := []Key{}
		pkhash, ok := hashes[""]
		if !ok {
			panic("dynami: primary key is not tagged")
		}
		pkey = append(pkey, *pkhash)
		pkrange, ok := ranges[""]
		if ok {
			pkey = append(pkey, *pkrange)
		}

		// Project primary key to all indices
		for _, projs := range projs {
			for _, pk := range pkey {
				projs[pk.Name] = true
			}
		}

		// Create secondary indices
		localIdxs := []SecondaryIndex{}
		globalIdxs := []SecondaryIndex{}
		for idx := range indices {
			isLocalIdx := false
			sidx := SecondaryIndex{Name: idx}

			// Add projection
			proj := &Projection{Type: ProjectInclude}
			for p := range projs[idx] {
				proj.Include = append(proj.Include, p)
			}
			sidx.Projection = proj

			// Add hash key
			hk, ok := hashes[idx]
			if !ok {
				// No hash provided. Get the primary
				// hash key. This is the case for local
				// secondary indices.
				hk = hashes[""]
				isLocalIdx = true
			}
			sidx.Key = append(sidx.Key, *hk)

			// Add range key
			if rk, ok := ranges[idx]; ok {
				sidx.Key = append(sidx.Key, *rk)
			}

			// Add to global or local secondary index
			if isLocalIdx {
				localIdxs = append(localIdxs, sidx)
			} else {
				globalIdxs = append(globalIdxs, sidx)
			}
		}

		// Create attribute definitions
		attributes := make([]Attribute, 0, len(defs))
		for _, def := range defs {
			attributes = append(attributes, *def)
		}

		s = &Table{
			Attributes: attributes,
			Key:        pkey,
			LocalSecondaryIndexes:  localIdxs,
			GlobalSecondaryIndexes: globalIdxs,
		}

		// Register schema
		register.mutex.Lock()
		register.schemas[v.Type()] = s
		register.mutex.Unlock()
	}

	return s
}

func getAttrType(t reflect.Type) AttributeType {
	switch t.Kind() {
	case reflect.String:
		return StringType
	case reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint,
		reflect.Uint8, reflect.Uint16, reflect.Uint32,
		reflect.Uint64, reflect.Float32, reflect.Float64:

		return NumberType
	case reflect.Array, reflect.Slice:
		et := t.Elem().Kind()
		if et != reflect.Uint8 {
			panic("dynami: field must be a byte slice, number or string")
		}
		return StringType
	default:
		panic("dynami: field must be a byte slice, number or string")
	}
}
