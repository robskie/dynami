package dynami

import (
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	db "github.com/aws/aws-sdk-go/service/dynamodb"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
)

// Query represents a client query. This may perform
// a DynamoDB query or scan operation depending on whether
// a hash filter is added or not.
type Query struct {
	db *db.DynamoDB

	table string
	index string

	hashExpr  string
	rangeExpr string

	nfilters   int
	filterExpr string

	attributeNames  map[string]*string
	attributeValues map[string]*db.AttributeValue

	limit          int
	scanForward    bool
	consistentRead bool

	err error
}

// Query returns a new query for the given table.
func (c *Client) Query(tableName string) *Query {
	if tableName == "" {
		return &Query{
			err: fmt.Errorf("dynami: empty table name"),
		}
	}

	return &Query{
		db:             c.db,
		table:          tableName,
		limit:          -1,
		scanForward:    true,
		consistentRead: false,
	}
}

// Index specifies which secondary index to query.
func (q *Query) Index(indexName string) *Query {
	if q.err != nil {
		return q
	}

	q.index = indexName
	return q
}

// Limit limits the number of results returned.
func (q *Query) Limit(limit int) *Query {
	if q.err != nil {
		return q
	} else if limit <= 0 {
		q.err = fmt.Errorf("dynami: limit must be greater than zero")
		return q
	}

	q.limit = limit
	return q
}

// Desc arranges the result by range key in
// descending order. The default order is ascending.
func (q *Query) Desc() *Query {
	q.scanForward = false
	return q
}

// Consistent sets the query to use strongly consistent reads.
// This is ignored for queries on global secondary indices.
func (q *Query) Consistent() *Query {
	q.consistentRead = true
	return q
}

func (q *Query) addAttributeName(placeholder string, value *string) {
	if q.attributeNames == nil {
		q.attributeNames = map[string]*string{}
	}
	q.attributeNames[placeholder] = value
}

func (q *Query) addAttributeValue(
	placeholder string,
	value *db.AttributeValue) {

	if q.attributeValues == nil {
		q.attributeValues = map[string]*db.AttributeValue{}
	}
	q.attributeValues[placeholder] = value
}

// HashFilter adds a hash filter to this query. Adding a hash filter
// to a query makes it perform a DynamoDB query operation instead
// of a scan operation.
func (q *Query) HashFilter(name string, value interface{}) *Query {
	if q.err != nil {
		return q
	}

	if name != "" && value != nil {
		q.hashExpr = "#H = :hv"
		q.addAttributeName("#H", aws.String(name))

		attr, err := dbattribute.ConvertTo(value)
		if err != nil {
			q.err = fmt.Errorf("dynami: hash filter value is invalid (%v)", err)
			return q
		}
		q.addAttributeValue(":hv", attr)
	}

	return q
}

// RangeFilter adds a range filter to this query.
// Valid expressions are comparison, BETWEEN, and
// begins_with filter expressions.
func (q *Query) RangeFilter(expr string, values ...interface{}) *Query {
	if q.err != nil {
		return q
	}

	if expr != "" && len(values) > 0 {
		v, err := parseExpression(expr, values)
		if err != nil {
			q.err = err
			return q
		}

		q.rangeExpr = " AND " + v.expr
		for _, n := range v.attrNames {
			q.addAttributeName(n.placeholder, n.value)
		}
		for _, v := range v.attrValues {
			q.addAttributeValue(v.placeholder, v.value)
		}
	}

	return q
}

// Filter adds a post filter expression to the
// query. Multiple filters are AND'ed together.
func (q *Query) Filter(expr string, values ...interface{}) *Query {
	if q.err != nil {
		return q
	}

	if expr != "" {
		v, err := parseExpression(expr, values)
		if err != nil {
			q.err = err
			return q
		}

		if q.nfilters > 0 {
			q.filterExpr += " AND "
		}
		q.filterExpr += v.expr
		q.nfilters++

		for _, n := range v.attrNames {
			q.addAttributeName(n.placeholder, n.value)
		}
		for _, v := range v.attrValues {
			q.addAttributeValue(v.placeholder, v.value)
		}
	}

	return q
}

// Run executes the query and returns a result iterator.
func (q *Query) Run() *ItemIterator {
	if q.err != nil {
		return &ItemIterator{}
	}

	qdb := q.db
	var lastKey map[string]*db.AttributeValue
	var outpItems []map[string]*db.AttributeValue

	// Query has key condition.
	// Perform a dynamodb Query operation.
	var queryInput interface{}
	if q.hashExpr != "" {
		keyExpr := q.hashExpr + q.rangeExpr
		qinput := &db.QueryInput{
			TableName:                 toPtr(q.table).(*string),
			IndexName:                 toPtr(q.index).(*string),
			KeyConditionExpression:    toPtr(keyExpr).(*string),
			FilterExpression:          toPtr(q.filterExpr).(*string),
			ExpressionAttributeNames:  q.attributeNames,
			ExpressionAttributeValues: q.attributeValues,
			Limit:            toPtr(int64(q.limit)).(*int64),
			ScanIndexForward: toPtr(q.scanForward).(*bool),
			ConsistentRead:   toPtr(q.consistentRead).(*bool),
		}
		qoutput, _ := qdb.Query(qinput)
		lastKey = qoutput.LastEvaluatedKey
		outpItems = qoutput.Items

		queryInput = qinput

	} else { // Perform a dynamodb Scan operation

		// If range expression is present,
		// append it to the filter expression.
		filterExpr := q.filterExpr + q.rangeExpr
		sinput := &db.ScanInput{
			TableName:                 toPtr(q.table).(*string),
			IndexName:                 toPtr(q.index).(*string),
			FilterExpression:          toPtr(filterExpr).(*string),
			ExpressionAttributeNames:  q.attributeNames,
			ExpressionAttributeValues: q.attributeValues,
			Limit:          toPtr(int64(q.limit)).(*int64),
			ConsistentRead: toPtr(q.consistentRead).(*bool),
		}

		soutput, _ := qdb.Scan(sinput)
		lastKey = soutput.LastEvaluatedKey
		outpItems = soutput.Items

		queryInput = sinput
	}

	return &ItemIterator{
		db:         qdb,
		limit:      q.limit,
		items:      outpItems,
		lastKey:    lastKey,
		queryInput: queryInput,
	}
}

// ItemIterator iterates over the result of a query.
type ItemIterator struct {
	db *db.DynamoDB

	index int
	limit int
	items []map[string]*db.AttributeValue

	// This is used to continue the query
	// if the results are greater than 1MB
	lastKey map[string]*db.AttributeValue

	// This can be a *dynamodb.ScanInput
	// or *dynamodb.QueryInput
	queryInput interface{}
}

// HasNext returns true if there are
// more query results to iterate over.
func (it *ItemIterator) HasNext() bool {
	if it.index < len(it.items) {
		return true
	}

	if len(it.lastKey) > 0 && (it.limit == -1 || it.index < it.limit) {
		var lastKey map[string]*db.AttributeValue
		var outpItems []map[string]*db.AttributeValue

		switch qin := it.queryInput.(type) {
		case *db.ScanInput:
			qin.ExclusiveStartKey = it.lastKey
			qout, _ := it.db.Scan(qin)

			outpItems = qout.Items
			lastKey = qout.LastEvaluatedKey
		case *db.QueryInput:
			qin.ExclusiveStartKey = it.lastKey
			qout, _ := it.db.Query(qin)

			outpItems = qout.Items
			lastKey = qout.LastEvaluatedKey
		}

		if len(outpItems) == 0 {
			return false
		}

		it.index = 0
		it.items = outpItems
		it.lastKey = lastKey

		return true
	}

	return false
}

// Next loads the next result to item. item must be a
// pointer to map[string]interface{} or a pointer to struct.
func (it *ItemIterator) Next(item interface{}) error {
	if !it.HasNext() {
		return fmt.Errorf("dynami: no more items to return")
	}

	if item != nil {
		err := dbattribute.UnmarshalMap(it.items[it.index], item)
		if err != nil {
			return fmt.Errorf("dynami: invalid item (%v)", err)
		}
	}

	it.index++
	return nil
}

type exprValue struct {
	expr       string
	attrNames  []attrName
	attrValues []attrValue
}

type attrName struct {
	value       *string
	placeholder string
}

type attrValue struct {
	value       *db.AttributeValue
	placeholder string
}

var (
	// This is used to replace the 'AND' in BETWEEN expressions so that
	// compound expressions joined by 'AND' can be easily extracted via
	// strings.Split function.
	reAndRepl = regexp.MustCompile(`(:[^\s]+\s)(AND)(\s:)`)

	// Matches a comparator expression. Capturing group 1 has the
	// attribute name and group 2 has the attribute value placeholder.
	reComp = regexp.MustCompile(`(.+)\s(?:>=|>|=|<|<=)\s(.+)`)

	// Matches a BETWEEN expression.
	// Note that 'AND' is replaced with '&'.
	reBetween = regexp.MustCompile(`(.+)\sBETWEEN\s(.+)\s&\s(.+)`)

	reFunc       = regexp.MustCompile(`(.+)\((.+)\)`)
	reIn         = regexp.MustCompile(`(.+)\sIN\s\((.+)\)`)
	reBeginsWith = regexp.MustCompile(`begins_with\((.+),\s(.+)\)`)
)

var errNoMatch = errors.New("dynami: no matches found")

var exprParser = []func(string) (string, []string, error){
	parseCompExpr,
	parseBetweenExpr,
	parseInExpr,
	parseFuncExpr,
}

func parseExpression(expr string, values []interface{}) (*exprValue, error) {
	v := &exprValue{}
	vphs := map[string]bool{}

	// Replace 'AND' in BETWEEN expressions to '&'
	expr = reAndRepl.ReplaceAllString(expr, "$1&$3")

	andSubExpr := strings.Split(expr, " AND ")
	for i, andExpr := range andSubExpr {
		orSubExpr := strings.Split(andExpr, " OR ")
		for j, orExpr := range orSubExpr {
			var attrName string
			var valuePlaceholder []string

			// Remove NOT keyword from the expression
			subExpr := strings.Replace(orExpr, "NOT ", "", -1)

			// Get the attribute name and value
			// placeholder from each subexpression
			var err error
			for _, parser := range exprParser {
				attrName, valuePlaceholder, err = parser(subExpr)
				if err == nil {
					break
				} else if err != errNoMatch {
					return nil, err
				}
			}

			if err != nil {
				return nil, fmt.Errorf("dynami: invalid expression")
			}

			// Check for duplicate value placeholders
			for _, ph := range valuePlaceholder {
				if _, ok := vphs[ph]; ok {
					return nil, fmt.Errorf("dynami: duplicate placeholder (%v)", ph)
				}
				vphs[ph] = true
			}

			// Replace actual attribute names with placeholders
			// and create attribute name placeholder and value pairs.
			e, attrNames := parseExprAttrName(orExpr, attrName)
			orSubExpr[j] = e

			// Create attribute value placeholder and value pairs.
			attrValues, err := parseExprAttrValue(valuePlaceholder, values)
			if err != nil {
				return nil, err
			}

			v.attrNames = append(v.attrNames, attrNames...)
			v.attrValues = append(v.attrValues, attrValues...)
			values = values[len(attrValues):]
		}

		andSubExpr[i] = strings.Join(orSubExpr, " OR ")
	}

	v.expr = strings.Join(andSubExpr, " AND ")
	v.expr = strings.Replace(v.expr, "&", "AND", -1)
	return v, nil
}

func parseFuncExpr(expr string) (string, []string, error) {
	if m := reFunc.FindStringSubmatch(expr); len(m) > 0 {
		if len(m) != 3 {
			return "", nil, fmt.Errorf("dynami: invalid expression (%s)", expr)
		}

		attrName := ""
		vph := []string{}
		funcName := trimLeftP(m[1])

		switch funcName {
		case "attribute_exists", "attribute_not_exists":
			attrName = trimRightP(m[2])

		case "attribute_type", "begins_with", "contains":
			parts := splitCSV(trimRightP(m[2]))
			if len(parts) != 2 {
				err := fmt.Errorf("dynami: invalid func expression (%s)", funcName)
				return "", nil, err
			}

			attrName = parts[0]
			vph = parts[1:]
		default:
			err := fmt.Errorf("dynami: unknown func expression (%s)", funcName)
			return "", nil, err
		}

		return attrName, vph, nil
	}

	return "", nil, errNoMatch
}

func parseInExpr(expr string) (string, []string, error) {
	if m := reIn.FindStringSubmatch(expr); len(m) > 0 {
		if len(m) != 3 {
			return "", nil, fmt.Errorf("dynami: invalid expression (%s)", expr)
		}

		return trimLeftP(m[1]), splitCSV(trimRightP(m[2])), nil
	}

	return "", nil, errNoMatch
}

func parseBetweenExpr(expr string) (string, []string, error) {
	if m := reBetween.FindStringSubmatch(expr); len(m) > 0 {
		if len(m) != 4 {
			return "", nil, fmt.Errorf("dynami: invalid expression (%s)", expr)
		}

		return trimLeftP(m[1]), []string{m[2], trimRightP(m[3])}, nil
	}

	return "", nil, errNoMatch
}

func parseCompExpr(expr string) (string, []string, error) {
	if m := reComp.FindStringSubmatch(expr); len(m) > 0 {
		if len(m) != 3 {
			return "", nil, fmt.Errorf("dynami: invalid expression (%s)", expr)
		}

		// Parse size function if present
		attrName := trimLeftP(m[1])
		if mf := reFunc.FindStringSubmatch(attrName); len(mf) > 0 {
			if len(mf) != 3 || mf[1] != "size" {
				return "", nil, fmt.Errorf("dynami: invalid expression (%s)", expr)
			}
			attrName = mf[2]
		}

		return attrName, []string{trimRightP(m[2])}, nil
	}

	return "", nil, errNoMatch
}

func parseExprAttrName(
	expr string,
	exprAttrName string) (string, []attrName) {

	// Parse dot separator
	names := strings.Split(exprAttrName, ".")

	attrs := make([]attrName, len(names))
	for i, n := range names {
		ph := "#" + n + "_PH"
		expr = strings.Replace(expr, n, ph, 1)
		attrs[i] = attrName{
			value:       aws.String(n),
			placeholder: ph,
		}
	}

	return expr, attrs
}

func parseExprAttrValue(
	placeholder []string,
	exprAttrValue []interface{}) ([]attrValue, error) {

	if len(placeholder) > len(exprAttrValue) {
		return nil, fmt.Errorf("dynami: inadequate expression values")
	}

	attrs := make([]attrValue, len(placeholder))
	for i, ph := range placeholder {
		if ph[0] != ':' {
			return nil, fmt.Errorf("dynami: invalid value placeholder (%v)", ph)
		}

		attr, err := dbattribute.ConvertTo(exprAttrValue[i])
		if err != nil {
			return nil, fmt.Errorf("dynami: invalid expression value (%v)", err)
		}

		attrs[i] = attrValue{
			value:       attr,
			placeholder: ph,
		}
	}

	return attrs, nil
}

func trimLeftP(s string) string {
	return strings.TrimLeft(s, "(")
}

func trimRightP(s string) string {
	return strings.TrimRight(s, ")")
}

func splitCSV(s string) []string {
	v := strings.Split(s, ",")
	for i, vv := range v {
		v[i] = strings.TrimSpace(vv)
	}

	return v
}
