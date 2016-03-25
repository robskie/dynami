package dynami

import (
	"fmt"
	"sort"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	dbattribute "github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/dynamodbstreams"
)

const errResourceNotFound = "ResourceNotFoundException"

// RecordType tells if an item is
// added, updated, or deleted from
// a table.
type RecordType string

// These are the valid record types. A record is created
// when an item is added, updated, or deleted from a table.
const (
	AddedRecord   RecordType = dynamodbstreams.OperationTypeInsert
	UpdatedRecord RecordType = dynamodbstreams.OperationTypeModify
	DeletedRecord RecordType = dynamodbstreams.OperationTypeRemove
	unknownRecord RecordType = "UNKNOWN"
)

type record struct {
	recordType RecordType
	dbitem     dbitem
}

type seqNum string

func (sn *seqNum) less(num *seqNum) bool {
	if num == nil {
		return true
	}

	if len(*sn) != len(*num) {
		return len(*sn) < len(*num)
	}

	return *sn < *num
}

func (sn *seqNum) lessEq(num *seqNum) bool {
	if *sn == *num {
		return true
	}

	return sn.less(num)
}

type seqNumRange struct {
	start *seqNum
	end   *seqNum
}

func (sr seqNumRange) has(sn *seqNum) bool {
	if sn == nil {
		return false
	}
	return sr.start.lessEq(sn) && (sr.end == nil || !sr.end.less(sn))
}

type shard struct {
	id       string
	seqRange *seqNumRange
}

type bySeqNum []shard

func (s bySeqNum) Len() int      { return len(s) }
func (s bySeqNum) Swap(i, j int) { s[i], s[j] = s[j], s[i] }
func (s bySeqNum) Less(i, j int) bool {
	return s[i].seqRange.start.less(s[j].seqRange.start)
}

type shardIterator struct {
	dbs *dynamodbstreams.DynamoDBStreams

	id    string
	arn   string
	shard *shard
}

func newShardIterator(
	dbs *dynamodbstreams.DynamoDBStreams,
	arn string,
	shard *shard,
	exclStartSeqNum *seqNum) (*shardIterator, error) {

	st := &shardIterator{
		dbs:   dbs,
		arn:   arn,
		shard: shard,
	}

	if shard.seqRange.has(exclStartSeqNum) {
		resp, err := dbs.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
			StreamArn:         aws.String(arn),
			ShardId:           aws.String(shard.id),
			SequenceNumber:    (*string)(exclStartSeqNum),
			ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeAfterSequenceNumber),
		})
		if err == nil {
			st.id = *resp.ShardIterator
			return st, nil
		}
	}

	resp, err := dbs.GetShardIterator(&dynamodbstreams.GetShardIteratorInput{
		StreamArn:         aws.String(arn),
		ShardId:           aws.String(shard.id),
		ShardIteratorType: aws.String(dynamodbstreams.ShardIteratorTypeTrimHorizon),
	})
	if err != nil {
		return nil, err
	}

	st.id = *resp.ShardIterator
	return st, nil
}

func (st *shardIterator) refresh(exclStartSeqNum *seqNum) error {
	st, err := newShardIterator(st.dbs, st.arn, st.shard, exclStartSeqNum)
	return err
}

// GetStream returns the stream record iterator for the given table.
func (c *Client) GetStream(tableName string) (*RecordIterator, error) {
	table, err := c.DescribeTable(tableName)
	if err != nil {
		return nil, fmt.Errorf("dynami: cannot get stream (%v)", err)
	}

	it := &RecordIterator{
		arn:               table.PStreamARN,
		dbs:               dynamodbstreams.New(c.session),
		processedShardIDs: map[string]bool{},
	}

	return it, nil
}

// RecordIterator iterates through stream records.
type RecordIterator struct {
	arn string

	index         int
	records       []record
	lastRecSeqNum *seqNum

	lastShardID       *string
	processedShardIDs map[string]bool

	dbs *dynamodbstreams.DynamoDBStreams
}

// HasNext returns true if there are
// still some records to iterate over.
// Use this method to get all currently
// available records.
func (it *RecordIterator) HasNext() bool {
	return it.getNext(false)
}

// WaitNext waits for the next record in the
// stream. This returns false if the stream is
// closed. Use this to get current and future
// records.
func (it *RecordIterator) WaitNext() bool {
	return it.getNext(true)
}

// Next loads the next record. record must be a pointer
// to struct or a pointer to a map[string]interface{}.
func (it *RecordIterator) Next(record interface{}) (RecordType, error) {
	if it.index >= len(it.records) {
		return unknownRecord, fmt.Errorf("dynami: no more records to return")
	}

	rec := it.records[it.index]
	if record != nil {
		err := dbattribute.ConvertFromMap(rec.dbitem, record)
		if err != nil {
			return unknownRecord, fmt.Errorf("dynami: invalid record (%v)", err)
		}
	}

	it.index++
	return rec.recordType, nil
}

func (it *RecordIterator) getNext(wait bool) bool {
	if it.arn == "" {
		return false
	} else if it.index < len(it.records) {
		return true
	}

	// Reset index and records buffer
	it.index = 0
	it.records = it.records[:0]

	dbs := it.dbs
	ticker := time.NewTicker(200 * time.Millisecond)
	for range ticker.C {
		// Describe streams
		resp, err := dbs.DescribeStream(&dynamodbstreams.DescribeStreamInput{
			StreamArn:             aws.String(it.arn),
			ExclusiveStartShardId: it.lastShardID,
		})
		if err != nil && err.(awserr.Error).Code() == errResourceNotFound {
			return false
		}

		desc := resp.StreamDescription
		if desc.LastEvaluatedShardId != nil {
			it.lastShardID = desc.LastEvaluatedShardId
		}

		// If the stream has been disabled, no need to wait for more records.
		if *desc.StreamStatus == dynamodbstreams.StreamStatusDisabled {
			wait = false
		}

		// Process each shard
		shards := it.getShards(desc.Shards)
		for _, s := range shards {
			st, err := newShardIterator(it.dbs, it.arn, &s, it.lastRecSeqNum)
			if err != nil {
				if err.(awserr.Error).Code() == errResourceNotFound {
					return false
				}

				break
			}

			recs, err := it.getRecords(st)
			it.records = append(it.records, recs...)
			if err != nil {
				if err.(awserr.Error).Code() == errResourceNotFound {
					return false
				}
			}
		}

		// No more data to be retrieved and waiting is disabled
		if desc.LastEvaluatedShardId == nil && !wait {
			break
		} else if len(it.records) > 0 {
			break
		}
	}

	return len(it.records) > 0
}

func (it *RecordIterator) getShards(dbShards []*dynamodbstreams.Shard) []shard {
	if len(dbShards) == 0 {
		return nil
	}

	shards := []shard{}
	for _, dbs := range dbShards {
		id := *dbs.ShardId
		if it.processedShardIDs[id] {
			continue
		}

		dbSeqNumRange := dbs.SequenceNumberRange
		seqRange := &seqNumRange{
			start: (*seqNum)(dbSeqNumRange.StartingSequenceNumber),
			end:   (*seqNum)(dbSeqNumRange.EndingSequenceNumber),
		}

		shards = append(shards, shard{
			id:       *dbs.ShardId,
			seqRange: seqRange,
		})
	}

	sort.Sort(bySeqNum(shards))
	return shards
}

func (it *RecordIterator) getRecords(st *shardIterator) ([]record, error) {
	records := []record{}

	dbs := st.dbs
	shardIt := &st.id
	ticker := time.NewTicker(200 * time.Millisecond)
	for range ticker.C {
		resp, err := dbs.GetRecords(&dynamodbstreams.GetRecordsInput{
			ShardIterator: shardIt,
		})
		if err != nil {
			if err.(awserr.Error).Code() == errResourceNotFound {
				return records, err
			}

			err = st.refresh(it.lastRecSeqNum)
			if err != nil {
				return records, err
			}
			shardIt = &st.id

			continue
		}

		for _, r := range resp.Records {
			rtype := RecordType(*r.EventName)
			it.lastRecSeqNum = (*seqNum)(r.Dynamodb.SequenceNumber)
			rec := record{recordType: rtype}

			if rtype == DeletedRecord {
				rec.dbitem = r.Dynamodb.OldImage
			} else {
				rec.dbitem = r.Dynamodb.NewImage
			}

			records = append(records, rec)
		}

		// Shard iterator has been closed
		if resp.NextShardIterator == nil {
			it.processedShardIDs[st.shard.id] = true
			break
		}
		shardIt = resp.NextShardIterator

		// No more records for now
		if len(resp.Records) == 0 {
			break
		}
	}

	return records, nil
}
