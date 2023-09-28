package example_example

import (
	bytes "bytes"
	context "context"
	json "encoding/json"
	fmt "fmt"
	env "github.com/catalystsquad/app-utils-go/env"
	errorutils "github.com/catalystsquad/app-utils-go/errorutils"
	logging "github.com/catalystsquad/app-utils-go/logging"
	v8 "github.com/elastic/go-elasticsearch/v8"
	esapi "github.com/elastic/go-elasticsearch/v8/esapi"
	esutil "github.com/elastic/go-elasticsearch/v8/esutil"
	errorx "github.com/joomcode/errorx"
	lo "github.com/samber/lo"
	logrus "github.com/sirupsen/logrus"
	strings "strings"
)

type Document struct {
	Id       string     `json:"id,omitempty"`
	Type     string     `json:"type,omitempty"`
	Metadata []Metadata `json:"metadata,omitempty"`
}

type Metadata struct {
	Key          *string  `json:"key,omitempty"`
	KeywordValue *string  `json:"keywordValue,omitempty"`
	StringValue  *string  `json:"stringValue,omitempty"`
	LongValue    *int64   `json:"longValue,omitempty"`
	DoubleValue  *float64 `json:"doubleValue,omitempty"`
	DateValue    *int64   `json:"dateValue,omitempty"`
	BoolValue    *bool    `json:"boolValue,omitempty"`
}

const ThingEsType = "Thing"
const Thing2EsType = "Thing2"

const indexName = "data"

var addresses = env.GetEnvOrDefault("ELASTICSEARCH_ADDRESSES", "http://localhost:9200")
var flushInterval = env.GetEnvAsDurationOrDefault("ELASTICSEARCH_FLUSH_INTERVAL", "5s")
var Client *v8.Client
var BulkIndexer esutil.BulkIndexer

func init() {
	cfg := v8.Config{
		Addresses: strings.Split(addresses, ","),
	}
	var err error
	Client, err = v8.NewClient(cfg)
	errorutils.LogOnErr(logging.Log.WithFields(logrus.Fields{"addresses": addresses}), "error creating elasticsearch client", err)
	if err != nil {
		logging.Log.Info("elasticsearch client initialized")
	}
	if err == nil {
		BulkIndexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:         indexName,
			Client:        Client,
			FlushInterval: flushInterval,
		})
		errorutils.LogOnErr(logging.Log.WithFields(logrus.Fields{"addresses": addresses}), "error creating elasticsearch bulk indexer", err)
		if err == nil {
			logging.Log.Info("elasticsearch bulk indexer initialized")
		}
	}
}

func EnsureIndex(client *v8.Client) error {
	exists, err := indexExists(client)
	if err != nil {
		return err
	}
	if !exists {
		err = createIndex(client)
		if err != nil {
			return err
		}
	}
	return putMappings(client)
}

func createIndex(client *v8.Client) error {
	req := esapi.IndicesCreateRequest{
		Index: indexName,
	}
	response, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return errorx.IllegalState.New("unexpected status code creating index: %d", response.StatusCode)
	}
	return nil
}

func putMappings(client *v8.Client) error {
	settings := strings.NewReader("{\"properties\":{\"id\":{\"type\":\"keyword\"},\"type\":{\"type\":\"keyword\"},\"metadata\":{\"type\":\"nested\",\"properties\":{\"key\":{\"type\":\"keyword\"},\"keywordValue\":{\"type\":\"keyword\"},\"stringValue\":{\"type\":\"text\"},\"longValue\":{\"type\":\"long\"},\"doubleValue\":{\"type\":\"double\"},\"dateValue\":{\"type\":\"date\"},\"boolValue\":{\"type\":\"boolean\"}}}}}")
	req := esapi.IndicesPutMappingRequest{
		Index: []string{indexName},
		Body:  settings,
	}
	response, err := req.Do(context.Background(), client)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return errorx.IllegalState.New("unexpected status code putting index mappings: %d", response.StatusCode)
	}
	return nil
}

func indexExists(client *v8.Client) (bool, error) {
	req := esapi.IndicesGetRequest{
		Index: []string{indexName},
	}
	response, err := req.Do(context.Background(), client)
	if err != nil {
		errorutils.LogOnErr(logging.Log.WithFields(logrus.Fields{"index": indexName}), "error getting index", err)
		return false, err
	}
	if response.StatusCode == 404 {
		return false, nil
	}
	if response.StatusCode == 200 {
		return true, nil
	}
	return false, errorx.IllegalState.New("unexpected status code getting index: %d", response.StatusCode)
}

func QueueDocsForIndexing(ctx context.Context, docs []Document, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	for _, doc := range docs {
		if err := QueueDocForIndexing(ctx, doc, onSuccess, onFailure); err != nil {
			return err
		}
	}
	return nil
}

func QueueDocForIndexing(ctx context.Context, doc Document, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	data, err := json.Marshal(doc)
	if err != nil {
		errorutils.LogOnErr(nil, "error marshalling document to json", err)
		return err
	}
	item := esutil.BulkIndexerItem{
		Action:     "index",
		Index:      indexName,
		DocumentID: doc.Id,
		Body:       bytes.NewReader(data),
	}
	if onSuccess != nil {
		item.OnSuccess = onSuccess
	}
	if onFailure != nil {
		item.OnFailure = onFailure
	}
	err = BulkIndexer.Add(ctx, item)
	errorutils.LogOnErr(nil, "error adding item to bulk indexer", err)
	return err
}

func (s *Thing) ToEsDocuments() ([]Document, error) {
	docs := []Document{}
	doc := Document{
		Id:       *s.Id,
		Type:     ThingEsType,
		Metadata: []Metadata{},
	}

	if s.Id != nil {

		IdMetaData := Metadata{
			Key: lo.ToPtr("Id"),

			StringValue:  lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.Id))),
			KeywordValue: lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.Id))),
		}

		doc.Metadata = append(doc.Metadata, IdMetaData)

	}

	ADoubleMetaData := Metadata{
		Key: lo.ToPtr("ADouble"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.ADouble)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.ADouble)),
	}

	ADoubleMetaData.LongValue = lo.ToPtr(int64(s.ADouble))
	ADoubleMetaData.DoubleValue = lo.ToPtr(float64(s.ADouble))

	doc.Metadata = append(doc.Metadata, ADoubleMetaData)

	AFloatMetaData := Metadata{
		Key: lo.ToPtr("AFloat"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.AFloat)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.AFloat)),
	}

	AFloatMetaData.LongValue = lo.ToPtr(int64(s.AFloat))
	AFloatMetaData.DoubleValue = lo.ToPtr(float64(s.AFloat))

	doc.Metadata = append(doc.Metadata, AFloatMetaData)

	AnInt32MetaData := Metadata{
		Key: lo.ToPtr("AnInt32"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.AnInt32)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.AnInt32)),
	}

	AnInt32MetaData.LongValue = lo.ToPtr(int64(s.AnInt32))
	AnInt32MetaData.DoubleValue = lo.ToPtr(float64(s.AnInt32))

	doc.Metadata = append(doc.Metadata, AnInt32MetaData)

	AnInt64MetaData := Metadata{
		Key: lo.ToPtr("AnInt64"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.AnInt64)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.AnInt64)),
	}

	AnInt64MetaData.LongValue = lo.ToPtr(int64(s.AnInt64))
	AnInt64MetaData.DoubleValue = lo.ToPtr(float64(s.AnInt64))

	doc.Metadata = append(doc.Metadata, AnInt64MetaData)

	ABoolMetaData := Metadata{
		Key: lo.ToPtr("ABool"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.ABool)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.ABool)),
	}

	ABoolMetaData.BoolValue = lo.ToPtr(s.ABool)

	doc.Metadata = append(doc.Metadata, ABoolMetaData)

	AStringMetaData := Metadata{
		Key: lo.ToPtr("AString"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.AString)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.AString)),
	}

	doc.Metadata = append(doc.Metadata, AStringMetaData)

	for _, val := range s.RepeatedScalarField {
		metaData := Metadata{
			Key: lo.ToPtr("RepeatedScalarField"),

			StringValue:  lo.ToPtr(fmt.Sprintf("%v", val)),
			KeywordValue: lo.ToPtr(fmt.Sprintf("%v", val)),
		}

		doc.Metadata = append(doc.Metadata, metaData)
	}

	if s.OptionalScalarField != nil {

		OptionalScalarFieldMetaData := Metadata{
			Key: lo.ToPtr("OptionalScalarField"),

			StringValue:  lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.OptionalScalarField))),
			KeywordValue: lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.OptionalScalarField))),
		}

		doc.Metadata = append(doc.Metadata, OptionalScalarFieldMetaData)

	}

	if s.AssociatedThing != nil {

		ThingAssociatedThingDoc := Document{
			Type: "ThingAssociatedThing",
			Metadata: []Metadata{
				{
					Key:          lo.ToPtr("ThingId"),
					KeywordValue: s.Id,
				},
				{
					Key:          lo.ToPtr("Thing2Id"),
					KeywordValue: s.AssociatedThing.Id,
				},
			},
		}
		docs = append(docs, ThingAssociatedThingDoc)

	}

	if s.OptionalAssociatedThing != nil {

		ThingOptionalAssociatedThingDoc := Document{
			Type: "ThingOptionalAssociatedThing",
			Metadata: []Metadata{
				{
					Key:          lo.ToPtr("ThingId"),
					KeywordValue: s.Id,
				},
				{
					Key:          lo.ToPtr("Thing2Id"),
					KeywordValue: s.OptionalAssociatedThing.Id,
				},
			},
		}
		docs = append(docs, ThingOptionalAssociatedThingDoc)

	}

	if s.RepeatedMessages != nil {

		for _, message := range s.RepeatedMessages {
			ThingRepeatedMessagesDoc := Document{
				Type: "ThingRepeatedMessages",
				Metadata: []Metadata{
					{
						Key:          lo.ToPtr("ThingId"),
						KeywordValue: s.Id,
					},
					{
						Key:          lo.ToPtr("Thing2Id"),
						KeywordValue: message.Id,
					},
				},
			}
			docs = append(docs, ThingRepeatedMessagesDoc)
		}

	}

	ATimestampMetaData := Metadata{
		Key: lo.ToPtr("ATimestamp"),
	}

	ATimestampMetaData.DateValue = lo.ToPtr(s.ATimestamp.AsTime().UTC().UnixMilli())

	doc.Metadata = append(doc.Metadata, ATimestampMetaData)

	AnEnumMetaData := Metadata{
		Key: lo.ToPtr("AnEnum"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.AnEnum)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.AnEnum)),
	}

	AnEnumMetaData.LongValue = lo.ToPtr(int64(s.AnEnum.Number()))

	doc.Metadata = append(doc.Metadata, AnEnumMetaData)

	if s.AnOptionalInt != nil {

		AnOptionalIntMetaData := Metadata{
			Key: lo.ToPtr("AnOptionalInt"),

			StringValue:  lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.AnOptionalInt))),
			KeywordValue: lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.AnOptionalInt))),
		}

		AnOptionalIntMetaData.LongValue = lo.ToPtr(int64(lo.FromPtr(s.AnOptionalInt)))
		AnOptionalIntMetaData.DoubleValue = lo.ToPtr(float64(lo.FromPtr(s.AnOptionalInt)))

		doc.Metadata = append(doc.Metadata, AnOptionalIntMetaData)

	}

	if s.OptionalTimestamp != nil {

		OptionalTimestampMetaData := Metadata{
			Key: lo.ToPtr("OptionalTimestamp"),
		}

		OptionalTimestampMetaData.DateValue = lo.ToPtr(s.OptionalTimestamp.AsTime().UTC().UnixMilli())

		doc.Metadata = append(doc.Metadata, OptionalTimestampMetaData)

	}

	for _, val := range s.RepeatedInt32 {
		metaData := Metadata{
			Key: lo.ToPtr("RepeatedInt32"),

			StringValue:  lo.ToPtr(fmt.Sprintf("%v", val)),
			KeywordValue: lo.ToPtr(fmt.Sprintf("%v", val)),
		}

		metaData.LongValue = lo.ToPtr(int64(val))
		metaData.DoubleValue = lo.ToPtr(float64(val))

		doc.Metadata = append(doc.Metadata, metaData)
	}

	docs = append(docs, doc)
	return docs, nil
}

func (s *Thing) Index(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *Thing2) ToEsDocuments() ([]Document, error) {
	docs := []Document{}
	doc := Document{
		Id:       *s.Id,
		Type:     Thing2EsType,
		Metadata: []Metadata{},
	}

	if s.Id != nil {

		IdMetaData := Metadata{
			Key: lo.ToPtr("Id"),

			StringValue:  lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.Id))),
			KeywordValue: lo.ToPtr(fmt.Sprintf("%v", lo.FromPtr(s.Id))),
		}

		doc.Metadata = append(doc.Metadata, IdMetaData)

	}

	NameMetaData := Metadata{
		Key: lo.ToPtr("Name"),

		StringValue:  lo.ToPtr(fmt.Sprintf("%v", s.Name)),
		KeywordValue: lo.ToPtr(fmt.Sprintf("%v", s.Name)),
	}

	doc.Metadata = append(doc.Metadata, NameMetaData)

	docs = append(docs, doc)
	return docs, nil
}

func (s *Thing2) Index(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}
