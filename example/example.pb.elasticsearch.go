package example_example

import (
	bytes "bytes"
	context "context"
	json "encoding/json"
	fmt "fmt"
	errorutils "github.com/catalystsquad/app-utils-go/errorutils"
	logging "github.com/catalystsquad/app-utils-go/logging"
	v8 "github.com/elastic/go-elasticsearch/v8"
	esapi "github.com/elastic/go-elasticsearch/v8/esapi"
	esutil "github.com/elastic/go-elasticsearch/v8/esutil"
	types "github.com/elastic/go-elasticsearch/v8/typedapi/types"
	errorx "github.com/joomcode/errorx"
	lo "github.com/samber/lo"
	logrus "github.com/sirupsen/logrus"
	io "io"
	strings "strings"
	time "time"
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

var ElasticsearchIndexName string
var ElasticsearchClient *v8.Client
var ElasticsearchBulkIndexer esutil.BulkIndexer

func GetClient() (*v8.Client, error) {
	if ElasticsearchClient == nil {
		return nil, errorx.IllegalState.New("client has not been initialized")
	}
	return ElasticsearchClient, nil
}

func InitializeWithClients(indexName string, client *v8.Client, bulkIndexer esutil.BulkIndexer) error {
	if indexName == "" {
		return errorx.IllegalArgument.New("An index name must be provided")
	}
	if client == nil {
		return errorx.IllegalArgument.New("Client cannot be nil")
	}

	ElasticsearchIndexName = indexName
	ElasticsearchClient = client
	ElasticsearchBulkIndexer = bulkIndexer
	return nil
}

func InitializeWithConfigs(indexName string, clientConfig *v8.Config, bulkIndexerConfig *esutil.BulkIndexerConfig) error {
	if indexName == "" {
		return errorx.IllegalArgument.New("An index name must be provided")
	}
	if clientConfig == nil {
		return errorx.IllegalArgument.New("Client config cannot be nil")
	}
	if bulkIndexerConfig == nil {
		return errorx.IllegalArgument.New("Bulk indexer config cannot be nil")
	}
	if bulkIndexerConfig.Index != indexName {
		return errorx.IllegalArgument.New("bulk indexer config index must match given index name. given index name: %s, bulk indexer config index name: %s", ElasticsearchIndexName, bulkIndexerConfig.Index)
	}
	var err error
	ElasticsearchIndexName = indexName
	ElasticsearchClient, err = v8.NewClient(*clientConfig)
	if err != nil {
		errorutils.LogOnErr(nil, "error creating elasticsearch client", err)
		return err
	}
	if bulkIndexerConfig.Client == nil {
		bulkIndexerConfig.Client = ElasticsearchClient
	}
	bulkIndexer, err := esutil.NewBulkIndexer(*bulkIndexerConfig)
	if err != nil {
		errorutils.LogOnErr(nil, "error creating elasticsearch bulk indexer", err)
	}
	ElasticsearchBulkIndexer = bulkIndexer
	return nil
}

func Initialize(indexName, refresh string, addresses []string, numWorkers, flushBytes int, flushInterval, timeout time.Duration) error {
	clientConvig := &v8.Config{Addresses: addresses}
	bulkIndexerConfig := &esutil.BulkIndexerConfig{
		Index:         indexName,
		NumWorkers:    numWorkers,
		FlushBytes:    flushBytes,
		FlushInterval: flushInterval,
		Refresh:       refresh,
		Timeout:       timeout,
		OnError: func(ctx context.Context, err error) {
			errorutils.LogOnErr(nil, "error encountered in bulk indexer", err)
		},
	}
	return InitializeWithConfigs(indexName, clientConvig, bulkIndexerConfig)
}

func InitializeWithDefaults(addresses []string) error {
	return Initialize("data", "false", addresses, 2, 5000000, 5*time.Second, 1*time.Minute)
}

func newRequestBulkIndexerWithRefresh(refresh string) (esutil.BulkIndexer, error) {
	return esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     ElasticsearchClient,
		Index:      ElasticsearchIndexName,
		NumWorkers: 1,
		Refresh:    refresh,
		OnError: func(ctx context.Context, err error) {
			errorutils.LogOnErr(nil, "error encountered in bulk indexer", err)
		},
	})
}

func IndexSync(ctx context.Context, docs []Document, refresh string) error {
	for _, doc := range docs {
		data, err := json.Marshal(doc)
		if err != nil {
			errorutils.LogOnErr(nil, "error marshalling document to json", err)
			return err
		}
		req := esapi.IndexRequest{
			Index:      ElasticsearchIndexName,
			DocumentID: doc.Id,
			Body:       bytes.NewReader(data),
			Refresh:    refresh,
		}
		client, err := GetClient()
		if err != nil {
			return err
		}
		response, err := req.Do(ctx, client)
		if err != nil {
			return err
		}
		if response.StatusCode != 200 && response.StatusCode != 201 {
			bodyBytes, _ := io.ReadAll(response.Body)
			return errorx.IllegalState.New("unexpected status code indexing with refresh: %d with body: %s", response.StatusCode, string(bodyBytes))
		}
	}

	return nil
}

func BulkIndexSync(ctx context.Context, docs []Document, refresh string) error {
	reqBulkIndexer, err := newRequestBulkIndexerWithRefresh(refresh)
	if err != nil {
		return err
	}

	for _, doc := range docs {
		data, err := json.Marshal(doc)
		if err != nil {
			errorutils.LogOnErr(nil, "error marshalling document to json", err)
			return err
		}
		item := esutil.BulkIndexerItem{
			Action:     "index",
			Index:      ElasticsearchIndexName,
			DocumentID: doc.Id,
			Body:       bytes.NewReader(data),
		}
		err = reqBulkIndexer.Add(ctx, item)
		if err != nil {
			errorutils.LogOnErr(nil, "error adding item to request bulk indexer", err)
			return err
		}
	}

	err = reqBulkIndexer.Close(ctx)
	if err != nil {
		errorutils.LogOnErr(nil, "error closing request bulk indexer", err)
		return err
	}

	return nil
}

func BulkDeleteSync(ctx context.Context, ids []string, refresh string) error {
	reqBulkIndexer, err := newRequestBulkIndexerWithRefresh(refresh)
	if err != nil {
		return err
	}

	for _, id := range ids {
		item := esutil.BulkIndexerItem{
			Action:     "delete",
			Index:      ElasticsearchIndexName,
			DocumentID: id,
		}
		err = reqBulkIndexer.Add(ctx, item)
		if err != nil {
			errorutils.LogOnErr(nil, "error adding item to request bulk indexer", err)
			return err
		}
	}

	err = reqBulkIndexer.Close(ctx)
	if err != nil {
		errorutils.LogOnErr(nil, "error closing request bulk indexer", err)
		return err
	}

	return nil
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

type searchResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
		Hits []struct {
			Source Document      `json:"_source"`
			Sort   []interface{} `json:"sort"`
		} `json:"hits"`
	} `json:"hits"`
}

func executeSearch(ctx context.Context, query types.Query, size int64, searchAfter []interface{}) (*searchResponse, error) {
	req, err := buildSearchRequest(query, size, searchAfter)
	if err != nil {
		return nil, err
	}
	client, err := GetClient()
	if err != nil {
		return nil, err
	}
	response, err := req.Do(ctx, client)
	if err != nil {
		return nil, err
	}
	bodyBytes, _ := io.ReadAll(response.Body)
	if response.StatusCode != 200 {
		return nil, errorx.IllegalState.New("unexpected status code searching: %d with body: %s", response.StatusCode, string(bodyBytes))
	}
	var searchResponse *searchResponse
	if err := json.Unmarshal(bodyBytes, &searchResponse); err != nil {
		return nil, err
	}
	return searchResponse, nil
}

func buildSearchRequest(query types.Query, size int64, searchAfter []interface{}) (*esapi.SearchRequest, error) {
	body := map[string]interface{}{
		"query": query,
		"size":  size,
		"sort": []map[string]string{
			{"id": "asc"},
		},
		"_source": true,
	}
	if searchAfter != nil {
		body["search_after"] = searchAfter
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &esapi.SearchRequest{
		Index: []string{ElasticsearchIndexName},
		Body:  bytes.NewReader(bodyBytes),
	}, nil
}

func getKeywordQuery(theType, key, query string) types.Query {
	return types.Query{
		Bool: &types.BoolQuery{
			Must: []types.Query{
				{
					Term: map[string]types.TermQuery{"type": {Value: theType}},
				},
				{
					Nested: &types.NestedQuery{
						Path: "metadata",
						Query: &types.Query{
							Bool: &types.BoolQuery{
								Must: []types.Query{
									{
										Match: map[string]types.MatchQuery{"metadata.key": {Query: key}},
									},
									{
										Match: map[string]types.MatchQuery{"metadata.keywordValue": {Query: query}},
									},
								},
							},
						},
					},
				},
			},
		},
	}
}

func createIndex(client *v8.Client) error {
	req := esapi.IndicesCreateRequest{
		Index: ElasticsearchIndexName,
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
		Index: []string{ElasticsearchIndexName},
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
		Index: []string{ElasticsearchIndexName},
	}
	response, err := req.Do(context.Background(), client)
	if err != nil {
		errorutils.LogOnErr(logging.Log.WithFields(logrus.Fields{"index": ElasticsearchIndexName}), "error getting index", err)
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

func QueueDocsForDeletion(ctx context.Context, docs []Document, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	for _, doc := range docs {
		if err := QueueDocForIndexing(ctx, doc, onSuccess, onFailure); err != nil {
			return err
		}
	}
	return nil
}

func QueueBulkIndexItem(ctx context.Context, id, action string, body []byte, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	item := esutil.BulkIndexerItem{
		Action:     action,
		Index:      ElasticsearchIndexName,
		DocumentID: id,
	}
	if body != nil {
		item.Body = bytes.NewReader(body)
	}
	if onSuccess != nil {
		item.OnSuccess = onSuccess
	}
	if onFailure != nil {
		item.OnFailure = onFailure
	}
	err := ElasticsearchBulkIndexer.Add(ctx, item)
	errorutils.LogOnErr(nil, "error adding item to bulk indexer", err)
	return err
}

func QueueDocForIndexing(ctx context.Context, doc Document, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	data, err := json.Marshal(doc)
	if err != nil {
		errorutils.LogOnErr(nil, "error marshalling document to json", err)
		return err
	}
	return QueueBulkIndexItem(ctx, doc.Id, "index", data, onSuccess, onFailure)
}

func QueueDocForDeletion(ctx context.Context, doc Document, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	return QueueBulkIndexItem(ctx, doc.Id, "delete", nil, onSuccess, onFailure)
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

		AssociatedThingDocs, err := s.AssociatedThing.ToEsDocuments()
		if err != nil {
			return nil, err
		}
		for _, AssociatedThingDoc := range AssociatedThingDocs {
			for _, metadata := range AssociatedThingDoc.Metadata {
				metadata.Key = lo.ToPtr(fmt.Sprintf("AssociatedThing%s", *metadata.Key))
				doc.Metadata = append(doc.Metadata, metadata)
			}
		}

	}

	if s.RepeatedMessages != nil {

		for _, message := range s.RepeatedMessages {
			messageDocs, err := message.ToEsDocuments()
			if err != nil {
				return nil, err
			}
			for _, messageDoc := range messageDocs {
				for _, metadata := range messageDoc.Metadata {
					metadata.Key = lo.ToPtr(fmt.Sprintf("RepeatedMessages%s", *metadata.Key))
					doc.Metadata = append(doc.Metadata, metadata)
				}
			}
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

func (s *Thing) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *Thing) IndexSyncWithRefresh(ctx context.Context) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, "wait_for")
}

func (s *Thing) IndexSync(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, refresh)
}

func (s *Thing) Delete(ctx context.Context, refresh string) error {
	req := esapi.DeleteRequest{
		Index:      ElasticsearchIndexName,
		DocumentID: *s.Id,
		Refresh:    refresh,
	}
	client, err := GetClient()
	if err != nil {
		return err
	}
	response, err := req.Do(ctx, client)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 && response.StatusCode != 404 {
		bodyBytes, _ := io.ReadAll(response.Body)
		return errorx.IllegalState.New("unexpected status code deleting Thing: %d with body: %s", response.StatusCode, string(bodyBytes))
	}
	return nil
}

func (s *Thing) DeleteWithRefresh(ctx context.Context) error {
	return s.Delete(ctx, "wait_for")
}

type ThingBulkEsModel []*Thing

func (s *ThingBulkEsModel) ToEsDocuments() ([]Document, error) {
	if s == nil {
		return nil, nil
	}
	docs := []Document{}
	for _, item := range *s {
		itemDocs, err := item.ToEsDocuments()
		if err != nil {
			return nil, err
		}
		docs = append(docs, itemDocs...)
	}
	return docs, nil
}

func (s *ThingBulkEsModel) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *ThingBulkEsModel) IndexSyncWithRefresh(ctx context.Context) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return BulkIndexSync(ctx, docs, "wait_for")
}

func (s *ThingBulkEsModel) IndexSync(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return BulkIndexSync(ctx, docs, refresh)
}

func (s *ThingBulkEsModel) Delete(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	ids := []string{}
	for _, doc := range docs {
		ids = append(ids, doc.Id)
	}
	return BulkDeleteSync(ctx, ids, refresh)
}

func (s *ThingBulkEsModel) DeleteWithRefresh(ctx context.Context) error {
	return s.Delete(ctx, "wait_for")
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

func (s *Thing2) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *Thing2) IndexSyncWithRefresh(ctx context.Context) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, "wait_for")
}

func (s *Thing2) IndexSync(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, refresh)
}
func (s *Thing2) ReindexRelatedDocumentsAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	nestedDocs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	if len(nestedDocs) != 1 {
		return errorx.IllegalState.New("expected exactly one es document, got %d", len(nestedDocs))
	}
	nestedDoc := nestedDocs[0]

	size := int64(100)
	var handled int
	var searchAfter []interface{}

	query := getKeywordQuery(ThingEsType, "AssociatedThingId", *s.Id)
	for {
		res, err := executeSearch(ctx, query, size, searchAfter)
		if err != nil {
			return err
		}
		if len(res.Hits.Hits) == 0 {
			break
		}

		for _, hit := range res.Hits.Hits {
			doc := hit.Source
			for _, metadata := range nestedDoc.Metadata {
				metadata.Key = lo.ToPtr(fmt.Sprintf("AssociatedThing%s", *metadata.Key))
				for i := range doc.Metadata {
					if *doc.Metadata[i].Key == *metadata.Key {
						doc.Metadata[i] = metadata
					}
				}
			}
			err = QueueDocForIndexing(ctx, doc, onSuccess, onFailure)
			if err != nil {
				return err
			}
			handled++
		}

		if handled >= res.Hits.Total.Value {
			break
		}
		searchAfter = res.Hits.Hits[len(res.Hits.Hits)-1].Sort
	}

	return nil
}

func (s *Thing2) Delete(ctx context.Context, refresh string) error {
	req := esapi.DeleteRequest{
		Index:      ElasticsearchIndexName,
		DocumentID: *s.Id,
		Refresh:    refresh,
	}
	client, err := GetClient()
	if err != nil {
		return err
	}
	response, err := req.Do(ctx, client)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 && response.StatusCode != 404 {
		bodyBytes, _ := io.ReadAll(response.Body)
		return errorx.IllegalState.New("unexpected status code deleting Thing2: %d with body: %s", response.StatusCode, string(bodyBytes))
	}
	return nil
}

func (s *Thing2) DeleteWithRefresh(ctx context.Context) error {
	return s.Delete(ctx, "wait_for")
}

type Thing2BulkEsModel []*Thing2

func (s *Thing2BulkEsModel) ToEsDocuments() ([]Document, error) {
	if s == nil {
		return nil, nil
	}
	docs := []Document{}
	for _, item := range *s {
		itemDocs, err := item.ToEsDocuments()
		if err != nil {
			return nil, err
		}
		docs = append(docs, itemDocs...)
	}
	return docs, nil
}

func (s *Thing2BulkEsModel) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *Thing2BulkEsModel) IndexSyncWithRefresh(ctx context.Context) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return BulkIndexSync(ctx, docs, "wait_for")
}

func (s *Thing2BulkEsModel) IndexSync(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return BulkIndexSync(ctx, docs, refresh)
}

func (s *Thing2BulkEsModel) Delete(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	ids := []string{}
	for _, doc := range docs {
		ids = append(ids, doc.Id)
	}
	return BulkDeleteSync(ctx, ids, refresh)
}

func (s *Thing2BulkEsModel) DeleteWithRefresh(ctx context.Context) error {
	return s.Delete(ctx, "wait_for")
}
