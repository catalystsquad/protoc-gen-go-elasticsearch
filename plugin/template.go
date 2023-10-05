package plugin

const elasticsearchTemplate = `
package {{ package }}

type Document struct {
	Id       string     ` + "`json:\"id,omitempty\"`" + `
	Type     string     ` + "`json:\"type,omitempty\"`" + `
	Metadata []Metadata ` + "`json:\"metadata,omitempty\"`" + `
}

type Metadata struct {
	Key          *string    ` + "`json:\"key,omitempty\"`" + `
	KeywordValue *string    ` + "`json:\"keywordValue,omitempty\"`" + `
	StringValue  *string    ` + "`json:\"stringValue,omitempty\"`" + `
	LongValue    *int64     ` + "`json:\"longValue,omitempty\"`" + `
	DoubleValue  *float64   ` + "`json:\"doubleValue,omitempty\"`" + `
	DateValue    *int64 ` + "`json:\"dateValue,omitempty\"`" + `
	BoolValue    *bool      ` + "`json:\"boolValue,omitempty\"`" + `
	NestedValue  []interface{}      ` + "`json:\"nestedValue,omitempty\"`" + `
}

{{ range .messages }}
{{- if includeMessage . }}
const {{ .Desc.Name}}EsType = "{{ .Desc.Name }}"
{{- end -}}
{{ end }}

var ElasticsearchIndexName string
var ElasticsearchClient *v8.Client
var ElasticsearchBulkIndexer *esutil.BulkIndexer

func GetClient() (*v8.Client, error) {
	if ElasticsearchClient == nil {
		return nil, errorx.IllegalState.New("client has not been initialized")
	}
	return ElasticsearchClient, nil
}

func GetBulkIndexer() (esutil.BulkIndexer, error) {
	if ElasticsearchBulkIndexer == nil {
		return nil, errorx.IllegalState.New("bulk indexer has not been initialized")
	}
	return *ElasticsearchBulkIndexer, nil
}

func InitializeWithClients(indexName string, client *v8.Client, bulkIndexer *esutil.BulkIndexer) error {
	if indexName == "" {
		return errorx.IllegalArgument.New("An index name must be provided")
	}
	if client == nil {
		return errorx.IllegalArgument.New("Client cannot be nil")
	}
	if bulkIndexer == nil {
		return errorx.IllegalArgument.New("Bulk indexer cannot be nil")
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
	bulkIndexer, err := esutil.NewBulkIndexer(*bulkIndexerConfig)
	if err != nil {
		errorutils.LogOnErr(nil, "error creating elasticsearch bulk indexer", err)
	}
	ElasticsearchBulkIndexer = &bulkIndexer
	return nil
}

func Initialize(indexName, refresh string, addresses []string, numWorkers, flushBytes int, flushInterval, timeout time.Duration) error {
	clientConvig := &v8.Config{Addresses: addresses}
	bulkIndexerConfig := &esutil.BulkIndexerConfig{
		Index: indexName,
		NumWorkers:    numWorkers,
		FlushBytes:    flushBytes,
		FlushInterval: flushInterval,
		Refresh:       refresh,
		Timeout:       timeout,
		OnError: func(ctx context.Context, err error) {
			errorutils.LogOnErr(nil, "error indexing item", err)
		},
		OnFlushStart: func(ctx context.Context) context.Context {
			logging.Log.Info("bulk indexer starting flush")
			return ctx
		},
		OnFlushEnd: func(ctx context.Context) {
			logging.Log.Info("bulk indexer flush complete")
		},
	}
	return InitializeWithConfigs(indexName, clientConvig, bulkIndexerConfig)
}

func InitializeWithDefaults(addresses []string) error {
	return Initialize("data", "false", addresses, 2, 5000000, 5*time.Second, 1*time.Minute)
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
	settings := strings.NewReader("{\"properties\":{\"id\":{\"type\":\"keyword\"},\"type\":{\"type\":\"keyword\"},\"metadata\":{\"type\":\"nested\",\"properties\":{\"key\":{\"type\":\"keyword\"},\"keywordValue\":{\"type\":\"keyword\"},\"stringValue\":{\"type\":\"text\"},\"longValue\":{\"type\":\"long\"},\"doubleValue\":{\"type\":\"double\"},\"dateValue\":{\"type\":\"date\"},\"boolValue\":{\"type\":\"boolean\"},\"nestedValue\":{\"type\":\"nested\"}}}}}")
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
	bulkIndexer, err := GetBulkIndexer()
	if err != nil {
		return err
	}
	err = bulkIndexer.Add(ctx, item)
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

{{ range .messages }}
{{ if includeMessage . }}
func (s *{{ .Desc.Name }}) ToEsDocuments() ([]Document, error) {
	docs := []Document{}
	doc := Document{
		Id:       *s.Id,
		Type:     {{ .Desc.Name}}EsType,
		Metadata: []Metadata{},
	}
	{{ range .Fields }}
		{{ if and (includeField .) (not (isBytes .)) (not (isStructPb .)) (not (isRelationship .)) }}
			{{ if or (isReference .) (.Desc.HasOptionalKeyword) }}
			if s.{{ .GoName}} != nil {
			{{ end }}
			{{ if .Desc.IsList }}
			for _, val := range s.{{ .GoName }} {
				metaData := Metadata{
				Key: lo.ToPtr("{{ .GoName }}"),
				{{ if eq (isTimestamp .) false }}
				StringValue: lo.ToPtr(fmt.Sprintf("%v", {{ fieldValueString . }})),
				KeywordValue: lo.ToPtr(fmt.Sprintf("%v", {{ fieldValueString . }})),
				{{ end }}
				}
				{{ if isNumeric . }}
				metaData.LongValue = lo.ToPtr(int64({{ fieldValueString . }}))
				metaData.DoubleValue = lo.ToPtr(float64({{ fieldValueString . }}))
				{{ else if isBoolean . }}
				metaData.BoolValue = lo.ToPtr({{ fieldValueString . }})
				{{ else if isTimestamp . }}
				metaData.DateValue = lo.ToPtr(s.{{ .GoName }}.AsTime().UTC().UnixMilli())
				{{ else if .Enum }}
				metaData.LongValue = lo.ToPtr(int64(s.{{ .GoName }}.Number()))
				{{ end }}
				doc.Metadata = append(doc.Metadata, metaData)
			}
			{{ else }}
			{{ .GoName}}MetaData := Metadata{
				Key: lo.ToPtr("{{ .GoName }}"),
				{{ if eq (isTimestamp .) false }}
				StringValue: lo.ToPtr(fmt.Sprintf("%v", {{ fieldValueString . }})),
				KeywordValue: lo.ToPtr(fmt.Sprintf("%v", {{ fieldValueString . }})),
				{{ end }}
			}
			{{ if isNumeric . }}
			{{ .GoName}}MetaData.LongValue = lo.ToPtr(int64({{ fieldValueString . }}))
			{{ .GoName}}MetaData.DoubleValue = lo.ToPtr(float64({{ fieldValueString . }}))
			{{ else if isBoolean . }}
			{{ .GoName}}MetaData.BoolValue = lo.ToPtr({{ fieldValueString . }})
			{{ else if isTimestamp . }}
			{{ .GoName}}MetaData.DateValue = lo.ToPtr(s.{{ .GoName }}.AsTime().UTC().UnixMilli())
			{{ else if .Enum }}
			{{ .GoName}}MetaData.LongValue = lo.ToPtr(int64(s.{{ .GoName }}.Number()))
			{{ end }}
			doc.Metadata = append(doc.Metadata, {{ .GoName}}MetaData)
			{{ end }}
			{{ if or (isReference .) (.Desc.HasOptionalKeyword) }}
			}
			{{ end }}
		{{ end }}
		{{ if and (includeField .) (not (isBytes .)) (not (isStructPb .)) (isRelationship .) (isNested .) }}
		if s.{{ .GoName}} != nil {
		{{ if .Desc.IsList }}
			for _, message := range s.{{ .GoName }} {
				nestedMetadata := Metadata{
					Key: lo.ToPtr("{{ .GoName }}"),
					NestedValue: []interface{}{message},
				}
		
				doc.Metadata = append(doc.Metadata, nestedMetadata)
			}
			{{ else }}
			nestedMetadata := Metadata{
				Key: lo.ToPtr("{{ .GoName }}"),
				NestedValue: []interface{}{s.{{ .GoName }}},
			}
		
			doc.Metadata = append(doc.Metadata, nestedMetadata)
			{{ end }}
		}
		{{ end }}
	{{ end }}
	docs = append(docs, doc)
	return docs, nil
}

func (s *{{ .Desc.Name }}) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *{{ .Desc.Name }}) IndexSyncWithRefresh(ctx context.Context) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, "wait_for")
}

func (s *{{ .Desc.Name }}) IndexSync(ctx context.Context, refresh string) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, refresh)
}

func (s *{{ .Desc.Name }}) Delete(ctx context.Context, refresh string) error {
	req := esapi.DeleteRequest{
		Index: ElasticsearchIndexName,
		DocumentID: *s.Id,
        Refresh: refresh,
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
		return errorx.IllegalState.New("unexpected status code deleting {{ .Desc.Name }}: %d with body: %s", response.StatusCode, string(bodyBytes))
	}
	return nil
}

func (s *{{ .Desc.Name }}) DeleteWithRefresh(ctx context.Context) error {
	return s.Delete(ctx, "wait_for")
}
{{ end }}
{{ end }}
`
