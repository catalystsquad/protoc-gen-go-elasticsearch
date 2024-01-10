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
}

{{- range .messages }}
{{- if includeMessage . }}
const {{ .Desc.Name}}EsType = "{{ .Desc.Name }}"
{{- end }}
{{- end }}

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
		Index: indexName,
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
		Client:        ElasticsearchClient,
		Index:         ElasticsearchIndexName,
		NumWorkers:    1,
		Refresh:       refresh,
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

func BulkIndexSync(ctx context.Context, docs []Document, refresh string, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
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
			OnSuccess:  onSuccess,
			OnFailure:  onFailure,
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

func BulkDeleteSync(ctx context.Context, ids []string, refresh string, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	reqBulkIndexer, err := newRequestBulkIndexerWithRefresh(refresh)
	if err != nil {
		return err
	}

	for _, id := range ids {
		item := esutil.BulkIndexerItem{
			Action:     "delete",
			Index:      ElasticsearchIndexName,
			DocumentID: id,
			OnSuccess:  onSuccess,
			OnFailure:  onFailure,
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
			Value int ` + "`json:\"value\"`" + `
		} ` + "`json:\"total\"`" + `
		Hits []struct {
			Source Document      ` + "`json:\"_source\"`" + `
			Sort   []interface{} ` + "`json:\"sort\"`" + `
		} ` + "`json:\"hits\"`" + `
	} ` + "`json:\"hits\"`" + `
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

{{- range .messages }}
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
				messageDocs, err := message.ToEsDocuments()
				if err != nil {
					return nil, err
				}
				for _, messageDoc := range messageDocs {
					for _, metadata := range messageDoc.Metadata {
						metadata.Key = lo.ToPtr(fmt.Sprintf("{{ .GoName }}%s", *metadata.Key))
						doc.Metadata = append(doc.Metadata, metadata)
					}
				}
			}
			{{ else }}
			{{ .GoName }}Docs, err := s.{{ .GoName }}.ToEsDocuments()
				if err != nil {
					return nil, err
				}
				for _, {{ .GoName }}Doc := range {{ .GoName }}Docs {
					for _, metadata := range {{ .GoName }}Doc.Metadata {
						metadata.Key = lo.ToPtr(fmt.Sprintf("{{ .GoName }}%s", *metadata.Key))
						doc.Metadata = append(doc.Metadata, metadata)
					}
				}
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

{{ if and (hasParentMessages .) (not (hasDisableReindexRelatedOption .)) }}
func (s *{{ .Desc.Name }}) ReindexRelatedDocumentsBulk(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	nestedDocs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	if len(nestedDocs) != 1 {
		return errorx.IllegalState.New("expected exactly one es document, got %d", len(nestedDocs))
	}
	nestedDoc := nestedDocs[0]

	reqBulkIndexer, err := newRequestBulkIndexerWithRefresh("wait_for")
	if err != nil {
		return err
	}

	size := int64(100)
	var handled int
	var searchAfter []interface{}

	{{/* save the message to a var so that it is accessible inside of the range scope */}}
	{{- $message := . }}
	{{- range $index, $parentMessageName := getParentMessageNames . }}
	{{- $childMessageNestedOnFields := getChildMessageNestedOnFieldNames $message $parentMessageName }}
	{{- range $index2, $nestedOnField := $childMessageNestedOnFields }}
	{{- if eq (add $index $index2) 0 }}
	query := getKeywordQuery({{ $parentMessageName }}EsType, "{{ . }}Id", *s.Id)
	{{- else }}
	handled = 0
	searchAfter = nil

	query = getKeywordQuery({{ $parentMessageName }}EsType, "{{ . }}Id", *s.Id)
	{{- end }}
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
			metadataIndexByKey := map[string]int{}
			for i := range doc.Metadata {
				metadataIndexByKey[*doc.Metadata[i].Key] = i
			}
			var hasChanged bool
			for _, metadata := range nestedDoc.Metadata {
				metadata.Key = lo.ToPtr(fmt.Sprintf("{{ $nestedOnField }}%s", *metadata.Key))
				if i, ok := metadataIndexByKey[*metadata.Key]; ok {
					if doc.Metadata[i].StringValue != nil && metadata.StringValue != nil &&
						*doc.Metadata[i].StringValue != *metadata.StringValue {
						doc.Metadata[i] = metadata
						hasChanged = true
					}
				}
			}
			if !hasChanged {
				continue
			}
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
				OnSuccess:  onSuccess,
				OnFailure:  onFailure,
			}
			err = reqBulkIndexer.Add(ctx, item)
			if err != nil {
				errorutils.LogOnErr(nil, "error adding item to request bulk indexer", err)
				return err
			}
			handled++
		}

		if handled >= res.Hits.Total.Value {
			break
		}
		searchAfter = res.Hits.Hits[len(res.Hits.Hits)-1].Sort
	}
	{{- end }}
	{{- end }}

	return reqBulkIndexer.Close(ctx)
}

func (s *{{ .Desc.Name }}) ReindexRelatedDocumentsAfterDeleteBulk(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	reqBulkIndexer, err := newRequestBulkIndexerWithRefresh("wait_for")
	if err != nil {
		return err
	}

	size := int64(100)
	var handled int
	var searchAfter []interface{}

	{{/* save the message to a var so that it is accessible inside of the range scope */}}
	{{- $message := . }}
	{{- range $index, $parentMessageName := getParentMessageNames . }}
	{{- $childMessageNestedOnFields := getChildMessageNestedOnFieldNames $message $parentMessageName }}
	{{- range $index2, $nestedOnField := $childMessageNestedOnFields }}
	{{- if eq (add $index $index2) 0 }}
	query := getKeywordQuery({{ $parentMessageName }}EsType, "{{ . }}Id", *s.Id)
	{{- else }}
	handled = 0
	searchAfter = nil

	query = getKeywordQuery({{ $parentMessageName }}EsType, "{{ . }}Id", *s.Id)
	{{- end }}
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
			newMetadata := []Metadata{}
			for i := range doc.Metadata {
				if !strings.HasPrefix(*doc.Metadata[i].Key, "{{ $nestedOnField }}") {
					newMetadata = append(newMetadata, doc.Metadata[i])
				}
			}
			doc.Metadata = newMetadata
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
				OnSuccess:  onSuccess,
				OnFailure:  onFailure,
			}
			err = reqBulkIndexer.Add(ctx, item)
			if err != nil {
				errorutils.LogOnErr(nil, "error adding item to request bulk indexer", err)
				return err
			}
			handled++
		}

		if handled >= res.Hits.Total.Value {
			break
		}
		searchAfter = res.Hits.Hits[len(res.Hits.Hits)-1].Sort
	}
	{{- end }}
	{{- end }}

	return reqBulkIndexer.Close(ctx)
}

{{ if hasParentMessagesWithCascadeDeleteFromChild . }}
func (s *{{ .Desc.Name }}) DeleteRelatedDocumentsAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	reqBulkIndexer, err := newRequestBulkIndexerWithRefresh("wait_for")
	if err != nil {
		return err
	}

	size := int64(100)
	var handled int
	var searchAfter []interface{}

	{{/* save the message to a var so that it is accessible inside of the range scope */}}
	{{- $message := . }}
	{{- range $index, $parentMessageName := getParentMessageNamesWithCascadeDeleteFromChild . }}
	{{- $childMessageNestedOnFields := getChildMessageWithCascadeDeleteFromChildNestedOnFieldNames $message $parentMessageName }}
	{{- range $index2, $nestedOnField := $childMessageNestedOnFields }}
	{{- if eq (add $index $index2) 0 }}
	query := getKeywordQuery({{ $parentMessageName }}EsType, "{{ . }}Id", *s.Id)
	{{- else }}
	handled = 0
	searchAfter = nil

	query = getKeywordQuery({{ $parentMessageName }}EsType, "{{ . }}Id", *s.Id)
	{{- end }}
	for {
		res, err := executeSearch(ctx, query, size, searchAfter)
		if err != nil {
			return err
		}
		if len(res.Hits.Hits) == 0 {
			break
		}

		for _, hit := range res.Hits.Hits {
			item := esutil.BulkIndexerItem{
				Action:     "delete",
				Index:      ElasticsearchIndexName,
				DocumentID: hit.Source.Id,
				OnSuccess:  onSuccess,
				OnFailure:  onFailure,
			}
			err = reqBulkIndexer.Add(ctx, item)
			if err != nil {
				errorutils.LogOnErr(nil, "error adding item to request bulk indexer", err)
				return err
			}
			handled++
		}

		if handled >= res.Hits.Total.Value {
			break
		}
		searchAfter = res.Hits.Hits[len(res.Hits.Hits)-1].Sort
	}
	{{- end }}
	{{- end }}

	return reqBulkIndexer.Close(ctx)
}
{{- end }}
{{- end }}

type {{ .Desc.Name }}BulkEsModel []*{{ .Desc.Name }}

func (s *{{ .Desc.Name }}BulkEsModel) ToEsDocuments() ([]Document, error) {
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

func (s *{{ .Desc.Name }}BulkEsModel) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *{{ .Desc.Name }}BulkEsModel) IndexSyncWithRefresh(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return BulkIndexSync(ctx, docs, "wait_for", onSuccess, onFailure)
}

func (s *{{ .Desc.Name }}BulkEsModel) IndexSync(ctx context.Context, refresh string, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return BulkIndexSync(ctx, docs, refresh, onSuccess, onFailure)
}

func (s *{{ .Desc.Name }}BulkEsModel) Delete(ctx context.Context, refresh string, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	ids := []string{}
	for _, doc := range docs {
		ids = append(ids, doc.Id)
	}
	return BulkDeleteSync(ctx, ids, refresh, onSuccess, onFailure)
}

func (s *{{ .Desc.Name }}BulkEsModel) DeleteWithRefresh(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	return s.Delete(ctx, "wait_for", onSuccess, onFailure)
}
{{- end }}
{{- end }}
`
