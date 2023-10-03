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

{{ range .messages }}
{{- if includeMessage . }}
const {{ .Desc.Name}}EsType = "{{ .Desc.Name }}"
{{- end -}}
{{ end }}

const ElasticsearchIndexName = "{{ indexName .file }}"
var addresses = env.GetEnvOrDefault("ELASTICSEARCH_ADDRESSES", "http://localhost:9200")
var flushInterval = env.GetEnvAsDurationOrDefault("ELASTICSEARCH_FLUSH_INTERVAL", "1s")
var ElasticsearchClient *v8.Client
var ElasticsearchBulkIndexer esutil.BulkIndexer

func init() {
	cfg := v8.Config{
		Addresses: strings.Split(addresses, ","),
	}
	var err error
	ElasticsearchClient, err = v8.NewClient(cfg)
	errorutils.LogOnErr(logging.Log.WithFields(logrus.Fields{"addresses": addresses}), "error creating elasticsearch client", err)
	if err != nil {
		logging.Log.Info("elasticsearch client initialized")
	}
	if err == nil {
		ElasticsearchBulkIndexer, err = esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
			Index:         ElasticsearchIndexName,
			Client:        ElasticsearchClient,
			FlushInterval: flushInterval,
		})
		errorutils.LogOnErr(logging.Log.WithFields(logrus.Fields{"addresses": addresses}), "error creating elasticsearch bulk indexer", err)
		if err == nil {
			logging.Log.Info("elasticsearch bulk indexer initialized")
		}
	}
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
		response, err := req.Do(ctx, ElasticsearchClient)
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
	{{ if and (includeField .) (not (isBytes .)) (not (isStructPb .)) }}
	{{ if or (isReference .) (.Desc.HasOptionalKeyword) }}
    if s.{{ .GoName}} != nil {
    {{ end }}
	{{ if isRelationship . }}
	{{ if .Desc.IsList }}
	for _, message := range s.{{ .GoName }} {
		doc := Document{
			Type: "joinRecord",
			Metadata: []Metadata{
				{
					Key: lo.ToPtr("fieldName"),
					KeywordValue: lo.ToPtr("{{ .GoName}}"),
				},
				{
					Key: lo.ToPtr("parentType"),
					KeywordValue: lo.ToPtr("{{ .Parent.GoIdent.GoName }}"),
				},
				{
					Key: lo.ToPtr("childType"),
					KeywordValue: lo.ToPtr("{{ .Message.GoIdent.GoName }}"),
				},
				{
					Key: lo.ToPtr("parentId"),
					KeywordValue: s.Id,
				},
				{
					Key: lo.ToPtr("childId"),
					KeywordValue: message.Id,
				},
			},
		}
		docs = append(docs, doc)
	}
	{{ else }}
	doc := Document{
		Type: "joinRecord",
		Metadata: []Metadata{
			{
				Key: lo.ToPtr("fieldName"),
				KeywordValue: lo.ToPtr("{{ .GoName}}"),
			},
			{
				Key: lo.ToPtr("parentType"),
				KeywordValue: lo.ToPtr("{{ .Parent.GoIdent.GoName }}"),
			},
			{
				Key: lo.ToPtr("childType"),
				KeywordValue: lo.ToPtr("{{ .Message.GoIdent.GoName }}"),
			},
			{
				Key: lo.ToPtr("parentId"),
				KeywordValue: s.Id,
			},
			{
				Key: lo.ToPtr("childId"),
				KeywordValue: s.{{ .GoName}}.Id,
			},
		},
	}
	docs = append(docs, doc)
	{{ end }}
	{{ else }}
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
    {{ end }}
    {{ if or (isReference .) (.Desc.HasOptionalKeyword) }}
	}
    {{ end }}
	{{ end }}
	{{ end }}
	docs = append(docs, doc)
	return docs, nil
}

func (s *{{ .Desc.Name }}) IndexAsync(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	err := s.Clear(ctx)
	if err != nil {
		return err
	}
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return QueueDocsForIndexing(ctx, docs, onSuccess, onFailure)
}

func (s *{{ .Desc.Name }}) IndexSyncWithRefresh(ctx context.Context) error {
	err := s.Clear(ctx)
	if err != nil {
		return err
	}
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, "wait_for")
}

func (s *{{ .Desc.Name }}) IndexSync(ctx context.Context, refresh string) error {
	err := s.Clear(ctx)
	if err != nil {
		return err
	}
	docs, err := s.ToEsDocuments()
	if err != nil {
		return err
	}
	return IndexSync(ctx, docs, refresh)
}

func (s *{{ .Desc.Name }}) Clear(ctx context.Context) error {
	req := esapi.DeleteByQueryRequest{
		Index: []string{ElasticsearchIndexName},
		Body:  bytes.NewReader([]byte(s.GetClearQuery())),
	}
	response, err := req.Do(ctx, ElasticsearchClient)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(response.Body)
		return errorx.IllegalState.New("unexpected status code clearing {{ .Desc.Name }}: %d with body: %s", response.StatusCode, string(bodyBytes))
	}
	return nil
}

func (s *{{ .Desc.Name }}) GetClearQuery() string {
	return fmt.Sprintf(` + "`" + `
{
	"query": {
    	"bool": {
			"must": [
				{
					"term": {
						"type": "joinRecord"
					}
				},
				{
					"nested": {
						"path": "metadata",
						"query": {
							"bool": {
								"must": [
									{ "match": { "metadata.key": "parentType" } },
									{ "match": { "metadata.keywordValue": "{{ .Desc.Name }}" } }
								]
							}
						}
					}
				},
				{
					"nested": {
						"path": "metadata",
						"query": {
							"bool": {
								"must": [
									{ "match": { "metadata.key": "parentId" } },
									{ "match": { "metadata.keywordValue": "%s" } }
								]
							}
						}
					}
				}
			]
		}
	}
}` + "`" + `, *s.Id)
}

func (s *{{ .Desc.Name }}) Delete(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	req := esapi.DeleteByQueryRequest{
		Index: []string{ElasticsearchIndexName},
		Body:  bytes.NewReader([]byte(s.GetDeleteQuery())),
	}
	response, err := req.Do(ctx, ElasticsearchClient)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		bodyBytes, _ := io.ReadAll(response.Body)
		return errorx.IllegalState.New("unexpected status code deleting {{ .Desc.Name }}: %d with body: %s", response.StatusCode, string(bodyBytes))
	}
	return nil
}

func (s *{{ .Desc.Name }}) GetDeleteQuery() string {
	return fmt.Sprintf(` + "`" + `
{
  "query": {
    "bool": {
      "should": [
        {
          "term": {
            "id": "%s"
          }
        },
        {
          "bool": {
            "must": [
              {
                "term": {
                  "type": "joinRecord"
                }
              },
              {
                "bool": {
                  "should": [
                    {
                      "nested": {
                        "path": "metadata",
                        "query": {
                          "bool": {
                            "must": [
                              {
                                "match": {
                                  "metadata.key": "parentId"
                                }
                              },
                              {
                                "match": {
                                  "metadata.keywordValue": "%s"
                                }
                              }
                            ]
                          }
                        }
                      }
                    },
                    {
                      "nested": {
                        "path": "metadata",
                        "query": {
                          "bool": {
                            "must": [
                              {
                                "match": {
                                  "metadata.key": "childId"
                                }
                              },
                              {
                                "match": {
                                  "metadata.keywordValue": "%s"
                                }
                              }
                            ]
                          }
                        }
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
      ]
    }
  }
}` + "`" + `, *s.Id, *s.Id, *s.Id)
}
{{ end }}
{{ end }}
`
