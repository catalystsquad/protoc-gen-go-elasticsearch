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
const {{ .Desc.Name}}EsType = "{{ .Desc.Name | toString | toLower }}"
{{- end -}}
{{ end }}

const indexName = "{{ indexName .file }}"
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

{{ range .messages }}
{{ if includeMessage . }}
func (s *{{ .Desc.Name }}) ToEsDocument() (Document, error) {
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
	{{ .GoName}}MetaData := Metadata{
		Key: lo.ToPtr("{{ .Desc.JSONName }}"),
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
    {{ if or (isReference .) (.Desc.HasOptionalKeyword) }}
	}
    {{ end }}
	{{ end }}
	{{ end }}
	return doc, nil
}

func (s *{{ .Desc.Name }}) Index(ctx context.Context, onSuccess func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem), onFailure func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem, err error)) error {
	doc, err := s.ToEsDocument()
	if err != nil {
		return err
	}
	return QueueDocForIndexing(ctx, doc, onSuccess, onFailure)
}
{{ end }}
{{ end }}
`
