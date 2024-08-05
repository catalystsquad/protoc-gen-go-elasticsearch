package plugin

import (
	"bytes"
	"fmt"
	elasticsearch "github.com/catalystcommunity/protoc-gen-go-elasticsearch/options"
	"github.com/iancoleman/strcase"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"reflect"
	"strings"
	"text/template"

	"golang.org/x/exp/slices"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/types/pluginpb"
)

type Builder struct {
	plugin       *protogen.Plugin
	messages     map[string]struct{}
	stringEnums  bool
	suppressWarn bool
}

var f *protogen.GeneratedFile
var defaultIndexName = "data"

// I can't find where the constant is for this in protogen, so I'm putting it here
const SUPPORTS_OPTIONAL_FIELDS = 1

var templateFuncs = map[string]any{
	"hasDisableReindexRelatedOption":                              hasDisableReindexRelatedOption,
	"hasParentMessages":                                           hasParentMessages,
	"getParentMessageNames":                                       getParentMessageNames,
	"getChildMessageNestedOnFieldNames":                           getChildMessageNestedOnFieldNames,
	"hasParentMessagesWithCascadeDeleteFromChild":                 hasParentMessagesWithCascadeDeleteFromChild,
	"getParentMessageNamesWithCascadeDeleteFromChild":             getParentMessageNamesWithCascadeDeleteFromChild,
	"getChildMessageWithCascadeDeleteFromChildNestedOnFieldNames": getChildMessageWithCascadeDeleteFromChildNestedOnFieldNames,
	"includeMessage":                                              includeMessage,
	"includeField":                                                includeField,
	"fieldComments":                                               fieldComments,
	"toString":                                                    toString,
	"toLower":                                                     strings.ToLower,
	"isNumeric":                                                   isNumeric,
	"isBoolean":                                                   isBoolean,
	"isTimestamp":                                                 isTimestamp,
	"isStructPb":                                                  isStructPb,
	"isBytes":                                                     isBytes,
	"isRelationship":                                              isRelationship,
	"isNested":                                                    isNested,
	"maybeDereference":                                            maybeDereference,
	"isReference":                                                 isReference,
	"indexName":                                                   getIndexName,
	"fieldValueString":                                            fieldValueString,
	"add":                                                         add,
	"nestedFieldSeparator":                                        getNestedFieldSeparator,
	"getKey":                                                      getKey,
	"maybeLowerCamelFieldName":                                    maybeLowerCamelFieldName,
}

func New(opts protogen.Options, request *pluginpb.CodeGeneratorRequest) (*Builder, error) {
	plugin, err := opts.New(request)
	if err != nil {
		return nil, err
	}
	plugin.SupportedFeatures = SUPPORTS_OPTIONAL_FIELDS
	builder := &Builder{
		plugin:   plugin,
		messages: make(map[string]struct{}),
	}

	params := parseParameter(request.GetParameter())

	if strings.EqualFold(params["enums"], "string") {
		builder.stringEnums = true
	}

	if _, ok := params["quiet"]; ok {
		builder.suppressWarn = true
	}

	return builder, nil
}

func parseParameter(param string) map[string]string {
	paramMap := make(map[string]string)

	params := strings.Split(param, ",")
	for _, param := range params {
		if strings.Contains(param, "=") {
			kv := strings.Split(param, "=")
			paramMap[kv[0]] = kv[1]
			continue
		}
		paramMap[param] = ""
	}

	return paramMap
}

func (b *Builder) Generate() (response *pluginpb.CodeGeneratorResponse, err error) {
	for _, protoFile := range b.plugin.Files {
		if shouldGenerateFile(protoFile) {
			var tpl *template.Template
			templateFuncs["package"] = func() string { return string(protoFile.GoPackageName) }
			if tpl, err = template.New("elasticsearch").Funcs(templateFuncs).Parse(elasticsearchTemplate); err != nil {
				return
			}
			fileName := protoFile.GeneratedFilenamePrefix + ".pb.elasticsearch.go"
			f = b.plugin.NewGeneratedFile(fileName, ".")
			initMessageMetadata(protoFile)
			writeGlobalImports(f)
			var data bytes.Buffer
			templateMap := map[string]any{
				"messages": protoFile.Messages,
				"file":     protoFile,
			}
			if err = tpl.Execute(&data, templateMap); err != nil {
				return
			}
			if _, err = f.Write(data.Bytes()); err != nil {
				return
			}
		}
	}
	response = b.plugin.Response()
	return
}

func writeGlobalImports(f *protogen.GeneratedFile) {
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "context"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "fmt"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "strings"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "bytes"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "io"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "time"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/samber/lo"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/elastic/go-elasticsearch/v8"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/elastic/go-elasticsearch/v8/esapi"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/joomcode/errorx"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/catalystcommunity/app-utils-go/errorutils"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/sirupsen/logrus"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/catalystcommunity/app-utils-go/logging"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/elastic/go-elasticsearch/v8/esutil"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "github.com/elastic/go-elasticsearch/v8/typedapi/types"})
	f.QualifiedGoIdent(protogen.GoIdent{GoImportPath: "encoding/json"})
}

type messageMeta struct {
	// parentMessageFields is a map of parent message names to a list of field
	// names
	parentMessageFields map[string][]string

	// parentMessageFieldsWithCascadeDeleteFromChild is a map of parent message
	// names to a list of field names that should trigger a cascade delete of
	// the parent message when the child message is deleted
	parentMessageFieldsWithCascadeDeleteFromChild map[string][]string
}

// messageMetadata is a map of message name to additional metadata
var messageMetadata map[string]*messageMeta

// initMessageMetadata creates a map of message name to additional metadata,
// which currently just contains a mapping from child message to parent message
// in order to determine which messages need to be reindexed when a child
// message is updated
func initMessageMetadata(file *protogen.File) {
	meta := map[string]*messageMeta{}

	// iterate over all messages to determine which messages are nested on other messages
	for _, message := range file.Messages {
		messageName := message.GoIdent.GoName
		if _, ok := meta[messageName]; !ok {
			meta[messageName] = &messageMeta{
				parentMessageFields:                           map[string][]string{},
				parentMessageFieldsWithCascadeDeleteFromChild: map[string][]string{},
			}
		}

		// check if message has nested fields
		for _, field := range message.Fields {
			// exclude repeated fields
			if isNested(field) && !field.Desc.IsList() {
				// if nested, add metadata to child message
				childFieldTypeName := field.Message.GoIdent.GoName
				if _, ok := meta[childFieldTypeName]; !ok {
					meta[childFieldTypeName] = &messageMeta{
						parentMessageFields:                           map[string][]string{},
						parentMessageFieldsWithCascadeDeleteFromChild: map[string][]string{},
					}
				}
				parentFieldName := field.GoName
				// add parent message name to child message metadata
				meta[childFieldTypeName].parentMessageFields[messageName] = append(meta[childFieldTypeName].parentMessageFields[messageName], parentFieldName)
				// add parent message name to child message metadata as
				// "delete from child" field if cascade delete is enabled
				fieldOptions := getFieldOptions(field)
				if fieldOptions != nil && fieldOptions.EnableCascadeDeleteFromChild {
					meta[childFieldTypeName].parentMessageFieldsWithCascadeDeleteFromChild[messageName] = append(meta[childFieldTypeName].parentMessageFieldsWithCascadeDeleteFromChild[messageName], parentFieldName)
				}
			}
		}
	}

	// ensure all of the nested slices are sorted to ensure consistent ordering when regenerating
	for _, meta := range meta {
		for _, parentMessageFields := range meta.parentMessageFields {
			slices.Sort(parentMessageFields)
		}
		for _, parentMessageFields := range meta.parentMessageFieldsWithCascadeDeleteFromChild {
			slices.Sort(parentMessageFields)
		}
	}

	messageMetadata = meta
}

func hasDisableReindexRelatedOption(message *protogen.Message) bool {
	return getMessageOptions(message).DisableReindexRelated
}

func hasParentMessages(message *protogen.Message) bool {
	return len(getParentMessageNames(message)) > 0
}

func getParentMessageNames(message *protogen.Message) []string {
	if message == nil {
		return []string{}
	}
	if meta, ok := messageMetadata[message.GoIdent.GoName]; ok {
		parentMessageTypes := []string{}
		for parentMessageType := range meta.parentMessageFields {
			parentMessageTypes = append(parentMessageTypes, parentMessageType)
		}
		slices.Sort(parentMessageTypes) // sort to ensure consistent ordering
		return parentMessageTypes
	}
	return []string{}
}

func getChildMessageNestedOnFieldNames(message *protogen.Message, parentName string) []string {
	if message == nil {
		return []string{}
	}
	if meta, ok := messageMetadata[message.GoIdent.GoName]; ok {
		if parentMessageFields, ok := meta.parentMessageFields[parentName]; ok {
			return parentMessageFields
		}
	}
	return []string{}
}

func hasParentMessagesWithCascadeDeleteFromChild(message *protogen.Message) bool {
	return len(getParentMessageNamesWithCascadeDeleteFromChild(message)) > 0
}

func getParentMessageNamesWithCascadeDeleteFromChild(message *protogen.Message) []string {
	if message == nil {
		return []string{}
	}
	if meta, ok := messageMetadata[message.GoIdent.GoName]; ok {
		cascadeDeleteFromChildFields := []string{}
		for parentMessageType := range meta.parentMessageFieldsWithCascadeDeleteFromChild {
			cascadeDeleteFromChildFields = append(cascadeDeleteFromChildFields, parentMessageType)
		}
		slices.Sort(cascadeDeleteFromChildFields) // sort to ensure consistent ordering
		return cascadeDeleteFromChildFields
	}
	return []string{}
}

func getChildMessageWithCascadeDeleteFromChildNestedOnFieldNames(message *protogen.Message, parentName string) []string {
	if message == nil {
		return []string{}
	}
	if meta, ok := messageMetadata[message.GoIdent.GoName]; ok {
		if parentMessageFields, ok := meta.parentMessageFieldsWithCascadeDeleteFromChild[parentName]; ok {
			return parentMessageFields
		}
	}
	return []string{}
}

func getFieldOptions(field *protogen.Field) *elasticsearch.ElasticsearchFieldOptions {
	options := field.Desc.Options().(*descriptorpb.FieldOptions)
	if options == nil {
		return &elasticsearch.ElasticsearchFieldOptions{}
	}

	v := proto.GetExtension(options, elasticsearch.E_ElasticsearchField)
	if v == nil {
		return nil
	}

	opts, ok := v.(*elasticsearch.ElasticsearchFieldOptions)
	if !ok {
		return nil
	}
	return opts
}

func getMessageOptions(message *protogen.Message) *elasticsearch.ElasticsearchMessageOptions {
	options := message.Desc.Options().(*descriptorpb.MessageOptions)
	if options == nil {
		return &elasticsearch.ElasticsearchMessageOptions{}
	}

	v := proto.GetExtension(options, elasticsearch.E_ElasticsearchOpts)
	if v == nil {
		return &elasticsearch.ElasticsearchMessageOptions{}
	}

	opts, ok := v.(*elasticsearch.ElasticsearchMessageOptions)
	if !ok || opts == nil {
		return &elasticsearch.ElasticsearchMessageOptions{}
	}
	return opts
}

func shouldGenerateFile(file *protogen.File) bool {
	options := getFileOptions(file)
	return options != nil && options.Generate
}

func getFileOptions(file *protogen.File) *elasticsearch.ElasticsearchFileOptions {
	options := file.Desc.Options().(*descriptorpb.FileOptions)
	if options == nil {
		return &elasticsearch.ElasticsearchFileOptions{}
	}
	v := proto.GetExtension(options, elasticsearch.E_ElasticsearchFileOpts)
	if reflect.ValueOf(v).IsNil() {
		return nil
	}
	opts, ok := v.(*elasticsearch.ElasticsearchFileOptions)
	if !ok {
		return nil
	}
	return opts
}

func includeMessage(message *protogen.Message) bool {
	options := getMessageOptions(message)
	return options.Generate
}

func includeField(f *protogen.Field) bool {
	options := getFieldOptions(f)
	if options != nil {
		return !options.Ignore
	}
	return true // default to include
}

func fieldComments(field *protogen.Field) string {
	return field.Comments.Leading.String() + field.Comments.Trailing.String()
}

func toString(item interface{}) string {
	return fmt.Sprintf("%v", item)
}

func isNumeric(field *protogen.Field) bool {
	kind := field.Desc.Kind()
	return kind == protoreflect.Int32Kind ||
		kind == protoreflect.Int64Kind ||
		kind == protoreflect.FloatKind ||
		kind == protoreflect.DoubleKind
}

func isBoolean(field *protogen.Field) bool {
	return field.Desc.Kind() == protoreflect.BoolKind
}

func isTimestamp(field *protogen.Field) bool {
	return field.Desc.Message() != nil && field.Desc.Message().FullName() == "google.protobuf.Timestamp"
}

func isStructPb(field *protogen.Field) bool {
	return field.Desc.Message() != nil && field.Desc.Message().FullName() == "google.protobuf.Struct"
}

func isBytes(field *protogen.Field) bool {
	return field.Desc.Kind() == protoreflect.BytesKind
}

func isRelationship(field *protogen.Field) bool {
	return field.Message != nil && !isTimestamp(field) && !isStructPb(field)
}

func isNested(field *protogen.Field) bool {
	opts := getFieldOptions(field)
	if opts == nil {
		return false
	}
	return opts.Nested
}

func maybeDereference(field *protogen.Field) string {
	if field.Desc.HasOptionalKeyword() {
		return "*"
	}
	return ""
}

func fieldValueString(field *protogen.Field) string {
	name := fmt.Sprintf("s.%s", field.GoName)
	if field.Desc.IsList() {
		// template uses val for repeated fields
		name = "val"
	}
	if field.Desc.HasOptionalKeyword() {
		return fmt.Sprintf("lo.FromPtr(%s)", name)
	}
	return name
}

func getNestedFieldSeparator() string {
	return *NestedFieldSeparator
}

func maybeLowerCamelFieldName(name string) string {
	if *LowerCamelCaseFieldNames {
		name = strcase.ToLowerCamel(name)
	}

	return name
}

func getKey(field *protogen.Field) string {
	return maybeLowerCamelFieldName(field.GoName)
}

func isReference(field *protogen.Field) bool {
	return field.Message != nil && getMessageOptions(field.Message) != nil && getMessageOptions(field.Message).Generate
}

func getIndexName(file *protogen.File) string {
	opts := getFileOptions(file)
	if opts.IndexName != "" {
		return opts.IndexName
	}
	return defaultIndexName
}

func add(a, b int) int {
	return a + b
}
