package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/catalystsquad/protoc-gen-go-elasticsearch/plugin"
	"google.golang.org/protobuf/compiler/protogen"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/pluginpb"
)

func main() {
	var flags flag.FlagSet
	plugin.NestedFieldSeparator = flags.String("nestedFieldSeparator", "", "separator to use for nested field names")
	plugin.LowerCamelCaseFieldNames = flags.Bool("lowerCamelCaseFieldNames", false, "when true, nested field names are formatted using lowerCamelCase ")
	input, err := io.ReadAll(os.Stdin)
	if err != nil {
		panic(err)
	}

	var request pluginpb.CodeGeneratorRequest
	err = proto.Unmarshal(input, &request)
	if err != nil {
		panic(err)
	}

	opts := protogen.Options{
		ParamFunc: flags.Set,
	}

	builder, err := plugin.New(opts, &request)
	if err != nil {
		panic(err)
	}

	response, err := builder.Generate()
	if err != nil {
		panic(err)
	}

	out, err := proto.Marshal(response)
	if err != nil {
		panic(err)
	}
	fmt.Print(string(out))
}
