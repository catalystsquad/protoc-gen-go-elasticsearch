package test

import (
	"context"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	example_example "github.com/catalystsquad/protoc-gen-go-elasticsearch/example"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/elastic"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/timestamppb"
	"io"
	"strings"
	"testing"
	"time"
)

type PluginSuite struct {
	suite.Suite
	container *gnomock.Container
	client    *elasticsearch.Client
}

func TestPluginSuite(t *testing.T) {
	suite.Run(t, new(PluginSuite))
}

func (s *PluginSuite) SetupSuite() {
	s.T().Parallel()
	s.startElasticsearch(s.T())
	err := example_example.EnsureIndex(s.client)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) TestSearchById() {
	thing := s.generateRandomThing()
	s.indexThing(thing)
	s.eventualKeywordSearch("thing", "id", *thing.Id, *thing.Id)
}

func (s *PluginSuite) TestSearchStringValue() {
	thing := s.generateRandomThing()
	thing.AString = "i like turtles and elephants because they're neat"
	s.indexThing(thing)
	s.eventualStringSearch("thing", "aString", thing.AString, *thing.Id)
	// test partial
	s.eventualStringSearch("thing", "aString", "turtles", *thing.Id)
}

func (s *PluginSuite) TestSearchLongValue() {
	thing := s.indexRandomThing()
	s.eventualLongSearch("thing", "anInt32", *thing.Id, int(thing.AnInt32))
	s.eventualStringSearch("thing", "anInt32", fmt.Sprintf("%d", thing.AnInt32), *thing.Id)
	s.eventualLongSearch("thing", "anInt64", *thing.Id, int(thing.AnInt64))
	s.eventualStringSearch("thing", "anInt64", fmt.Sprintf("%d", thing.AnInt64), *thing.Id)
}

func (s *PluginSuite) TestSearchDoubleValue() {
	thing := s.indexRandomThing()
	s.eventualDoubleSearch("thing", "aDouble", *thing.Id, thing.ADouble)
	s.eventualStringSearch("thing", "aDouble", fmt.Sprintf("%v", thing.ADouble), *thing.Id)
}

func (s *PluginSuite) TestSearchDateValue() {
	thing := s.indexRandomThing()
	s.eventualDateSearch("thing", "aTimestamp", *thing.Id, thing.ATimestamp.AsTime())
}

func (s *PluginSuite) TestSearchBoolValue() {
	thing := s.indexRandomThing()
	s.eventualBoolSearch("thing", "aBool", *thing.Id, thing.ABool)
	s.eventualStringSearch("thing", "aBool", fmt.Sprintf("%t", thing.ABool), *thing.Id)
}

func (s *PluginSuite) TestSearchKeywordValue() {
	thing := s.indexRandomThing()
	s.eventualKeywordSearch("thing", "aString", thing.AString, *thing.Id)
}

func (s *PluginSuite) startElasticsearch(t *testing.T) {
	var err error
	p := elastic.Preset()
	s.container, err = gnomock.Start(p, gnomock.WithCustomNamedPorts(gnomock.NamedPorts{"default": gnomock.Port{Protocol: "tcp", Port: 9200, HostPort: 9200}}))
	require.NoError(t, err)

	cfg := elasticsearch.Config{
		Addresses: []string{fmt.Sprintf("http://%s", s.container.DefaultAddress())},
	}
	s.client, err = elasticsearch.NewClient(cfg)
	require.NoError(t, err)
	t.Cleanup(func() {
		gnomock.Stop(s.container)
	})
}

func getStringQuery(theType, key, query string) string {
	return fmt.Sprintf(`
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "%s"
                    }
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
                                    { "match": { "metadata.key": "%s" } },
                                    { "match": { "metadata.stringValue": "%s" } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, theType, key, query)
}

func getLongQuery(theType, key string, query int) string {
	return fmt.Sprintf(`
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "%s"
                    }
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
                                    { "match": { "metadata.key": "%s" } },
                                    { "match": { "metadata.longValue": %d } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, theType, key, query)
}

func getDoubleQuery(theType, key string, query float64) string {
	return fmt.Sprintf(`
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "%s"
                    }
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
                                    { "match": { "metadata.key": "%s" } },
                                    { "match": { "metadata.doubleValue": %f } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, theType, key, query)
}

func getDateQuery(theType, key string, query time.Time) string {
	return fmt.Sprintf(`
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "%s"
                    }
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
                                    { "match": { "metadata.key": "%s" } },
                                    { "match": { "metadata.dateValue": %d } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, theType, key, query.UTC().UnixMilli())
}

func getBoolQuery(theType, key string, query bool) string {
	return fmt.Sprintf(`
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "%s"
                    }
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
                                    { "match": { "metadata.key": "%s" } },
                                    { "match": { "metadata.boolValue": %t } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, theType, key, query)
}

func getKeywordQuery(theType, key, query string) string {
	return fmt.Sprintf(`
{
    "query": {
        "bool": {
            "must": [
                {
                    "term": {
                        "type": "%s"
                    }
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
                                    { "match": { "metadata.key": "%s" } },
                                    { "match": { "metadata.keywordValue": "%s" } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, theType, key, query)
}

func (s *PluginSuite) indexRandomThing() *example_example.Thing {
	thing := s.generateRandomThing()
	s.indexThing(thing)
	return thing
}

func (s *PluginSuite) generateRandomThing() *example_example.Thing {
	thing := &example_example.Thing{}
	err := gofakeit.Struct(&thing)
	require.NoError(s.T(), err)
	thing.ATimestamp = timestamppb.New(gofakeit.FutureDate())
	return thing
}

func (s *PluginSuite) indexThing(thing *example_example.Thing) {
	err := thing.Index(context.Background(), nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) eventualDoubleSearch(theType, key, expectedId string, query float64) {
	queryDouble := getDoubleQuery(theType, key, query)
	s.eventualSearch(queryDouble, expectedId)
}

func (s *PluginSuite) doubleSearch(theType, key string, query float64) string {
	queryDouble := getDoubleQuery(theType, key, query)
	return s.search(queryDouble)
}

func (s *PluginSuite) eventualLongSearch(theType, key, expectedId string, query int) {
	queryLong := getLongQuery(theType, key, query)
	s.eventualSearch(queryLong, expectedId)
}

func (s *PluginSuite) longSearch(theType, key string, query int) string {
	queryLong := getLongQuery(theType, key, query)
	return s.search(queryLong)
}

func (s *PluginSuite) eventualStringSearch(theType, key, query, expectedId string) {
	queryString := getStringQuery(theType, key, query)
	s.eventualSearch(queryString, expectedId)
}

func (s *PluginSuite) stringSearch(theType, key, query string) string {
	queryString := getStringQuery(theType, key, query)
	return s.search(queryString)
}

func (s *PluginSuite) eventualKeywordSearch(theType, key, query, expectedId string) {
	queryString := getKeywordQuery(theType, key, query)
	s.eventualSearch(queryString, expectedId)
}

func (s *PluginSuite) keywordSearch(theType, key, query string) string {
	queryString := getKeywordQuery(theType, key, query)
	return s.search(queryString)
}

func (s *PluginSuite) eventualDateSearch(theType, key, expectedId string, query time.Time) {
	queryString := getDateQuery(theType, key, query)
	s.eventualSearch(queryString, expectedId)
}

func (s *PluginSuite) dateSearch(theType, key string, query time.Time) string {
	queryString := getDateQuery(theType, key, query)
	return s.search(queryString)
}

func (s *PluginSuite) eventualBoolSearch(theType, key, expectedId string, query bool) {
	queryString := getBoolQuery(theType, key, query)
	s.eventualSearch(queryString, expectedId)
}

func (s *PluginSuite) boolSearch(theType, key string, query bool) string {
	queryString := getBoolQuery(theType, key, query)
	return s.search(queryString)
}

func (s *PluginSuite) eventualSearch(query, expected string) {
	require.Eventually(s.T(), func() bool {
		body := s.search(query)
		return strings.Contains(body, expected)
	}, 10*time.Second, 1*time.Second)
}

func (s *PluginSuite) search(query string) string {
	response, err := example_example.Client.Search(example_example.Client.Search.WithIndex("data"), example_example.Client.Search.WithBody(strings.NewReader(query)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), response.StatusCode, 200)
	body, err := io.ReadAll(response.Body)
	require.NoError(s.T(), err)
	return string(body)
}
