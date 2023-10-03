package test

import (
	"context"
	"fmt"
	"github.com/brianvoe/gofakeit/v6"
	example_example "github.com/catalystsquad/protoc-gen-go-elasticsearch/example"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
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
	s.eventualKeywordSearch("Thing", "Id", *thing.Id, *thing.Id)
}

func (s *PluginSuite) TestSearchEnum() {
	thing := s.generateRandomThing()
	s.indexThing(thing)
	s.eventualKeywordSearch("Thing", "AnEnum", thing.AnEnum.String(), *thing.Id)
	s.eventualLongSearch("Thing", "AnEnum", *thing.Id, int(thing.AnEnum.Number()))
}

func (s *PluginSuite) TestSearchStringValue() {
	thing := s.generateRandomThing()
	thing.AString = "i like turtles and elephants because they're neat"
	s.indexThing(thing)
	s.eventualStringSearch("Thing", "AString", thing.AString, *thing.Id)
	// test partial
	s.eventualStringSearch("Thing", "AString", "turtles", *thing.Id)
}

func (s *PluginSuite) TestSearchLongValue() {
	thing := s.indexRandomThing()
	s.eventualLongSearch("Thing", "AnInt32", *thing.Id, int(thing.AnInt32))
	s.eventualStringSearch("Thing", "AnInt32", fmt.Sprintf("%d", thing.AnInt32), *thing.Id)
	s.eventualLongSearch("Thing", "AnInt64", *thing.Id, int(thing.AnInt64))
	s.eventualStringSearch("Thing", "AnInt64", fmt.Sprintf("%d", thing.AnInt64), *thing.Id)
}

func (s *PluginSuite) TestSearchDoubleValue() {
	thing := s.indexRandomThing()
	s.eventualDoubleSearch("Thing", "ADouble", *thing.Id, thing.ADouble)
	s.eventualStringSearch("Thing", "ADouble", fmt.Sprintf("%v", thing.ADouble), *thing.Id)
}

func (s *PluginSuite) TestSearchDateValue() {
	thing := s.indexRandomThing()
	s.eventualDateSearch("Thing", "ATimestamp", *thing.Id, thing.ATimestamp.AsTime())
}

func (s *PluginSuite) TestSearchBoolValue() {
	thing := s.indexRandomThing()
	s.eventualBoolSearch("Thing", "ABool", *thing.Id, thing.ABool)
	s.eventualStringSearch("Thing", "ABool", fmt.Sprintf("%t", thing.ABool), *thing.Id)
}

func (s *PluginSuite) TestSearchKeywordValue() {
	thing := s.indexRandomThing()
	s.eventualKeywordSearch("Thing", "AString", thing.AString, *thing.Id)
}

func (s *PluginSuite) TestSearchRepeatedValue() {
	thing := s.indexRandomThing()
	require.GreaterOrEqual(s.T(), len(thing.RepeatedInt32), 1)
	for _, num := range thing.RepeatedInt32 {
		s.eventualLongSearch("Thing", "RepeatedInt32", *thing.Id, int(num))
	}
}

func (s *PluginSuite) TestSearchRelatedObject() {
	thing := s.indexRandomThingWithRelationships()
	// search for associated thing exact match
	s.eventualJoinRecordSearch("AssociatedThing", "Thing", "Thing2", *thing.Id, *thing.AssociatedThing.Id)
	//// search for repeated relationship objects
	s.eventualJoinRecordSearch("RepeatedMessages", "Thing", "Thing2", *thing.Id, *thing.RepeatedMessages[0].Id)
	s.eventualJoinRecordSearch("RepeatedMessages", "Thing", "Thing2", *thing.Id, *thing.RepeatedMessages[1].Id)
}

func (s *PluginSuite) TestClear() {
	thing := s.indexRandomThingWithRelationships()
	// search for associated thing exact match so we know it was indexed
	s.eventualJoinRecordSearch("AssociatedThing", "Thing", "Thing2", *thing.Id, *thing.AssociatedThing.Id)
	err := thing.Clear(context.Background())
	require.NoError(s.T(), err)
	// verify we can still find thing, and the associated thing
	s.eventualKeywordSearch("Thing", "Id", *thing.Id, *thing.Id)
	s.eventualKeywordSearch("Thing2", "Id", *thing.AssociatedThing.Id, *thing.AssociatedThing.Id)
	// verify there are no joinRecords
	result := s.joinRecordSearch("AssociatedThing", "Thing", "Thing2", *thing.Id, *thing.AssociatedThing.Id)
	require.False(s.T(), strings.Contains(result, *thing.Id))
	require.False(s.T(), strings.Contains(result, *thing.AssociatedThing.Id))
}

func (s *PluginSuite) TestDelete() {
	thing := s.indexRandomThingWithRelationships()
	// search for thing, joinRecords, joined object so we know they were all indexed
	s.eventualKeywordSearch("Thing", "Id", *thing.Id, *thing.Id)
	s.eventualKeywordSearch("Thing2", "Id", *thing.AssociatedThing.Id, *thing.AssociatedThing.Id)
	s.eventualJoinRecordSearch("AssociatedThing", "Thing", "Thing2", *thing.Id, *thing.AssociatedThing.Id)
	err := thing.Delete(context.Background(), func(ctx context.Context, item esutil.BulkIndexerItem, item2 esutil.BulkIndexerResponseItem) {
		// Using success func because it's deterministic rather than using waits or eventual searches
		// ensure thing is deleted
		result := s.keywordSearch("Thing", "Id", *thing.Id)
		require.False(s.T(), strings.Contains(result, *thing.Id))
		// ensure join records are deleted
		result = s.joinRecordSearch("AssociatedThing", "Thing", "Thing2", *thing.Id, *thing.AssociatedThing.Id)
		require.False(s.T(), strings.Contains(result, *thing.Id))
		// ensure thing2 remains
		result = s.keywordSearch("Thing2", "Id", *thing.AssociatedThing.Id)
		require.True(s.T(), strings.Contains(result, *thing.AssociatedThing.Id))
	}, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) TestIndexSyncWithRefresh() {
	thing := s.generateRandomThing()
	err := thing.IndexSyncWithRefresh(context.Background())
	require.NoError(s.T(), err)
	response := s.keywordSearch("Thing", "Id", *thing.Id)
	require.Contains(s.T(), response, *thing.Id)
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

// TODO: Refactor this to stop repeating myself in query building
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

func getJoinRecordQuery(fieldName, parentType, childType, parentId, childId string) string {
	return fmt.Sprintf(`
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
									{ "match": { "metadata.key": "fieldName" } },
                                    { "match": { "metadata.keywordValue": "%s" } }
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
									{ "match": { "metadata.key": "parentType" } },
                                    { "match": { "metadata.keywordValue": "%s" } }
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
									{ "match": { "metadata.key": "childType" } },
                                    { "match": { "metadata.keywordValue": "%s" } }
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
                },
                {
                    "nested": {
                        "path": "metadata",
                        "query": {
                            "bool": {
                                "must": [
									{ "match": { "metadata.key": "childId" } },
                                    { "match": { "metadata.keywordValue": "%s" } }
                                ]
                            }
                        }
                    }
                }
            ]
        }
    }
}`, fieldName, parentType, childType, parentId, childId)
}

func (s *PluginSuite) indexRandomThing() *example_example.Thing {
	thing := s.generateRandomThing()
	s.indexThing(thing)
	return thing
}

func (s *PluginSuite) indexRandomThingWithRelationships() *example_example.Thing {
	thing := s.generateRandomThing()
	thing2 := s.generateRandomThing2()
	repeatedThing2A := s.generateRandomThing2()
	repeatedThing2B := s.generateRandomThing2()
	thing.AssociatedThing = thing2
	thing.RepeatedMessages = []*example_example.Thing2{
		repeatedThing2A,
		repeatedThing2B,
	}
	s.indexThing(thing)
	s.indexThing2(thing2)
	s.indexThing2(repeatedThing2A)
	s.indexThing2(repeatedThing2B)
	return thing
}

func (s *PluginSuite) generateRandomThing() *example_example.Thing {
	thing := &example_example.Thing{}
	err := gofakeit.Struct(&thing)
	require.NoError(s.T(), err)
	thing.ATimestamp = timestamppb.New(gofakeit.FutureDate())
	return thing
}

func (s *PluginSuite) generateRandomThing2() *example_example.Thing2 {
	thing2 := &example_example.Thing2{}
	err := gofakeit.Struct(&thing2)
	require.NoError(s.T(), err)
	return thing2
}

func (s *PluginSuite) indexThing(thing *example_example.Thing) {
	err := thing.IndexAsync(context.Background(), nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) indexThing2(thing2 *example_example.Thing2) {
	err := thing2.IndexAsync(context.Background(), nil, nil)
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

func (s *PluginSuite) eventualJoinRecordSearch(fieldName, parentType, childType, parentId, childId string) {
	queryString := getJoinRecordQuery(fieldName, parentType, childType, parentId, childId)
	s.eventualSearch(queryString, parentId)
}

func (s *PluginSuite) joinRecordSearch(fieldName, parentType, childType, parentId, childId string) string {
	queryString := getJoinRecordQuery(fieldName, parentType, childType, parentId, childId)
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
	response, err := example_example.ElasticsearchClient.Search(example_example.ElasticsearchClient.Search.WithIndex("data"), example_example.ElasticsearchClient.Search.WithBody(strings.NewReader(query)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), response.StatusCode, 200)
	body, err := io.ReadAll(response.Body)
	require.NoError(s.T(), err)
	return string(body)
}
