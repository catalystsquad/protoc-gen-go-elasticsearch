package test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/catalystcommunity/app-utils-go/errorutils"
	example_example "github.com/catalystcommunity/protoc-gen-go-elasticsearch/example"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
	"github.com/orlangure/gnomock"
	"github.com/orlangure/gnomock/preset/elastic"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/types/known/timestamppb"
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
	err := example_example.InitializeWithDefaults([]string{"http://localhost:9200"})
	require.NoError(s.T(), err)
	s.T().Parallel()
	s.startElasticsearch(s.T())
	err = example_example.EnsureIndex(s.client)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) TestSearchById() {
	thing := s.generateRandomThing()
	s.indexThing(thing)
	s.eventualKeywordSearch("Thing", "id", *thing.Id, *thing.Id)
}

func (s *PluginSuite) TestSearchEnum() {
	thing := s.generateRandomThing()
	s.indexThing(thing)
	s.eventualKeywordSearch("Thing", "anEnum", thing.AnEnum.String(), *thing.Id)
	s.eventualLongSearch("Thing", "anEnum", *thing.Id, int(thing.AnEnum.Number()))
}

func (s *PluginSuite) TestSearchStringValue() {
	thing := s.generateRandomThing()
	thing.AString = "i like turtles and elephants because they're neat"
	s.indexThing(thing)
	s.eventualStringSearch("Thing", "astring", thing.AString, *thing.Id)
	// test partial
	s.eventualStringSearch("Thing", "astring", "turtles", *thing.Id)
}

func (s *PluginSuite) TestSearchLongValue() {
	thing := s.indexRandomThing()
	s.eventualLongSearch("Thing", "anInt32", *thing.Id, int(thing.AnInt32))
	s.eventualStringSearch("Thing", "anInt32", fmt.Sprintf("%d", thing.AnInt32), *thing.Id)
	s.eventualLongSearch("Thing", "anInt64", *thing.Id, int(thing.AnInt64))
	s.eventualStringSearch("Thing", "anInt64", fmt.Sprintf("%d", thing.AnInt64), *thing.Id)
}

func (s *PluginSuite) TestSearchDoubleValue() {
	thing := s.indexRandomThing()
	s.eventualDoubleSearch("Thing", "adouble", *thing.Id, thing.ADouble)
	s.eventualStringSearch("Thing", "adouble", fmt.Sprintf("%v", thing.ADouble), *thing.Id)
}

func (s *PluginSuite) TestSearchDateValue() {
	thing := s.indexRandomThing()
	s.eventualDateSearch("Thing", "atimestamp", *thing.Id, thing.ATimestamp.AsTime())
}

func (s *PluginSuite) TestSearchBoolValue() {
	thing := s.indexRandomThing()
	s.eventualBoolSearch("Thing", "abool", *thing.Id, thing.ABool)
	s.eventualStringSearch("Thing", "abool", fmt.Sprintf("%t", thing.ABool), *thing.Id)
}

func (s *PluginSuite) TestSearchKeywordValue() {
	thing := s.indexRandomThing()
	s.eventualKeywordSearch("Thing", "astring", thing.AString, *thing.Id)
}

func (s *PluginSuite) TestSearchRepeatedValue() {
	thing := s.indexRandomThing()
	require.GreaterOrEqual(s.T(), len(thing.RepeatedInt32), 1)
	for _, num := range thing.RepeatedInt32 {
		s.eventualLongSearch("Thing", "repeatedInt32", *thing.Id, int(num))
	}
}

func (s *PluginSuite) TestSearchNestedObject() {
	thing := s.indexRandomThingWithRelationships()
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.name", thing.AssociatedThing.Name, *thing.Id)
	//// search for repeated relationship objects
	s.eventualKeywordSearch("Thing", "repeatedMessages.name", thing.RepeatedMessages[0].Name, *thing.Id)
	s.eventualKeywordSearch("Thing", "repeatedMessages.name", thing.RepeatedMessages[1].Name, *thing.Id)
}

func (s *PluginSuite) TestReindexRelated() {
	thing, associatedThing2 := s.indexRandomThingAndThing2()
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.name", associatedThing2.Name, *thing.Id)
	// update the associatedThing2 and reindex
	oldName := associatedThing2.Name
	associatedThing2.Name = "new name for TestReindexRelated"
	s.reindexThing2RelatedDocsBulk(associatedThing2)
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.name", associatedThing2.Name, *thing.Id)
	// ensure that the old name is not in the index
	require.Equal(s.T(), 0, s.searchCount(getKeywordQuery("Thing", "associatedThing.name", oldName)))
}

func (s *PluginSuite) TestReindexRelatedPagination() {
	thing2 := s.generateRandomThing2()
	count := 500
	things := s.bulkIndexRandomThingsWithThing2Relationship(thing2, count)
	// update the associatedThing2 and reindex
	originalName := thing2.Name
	thing2.Name = "new name for TestReindexRelatedPagination"
	s.reindexThing2RelatedDocsBulk(thing2)
	s.eventualSearchCount(getKeywordQuery("Thing", "associatedThing.name", thing2.Name), count)
	// ensure that the old name is not in the index
	require.Equal(s.T(), 0, s.searchCount(getKeywordQuery("Thing", "associatedThing.name", originalName)))
	// cleanup so that the excessive amount of left over docs don't affect other tests
	err := thing2.DeleteWithRefresh(context.Background())
	require.NoError(s.T(), err)
	thingsProto := example_example.ThingBulkEsModel(things)
	err = thingsProto.DeleteWithRefresh(context.Background(), nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) TestReindexRelatedWithCustomBulkIndexer() {
	// created two of each object and update both using a shared bulk indexer
	firstThing, firstAssociatedThing2 := s.indexRandomThingAndThing2()
	secondThing, secondAssociatedThing2 := s.indexRandomThingAndThing2()
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.name", firstAssociatedThing2.Name, *firstThing.Id)
	s.eventualKeywordSearch("Thing", "associatedThing.name", secondAssociatedThing2.Name, *secondThing.Id)
	// update the associatedThing2 and reindex
	firstOldName := firstAssociatedThing2.Name
	firstAssociatedThing2.Name = "new name1 for TestReindexRelated"
	secondOldName := secondAssociatedThing2.Name
	secondAssociatedThing2.Name = "new name2 for TestReindexRelated"
	// create a bulk indexer and handle both reindex calls with the same indexer
	indexer, err := newRequestBulkIndexerWithRefresh("wait_for")
	require.NoError(s.T(), err)
	s.reindexThing2RelatedDocsBulkWithCustomIndexer(firstAssociatedThing2, indexer)
	s.reindexThing2RelatedDocsBulkWithCustomIndexer(secondAssociatedThing2, indexer)
	err = indexer.Close(context.Background())
	require.NoError(s.T(), err)
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.name", firstAssociatedThing2.Name, *firstThing.Id)
	s.eventualKeywordSearch("Thing", "associatedThing.name", secondAssociatedThing2.Name, *secondThing.Id)
	// ensure that the old name is not in the index
	require.Equal(s.T(), 0, s.searchCount(getKeywordQuery("Thing", "associatedThing.name", firstOldName)))
	require.Equal(s.T(), 0, s.searchCount(getKeywordQuery("Thing", "associatedThing.name", secondOldName)))
}

func (s *PluginSuite) TestReindexRelatedAfterDelete() {
	thing, associatedThing2 := s.indexRandomThingAndThing2()
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.id", *associatedThing2.Id, *thing.Id)
	// delete the associatedThing2 and reindex
	err := associatedThing2.DeleteWithRefresh(context.Background())
	require.NoError(s.T(), err)
	s.reindexThing2RelatedDocsAfterDeleteBulk(associatedThing2)
	// ensure that the old id is not in the index
	s.eventualSearchCount(getKeywordQuery("Thing", "associatedThing.id", *associatedThing2.Id), 0)
}

func (s *PluginSuite) TestReindexRelatedAfterDeleteWithCustomBulkIndexer() {
	firstThing, firstAssociatedThing2 := s.indexRandomThingAndThing2()
	secondThing, secondAssociatedThing2 := s.indexRandomThingAndThing2()
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThing.id", *firstAssociatedThing2.Id, *firstThing.Id)
	s.eventualKeywordSearch("Thing", "associatedThing.id", *secondAssociatedThing2.Id, *secondThing.Id)
	// delete the associatedThing2 and reindex
	bulkModel := example_example.ThingBulkEsModel([]*example_example.Thing{firstThing, secondThing})
	err := bulkModel.DeleteWithRefresh(context.Background(), nil, nil)
	require.NoError(s.T(), err)
	// create a bulk indexer and handle both reindex calls with the same indexer
	indexer, err := newRequestBulkIndexerWithRefresh("wait_for")
	require.NoError(s.T(), err)
	s.reindexThing2RelatedDocsAfterDeleteBulkWithCustomIndexer(firstAssociatedThing2, indexer)
	s.reindexThing2RelatedDocsAfterDeleteBulkWithCustomIndexer(secondAssociatedThing2, indexer)
	err = indexer.Close(context.Background())
	require.NoError(s.T(), err)
	// ensure that the old id is not in the index
	s.eventualSearchCount(getKeywordQuery("Thing", "associatedThing.id", *firstAssociatedThing2.Id), 0)
	s.eventualSearchCount(getKeywordQuery("Thing", "associatedThing.id", *secondAssociatedThing2.Id), 0)

}

func (s *PluginSuite) TestDeleteRelated() {
	thing, associatedThing2 := s.indexRandomThingAndThing2viaWithCascadeDelete()
	s.eventualKeywordSearch("Thing", "id", *thing.Id, *thing.Id)
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThingWithCascadeDelete.id", *associatedThing2.Id, *thing.Id)
	// delete the associatedThing2 and call delete related
	err := associatedThing2.DeleteWithRefresh(context.Background())
	require.NoError(s.T(), err)
	s.deleteThing2RelatedDocsBulk(associatedThing2)
	// ensure that thing1 is not in the index
	s.eventualSearchCount(getKeywordQuery("Thing", "id", *thing.Id), 0)
}

func (s *PluginSuite) TestDeleteRelatedWithCustomBulkIndexer() {
	firstThing, firstAssociatedThing2 := s.indexRandomThingAndThing2viaWithCascadeDelete()
	secondThing, secondAssociatedThing2 := s.indexRandomThingAndThing2viaWithCascadeDelete()
	s.eventualKeywordSearch("Thing", "id", *firstThing.Id, *firstThing.Id)
	s.eventualKeywordSearch("Thing", "id", *secondThing.Id, *secondThing.Id)
	// search for associated thing exact match
	s.eventualKeywordSearch("Thing", "associatedThingWithCascadeDelete.id", *firstAssociatedThing2.Id, *firstThing.Id)
	s.eventualKeywordSearch("Thing", "associatedThingWithCascadeDelete.id", *secondAssociatedThing2.Id, *secondThing.Id)
	// delete the associatedThing2 and call delete related
	bulkModel := example_example.Thing2BulkEsModel([]*example_example.Thing2{firstAssociatedThing2, secondAssociatedThing2})
	err := bulkModel.DeleteWithRefresh(context.Background(), nil, nil)
	require.NoError(s.T(), err)
	// create a bulk indexer and handle both reindex calls with the same indexer
	indexer, err := newRequestBulkIndexerWithRefresh("wait_for")
	require.NoError(s.T(), err)
	s.deleteThing2RelatedDocsBulkWithCustomIndexer(firstAssociatedThing2, indexer)
	s.deleteThing2RelatedDocsBulkWithCustomIndexer(secondAssociatedThing2, indexer)
	err = indexer.Close(context.Background())
	require.NoError(s.T(), err)
	// ensure that thing1 is not in the index
	s.eventualSearchCount(getKeywordQuery("Thing", "id", *firstThing.Id), 0)
	s.eventualSearchCount(getKeywordQuery("Thing", "id", *secondThing.Id), 0)
}

func (s *PluginSuite) TestDelete() {
	thing := s.indexRandomThing()
	s.eventualKeywordSearch("Thing", "id", *thing.Id, *thing.Id)
	err := thing.DeleteWithRefresh(context.Background())
	require.NoError(s.T(), err)
	response := s.keywordSearch("Thing", "id", *thing.Id)
	require.NotContains(s.T(), response, *thing.Id)
}

func (s *PluginSuite) TestIndexSyncWithRefresh() {
	thing := s.generateRandomThing()
	err := thing.IndexSyncWithRefresh(context.Background())
	require.NoError(s.T(), err)
	response := s.keywordSearch("Thing", "id", *thing.Id)
	require.Contains(s.T(), response, *thing.Id)
}

func (s *PluginSuite) TestBulkIndex() {
	things := s.generateRandomThings(3)
	s.indexThings(things)
	// do a simple search by id to verify that all things were indexed
	thing1, thing2, thing3 := things[0], things[1], things[2]
	s.eventualKeywordSearch("Thing", "id", *thing1.Id, *thing1.Id)
	s.eventualKeywordSearch("Thing", "id", *thing2.Id, *thing2.Id)
	s.eventualKeywordSearch("Thing", "id", *thing3.Id, *thing3.Id)
}

func (s *PluginSuite) TestBulkDelete() {
	things := s.generateRandomThings(3)
	s.indexThings(things)
	thing1, thing2, thing3 := things[0], things[1], things[2]
	s.eventualKeywordSearch("Thing", "id", *thing1.Id, *thing1.Id)
	s.eventualKeywordSearch("Thing", "id", *thing2.Id, *thing2.Id)
	s.eventualKeywordSearch("Thing", "id", *thing3.Id, *thing3.Id)
	// delete all things
	thingsProto := example_example.ThingBulkEsModel(things)
	err := thingsProto.DeleteWithRefresh(context.Background(), nil, nil)
	require.NoError(s.T(), err)
	// verify that all things were deleted
	response := s.keywordSearch("Thing", "id", *thing1.Id)
	require.NotContains(s.T(), response, *thing1.Id)
	response = s.keywordSearch("Thing", "id", *thing2.Id)
	require.NotContains(s.T(), response, *thing2.Id)
	response = s.keywordSearch("Thing", "id", *thing3.Id)
	require.NotContains(s.T(), response, *thing3.Id)
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

func (s *PluginSuite) indexRandomThingWithThing2Relationship(thing2 *example_example.Thing2) *example_example.Thing {
	thing := s.generateRandomThing()
	thing.AssociatedThing = thing2
	s.indexThing(thing)
	return thing
}

func (s *PluginSuite) bulkIndexRandomThingsWithThing2Relationship(thing2 *example_example.Thing2, num int) []*example_example.Thing {
	things := []*example_example.Thing{}
	for i := 0; i < num; i++ {
		thing := s.generateRandomThing()
		thing.AssociatedThing = thing2
		things = append(things, thing)
	}
	s.indexThings(things)
	return things
}

func (s *PluginSuite) indexRandomThingAndThing2() (*example_example.Thing, *example_example.Thing2) {
	thing := s.generateRandomThing()
	thing2 := s.generateRandomThing2()
	thing.AssociatedThing = thing2
	s.indexThing(thing)
	s.indexThing2(thing2)
	return thing, thing2
}

func (s *PluginSuite) indexRandomThingAndThing2viaWithCascadeDelete() (*example_example.Thing, *example_example.Thing2) {
	thing := s.generateRandomThing()
	thing2 := s.generateRandomThing2()
	thing.AssociatedThingWithCascadeDelete = thing2
	s.indexThing(thing)
	s.indexThing2(thing2)
	return thing, thing2
}

func (s *PluginSuite) generateRandomThing() *example_example.Thing {
	thing := &example_example.Thing{}
	err := gofakeit.Struct(&thing)
	require.NoError(s.T(), err)
	thing.ATimestamp = timestamppb.New(gofakeit.FutureDate())
	return thing
}

func (s *PluginSuite) generateRandomThings(num int) []*example_example.Thing {
	things := []*example_example.Thing{}
	for i := 0; i < num; i++ {
		thing := &example_example.Thing{}
		err := gofakeit.Struct(&thing)
		require.NoError(s.T(), err)
		thing.ATimestamp = timestamppb.New(gofakeit.FutureDate())
		things = append(things, thing)
	}
	return things
}

func (s *PluginSuite) generateRandomThing2() *example_example.Thing2 {
	thing2 := &example_example.Thing2{}
	err := gofakeit.Struct(&thing2)
	require.NoError(s.T(), err)
	return thing2
}

func (s *PluginSuite) indexThing(thing *example_example.Thing) {
	err := thing.IndexSyncWithRefresh(context.Background())
	require.NoError(s.T(), err)
}

func (s *PluginSuite) indexThings(things []*example_example.Thing) {
	thingsProto := example_example.ThingBulkEsModel(things)
	err := thingsProto.IndexSyncWithRefresh(context.Background(), nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) indexThing2(thing2 *example_example.Thing2) {
	err := thing2.IndexSyncWithRefresh(context.Background())
	require.NoError(s.T(), err)
}

func (s *PluginSuite) reindexThing2RelatedDocsBulk(thing2 *example_example.Thing2) {
	err := thing2.ReindexRelatedDocumentsBulk(context.Background(), nil, nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) reindexThing2RelatedDocsBulkWithCustomIndexer(thing2 *example_example.Thing2, indexer esutil.BulkIndexer) {
	err := thing2.ReindexRelatedDocumentsBulk(context.Background(), nil, nil, indexer)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) reindexThing2RelatedDocsAfterDeleteBulk(thing2 *example_example.Thing2) {
	err := thing2.ReindexRelatedDocumentsAfterDeleteBulk(context.Background(), nil, nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) reindexThing2RelatedDocsAfterDeleteBulkWithCustomIndexer(thing2 *example_example.Thing2, indexer esutil.BulkIndexer) {
	err := thing2.ReindexRelatedDocumentsAfterDeleteBulk(context.Background(), nil, nil, indexer)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) deleteThing2RelatedDocsBulk(thing2 *example_example.Thing2) {
	err := thing2.DeleteRelatedDocumentsBulk(context.Background(), nil, nil, nil)
	require.NoError(s.T(), err)
}

func (s *PluginSuite) deleteThing2RelatedDocsBulkWithCustomIndexer(thing2 *example_example.Thing2, indexer esutil.BulkIndexer) {
	err := thing2.DeleteRelatedDocumentsBulk(context.Background(), nil, nil, indexer)
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

type searchResponse struct {
	Hits struct {
		Total struct {
			Value int `json:"value"`
		} `json:"total"`
	} `json:"hits"`
}

func (s *PluginSuite) eventualSearchCount(query string, expectedCount int) {
	require.Eventually(s.T(), func() bool {
		return s.searchCount(query) == expectedCount
	}, 10*time.Second, 1*time.Second)
}

func (s *PluginSuite) searchCount(query string) int {
	body := s.search(query)
	var bodyMap searchResponse
	_ = json.Unmarshal([]byte(body), &bodyMap)
	return bodyMap.Hits.Total.Value
}

func (s *PluginSuite) search(query string) string {
	response, err := example_example.ElasticsearchClient.Search(example_example.ElasticsearchClient.Search.WithIndex("data"), example_example.ElasticsearchClient.Search.WithBody(strings.NewReader(query)))
	require.NoError(s.T(), err)
	require.Equal(s.T(), response.StatusCode, 200)
	body, err := io.ReadAll(response.Body)
	require.NoError(s.T(), err)
	return string(body)
}

func newRequestBulkIndexerWithRefresh(refresh string) (esutil.BulkIndexer, error) {
	return esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Client:     example_example.ElasticsearchClient,
		Index:      example_example.ElasticsearchIndexName,
		NumWorkers: 1,
		Refresh:    refresh,
		OnError: func(ctx context.Context, err error) {
			errorutils.LogOnErr(nil, "error encountered in bulk indexer", err)
		},
	})
}
