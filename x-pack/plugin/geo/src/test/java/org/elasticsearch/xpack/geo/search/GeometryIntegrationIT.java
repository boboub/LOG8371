/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.xpack.geo.search;

import org.apache.lucene.geo.XShapeTestUtil;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.compress.CompressedXContent;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.mapper.DocumentMapper;
import org.elasticsearch.index.mapper.DocumentMapperParser;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.geo.Geo;
import org.junit.Before;

import java.util.Collection;

import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_REPLICAS;
import static org.elasticsearch.cluster.metadata.IndexMetaData.SETTING_NUMBER_OF_SHARDS;
import static org.elasticsearch.index.query.QueryBuilders.geoShapeQuery;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class GeometryIntegrationIT extends ESSingleNodeTestCase {
    protected boolean isGeo = false;
    private DocumentMapper mapper;

    private static String INDEX = "test";
    private static String FIELD_TYPE = "geometry";
    private static String FIELD = "shape";

    @Before
    public void setUpMapper() throws Exception {
//        IndexService indexService =  createIndex(INDEX);
//        DocumentMapperParser parser = indexService.mapperService().documentMapperParser();
//        String mapping = Strings.toString(XContentFactory.jsonBuilder()
//            .startObject()
//            .startObject("_doc")
//            .startObject("properties")
//            .startObject(FIELD).field("type", "geometry")
//            .endObject()
//            .endObject()
//            .endObject()
//            .endObject());
//        mapper = parser.parse("_doc", new CompressedXContent(mapping));
    }

//    @Override
//    public void setUp() throws Exception {
//        super.setUp();
//        String mapping = Strings.toString(XContentFactory.jsonBuilder()
//            .startObject()
//            .startObject(FIELD_TYPE)
//            .startObject("properties")
//            .startObject(FIELD).field("type", "geometry")
//            .endObject()
//            .endObject()
//            .endObject()
//            .endObject());
//        assertAcked(client().admin().indices().prepareCreate(INDEX)
//            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put(SETTING_NUMBER_OF_REPLICAS, 0))
//            .addMapping(FIELD_TYPE,
//                "decade", "type=keyword",
//                "people", "type=keyword",
//                "description", "type=text,fielddata=true"));
//        createIndex("idx_unmapped");
//
//        ensureGreen();
//    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(Geo.class, XPackPlugin.class);
    }


//    /**
//     * Test that orientation parameter correctly persists across cluster restart
//     */
//    public void testOrientationPersistence() throws Exception {
//        String idxName = "orientation";
//        String mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("shape")
//            .startObject("properties").startObject("location")
//            .field("type", "geo_shape")
//            .field("orientation", "left")
//            .endObject().endObject()
//            .endObject().endObject());
//
//        // create index
//        assertAcked(prepareCreate(idxName).addMapping("shape", mapping, XContentType.JSON));
//
//        mapping = Strings.toString(XContentFactory.jsonBuilder().startObject().startObject("shape")
//            .startObject("properties").startObject("location")
//            .field("type", "geo_shape")
//            .field("orientation", "right")
//            .endObject().endObject()
//            .endObject().endObject());
//
//        assertAcked(prepareCreate(idxName+"2").addMapping("shape", mapping, XContentType.JSON));
//        ensureGreen(idxName, idxName+"2");
//
//        internalCluster().fullRestart();
//        ensureGreen(idxName, idxName+"2");
//
//        // left orientation test
//        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName));
//        IndexService indexService = indicesService.indexService(resolveIndex(idxName));
//        MappedFieldType fieldType = indexService.mapperService().fullName("location");
//        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));
//
//        GeoShapeFieldMapper.GeoShapeFieldType gsfm = (GeoShapeFieldMapper.GeoShapeFieldType)fieldType;
//        ShapeBuilder.Orientation orientation = gsfm.orientation();
//        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CLOCKWISE));
//        assertThat(orientation, equalTo(ShapeBuilder.Orientation.LEFT));
//        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CW));
//
//        // right orientation test
//        indicesService = internalCluster().getInstance(IndicesService.class, findNodeName(idxName+"2"));
//        indexService = indicesService.indexService(resolveIndex((idxName+"2")));
//        fieldType = indexService.mapperService().fullName("location");
//        assertThat(fieldType, instanceOf(GeoShapeFieldMapper.GeoShapeFieldType.class));
//
//        gsfm = (GeoShapeFieldMapper.GeoShapeFieldType)fieldType;
//        orientation = gsfm.orientation();
//        assertThat(orientation, equalTo(ShapeBuilder.Orientation.COUNTER_CLOCKWISE));
//        assertThat(orientation, equalTo(ShapeBuilder.Orientation.RIGHT));
//        assertThat(orientation, equalTo(ShapeBuilder.Orientation.CCW));
//    }

    /**
     * Test that ignore_malformed on GeoShapeFieldMapper does not fail the entire document
     */
    public void testIgnoreMalformed() throws Exception {
        // create index
        assertAcked(client().admin().indices().prepareCreate(INDEX)
            .addMapping(FIELD_TYPE, FIELD, "type=geometry,ignore_malformed=true").get());
        ensureGreen();

        double startX = XShapeTestUtil.nextDouble();
        double startY = XShapeTestUtil.nextDouble();
        // test self crossing ccw poly not crossing dateline
        XContentBuilder polygonGeoJson = XContentFactory.jsonBuilder().startObject()
            .startObject(FIELD).field("type", "Polygons")
            .startArray("coordinates")
            .startArray()
            .startArray().value(startX).value(startY).endArray()
            .startArray().value(XShapeTestUtil.nextDouble()).value(XShapeTestUtil.nextDouble()).endArray()
            .startArray().value(XShapeTestUtil.nextDouble()).value(XShapeTestUtil.nextDouble()).endArray()
            .startArray().value(XShapeTestUtil.nextDouble()).value(XShapeTestUtil.nextDouble()).endArray()
            .startArray().value(XShapeTestUtil.nextDouble()).value(XShapeTestUtil.nextDouble()).endArray()
            .startArray().value(XShapeTestUtil.nextDouble()).value(XShapeTestUtil.nextDouble()).endArray()
            .startArray().value(startX).value(startY).endArray()
            .endArray()
            .endArray()
            .endObject()
            .endObject();

        client().prepareIndex(INDEX, FIELD_TYPE).setSource(polygonGeoJson).get();
        client().admin().indices().prepareRefresh(INDEX).get();

        //SearchResponse searchResponse = client().prepareSearch(INDEX).setQuery(matchAllQuery()).get();
        assertHitCount(client().prepareSearch(INDEX).setQuery(matchAllQuery()).get(), 1);

//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

    /**
     * Test that the indexed shape routing can be provided if it is required
     */
    public void testIndexShapeRouting() throws Exception {
        String mapping = "{\n" +
            "    \"_routing\": {\n" +
            "      \"required\": true\n" +
            "    },\n" +
            "    \"properties\": {\n" +
            "      \"shape\": {\n" +
            "        \"type\": \"geo_shape\"\n" +
            "      }\n" +
            "    }\n" +
            "  }";


        // create index
        assertAcked(client().admin().indices().prepareCreate("test").addMapping("doc", mapping, XContentType.JSON).get());
        ensureGreen();

        String source = "{\n" +
            "    \"shape\" : {\n" +
            "        \"type\" : \"bbox\",\n" +
            "        \"coordinates\" : [[-45.0, 45.0], [45.0, -45.0]]\n" +
            "    }\n" +
            "}";

        client().prepareIndex("test", "doc", "0").setSource(source, XContentType.JSON).setRouting("ABC");

        SearchResponse searchResponse = client().prepareSearch("test").setQuery(
            geoShapeQuery("shape", "0").indexedShapeIndex("test").indexedShapeRouting("ABC")
        ).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
    }

//    public void testIndexPolygonDateLine() throws Exception {
//        String mappingVector = "{\n" +
//            "    \"properties\": {\n" +
//            "      \"shape\": {\n" +
//            "        \"type\": \"geo_shape\"\n" +
//            "      }\n" +
//            "    }\n" +
//            "  }";
//
//        String mappingQuad = "{\n" +
//            "    \"properties\": {\n" +
//            "      \"shape\": {\n" +
//            "        \"type\": \"geo_shape\",\n" +
//            "        \"tree\": \"quadtree\"\n" +
//            "      }\n" +
//            "    }\n" +
//            "  }";
//
//
//        // create index
//        assertAcked(client().admin().indices().prepareCreate("vector").addMapping("doc", mappingVector, XContentType.JSON).get());
//        ensureGreen();
//
//        assertAcked(client().admin().indices().prepareCreate("quad").addMapping("doc", mappingQuad, XContentType.JSON).get());
//        ensureGreen();
//
//        String source = "{\n" +
//            "    \"shape\" : \"POLYGON((179 0, -179 0, -179 2, 179 2, 179 0))\""+
//            "}";
//
//        indexRandom(true, client().prepareIndex("quad", "doc", "0").setSource(source, XContentType.JSON));
//        indexRandom(true, client().prepareIndex("vector", "doc", "0").setSource(source, XContentType.JSON));
//
//        SearchResponse searchResponse = client().prepareSearch("quad").setQuery(
//            geoShapeQuery("shape", new PointBuilder(-179.75, 1, isGeo))
//        ).get();
//
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
//
//        searchResponse = client().prepareSearch("quad").setQuery(
//            geoShapeQuery("shape", new PointBuilder(90, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
//
//        searchResponse = client().prepareSearch("quad").setQuery(
//            geoShapeQuery("shape", new PointBuilder(-180, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
//        searchResponse = client().prepareSearch("quad").setQuery(
//            geoShapeQuery("shape", new PointBuilder(180, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
//
//        searchResponse = client().prepareSearch("vector").setQuery(
//            geoShapeQuery("shape", new PointBuilder(90, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(0L));
//
//        searchResponse = client().prepareSearch("vector").setQuery(
//            geoShapeQuery("shape", new PointBuilder(-179.75, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
//
//        searchResponse = client().prepareSearch("vector").setQuery(
//            geoShapeQuery("shape", new PointBuilder(-180, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
//
//        searchResponse = client().prepareSearch("vector").setQuery(
//            geoShapeQuery("shape", new PointBuilder(180, 1, isGeo))
//        ).get();
//
//        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
//    }

    private String findNodeName(String index) {
        ClusterState state = client().admin().cluster().prepareState().get().getState();
        IndexShardRoutingTable shard = state.getRoutingTable().index(index).shard(0);
        String nodeId = shard.assignedShards().get(0).currentNodeId();
        return state.getNodes().get(nodeId).getName();
    }
}

