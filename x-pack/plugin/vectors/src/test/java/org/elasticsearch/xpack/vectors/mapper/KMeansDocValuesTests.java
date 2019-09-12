/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */


package org.elasticsearch.xpack.vectors.mapper;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;
import org.elasticsearch.xpack.vectors.Vectors;

import java.util.Collection;

public class KMeansDocValuesTests extends ESSingleNodeTestCase {
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(Vectors.class, LocalStateCompositeXPackPlugin.class);
    }

    public void testSimpleKMeans() throws Exception {
        Settings indexSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .put("index.codec", "KMeansCodec")
            .build();

        createIndex("index", indexSettings);
        client().admin().indices().preparePutMapping("index")
            .setType("_doc")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("_doc")
                        .startObject("properties")
                            .startObject("vector")
                                .field("type", "dense_vector")
                                .field("dims", 3)
                                .field("iters", 10)
                                .field("sample_fraction", 0.5)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
            .get();

        float[] vector1 = {1, 3, 5};
        float[] vector2 = {0, 3, 5};
        float[] vector3 = {0, 2, 4};
        float[] vector4 = {1, 3, 5};
        float[] vector5 = {1, 2, 4};

        float[] quantized = {0, 0, 0};

        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(client().prepareIndex("index", "_doc", "1").setSource("vector", vector1, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "2").setSource("vector", vector2, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "3").setSource("vector", vector3, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "4").setSource("vector", vector4, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "5").setSource("vector", vector5, "vector-quantized", quantized))
            .get();

        GetResponse response = client().prepareGet("index", "1").get();
        assertTrue(response.isExists());
    }

    public void testStreamingKMeans() throws Exception {
        Settings indexSettings = Settings.builder()
            .put("number_of_shards", 1)
            .put("number_of_replicas", 0)
            .put("index.codec", "KMeansCodec")
            .build();

        createIndex("index", indexSettings);
        client().admin().indices().preparePutMapping("index")
            .setType("_doc")
            .setSource(XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("_doc")
                        .startObject("properties")
                            .startObject("vector")
                                .field("type", "dense_vector")
                                .field("dims", 3)
                                .field("iters", 2)
                                .field("streaming", true)
                            .endObject()
                        .endObject()
                    .endObject()
                .endObject())
            .get();

        float[] vector1 = {1, 3, 5};
        float[] vector2 = {0, 3, 5};
        float[] vector3 = {0, 2, 4};
        float[] vector4 = {1, 3, 5};
        float[] vector5 = {1, 2, 4};

        float[] quantized = {0, 0, 0};

        client().prepareBulk()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(client().prepareIndex("index", "_doc", "1").setSource("vector", vector1, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "2").setSource("vector", vector2, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "3").setSource("vector", vector3, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "4").setSource("vector", vector4, "vector-quantized", quantized))
            .add(client().prepareIndex("index", "_doc", "5").setSource("vector", vector5, "vector-quantized", quantized))
            .get();

        GetResponse response = client().prepareGet("index", "1").get();
        assertTrue(response.isExists());
    }


}
