/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;

import java.io.IOException;

public class KMeansDocValuesFormat extends DocValuesFormat {
    private final DocValuesFormat delegate;

    protected KMeansDocValuesFormat(String name,
                                    DocValuesFormat delegate) {
        super(name);
        this.delegate = delegate;
    }

    @Override
    public DocValuesConsumer fieldsConsumer(SegmentWriteState state) throws IOException {
        DocValuesConsumer consumer = delegate.fieldsConsumer(state);
        return new KMeansDocValuesWriter(state, consumer);
    }

    @Override
    public DocValuesProducer fieldsProducer(SegmentReadState state) throws IOException {
        DocValuesProducer producer = delegate.fieldsProducer(state);
        return new KMeansDocValuesReader(state, producer);
    }
}
