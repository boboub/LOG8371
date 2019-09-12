/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.vectors.codec;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.IntArray;
import org.elasticsearch.xpack.vectors.mapper.VectorEncoderDecoder;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Random;

public class KMeansDocValuesWriter extends DocValuesConsumer {
    private final SegmentWriteState state;
    private final DocValuesConsumer delegate;

    private final Random random;
    private final BigArrays bigArrays;

    public KMeansDocValuesWriter(SegmentWriteState state,
                                 DocValuesConsumer delegate) {
        this.state = state;
        this.delegate = delegate;

        this.random = new Random(42L);
        this.bigArrays = BigArrays.NON_RECYCLING_INSTANCE;
    }

    @Override
    public void addBinaryField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        int numIters = Integer.parseInt(field.attributes().get("iters"));
        boolean streaming = Boolean.parseBoolean(field.attributes().get("streaming"));
        float sampleFraction = Float.parseFloat(field.attributes().get("sample_fraction"));

        int maxDoc = state.segmentInfo.maxDoc();
        int numCentroids = (int) Math.sqrt(maxDoc);

        if (field.name.equals("vector") == false) {
            delegate.addBinaryField(field, valuesProducer);
            return;
        }

        BinaryDocValues values = valuesProducer.getBinary(field);
        float[][] centroids = new float[numCentroids][];

        int numDocs = 0;
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            BytesRef bytes = values.binaryValue();

            if (numDocs < numCentroids) {
                centroids[numDocs] = decodeVector(bytes);
            } else if (random.nextDouble() < numCentroids * (1.0 / numDocs)) {
                int c = random.nextInt(numCentroids);
                centroids[c] = decodeVector(bytes);
            }
            numDocs++;
        }

        if (streaming == false) {
            runKMeans(field, valuesProducer, numDocs, centroids, numIters, sampleFraction);
        } else {
            runStreamingKMeans(field, valuesProducer, numDocs, centroids, numIters, sampleFraction);
        }

        System.out.println("Writing original vectors...");
        delegate.addBinaryField(field, valuesProducer);
        System.out.println("Finished writing original vectors.");
    }

    private void runKMeans(FieldInfo field,
                           DocValuesProducer valuesProducer,
                           int numDocs,
                           float[][] centroids,
                           int numIters,
                           float sampleFraction) throws IOException {
        System.out.println("Running k-means with [" + centroids.length + "] centroids on [" + numDocs + "] docs...");
        IntArray documentCentroids = bigArrays.newIntArray(numDocs);

        for (int iter = 0; iter < numIters; iter++) {
            float fraction = (iter < numIters - 1) && numDocs > 100000 ? sampleFraction : 1.0f;
            centroids = runKMeansStep(iter, fraction, field, valuesProducer, centroids, documentCentroids);
        }
        System.out.println("Finished k-means.");
    }

    /**
     * Runs one iteration of k-means. For each document vector, we first find the
     * nearest centroid, then update the location of the new centroid.
     */
    private float[][] runKMeansStep(int iter,
                                    float sampleFraction,
                                    FieldInfo field,
                                    DocValuesProducer valuesProducer,
                                    float[][] centroids,
                                    IntArray documentCentroids) throws IOException {
        double distToCentroid = 0.0;
        double distToOtherCentroids = 0.0;
        int numDocs = 0;

        float[][] newCentroids = new float[centroids.length][centroids[0].length];
        int[] newCentroidSize = new int[centroids.length];

        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (random.nextFloat() > sampleFraction) {
                continue;
            }

            BytesRef bytes = values.binaryValue();
            float[] vector = decodeVector(bytes);

            int bestCentroid = -1;
            double bestDist = Double.MAX_VALUE;
            for (int c = 0; c < centroids.length; c++) {
                double dist = l2norm(centroids[c], vector);
                distToOtherCentroids += dist;

                if (dist < bestDist) {
                    bestCentroid = c;
                    bestDist = dist;
                }
            }

            newCentroidSize[bestCentroid]++;
            for (int v = 0; v < vector.length; v++) {
                newCentroids[bestCentroid][v] += vector[v];
            }

            distToCentroid += bestDist;
            distToOtherCentroids -= bestDist;
            numDocs++;

            documentCentroids.set(values.docID(), bestCentroid);
        }

        for (int c = 0; c < newCentroids.length; c++) {
            for (int v = 0; v < newCentroids[c].length; v++) {
                newCentroids[c][v] /= newCentroidSize[c];
            }
        }

        distToCentroid /= numDocs;
        distToOtherCentroids /= numDocs * (centroids.length - 1);

        System.out.println("Finished iteration [" + iter + "]. Dist to centroid [" + distToCentroid +
            "], dist to other centroids [" + distToOtherCentroids + "].");
        return newCentroids;
    }


    private void runStreamingKMeans(FieldInfo field,
                                    DocValuesProducer valuesProducer,
                                    int numDocs,
                                    float[][] centroids,
                                    int numIters,
                                    float sampleFraction) throws IOException {
        System.out.println("Running streaming k-means with [" + centroids.length + "] centroids on [" + numDocs + "] docs...");
        IntArray documentCentroids = bigArrays.newIntArray(numDocs);
        for (int iter = 0; iter < numIters; iter++) {
            float fraction = (iter < numIters - 1) && numDocs > 100000 ? sampleFraction : 1.0f;
            runStreamingKMeansStep(iter, fraction, field, valuesProducer, centroids, documentCentroids);
        }

        double distToCentroid = 0.0;
        double distToOtherCentroids = 0.0;

        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            int centroid = documentCentroids.get(values.docID());

            BytesRef bytes = values.binaryValue();
            float[] vector = decodeVector(bytes);

            for (int c = 0; c < centroids.length; c++) {
                double dist = l2norm(centroids[c], vector);

                if (c == centroid) {
                    distToCentroid += dist;
                } else {
                    distToOtherCentroids += dist;
                }
            }
        }

        distToCentroid /= numDocs;
        distToOtherCentroids /= numDocs * (centroids.length - 1);

        System.out.println("Finished streaming k-means. Dist to centroid [" + distToCentroid +
            "], dist to other centroids [" + distToOtherCentroids + "].");
    }

    private void runStreamingKMeansStep(int iter,
                                        float sampleFraction,
                                        FieldInfo field,
                                        DocValuesProducer valuesProducer,
                                        float[][] centroids,
                                        IntArray documentCentroids) throws IOException {

        int[] centroidSize = new int[centroids.length];

        BinaryDocValues values = valuesProducer.getBinary(field);
        while (values.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (random.nextFloat() > sampleFraction) {
                continue;
            }

            BytesRef bytes = values.binaryValue();
            float[] vector = decodeVector(bytes);

            int bestCentroid = -1;
            double bestDist = Double.MAX_VALUE;
            for (int c = 0; c < centroids.length; c++) {
                double dist = l2norm(centroids[c], vector);

                if (dist < bestDist) {
                    bestCentroid = c;
                    bestDist = dist;
                }
            }

            centroidSize[bestCentroid]++;
            for (int v = 0; v < vector.length; v++) {
                int size = centroidSize[bestCentroid];
                float diff = vector[v] - centroids[bestCentroid][v];
                centroids[bestCentroid][v] += diff / size;
            }
            
            documentCentroids.set(values.docID(), bestCentroid);
        }

        System.out.println("Finished streaming k-means iteration [" + iter + "].");
    }

    private float[] decodeVector(BytesRef bytes) {
        int vectorLength = VectorEncoderDecoder.denseVectorLength(Version.V_7_5_0, bytes);
        float[] vector = new float[vectorLength];

        ByteBuffer byteBuffer = ByteBuffer.wrap(bytes.bytes, bytes.offset, bytes.length);
        for (int v = 0; v < vectorLength; v++) {
            vector[v] = byteBuffer.getFloat();
        }
        return vector;
    }
    
    private double l2norm(float[] first, float[] second) {
        double l2norm = 0;
        for (int v = 0; v < first.length; v++) {
            double diff = first[v] - second[v];
            l2norm += diff * diff;
        }
        return Math.sqrt(l2norm);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void addNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedField(field, valuesProducer);
    }

    @Override
    public void addSortedNumericField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedNumericField(field, valuesProducer);
    }

    @Override
    public void addSortedSetField(FieldInfo field, DocValuesProducer valuesProducer) throws IOException {
        delegate.addSortedSetField(field, valuesProducer);
    }
}
