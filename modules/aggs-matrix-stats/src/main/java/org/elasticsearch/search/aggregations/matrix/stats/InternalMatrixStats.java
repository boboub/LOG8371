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
package org.elasticsearch.search.aggregations.matrix.stats;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * Adds correlation computation to Computes distribution statistics over multiple fields
 */
public class InternalMatrixStats extends BaseInternalMatrixStats {

    /** per shard ctor */
    InternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults, MatrixStatsResults results,
                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, count, multiFieldStatsResults, results, pipelineAggregators, metaData);
    }

    /**
     * Read from a stream.
     */
    public InternalMatrixStats(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return MatrixStatsAggregationBuilder.NAME;
    }

    @Override
    public InternalMatrixStats newInternalMatrixStats(String name, long count, RunningStats multiFieldStatsResults,
            MatrixStatsResults results, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        return new InternalMatrixStats(name, count, multiFieldStatsResults, results, pipelineAggregators, metaData);
    }

    @Override
    public MatrixStatsResults newMatrixStatsResults(RunningStats runningStats) {
        return new MatrixStatsResults(runningStats);
    }

    @Override
    public MatrixStatsResults newMatrixStatsResults() {
        return new MatrixStatsResults();
    }

    public static class Fields extends BaseInternalMatrixStats.Fields {
        public static final String MEAN = "mean";
        public static final String VARIANCE = "variance";
        public static final String SKEWNESS = "skewness";
        public static final String KURTOSIS = "kurtosis";
        public static final String COVARIANCE = "covariance";
        public static final String CORRELATION = "correlation";
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), getDocCount());
        if (results != null && results.getFieldCounts().keySet().isEmpty() == false) {
            builder.startArray(Fields.FIELDS);
            for (String fieldName : results.getFieldCounts().keySet()) {
                builder.startObject();
                // name
                builder.field(Fields.NAME, fieldName);
                // count
                builder.field(Fields.COUNT, results.getFieldCount(fieldName));
                // mean
                builder.field(Fields.MEAN, results.getMean(fieldName));
                // variance
                builder.field(Fields.VARIANCE, results.getVariance(fieldName));
                // skewness
                builder.field(Fields.SKEWNESS, results.getSkewness(fieldName));
                // kurtosis
                builder.field(Fields.KURTOSIS, results.getKurtosis(fieldName));
                // covariance
                builder.startObject(Fields.COVARIANCE);
                for (String fieldB : results.getFieldCounts().keySet()) {
                    builder.field(fieldB, results.getCovariance(fieldName, fieldB));
                }
                builder.endObject();
                // correlation
                builder.startObject(Fields.CORRELATION);
                for (String fieldB : results.getFieldCounts().keySet()) {
                    builder.field(fieldB, results.getCorrelation(fieldName, fieldB));
                }
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
        }
        return builder;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.size() == 1) {
            String element = path.get(0);
            if (results == null) {
                return emptyMap();
            }
            switch (element) {
                case "counts":
                    return results.getFieldCounts();
                case "means":
                    return results.getMeans();
                case "variances":
                    return results.getVariances();
                case "skewness":
                    return results.getSkewness();
                case "kurtosis":
                    return results.getKurtosis();
                case "covariance":
                    return results.getCovariances();
                case "correlation":
                    return results.getCorrelations();
                default:
                    throw new IllegalArgumentException("Found unknown path element [" + element + "] in [" + getName() + "]");
            }
        } else {
            throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
        }
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        // merge stats across all shards
        List<InternalAggregation> aggs = new ArrayList<>(aggregations);
        aggs.removeIf(p -> ((InternalMatrixStats)p).stats == null);

        // return empty result iff all stats are null
        if (aggs.isEmpty()) {
            return new InternalMatrixStats(name, 0, null, new MatrixStatsResults(), pipelineAggregators(), getMetaData());
        }

        RunningStats runningStats = new RunningStats();
        for (InternalAggregation agg : aggs) {
            runningStats.merge(((InternalMatrixStats) agg).stats);
        }

        if (reduceContext.isFinalReduce()) {
            MatrixStatsResults results = new MatrixStatsResults(runningStats);
            return new InternalMatrixStats(name, results.getDocCount(), runningStats, results, pipelineAggregators(), getMetaData());
        }
        return new InternalMatrixStats(name, runningStats.docCount, runningStats, null, pipelineAggregators(), getMetaData());
    }
}
