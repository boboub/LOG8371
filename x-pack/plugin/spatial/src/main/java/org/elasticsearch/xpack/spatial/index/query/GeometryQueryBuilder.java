/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.query;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.geo.GeoShapeType;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.logging.DeprecationLogger;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.AbstractShapeQueryBuilder;
import org.elasticsearch.index.query.GeoShapeQueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.spatial.index.mapper.GeometryFieldMapper;
import org.elasticsearch.xpack.spatial.parser.XYGeometryParser;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.xpack.spatial.index.mapper.GeometryFieldMapper.toLucenePolygon;

public class GeometryQueryBuilder extends AbstractShapeQueryBuilder<GeometryQueryBuilder> {
    public static final String NAME = "geometry";

    private static final DeprecationLogger deprecationLogger = new DeprecationLogger(
        LogManager.getLogger(GeoShapeQueryBuilder.class));

    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Types are deprecated in [geo_shape] queries. " +
        "The type should no longer be specified in the [indexed_shape] section.";


    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     */
    protected GeometryQueryBuilder(String fieldName, ShapeBuilder shape) {
        super(fieldName, shape);
    }

    protected GeometryQueryBuilder(String fieldName, Supplier<ShapeBuilder> shapeSupplier, String indexedShapeId, @Nullable String indexedShapeType) {
        super(fieldName, shapeSupplier, indexedShapeId, indexedShapeType);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name and will use the Shape found with the given ID
     *
     * @param fieldName
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     */
    public GeometryQueryBuilder(String fieldName, String indexedShapeId) {
        super(fieldName, indexedShapeId);
    }

    @Deprecated
    protected GeometryQueryBuilder(String fieldName, String indexedShapeId, String indexedShapeType) {
        super(fieldName, (ShapeBuilder) null, indexedShapeId, indexedShapeType);
    }

    public GeometryQueryBuilder(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        super.doWriteTo(out);
    }

    @Override
    protected GeometryQueryBuilder newSpatialQueryBuilder(String fieldName, ShapeBuilder shape) {
        return new GeometryQueryBuilder(fieldName, shape);
    }

    @Override
    protected GeometryQueryBuilder newSpatialQueryBuilder(String fieldName, Supplier<ShapeBuilder> shapeSupplier, String indexedShapeId, String indexedShapeType) {
        return new GeometryQueryBuilder(fieldName, shapeSupplier, indexedShapeId, indexedShapeType);
    }

    @Override
    public String queryType() {
        return NAME;
    }

    @Override
    protected List newValidContentTypes() {
        return Arrays.asList(GeometryFieldMapper.CONTENT_TYPE);
    }

    @Override
    public Query buildShapeQuery(QueryShardContext context, MappedFieldType fieldType, ShapeBuilder shapeToQuery) {
        // CONTAINS queries are not yet supported by VECTOR strategy
        if (relation == ShapeRelation.CONTAINS) {
            throw new QueryShardException(context,
                ShapeRelation.CONTAINS + " query relation not supported for Field [" + fieldName + "]");
        }

        // wrap geometry Query as a ConstantScoreQuery
        return new ConstantScoreQuery(shapeToQuery.buildGeometry().visit(new ShapeVisitor(context)));
    }

    @Override
    public void doGeoXContent(XContentBuilder builder, Params params) throws IOException {
        // noop
    }

    @Override
    protected GeometryQueryBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return (GeometryQueryBuilder)super.doRewrite(queryRewriteContext);
    }

    @Override
    protected boolean doEquals(GeometryQueryBuilder other) {
        return super.doEquals((AbstractShapeQueryBuilder)other);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(super.doHashCode());
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    private class ShapeVisitor implements GeometryVisitor<Query, RuntimeException> {
        QueryShardContext context;
        MappedFieldType fieldType;

        ShapeVisitor(QueryShardContext context) {
            this.context = context;
            this.fieldType = context.fieldMapper(fieldName);
        }

        @Override
        public Query visit(Circle circle) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unknown shape Circle");
        }

        @Override
        public Query visit(GeometryCollection<?> collection) {
            BooleanQuery.Builder bqb = new BooleanQuery.Builder();
            visit(bqb, collection);
            return bqb.build();
        }

        private void visit(BooleanQuery.Builder bqb, GeometryCollection<?> collection) {
            for (Geometry shape : collection) {
                if (shape instanceof MultiPoint) {
                    // Flatten multipoints
                    visit(bqb, (GeometryCollection<?>) shape);
                } else {
                    bqb.add(shape.visit(this), BooleanClause.Occur.SHOULD);
                }
            }
        }

        @Override
        public Query visit(org.elasticsearch.geo.geometry.Line line) {
            return XYShape.newLineQuery(fieldName(), relation.getLuceneRelation(),
                new XYLine(doubleArrayToFloatArray(line.getLons()), doubleArrayToFloatArray(line.getLats())));
        }

        @Override
        public Query visit(LinearRing ring) {
            throw new QueryShardException(context, "Field [" + fieldName + "] found and unsupported shape LinearRing");
        }

        @Override
        public Query visit(MultiLine multiLine) {
            XYLine[] lines = new XYLine[multiLine.size()];
            for (int i=0; i<multiLine.size(); i++) {
                lines[i] = new XYLine(doubleArrayToFloatArray(multiLine.get(i).getLons()), doubleArrayToFloatArray(multiLine.get(i).getLats()));
            }
            return XYShape.newLineQuery(fieldName(), relation.getLuceneRelation(), lines);
        }

        @Override
        public Query visit(MultiPoint multiPoint) {
            throw new QueryShardException(context, "Field [" + fieldName + "] does not support " + GeoShapeType.MULTIPOINT +
                " queries");
        }

        @Override
        public Query visit(MultiPolygon multiPolygon) {
            XYPolygon[] polygons = new XYPolygon[multiPolygon.size()];
            for (int i=0; i<multiPolygon.size(); i++) {
                polygons[i] = toLucenePolygon(multiPolygon.get(i));
            }
            return visitMultiPolygon(polygons);
        }

        private Query visitMultiPolygon(XYPolygon... polygons) {
            return XYShape.newPolygonQuery(fieldName(), relation.getLuceneRelation(), polygons);
        }

        @Override
        public Query visit(Point point) {
            return XYShape.newBoxQuery(fieldName, relation.getLuceneRelation(),
                (float)point.getLon(), (float)point.getLon(), (float)point.getLat(), (float)point.getLat());
        }

        @Override
        public Query visit(org.elasticsearch.geo.geometry.Polygon polygon) {
            return XYShape.newPolygonQuery(fieldName(), relation.getLuceneRelation(), toLucenePolygon(polygon));
        }

        @Override
        public Query visit(org.elasticsearch.geo.geometry.Rectangle r) {
            return XYShape.newBoxQuery(fieldName(), relation.getLuceneRelation(),
                (float)r.getMinLon(), (float)r.getMaxLon(), (float)r.getMinLat(), (float)r.getMaxLat());
        }
    }

    private static class ParsedGeometryQueryBuilder extends ParsedQueryBuilder {
        @Override
        protected boolean parseXContentField(XContentParser parser) throws IOException {
            if (SHAPE_FIELD.match(parser.currentName(), parser.getDeprecationHandler())) {
                this.shape = XYGeometryParser.parse(parser);
                return true;
            }
            return false;
        }
    }

    public static GeometryQueryBuilder fromXContent(XContentParser parser) throws IOException {
        ParsedQueryBuilder pgsqb = AbstractShapeQueryBuilder.parsedQBFromXContent(parser, new ParsedGeometryQueryBuilder());

        GeometryQueryBuilder builder;
        if (pgsqb.type != null) {
            deprecationLogger.deprecatedAndMaybeLog(
                "geo_share_query_with_types", TYPES_DEPRECATION_MESSAGE);
        }

        if (pgsqb.shape != null) {
            builder = new GeometryQueryBuilder(pgsqb.fieldName, pgsqb.shape);
        } else {
            builder = new GeometryQueryBuilder(pgsqb.fieldName, pgsqb.id, pgsqb.type);
        }
        if (pgsqb.index != null) {
            builder.indexedShapeIndex(pgsqb.index);
        }
        if (pgsqb.shapePath != null) {
            builder.indexedShapePath(pgsqb.shapePath);
        }
        if (pgsqb.shapeRouting != null) {
            builder.indexedShapeRouting(pgsqb.shapeRouting);
        }
        if (pgsqb.relation != null) {
            builder.relation(pgsqb.relation);
        }
        if (pgsqb.queryName != null) {
            builder.queryName(pgsqb.queryName);
        }
        builder.boost(pgsqb.boost);
        builder.ignoreUnmapped(pgsqb.ignoreUnmapped);
        return builder;
    }

    private static float[] doubleArrayToFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = (float) array[i];
        }
        return result;
    }
}
