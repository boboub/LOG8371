/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.geo.mapper;

import org.apache.lucene.document.Field;
import org.apache.lucene.document.XYShape;
import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.index.IndexableField;
import org.apache.lucene.search.Query;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder.Orientation;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.support.XContentMapValues;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.GeometryVisitor;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.index.mapper.BaseGeoShapeFieldMapper;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.index.mapper.MapperParsingException;
import org.elasticsearch.index.mapper.ParseContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.index.query.QueryShardException;
import org.elasticsearch.xpack.geo.parser.XYGeometryParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.index.mapper.GeoPointFieldMapper.Names.IGNORE_MALFORMED;
import static org.elasticsearch.index.mapper.GeoPointFieldMapper.Names.IGNORE_Z_VALUE;
import static org.elasticsearch.index.mapper.TypeParsers.parseField;

public class GeometryFieldMapper extends BaseGeoShapeFieldMapper {
    public static final String CONTENT_TYPE = "geometry";

    public static class Defaults extends BaseGeoShapeFieldMapper.Defaults {
        public static final GeometryFieldType FIELD_TYPE = new GeometryFieldType();
        public static final Explicit<Boolean> IGNORE_Z_VALUE = new Explicit<>(true, false);
    }

    public static class Builder extends BaseGeoShapeFieldMapper.Builder<BaseGeoShapeFieldMapper.Builder, GeometryFieldMapper> {

        public Builder(String name) {
            super(name, Defaults.FIELD_TYPE, Defaults.FIELD_TYPE);
            builder = this;
        }

        @Override
        public GeometryFieldMapper build(BuilderContext context) {
            setupFieldType(context);
            return new GeometryFieldMapper(name, fieldType, defaultFieldType, ignoreMalformed(context), coerce(context),
                ignoreZValue(), context.indexSettings(), multiFieldsBuilder.build(this, context), copyTo);
        }
    }

    public static class TypeParser implements Mapper.TypeParser {
        @Override
        public Mapper.Builder parse(String name, Map<String, Object> node, ParserContext parserContext) throws MapperParsingException {
            final Builder builder = new GeometryFieldMapper.Builder(name);
            parseField(builder, name, node, parserContext);

            Boolean coerce = null;
            Boolean ignoreZ = null;
            Boolean ignoreMalformed = null;
            Orientation orientation = null;
            for (Iterator<Map.Entry<String, Object>> iterator = node.entrySet().iterator(); iterator.hasNext();) {
                Map.Entry<String, Object> entry = iterator.next();
                String fieldName = entry.getKey();
                Object fieldNode = entry.getValue();
                if (Names.ORIENTATION.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    orientation = ShapeBuilder.Orientation.fromString(fieldNode.toString());
                    iterator.remove();
                } else if (IGNORE_MALFORMED.equals(fieldName)) {
                    ignoreMalformed = XContentMapValues.nodeBooleanValue(fieldNode, name + ".ignore_malformed");
                    iterator.remove();
                } else if (Names.COERCE.match(fieldName, LoggingDeprecationHandler.INSTANCE)) {
                    coerce = XContentMapValues.nodeBooleanValue(fieldNode, name + "." + Names.COERCE.getPreferredName());
                    iterator.remove();
                } else if (GeoPointFieldMapper.Names.IGNORE_Z_VALUE.getPreferredName().equals(fieldName)) {
                    ignoreZ = XContentMapValues.nodeBooleanValue(fieldNode,
                        name + "." + IGNORE_Z_VALUE.getPreferredName());
                    iterator.remove();
                }
            }

            if (coerce != null) {
                builder.coerce(coerce);
            }

            if (ignoreZ != null) {
                builder.ignoreZValue(ignoreZ);
            }

            if (ignoreMalformed != null) {
                builder.ignoreMalformed(ignoreMalformed);
            }

            if (orientation != null) {
                builder.orientation(orientation);
            }

            return builder;
        }
    }

    public GeometryFieldMapper(String simpleName, MappedFieldType fieldType, MappedFieldType defaultFieldType,
                               Explicit<Boolean> ignoreMalformed, Explicit<Boolean> coerce,
                               Explicit<Boolean> ignoreZValue, Settings indexSettings,
                               MultiFields multiFields, CopyTo copyTo) {
        super(simpleName, fieldType, defaultFieldType, ignoreMalformed, coerce, ignoreZValue, indexSettings,
            multiFields, copyTo);
    }

    @Override
    protected String contentType() {
        return CONTENT_TYPE;
    }

    @Override
    protected void parseCreateField(ParseContext context, List<IndexableField> fields) throws IOException {
        throw new UnsupportedOperationException("Parsing is implemented in parse(), this method should NEVER be called");
    }

    @Override
    public void parse(ParseContext context) throws IOException {
        try {
            Object shape = context.parseExternalValue(Object.class);
            if (shape == null) {
                ShapeBuilder shapeBuilder = XYGeometryParser.parse(context.parser(), this);
                if (shapeBuilder == null) {
                    return;
                }
                shape = shapeBuilder.buildGeometry();
            }
            if (shape instanceof Geometry == false) {
                throw new IllegalArgumentException("invalid geometry type found [" + shape.getClass() + "] while indexing geometry");
            }
            ((Geometry) shape).visit(new GeometryIndexer(context));

        } catch (Exception e) {
            if (ignoreMalformed.value() == false) {
                throw new MapperParsingException("failed to parse field [{}] of type [{}]", e, fieldType().name(),
                    fieldType().typeName());
            }
            context.addIgnoredField(fieldType().name());
        }
    }

    public static final class GeometryFieldType extends BaseGeoShapeFieldType {
        public GeometryFieldType() {
            super();
        }

        public GeometryFieldType(GeometryFieldType ref) {
            super(ref);
        }

        @Override
        public GeometryFieldType clone() {
            return new GeometryFieldType(this);
        }

        @Override
        public String typeName() {
            return CONTENT_TYPE;
        }

        @Override
        public Query termQuery(Object value, QueryShardContext context) {
            throw new QueryShardException(context, "Geometry fields do not support exact searching, use dedicated geometry queries instead");
        }
    }

    private class GeometryIndexer implements GeometryVisitor<Void, RuntimeException> {
        private ParseContext context;

        private GeometryIndexer(ParseContext context) {
            this.context = context;
        }

        @Override
        public Void visit(Circle circle) {
            throw new IllegalArgumentException("invalid shape type found [Circle] while indexing shape");
        }

        @Override
        public Void visit(GeometryCollection<?> collection) {
            for (Geometry geometry : collection) {
                geometry.visit(this);
            }
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Line line) {
            float[] x = new float[line.length()];
            float[] y = new float[x.length];
            for (int i = 0; i < x.length; ++i) {
                x[i] = (float)line.getLon(i);
                y[i] = (float)line.getLat(i);
            }
            indexFields(context, XYShape.createIndexableFields(name(), new XYLine(x, y)));
            return null;
        }

        @Override
        public Void visit(LinearRing ring) {
            throw new IllegalArgumentException("invalid shape type found [LinearRing] while indexing shape");
        }

        @Override
        public Void visit(MultiLine multiLine) {
            for (org.elasticsearch.geo.geometry.Line line : multiLine) {
                visit(line);
            }
            return null;
        }

        @Override
        public Void visit(MultiPoint multiPoint) {
            for(Point point : multiPoint) {
                visit(point);
            }
            return null;
        }

        @Override
        public Void visit(MultiPolygon multiPolygon) {
            for(org.elasticsearch.geo.geometry.Polygon polygon : multiPolygon) {
                visit(polygon);
            }
            return null;
        }

        @Override
        public Void visit(Point point) {
            indexFields(context, XYShape.createIndexableFields(name(), point.getLon(), point.getLat()));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Polygon polygon) {
            indexFields(context, XYShape.createIndexableFields(name(), toLucenePolygon(polygon)));
            return null;
        }

        @Override
        public Void visit(org.elasticsearch.geo.geometry.Rectangle r) {
            XYPolygon p = new XYPolygon(new float[]{(float)r.getMinLon(), (float)r.getMaxLon(), (float)r.getMaxLon(), (float)r.getMinLon(), (float)r.getMinLon()},
                new float[]{(float)r.getMinLat(), (float)r.getMinLat(), (float)r.getMaxLat(), (float)r.getMaxLat(), (float)r.getMinLat()});
            indexFields(context, XYShape.createIndexableFields(name(), p));
            return null;
        }

        public XYPolygon toLucenePolygon(org.elasticsearch.geo.geometry.Polygon polygon) {
            XYPolygon[] holes = new XYPolygon[polygon.getNumberOfHoles()];
            LinearRing ring;
            for(int i = 0; i<holes.length; i++) {
                ring = polygon.getHole(i);
                float[] x = new float[ring.length()];
                float[] y = new float[x.length];
                for (int j = 0; j < x.length; ++j) {
                    x[j] = (float)ring.getLon(j);
                    y[j] = (float)ring.getLat(j);
                }
                holes[i] = new XYPolygon(x, y);
            }
            ring = polygon.getPolygon();
            float[] x = new float[ring.length()];
            float[] y = new float[x.length];
            for (int i = 0; i < x.length; ++i) {
                x[i] = (float)ring.getLon(i);
                y[i] = (float)ring.getLat(i);
            }
            return new XYPolygon(x, y, holes);
        }

        private void indexFields(ParseContext context, Field[] fields) {
            ArrayList<IndexableField> flist = new ArrayList<>(Arrays.asList(fields));
            createFieldNamesField(context, flist);
            for (IndexableField f : flist) {
                context.doc().add(f);
            }
        }
    }
}
