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
package org.elasticsearch.index.query;

import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.geo.ShapeRelation;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.mapper.MappedFieldType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

public abstract class AbstractShapeQueryBuilder<QB extends AbstractShapeQueryBuilder<QB>> extends AbstractQueryBuilder<QB> {

    static final String TYPES_DEPRECATION_MESSAGE = "[types removal] Types are deprecated in [geo_shape] queries. " +
        "The type should no longer be specified in the [indexed_shape] section.";

    public static final String DEFAULT_SHAPE_INDEX_NAME = "shapes";
    public static final String DEFAULT_SHAPE_FIELD_NAME = "shape";
    public static final ShapeRelation DEFAULT_SHAPE_RELATION = ShapeRelation.INTERSECTS;

    protected final List<String> validContentTypes = new ArrayList<>(newValidContentTypes());

    /**
     * The default value for ignore_unmapped.
     */
    public static final boolean DEFAULT_IGNORE_UNMAPPED = false;

    protected static final ParseField QUERY_FIELD = new ParseField("query");
    protected static final ParseField SHAPE_FIELD = new ParseField("shape");
    protected static final ParseField RELATION_FIELD = new ParseField("relation");
    protected static final ParseField INDEXED_SHAPE_FIELD = new ParseField("indexed_shape");
    protected static final ParseField SHAPE_ID_FIELD = new ParseField("id");
    protected static final ParseField SHAPE_TYPE_FIELD = new ParseField("type");
    protected static final ParseField SHAPE_INDEX_FIELD = new ParseField("index");
    protected static final ParseField SHAPE_PATH_FIELD = new ParseField("path");
    protected static final ParseField SHAPE_ROUTING_FIELD = new ParseField("routing");
    protected static final ParseField IGNORE_UNMAPPED_FIELD = new ParseField("ignore_unmapped");

    protected final String fieldName;
    protected final Supplier<ShapeBuilder> supplier;
    protected final String indexedShapeId;
    protected final String indexedShapeType;
    protected ShapeBuilder shape;

    protected String indexedShapeIndex = DEFAULT_SHAPE_INDEX_NAME;
    protected String indexedShapePath = DEFAULT_SHAPE_FIELD_NAME;
    protected String indexedShapeRouting;

    protected ShapeRelation relation = DEFAULT_SHAPE_RELATION;

    protected boolean ignoreUnmapped = DEFAULT_IGNORE_UNMAPPED;

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name using the given Shape
     *
     * @param fieldName
     *            Name of the field that will be queried
     * @param shape
     *            Shape used in the Query
     */
    protected AbstractShapeQueryBuilder(String fieldName, ShapeBuilder shape) {
        this(fieldName, shape, null, null);
    }


    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * @param fieldName
     * field name and will use the Shape found with the given ID
     *
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     */
    protected AbstractShapeQueryBuilder(String fieldName, String indexedShapeId) {
        this(fieldName, (ShapeBuilder) null, indexedShapeId, null);
    }

    /**
     * Creates a new GeoShapeQueryBuilder whose Query will be against the given
     * field name and will use the Shape found with the given ID in the given
     * type
     *
     * @param fieldName
     *            Name of the field that will be filtered
     * @param indexedShapeId
     *            ID of the indexed Shape that will be used in the Query
     * @param indexedShapeType
     *            Index type of the indexed Shapes
     * @deprecated use {@link #AbstractShapeQueryBuilder(String, ShapeBuilder, String, String)} instead
     */
    @Deprecated
    protected AbstractShapeQueryBuilder(String fieldName, String indexedShapeId, String indexedShapeType) {
        this(fieldName, (ShapeBuilder) null, indexedShapeId, indexedShapeType);
    }

    protected AbstractShapeQueryBuilder(String fieldName, ShapeBuilder shape, String indexedShapeId, @Nullable String indexedShapeType) {
        if (fieldName == null) {
            throw new IllegalArgumentException("fieldName is required");
        }
        if (shape == null && indexedShapeId == null) {
            throw new IllegalArgumentException("either shape or indexedShapeId is required");
        }
        this.fieldName = fieldName;
        this.shape = shape;
        this.indexedShapeId = indexedShapeId;
        this.indexedShapeType = indexedShapeType;
        this.supplier = null;
    }

    protected AbstractShapeQueryBuilder(String fieldName, Supplier<ShapeBuilder> supplier, String indexedShapeId,
                                        @Nullable String indexedShapeType) {
        this.fieldName = fieldName;
        this.shape = null;
        this.supplier = supplier;
        this.indexedShapeId = indexedShapeId;
        this.indexedShapeType = indexedShapeType;
    }

    /**
     * Read from a stream.
     */
    protected AbstractShapeQueryBuilder(StreamInput in) throws IOException {
        super(in);
        fieldName = in.readString();
        if (in.readBoolean()) {
            shape = in.readNamedWriteable(ShapeBuilder.class);
            indexedShapeId = null;
            indexedShapeType = null;
        } else {
            shape = null;
            indexedShapeId = in.readOptionalString();
            indexedShapeType = in.readOptionalString();
            indexedShapeIndex = in.readOptionalString();
            indexedShapePath = in.readOptionalString();
            indexedShapeRouting = in.readOptionalString();
        }
        relation = ShapeRelation.readFromStream(in);
        ignoreUnmapped = in.readBoolean();

        supplier = null;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        if (supplier != null) {
            throw new IllegalStateException("supplier must be null, can't serialize suppliers, missing a rewriteAndFetch?");
        }
        out.writeString(fieldName);
        boolean hasShape = shape != null;
        out.writeBoolean(hasShape);
        if (hasShape) {
            out.writeNamedWriteable(shape);
        } else {
            out.writeOptionalString(indexedShapeId);
            out.writeOptionalString(indexedShapeType);
            out.writeOptionalString(indexedShapeIndex);
            out.writeOptionalString(indexedShapePath);
            out.writeOptionalString(indexedShapeRouting);
        }
        relation.writeTo(out);
        out.writeBoolean(ignoreUnmapped);
    }

    /**
     * @return the name of the field that will be queried
     */
    public String fieldName() {
        return fieldName;
    }

    /**
     * Sets the shapeBuilder for the query shape.
     *
     * @param shapeBuilder the shapeBuilder
     * @return this
     */
    public AbstractShapeQueryBuilder<QB> shape(ShapeBuilder shapeBuilder) {
        if (shapeBuilder == null) {
            throw new IllegalArgumentException("No ShapeBuilder defined");
        }
        this.shape = shapeBuilder;
        return this;
    }

    /**
     * @return the shape used in the Query
     */
    public ShapeBuilder shape() {
        return shape;
    }

    /**
     * @return the ID of the indexed Shape that will be used in the Query
     */
    public String indexedShapeId() {
        return indexedShapeId;
    }

    /**
     * @return the document type of the indexed Shape that will be used in the
     *         Query
     *
     * @deprecated Types are in the process of being removed.
     */
    @Deprecated
    public String indexedShapeType() {
        return indexedShapeType;
    }

    /**
     * Sets the name of the index where the indexed Shape can be found
     *
     * @param indexedShapeIndex Name of the index where the indexed Shape is
     * @return this
     */
    public QB indexedShapeIndex(String indexedShapeIndex) {
        this.indexedShapeIndex = indexedShapeIndex;
        return (QB)this;
    }

    /**
     * @return the index name for the indexed Shape that will be used in the
     *         Query
     */
    public String indexedShapeIndex() {
        return indexedShapeIndex;
    }

    /**
     * Sets the path of the field in the indexed Shape document that has the Shape itself
     *
     * @param indexedShapePath Path of the field where the Shape itself is defined
     * @return this
     */
    public QB indexedShapePath(String indexedShapePath) {
        this.indexedShapePath = indexedShapePath;
        return (QB)this;
    }

    /**
     * @return the path of the indexed Shape that will be used in the Query
     */
    public String indexedShapePath() {
        return indexedShapePath;
    }

    /**
     * Sets the optional routing to the indexed Shape that will be used in the query
     *
     * @param indexedShapeRouting indexed shape routing
     * @return this
     */
    public QB indexedShapeRouting(String indexedShapeRouting) {
        this.indexedShapeRouting = indexedShapeRouting;
        return (QB)this;
    }


    /**
     * @return the optional routing to the indexed Shape that will be used in the
     *         Query
     */
    public String indexedShapeRouting() {
        return indexedShapeRouting;
    }

    /**
     * Sets whether the query builder should ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public AbstractShapeQueryBuilder ignoreUnmapped(boolean ignoreUnmapped) {
        this.ignoreUnmapped = ignoreUnmapped;
        return this;
    }

    /**
     * Gets whether the query builder will ignore unmapped fields (and run a
     * {@link MatchNoDocsQuery} in place of this query) or throw an exception if
     * the field is unmapped.
     */
    public boolean ignoreUnmapped() {
        return ignoreUnmapped;
    }

    /**
     * Sets the relation of query shape and indexed shape.
     *
     * @param relation relation of the shapes
     * @return this
     */
    public AbstractShapeQueryBuilder<QB> relation(ShapeRelation relation) {
        if (relation == null) {
            throw new IllegalArgumentException("No Shape Relation defined");
        }
        this.relation = relation;
        return this;
    }

    /**
     * @return the relation of query shape and indexed shape to use in the Query
     */
    public ShapeRelation relation() {
        return relation;
    }

//    protected abstract boolean parseXContentField(XContentParser parser) throws IOException;
    protected abstract List newValidContentTypes();
    protected abstract Query buildShapeQuery(QueryShardContext context, MappedFieldType fieldType, ShapeBuilder shapeToQuery);
    protected abstract String queryType();

    protected boolean isValidContentType(String typeName) {
        return validContentTypes.contains(typeName);
    }

    @Override
    protected Query doToQuery(QueryShardContext context) {
        if (shape == null || supplier != null) {
            throw new UnsupportedOperationException("query must be rewritten first");
        }
        final ShapeBuilder shapeToQuery = shape;
        final MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (fieldType == null) {
            if (ignoreUnmapped) {
                return new MatchNoDocsQuery();
            } else {
                throw new QueryShardException(context, "failed to find " + queryType() + " field [" + fieldName + "]");
            }
        } else if (isValidContentType(fieldType.typeName()) == false) {
            throw new QueryShardException(context,
                "Field [" + fieldName + "] is not of type [" + queryType() + "] but of type [" + fieldType.typeName() + "]");
        }
        return buildShapeQuery(context, fieldType, shapeToQuery);
    }

    /**
     * Fetches the Shape with the given ID in the given type and index.
     *
     * @param getRequest
     *            GetRequest containing index, type and id
     * @param path
     *            Name or path of the field in the Shape Document where the
     *            Shape itself is located
     */
    private void fetch(Client client, GetRequest getRequest, String path, ActionListener<ShapeBuilder> listener) {
        getRequest.preference("_local");
        client.get(getRequest, new ActionListener<GetResponse>() {

            @Override
            public void onResponse(GetResponse response) {
                try {
                    if (!response.isExists()) {
                        throw new IllegalArgumentException("Shape with ID [" + getRequest.id() + "] in type [" + getRequest.type()
                            + "] not found");
                    }
                    if (response.isSourceEmpty()) {
                        throw new IllegalArgumentException("Shape with ID [" + getRequest.id() + "] in type [" + getRequest.type() +
                            "] source disabled");
                    }

                    String[] pathElements = path.split("\\.");
                    int currentPathSlot = 0;

                    // It is safe to use EMPTY here because this never uses namedObject
                    try (XContentParser parser = XContentHelper
                        .createParser(NamedXContentRegistry.EMPTY,
                            LoggingDeprecationHandler.INSTANCE, response.getSourceAsBytesRef())) {
                        XContentParser.Token currentToken;
                        while ((currentToken = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                            if (currentToken == XContentParser.Token.FIELD_NAME) {
                                if (pathElements[currentPathSlot].equals(parser.currentName())) {
                                    parser.nextToken();
                                    if (++currentPathSlot == pathElements.length) {
                                        listener.onResponse(ShapeParser.parse(parser));
                                        return;
                                    }
                                } else {
                                    parser.nextToken();
                                    parser.skipChildren();
                                }
                            }
                        }
                        throw new IllegalStateException("Shape with name [" + getRequest.id() + "] found but missing " + path + " field");
                    }
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(Exception e) {
                listener.onFailure(e);
            }
        });
    }

    protected abstract void doGeoXContent(XContentBuilder builder, Params params) throws IOException;

    @Override
    protected void doXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(queryType());
        builder.startObject(fieldName);

        if (shape != null) {
            builder.field(SHAPE_FIELD.getPreferredName());
            shape.toXContent(builder, params);
        } else {
            builder.startObject(INDEXED_SHAPE_FIELD.getPreferredName())
                .field(SHAPE_ID_FIELD.getPreferredName(), indexedShapeId);
            if (indexedShapeType != null) {
                builder.field(SHAPE_TYPE_FIELD.getPreferredName(), indexedShapeType);
            }
            if (indexedShapeIndex != null) {
                builder.field(SHAPE_INDEX_FIELD.getPreferredName(), indexedShapeIndex);
            }
            if (indexedShapePath != null) {
                builder.field(SHAPE_PATH_FIELD.getPreferredName(), indexedShapePath);
            }
            if (indexedShapeRouting != null) {
                builder.field(SHAPE_ROUTING_FIELD.getPreferredName(), indexedShapeRouting);
            }
            builder.endObject();
        }

        if(relation != null) {
            builder.field(RELATION_FIELD.getPreferredName(), relation.getRelationName());
        }

        doGeoXContent(builder, params);
        builder.endObject();
        builder.field(IGNORE_UNMAPPED_FIELD.getPreferredName(), ignoreUnmapped);
        printBoostAndQueryName(builder);
        builder.endObject();
    }

    public static ParsedQueryBuilder parsedQBFromXContent(XContentParser parser, ParsedQueryBuilder pqb) throws IOException {
        String fieldName = null;
        XContentParser.Token token;
        String currentFieldName = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (fieldName != null) {
                    throw new ParsingException(parser.getTokenLocation(), "point specified twice. [" + currentFieldName + "]");
                }
                fieldName = currentFieldName;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (RELATION_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            pqb.relation = ShapeRelation.getRelationByName(parser.text());
                            if (pqb.relation == null) {
                                throw new ParsingException(parser.getTokenLocation(), "Unknown shape operation [" + parser.text() + " ]");
                            }
                        } else if (pqb.parseXContentField(parser)) {
                            continue;
                        } else if (INDEXED_SHAPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                                if (token == XContentParser.Token.FIELD_NAME) {
                                    currentFieldName = parser.currentName();
                                } else if (token.isValue()) {
                                    if (SHAPE_ID_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        pqb.id = parser.text();
                                    } else if (SHAPE_TYPE_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        pqb.type = parser.text();
                                    } else if (SHAPE_INDEX_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        pqb.index = parser.text();
                                    } else if (SHAPE_PATH_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        pqb.shapePath = parser.text();
                                    } else if (SHAPE_ROUTING_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                        pqb.shapeRouting = parser.text();
                                    }
                                } else {
                                    throw new ParsingException(parser.getTokenLocation(), "unknown token [" + token
                                        + "] after [" + currentFieldName + "]");
                                }
                            }
                        } else {
                            throw new ParsingException(parser.getTokenLocation(), "query does not support [" + currentFieldName + "]");
                        }
                    }
                }
            } else if (token.isValue()) {
                if (AbstractQueryBuilder.BOOST_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    pqb.boost = parser.floatValue();
                } else if (AbstractQueryBuilder.NAME_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    pqb.queryName = parser.text();
                } else if (IGNORE_UNMAPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    pqb.ignoreUnmapped = parser.booleanValue();
                } else {
                    throw new ParsingException(parser.getTokenLocation(), "query does not support [" + currentFieldName + "]");
                }
            }
        }
        pqb.fieldName = fieldName;
        return pqb;
    }

    protected static abstract class ParsedQueryBuilder {
        public String fieldName;
        public ShapeRelation relation;
        public ShapeBuilder shape;

        public String id = null;
        public String type = null;
        public String index = null;
        public String shapePath = null;
        public String shapeRouting = null;

        public float boost;
        public String queryName;
        public boolean ignoreUnmapped;

        protected abstract boolean parseXContentField(XContentParser parser) throws IOException;
    }


    @Override
    protected boolean doEquals(AbstractShapeQueryBuilder other) {
        return Objects.equals(fieldName, other.fieldName)
            && Objects.equals(indexedShapeId, other.indexedShapeId)
            && Objects.equals(indexedShapeIndex, other.indexedShapeIndex)
            && Objects.equals(indexedShapePath, other.indexedShapePath)
            && Objects.equals(indexedShapeType, other.indexedShapeType)
            && Objects.equals(indexedShapeRouting, other.indexedShapeRouting)
            && Objects.equals(relation, other.relation)
            && Objects.equals(shape, other.shape)
            && Objects.equals(supplier, other.supplier)
            && Objects.equals(ignoreUnmapped, other.ignoreUnmapped);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(fieldName, indexedShapeId, indexedShapeIndex,
            indexedShapePath, indexedShapeType, indexedShapeRouting, relation, shape, ignoreUnmapped, supplier);
    }

    protected abstract AbstractShapeQueryBuilder newSpatialQueryBuilder(String fieldName, ShapeBuilder shape);
    protected abstract AbstractShapeQueryBuilder newSpatialQueryBuilder(String fieldName, Supplier<ShapeBuilder> shapeSupplier, String indexedShapeId, String indexedShapeType);

    @Override
    protected AbstractShapeQueryBuilder<QB> doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        if (supplier != null) {
            return supplier.get() == null ? this : newSpatialQueryBuilder(this.fieldName, supplier.get()).relation(relation);//.strategy(strategy);
        } else if (this.shape == null) {
            SetOnce<ShapeBuilder> supplier = new SetOnce<>();
            queryRewriteContext.registerAsyncAction((client, listener) -> {
                GetRequest getRequest;
                if (indexedShapeType == null) {
                    getRequest = new GetRequest(indexedShapeIndex, indexedShapeId);
                } else {
                    getRequest = new GetRequest(indexedShapeIndex, indexedShapeType, indexedShapeId);
                }
                getRequest.routing(indexedShapeRouting);
                fetch(client, getRequest, indexedShapePath, ActionListener.wrap(builder -> {
                    supplier.set(builder);
                    listener.onResponse(null);
                }, listener::onFailure));
            });
            return newSpatialQueryBuilder(this.fieldName, supplier::get, this.indexedShapeId, this.indexedShapeType).relation(relation);//.strategy(strategy);
        }
        return this;
    }
}
