/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.geo.parser;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.Explicit;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.geo.parsers.GeoJsonParser;
import org.elasticsearch.common.geo.parsers.GeoWKTParser;
import org.elasticsearch.common.geo.parsers.ShapeParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.geo.mapper.GeometryFieldMapper;

import java.io.IOException;

public interface XYGeometryParser extends ShapeParser {
    static ShapeBuilder parse(XContentParser parser, GeometryFieldMapper fieldMapper) throws IOException {
        if (parser.currentToken() == XContentParser.Token.VALUE_NULL) {
            return null;
        } else if (parser.currentToken() == XContentParser.Token.START_OBJECT) {
            return parseGeoJson(parser, fieldMapper);
        } else if (parser.currentToken() == XContentParser.Token.VALUE_STRING) {
            return parseWKT(parser, fieldMapper);
        }
        throw new ElasticsearchParseException("shape must be an object consisting of type and coordinates");
    }

    static ShapeBuilder parseGeoJson(XContentParser parser, GeometryFieldMapper fieldMapper) throws IOException {
        ShapeBuilder.Orientation orientation = (fieldMapper == null)
            ? GeometryFieldMapper.Defaults.ORIENTATION.value()
            : fieldMapper.orientation();
        Explicit<Boolean> coerce = (fieldMapper == null)
            ? GeometryFieldMapper.Defaults.COERCE
            : fieldMapper.coerce();
        Explicit<Boolean> ignoreZValue = (fieldMapper == null)
            ? GeometryFieldMapper.Defaults.IGNORE_Z_VALUE
            : fieldMapper.ignoreZValue();

        return GeoJsonParser.parse(parser, orientation, coerce.value(), ignoreZValue.value(), false);
    }

    static ShapeBuilder parseWKT(XContentParser parser, GeometryFieldMapper fieldMapper) throws IOException {
        Explicit<Boolean> coerce = (fieldMapper == null)
            ? GeometryFieldMapper.Defaults.COERCE
            : fieldMapper.coerce();
        Explicit<Boolean> ignoreZValue = (fieldMapper == null)
            ? GeometryFieldMapper.Defaults.IGNORE_Z_VALUE
            : fieldMapper.ignoreZValue();
        return GeoWKTParser.parseExpectedType(parser, null, ignoreZValue.value(), coerce.value(), false);
    }
}
