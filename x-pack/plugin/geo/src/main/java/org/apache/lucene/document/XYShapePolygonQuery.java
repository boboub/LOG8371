/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.document;

import org.apache.lucene.geo.XYPolygon2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.xy.XYPolygon;
import org.apache.lucene.document.XYShape.QueryRelation;

import java.util.Arrays;

import static org.apache.lucene.xy.XYEncodingUtils.decode;

/**
 * Finds all previously indexed xy shapes that intersect the specified arbitrary polygon.
 *
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.XYShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class XYShapePolygonQuery extends XYShapeQuery {
    final XYPolygon[] polygons;
    final private XYPolygon2D poly2D;

    /**
     * Creates a query that matches all indexed shapes to the provided polygons
     */
    public XYShapePolygonQuery(String field, QueryRelation queryRelation, XYPolygon... polygons) {
        super(field, queryRelation);
        if (polygons == null) {
            throw new IllegalArgumentException("polygons must not be null");
        }
        if (polygons.length == 0) {
            throw new IllegalArgumentException("polygons must not be empty");
        }
        for (int i = 0; i < polygons.length; i++) {
            if (polygons[i] == null) {
                throw new IllegalArgumentException("polygon[" + i + "] must not be null");
            }
        }
        this.polygons = polygons.clone();
        this.poly2D = XYPolygon2D.create(polygons);
    }

    @Override
    protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                              int maxXOffset, int maxYOffset, byte[] maxTriangle) {

        double minY = decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
        double minX = decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
        double maxY = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
        double maxX = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

        // check internal node against query
        return poly2D.relate(minY, maxY, minX, maxX);
    }

    @Override
    protected boolean queryMatches(byte[] t, int[] scratchTriangle, QueryRelation queryRelation) {
        LatLonShape.decodeTriangle(t, scratchTriangle);

        double ay = decode(scratchTriangle[0]);
        double ax = decode(scratchTriangle[1]);
        double by = decode(scratchTriangle[2]);
        double bx = decode(scratchTriangle[3]);
        double cy = decode(scratchTriangle[4]);
        double cx = decode(scratchTriangle[5]);

        if (queryRelation == QueryRelation.WITHIN) {
            return poly2D.relateTriangle(ax, ay, bx, by, cx, cy) == Relation.CELL_INSIDE_QUERY;
        }
        // INTERSECTS
        return poly2D.relateTriangle(ax, ay, bx, by, cx, cy) != Relation.CELL_OUTSIDE_QUERY;
    }

    @Override
    public String toString(String field) {
        final StringBuilder sb = new StringBuilder();
        sb.append(getClass().getSimpleName());
        sb.append(':');
        if (this.field.equals(field) == false) {
            sb.append(" field=");
            sb.append(this.field);
            sb.append(':');
        }
        sb.append("Polygon(" + polygons[0].toGeoJSON() + ")");
        return sb.toString();
    }

    @Override
    protected boolean equalsTo(Object o) {
        return super.equalsTo(o) && Arrays.equals(polygons, ((XYShapePolygonQuery)o).polygons);
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = 31 * hash + Arrays.hashCode(polygons);
        return hash;
    }
}
