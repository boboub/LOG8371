/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.document;

import java.util.Arrays;

import org.apache.lucene.document.XYShape.QueryRelation;
import org.apache.lucene.geo.XYLine2D;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.util.NumericUtils;
import org.apache.lucene.xy.XYLine;

import static org.apache.lucene.xy.XYEncodingUtils.decode;

/**
 * Finds all previously indexed shapes that intersect the specified arbitrary {@code Line}.
 * <p>
 * Note:
 * <ul>
 *    <li>{@code QueryRelation.WITHIN} queries are not yet supported</li>
 *    <li>Dateline crossing is not yet supported</li>
 * </ul>
 * <p>
 * todo:
 * <ul>
 *   <li>Add distance support for buffered queries</li>
 * </ul>
 * <p>The field must be indexed using
 * {@link org.apache.lucene.document.LatLonShape#createIndexableFields} added per document.
 *
 *  @lucene.experimental
 **/
final class XYShapeLineQuery extends XYShapeQuery {
    final XYLine[] lines;
    final private XYLine2D line2D;

    public XYShapeLineQuery(String field, QueryRelation queryRelation, XYLine... lines) {
        super(field, queryRelation);
        /** line queries do not support within relations, only intersects and disjoint */
        if (queryRelation == QueryRelation.WITHIN) {
            throw new IllegalArgumentException("XYShapeLineQuery does not support " + QueryRelation.WITHIN + " queries");
        }

        if (lines == null) {
            throw new IllegalArgumentException("lines must not be null");
        }
        if (lines.length == 0) {
            throw new IllegalArgumentException("lines must not be empty");
        }
        for (int i = 0; i < lines.length; ++i) {
            if (lines[i] == null) {
                throw new IllegalArgumentException("line[" + i + "] must not be null");
            }
        }
        this.lines = lines.clone();
        this.line2D = XYLine2D.create(lines);
    }

    @Override
    protected Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                              int maxXOffset, int maxYOffset, byte[] maxTriangle) {
        double minY = decode(NumericUtils.sortableBytesToInt(minTriangle, minYOffset));
        double minX = decode(NumericUtils.sortableBytesToInt(minTriangle, minXOffset));
        double maxY = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxYOffset));
        double maxX = decode(NumericUtils.sortableBytesToInt(maxTriangle, maxXOffset));

        // check internal node against query
        return line2D.relate(minY, maxY, minX, maxX);
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

        if (queryRelation == XYShape.QueryRelation.WITHIN) {
            return line2D.relateTriangle(ax, ay, bx, by, cx, cy) == Relation.CELL_INSIDE_QUERY;
        }
        // INTERSECTS
        return line2D.relateTriangle(ax, ay, bx, by, cx, cy) != Relation.CELL_OUTSIDE_QUERY;
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
        //sb.append("Line(" + lines[0].toGeoJSON() + ")");
        return sb.toString();
    }

    @Override
    protected boolean equalsTo(Object o) {
        return super.equalsTo(o) && Arrays.equals(lines, ((XYShapeLineQuery)o).lines);
    }

    @Override
    public int hashCode() {
        int hash = super.hashCode();
        hash = 31 * hash + Arrays.hashCode(lines);
        return hash;
    }
}

