/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.geo;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.xy.XYLine;

/**
 * 2D line implementation represented as a balanced interval tree of edges.
 * <p>
 * Line {@code Line2D} Construction takes {@code O(n log n)} time for sorting and tree construction.
 * {@link #relate relate()} are {@code O(n)}, but for most practical lines are much faster than brute force.
 * @lucene.internal
 */
public final class XYLine2D extends EdgeTree {

    private XYLine2D(XYLine line) {
        super(line.minY, line.maxY, line.minX, line.maxX, line.getY(), line.getX());
    }

    /** create a Line2D edge tree from provided array of Linestrings */
    public static XYLine2D create(XYLine... lines) {
        XYLine2D components[] = new XYLine2D[lines.length];
        for (int i = 0; i < components.length; ++i) {
            components[i] = new XYLine2D(lines[i]);
        }
        return (XYLine2D)createTree(components, 0, components.length - 1, false);
    }

    @Override
    protected PointValues.Relation componentRelate(double minLat, double maxLat, double minLon, double maxLon) {
        if (tree.crosses(minLat, maxLat, minLon, maxLon)) {
            return PointValues.Relation.CELL_CROSSES_QUERY;
        }
        return PointValues.Relation.CELL_OUTSIDE_QUERY;
    }

    @Override
    protected PointValues.Relation componentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
        return tree.relateTriangle(ax, ay, bx, by, cx, cy);
    }
}
