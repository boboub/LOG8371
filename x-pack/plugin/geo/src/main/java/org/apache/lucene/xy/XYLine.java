/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.xy;

import java.util.Arrays;

/**
 * Represents a line on the earth's surface.  You can construct the Line directly with {@code double[]}
 * coordinates.
 * <p>
 * NOTES:
 * <ol>
 *   <li>All latitude/longitude values must be in decimal degrees.
 *   <li>For more advanced GeoSpatial indexing and query operations see the {@code spatial-extras} module
 * </ol>
 * @lucene.experimental
 */
public class XYLine {
    /** array of x coordinates */
    private final double[] x;
    /** array of y coordinates */
    private final double[] y;

    /** minimum x of this line's bounding box */
    public final double minX;
    /** maximum x of this line's bounding box */
    public final double maxX;
    /** minimum y of this line's bounding box */
    public final double minY;
    /** maximum y of this line's bounding box */
    public final double maxY;

    /**
     * Creates a new Line from the supplied latitude/longitude array.
     */
    public XYLine(double[] x, double[] y) {
        if (x == null) {
            throw new IllegalArgumentException("x must not be null");
        }
        if (y == null) {
            throw new IllegalArgumentException("lons must not be null");
        }
        if (x.length != y.length) {
            throw new IllegalArgumentException("lats and lons must be equal length");
        }
        if (x.length < 2) {
            throw new IllegalArgumentException("at least 2 line points required");
        }

        // compute bounding box
        double minX = x[0];
        double minY = y[0];
        double maxX = x[0];
        double maxY = y[0];
        for (int i = 0; i < x.length; ++i) {
            minX = Math.min(x[i], minX);
            minY = Math.min(y[i], minY);
            maxX = Math.max(x[i], maxX);
            maxY = Math.max(y[i], maxY);
        }

        this.x = x.clone();
        this.y = y.clone();
        this.minX = minX;
        this.maxX = maxX;
        this.minY = minY;
        this.maxY = maxY;
    }

    /** returns the number of vertex points */
    public int numPoints() {
        return x.length;
    }

    /** Returns x value at given index */
    public double getX(int vertex) {
        return x[vertex];
    }

    /** Returns y value at given index */
    public double getY(int vertex) {
        return y[vertex];
    }

    /** Returns a copy of the internal x array */
    public double[] getX() {
        return x.clone();
    }

    /** Returns a copy of the internal y array */
    public double[] getY() {
        return y.clone();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof XYLine)) return false;
        XYLine line = (XYLine) o;
        return Arrays.equals(x, line.x) && Arrays.equals(y, line.y);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(x);
        result = 31 * result + Arrays.hashCode(y);
        return result;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LINE(");
        for (int i = 0; i < x.length; i++) {
            sb.append("[")
                .append(x[i])
                .append(", ")
                .append(y[i])
                .append("]");
        }
        sb.append(')');
        return sb.toString();
    }

//    /** prints polygons as geojson */
//    public String toGeoJSON() {
//        StringBuilder sb = new StringBuilder();
//        sb.append("[");
//        sb.append(XYPolygon.verticesToGeoJSON(x, y));
//        sb.append("]");
//        return sb.toString();
//    }
}

