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
package org.apache.lucene.geo;

import org.apache.lucene.index.PointValues;
import org.apache.lucene.xy.XYPolygon;

import static org.apache.lucene.geo.GeoUtils.orient;

public class XYPolygon2D extends EdgeTree {
    // each component/hole is a node in an augmented 2d kd-tree: we alternate splitting between latitude/longitude,
    // and pull up max values for both dimensions to each parent node (regardless of split).
    /** tree of holes, or null */
    private final XYPolygon2D holes;

    private XYPolygon2D(XYPolygon polygon, XYPolygon2D holes) {
        super(polygon.minY, polygon.maxY, polygon.minX, polygon.maxX, polygon.getPolyY(), polygon.getPolyX());
        this.holes = holes;
    }

    /**
     * Returns true if the point is contained within this polygon.
     * <p>
     * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
     * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
     */
    public boolean contains(double x, double y) {
        if (x <= maxX && y <= maxY) {
            if (componentContains(x, y)) {
                return true;
            }
            if (left != null) {
                if (((XYPolygon2D)left).contains(x, y)) {
                    return true;
                }
            }
            if (right != null && ((splitX == false && y >= minLat) || (splitX && x >= minLon))) {
                if (((XYPolygon2D)right).contains(x, y)) {
                    return true;
                }
            }
        }
        return false;
    }

    /** Returns true if the point is contained within this polygon component. */
    private boolean componentContains(double x, double y) {
        // check bounding box
        if (x < minLon || x > maxLon || y < minLat || y > maxLat) {
            return false;
        }
        if (contains(tree, x, y)) {
            if (holes != null && holes.contains(x, y)) {
                return false;
            }
            return true;
        }
        return false;
    }

    @Override
    protected PointValues.Relation componentRelate(double minLat, double maxLat, double minLon, double maxLon) {
        // check any holes
        if (holes != null) {
            PointValues.Relation holeRelation = holes.relate(minLat, maxLat, minLon, maxLon);
            if (holeRelation == PointValues.Relation.CELL_CROSSES_QUERY) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            } else if (holeRelation == PointValues.Relation.CELL_INSIDE_QUERY) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
        }
        // check each corner: if < 4 && > 0 are present, its cheaper than crossesSlowly
        int numCorners = numberOfCorners(minLat, maxLat, minLon, maxLon);
        if (numCorners == 4) {
            if (tree.crosses(minLat, maxLat, minLon, maxLon)) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_INSIDE_QUERY;
        }  else if (numCorners == 0) {
            if (minLat >= tree.lat1 && maxLat <= tree.lat1 && minLon >= tree.lon2 && maxLon <= tree.lon2) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            if (tree.crosses(minLat, maxLat, minLon, maxLon)) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
        return PointValues.Relation.CELL_CROSSES_QUERY;
    }

    @Override
    protected PointValues.Relation componentRelateTriangle(double ax, double ay, double bx, double by, double cx, double cy) {
        // check any holes
        if (holes != null) {
            PointValues.Relation holeRelation = holes.relateTriangle(ax, ay, bx, by, cx, cy);
            if (holeRelation == PointValues.Relation.CELL_CROSSES_QUERY) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            } else if (holeRelation == PointValues.Relation.CELL_INSIDE_QUERY) {
                return PointValues.Relation.CELL_OUTSIDE_QUERY;
            }
        }
        // check each corner: if < 3 && > 0 are present, its cheaper than crossesSlowly
        int numCorners = numberOfTriangleCorners(ax, ay, bx, by, cx, cy);
        if (numCorners == 3) {
            if (tree.relateTriangle(ax, ay, bx, by, cx, cy) == PointValues.Relation.CELL_CROSSES_QUERY) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_INSIDE_QUERY;
        } else if (numCorners == 0) {
            if (pointInTriangle(tree.lon1, tree.lat1, ax, ay, bx, by, cx, cy) == true) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            if (tree.relateTriangle(ax, ay, bx, by, cx, cy) == PointValues.Relation.CELL_CROSSES_QUERY) {
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
            return PointValues.Relation.CELL_OUTSIDE_QUERY;
        }
        return PointValues.Relation.CELL_CROSSES_QUERY;
    }

    private int numberOfTriangleCorners(double ax, double ay, double bx, double by, double cx, double cy) {
        int containsCount = 0;
        if (componentContains(ax, ay)) {
            containsCount++;
        }
        if (componentContains(bx, by)) {
            containsCount++;
        }
        if (containsCount == 1) {
            return containsCount;
        }
        if (componentContains(cx, cy)) {
            containsCount++;
        }
        return containsCount;
    }

    // returns 0, 4, or something in between
    private int numberOfCorners(double minX, double maxX, double minY, double maxY) {
        int containsCount = 0;
        if (componentContains(minX, minY)) {
            containsCount++;
        }
        if (componentContains(minX, maxY)) {
            containsCount++;
        }
        if (containsCount == 1) {
            return containsCount;
        }
        if (componentContains(maxX, maxY)) {
            containsCount++;
        }
        if (containsCount == 2) {
            return containsCount;
        }
        if (componentContains(maxX, minY)) {
            containsCount++;
        }
        return containsCount;
    }

    /** Builds a Polygon2D from multipolygon */
    public static XYPolygon2D create(XYPolygon... polygons) {
        XYPolygon2D components[] = new XYPolygon2D[polygons.length];
        for (int i = 0; i < components.length; i++) {
            XYPolygon gon = polygons[i];
            XYPolygon gonHoles[] = gon.getHoles();
            XYPolygon2D holes = null;
            if (gonHoles.length > 0) {
                holes = create(gonHoles);
            }
            components[i] = new XYPolygon2D(gon, holes);
        }
        return (XYPolygon2D)createTree(components, 0, components.length - 1, false);
    }

    /**
     * Returns true if the point crosses this edge subtree an odd number of times
     * <p>
     * See <a href="https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html">
     * https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html</a> for more information.
     */
    // ported to java from https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html
    // original code under the BSD license (https://www.ecse.rpi.edu/~wrf/Research/Short_Notes/pnpoly.html#License%20to%20Use)
    //
    // Copyright (c) 1970-2003, Wm. Randolph Franklin
    //
    // Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated
    // documentation files (the "Software"), to deal in the Software without restriction, including without limitation
    // the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and
    // to permit persons to whom the Software is furnished to do so, subject to the following conditions:
    //
    // 1. Redistributions of source code must retain the above copyright
    //    notice, this list of conditions and the following disclaimers.
    // 2. Redistributions in binary form must reproduce the above copyright
    //    notice in the documentation and/or other materials provided with
    //    the distribution.
    // 3. The name of W. Randolph Franklin may not be used to endorse or
    //    promote products derived from this Software without specific
    //    prior written permission.
    //
    // THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED
    // TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL
    // THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF
    // CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
    // IN THE SOFTWARE.
    private static boolean contains(EdgeTree.Edge tree, double x, double y) {
        // crossings algorithm is an odd-even algorithm, so we descend the tree xor'ing results along our path
        boolean res = false;
        if (y <= tree.max) {
            if (tree.lat1 > y != tree.lat2 > y) {
                if (x < (tree.lon1 - tree.lon2) * (y - tree.lat2) / (tree.lat1 - tree.lat2) + tree.lon2) {
                    res = true;
                }
            }
            if (tree.left != null) {
                res ^= contains(tree.left, x, y);
            }
            if (tree.right != null && y >= tree.low) {
                res ^= contains(tree.right, x, y);
            }
        }
        return res;
    }

    /**
     * Compute whether the given x, y point is in a triangle; uses the winding order method */
    protected static boolean pointInTriangle (double x, double y, double ax, double ay, double bx, double by, double cx, double cy) {
        double minX = StrictMath.min(ax, StrictMath.min(bx, cx));
        double minY = StrictMath.min(ay, StrictMath.min(by, cy));
        double maxX = StrictMath.max(ax, StrictMath.max(bx, cx));
        double maxY = StrictMath.max(ay, StrictMath.max(by, cy));
        //check the bounding box because if the triangle is degenerated, e.g points and lines, we need to filter out
        //coplanar points that are not part of the triangle.
        if (x >= minX && x <= maxX && y >= minY && y <= maxY ) {
            int a = orient(x, y, ax, ay, bx, by);
            int b = orient(x, y, bx, by, cx, cy);
            if (a == 0 || b == 0 || a < 0 == b < 0) {
                int c = orient(x, y, cx, cy, ax, ay);
                return c == 0 || (c < 0 == (b < 0 || a < 0));
            }
            return false;
        } else {
            return false;
        }
    }
}
