/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.document;

import org.apache.lucene.xy.XYRectangle;
import org.apache.lucene.xy.XYRectangle2D;
import org.apache.lucene.index.PointValues;

public class XYShapeBoundingBoxQuery extends XYShapeQuery {
  final XYRectangle2D rectangle2D;

  public XYShapeBoundingBoxQuery(String field, XYShape.QueryRelation queryRelation, double minX, double maxX, double minY, double maxY) {
    super(field, queryRelation);
    XYRectangle rectangle = new XYRectangle(minX, maxX, minY, maxY);
    this.rectangle2D = XYRectangle2D.create(rectangle);
  }

  @Override
  protected PointValues.Relation relateRangeBBoxToQuery(int minXOffset, int minYOffset, byte[] minTriangle,
                                                        int maxXOffset, int maxYOffset, byte[] maxTriangle) {
    return rectangle2D.relateRangeBBox(minXOffset, minYOffset, minTriangle, maxXOffset, maxYOffset, maxTriangle);
  }

  /** returns true if the query matches the encoded triangle */
  @Override
  protected boolean queryMatches(byte[] t, int[] scratchTriangle, XYShape.QueryRelation queryRelation) {
    // decode indexed triangle
    LatLonShape.decodeTriangle(t, scratchTriangle);

    int aY = scratchTriangle[0];
    int aX = scratchTriangle[1];
    int bY = scratchTriangle[2];
    int bX = scratchTriangle[3];
    int cY = scratchTriangle[4];
    int cX = scratchTriangle[5];

    if (queryRelation == XYShape.QueryRelation.WITHIN) {
      return rectangle2D.containsTriangle(aX, aY, bX, bY, cX, cY);
    }
    return rectangle2D.intersectsTriangle(aX, aY, bX, bY, cX, cY);
  }

  @Override
  public boolean equals(Object o) {
    return sameClassAs(o) && equalsTo(getClass().cast(o));
  }

  @Override
  protected boolean equalsTo(Object o) {
    return super.equalsTo(o) && rectangle2D.equals(((XYShapeBoundingBoxQuery)o).rectangle2D);
  }

  @Override
  public int hashCode() {
    int hash = super.hashCode();
    hash = 31 * hash + rectangle2D.hashCode();
    return hash;
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
    sb.append(rectangle2D.toString());
    return sb.toString();
  }
}
