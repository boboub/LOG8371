/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.document;

import org.apache.lucene.xy.XYLine;
import org.apache.lucene.xy.XYTessellator;
import org.apache.lucene.xy.XYTessellator.Triangle;
import org.apache.lucene.xy.XYPolygon;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.BytesRef;

import java.util.ArrayList;
import java.util.List;

public class XYShape {
  static final int BYTES = Integer.BYTES;

  protected static final FieldType TYPE = new FieldType();
  static {
    TYPE.setDimensions(7, 4, BYTES);
    TYPE.freeze();
  }

  private XYShape() {
  }

  public static Field[] createIndexableFields(String fieldName, XYPolygon polygon) {
    List<Triangle> tessellation = XYTessellator.tessellate(polygon);
    List<XYTriangle> fields = new ArrayList<>(tessellation.size());
    for (Triangle t : tessellation) {
      fields.add(new XYTriangle(fieldName, t));
    }
    return fields.toArray(new Field[fields.size()]);
  }

  /** create a query to find all polygons that intersect a defined bounding box
   **/
  public static Query newBoxQuery(String field, QueryRelation queryRelation, double minX, double maxX, double minY, double maxY) {
    return new XYShapeBoundingBoxQuery(field, queryRelation, minX, maxX, minY, maxY);
  }

    /** create a query to find all polygons that intersect a provided linestring (or array of linestrings)
     *  note: does not support dateline crossing
     **/
    public static Query newLineQuery(String field, QueryRelation queryRelation, XYLine... lines) {
        return new XYShapeLineQuery(field, queryRelation, lines);
    }

    /** create a query to find all polygons that intersect a provided polygon (or array of polygons)
     *  note: does not support dateline crossing
     **/
    public static Query newPolygonQuery(String field, QueryRelation queryRelation, XYPolygon... polygons) {
        return new XYShapePolygonQuery(field, queryRelation, polygons);
    }

  private static class XYTriangle extends Field {
    XYTriangle(String name, int ax, int ay, int bx, int by, int cx, int cy) {
      super(name, TYPE);
      setTriangleValue(ax, ay, bx, by, cx, cy);
    }

    XYTriangle(String name, Triangle t) {
      super(name, TYPE);
      setTriangleValue(t.getEncodedX(0), t.getEncodedY(0), t.getEncodedX(1),
          t.getEncodedY(1), t.getEncodedX(2), t.getEncodedY(2));
    }

    public void setTriangleValue(int ax, int ay, int bx, int by, int cx, int cy) {
      final byte[] bytes;

      if (fieldsData == null) {
        bytes = new byte[7 * BYTES];
        fieldsData = new BytesRef(bytes);
      } else {
        bytes = ((BytesRef)fieldsData).bytes;
      }
      LatLonShape.encodeTriangle(bytes, ay, ax, by, bx, cy, cx);
    }
  }

  /** Query Relation Types **/
  public enum QueryRelation {
    INTERSECTS, WITHIN, DISJOINT
  }
}
