/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.xy;

public class XYRectangle {
  /** minimum x value */
  public final double minX;
  /** minimum y value */
  public final double maxX;
  /** maximum x value */
  public final double minY;
  /** maximum y value */
  public final double maxY;

  /**
   * Constructs a bounding box by first validating the provided latitude and longitude coordinates
   */
  public XYRectangle(double minX, double maxX, double minY, double maxY) {
    this.minX = minX;
    this.maxX = maxX;
    this.minY = minY;
    this.maxY = maxY;
    assert minX <= maxX;
    assert minY <= maxY;

    // NOTE: cannot assert maxLon >= minLon since this rect could cross the dateline
  }
}
