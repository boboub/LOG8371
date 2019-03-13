/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.apache.lucene.xy;

import org.apache.lucene.util.NumericUtils;

public class XYEncodingUtils {

  public static final double MIN_VAL_INCL = -Float.MAX_VALUE;
  public static final double MAX_VAL_INCL = Float.MAX_VALUE;

  // No instance:
  private XYEncodingUtils() {
  }

  public static void checkVal(double x) {
    if (Double.isNaN(x) || x < MIN_VAL_INCL || x > MAX_VAL_INCL) {
      throw new IllegalArgumentException("invalid x value " + x + "; must be between " + MIN_VAL_INCL + " and " + MAX_VAL_INCL);
    }
  }

  public static int encode(double x) {
    checkVal(x);
    return NumericUtils.floatToSortableInt((float)x);
  }

  public static double decode(int encoded) {
    double result = NumericUtils.sortableIntToFloat(encoded);
    assert result >=  MIN_VAL_INCL && result <= MAX_VAL_INCL;
    return result;
  }

  public static double decode(byte[] src, int offset) {
    return decode(NumericUtils.sortableBytesToInt(src, offset));
  }
}
