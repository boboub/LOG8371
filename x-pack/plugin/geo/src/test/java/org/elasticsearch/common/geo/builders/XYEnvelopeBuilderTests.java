/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.geo.builders;

import org.apache.lucene.geo.XShapeTestUtil;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.geo.XYRectangle;
import org.locationtech.jts.geom.Coordinate;

import java.io.IOException;

public class XYEnvelopeBuilderTests extends AbstractXYShapeBuilderTestCase<EnvelopeBuilder> {

    public void testInvalidConstructorArgs() {
        NullPointerException e;
        e = expectThrows(NullPointerException.class, () -> new EnvelopeBuilder(null, new Coordinate(1.0, -1.0), isGeo()));
        assertEquals("topLeft of envelope cannot be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> new EnvelopeBuilder(new Coordinate(1.0, -1.0), null, isGeo()));
        assertEquals("bottomRight of envelope cannot be null", e.getMessage());
    }

    @Override
    protected EnvelopeBuilder createTestShapeBuilder() {
        return createRandomShape(isGeo());
    }

    @Override
    protected EnvelopeBuilder createMutation(EnvelopeBuilder original) throws IOException {
        return mutate(original, isGeo());
    }

    static EnvelopeBuilder mutate(EnvelopeBuilder original, final boolean isGeo) throws IOException {
        EnvelopeBuilder mutation = (EnvelopeBuilder) copyShape(original);
        // move one corner to the middle of original
        switch (randomIntBetween(0, 3)) {
            case 0:
                mutation = new EnvelopeBuilder(
                    new Coordinate(randomDoubleBetween(XYEncodingUtils.MIN_VAL_INCL, original.bottomRight().x, true), original.topLeft().y),
                    original.bottomRight(), isGeo);
                break;
            case 1:
                mutation = new EnvelopeBuilder(new Coordinate(original.topLeft().x, randomDoubleBetween(original.bottomRight().y, XYEncodingUtils.MAX_VAL_INCL, true)),
                    original.bottomRight(), isGeo);
                break;
            case 2:
                mutation = new EnvelopeBuilder(original.topLeft(),
                    new Coordinate(randomDoubleBetween(original.topLeft().x, XYEncodingUtils.MAX_VAL_INCL, true), original.bottomRight().y), isGeo);
                break;
            case 3:
                mutation = new EnvelopeBuilder(original.topLeft(),
                    new Coordinate(original.bottomRight().x, randomDoubleBetween(XYEncodingUtils.MIN_VAL_INCL, original.topLeft().y, true)), isGeo);
                break;
        }
        return mutation;
    }

    static EnvelopeBuilder createRandomShape(final boolean isGeo) {
        XYRectangle box = XShapeTestUtil.nextBox();
        EnvelopeBuilder envelope = new EnvelopeBuilder(new Coordinate(box.minX, box.maxY),
            new Coordinate(box.maxX, box.minY), isGeo);
        return envelope;
    }
}
