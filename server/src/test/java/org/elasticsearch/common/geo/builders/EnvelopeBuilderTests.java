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

package org.elasticsearch.common.geo.builders;

import org.locationtech.jts.geom.Coordinate;

import org.elasticsearch.test.geo.RandomShapeGenerator;
import org.locationtech.spatial4j.shape.Rectangle;

import java.io.IOException;

public class EnvelopeBuilderTests extends AbstractShapeBuilderTestCase<EnvelopeBuilder> {
    protected static boolean isGeo = true;

    public void testInvalidConstructorArgs() {
        NullPointerException e;
        e = expectThrows(NullPointerException.class, () -> new EnvelopeBuilder(null, new Coordinate(1.0, -1.0), isGeo));
        assertEquals("topLeft of envelope cannot be null", e.getMessage());
        e = expectThrows(NullPointerException.class, () -> new EnvelopeBuilder(new Coordinate(1.0, -1.0), null, isGeo));
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
                    new Coordinate(randomDoubleBetween(-180.0, original.bottomRight().x, true), original.topLeft().y),
                    original.bottomRight(), isGeo);
            break;
        case 1:
            mutation = new EnvelopeBuilder(new Coordinate(original.topLeft().x, randomDoubleBetween(original.bottomRight().y, 90.0, true)),
                    original.bottomRight(), isGeo);
            break;
        case 2:
            mutation = new EnvelopeBuilder(original.topLeft(),
                    new Coordinate(randomDoubleBetween(original.topLeft().x, 180.0, true), original.bottomRight().y), isGeo);
            break;
        case 3:
            mutation = new EnvelopeBuilder(original.topLeft(),
                    new Coordinate(original.bottomRight().x, randomDoubleBetween(-90.0, original.topLeft().y, true)), isGeo);
            break;
        }
        return mutation;
    }

    static EnvelopeBuilder createRandomShape(final boolean isGeo) {
        Rectangle box = RandomShapeGenerator.xRandomRectangle(random(), RandomShapeGenerator.xRandomPoint(random()));
        EnvelopeBuilder envelope = new EnvelopeBuilder(new Coordinate(box.getMinX(), box.getMaxY()),
                new Coordinate(box.getMaxX(), box.getMinY()), isGeo);
        return envelope;
    }
}
