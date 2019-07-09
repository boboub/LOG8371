/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.common.geo.builders;

public abstract class AbstractXYShapeBuilderTestCase<SB extends ShapeBuilder<?,?,?>>  extends AbstractShapeBuilderTestCase<SB> {
    @Override
    protected boolean isGeo() {
        return false;
    }
}
