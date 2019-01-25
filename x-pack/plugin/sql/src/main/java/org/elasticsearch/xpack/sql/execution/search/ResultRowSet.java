/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.sql.execution.search;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.xpack.sql.session.AbstractRowSet;
import org.elasticsearch.xpack.sql.util.Check;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

abstract class ResultRowSet<E extends NamedWriteable> extends AbstractRowSet {

    private final List<E> extractors;
    private final int[] columns;

    ResultRowSet(List<E> extractors, int[] columns) {
        this.extractors = extractors;
        this.columns = columns;
        Check.isTrue(columns.length <= extractors.size(), "Invalid number of extracted columns specified");
    }

    @Override
    public final int columnCount() {
        return columns.length;
    }

    @Override
    protected Object getColumn(int column) {
        return extractValue(userExtractor(column));
    }

    List<E> extractors() {
        return extractors;
    }

    int[] columns() {
        return columns;
    }

    E userExtractor(int column) {
        int index = columns[column];
        return extractors.get(index);
    }

    Object resultColumn(int column) {
        return extractValue(extractors().get(column));
    }

    int resultColumnCount() {
        return extractors.size();
    }

    void forEachResultColumn(Consumer<? super Object> action) {
        Objects.requireNonNull(action);
        int rowSize = resultColumnCount();
        for (int i = 0; i < rowSize; i++) {
            action.accept(resultColumn(i));
        }
    }
    

    protected abstract Object extractValue(E e);
}
