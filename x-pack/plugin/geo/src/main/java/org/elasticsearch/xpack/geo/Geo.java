/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.geo;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.xpack.core.action.XPackInfoFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.geo.index.query.GeometryQueryBuilder;
import org.elasticsearch.xpack.geo.mapper.GeometryFieldMapper;
import org.elasticsearch.index.mapper.Mapper;
import org.elasticsearch.plugins.MapperPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;

public class Geo extends Plugin implements MapperPlugin, ActionPlugin, SearchPlugin {
    protected final boolean enabled;

    public Geo(Settings settings) {
        this.enabled = XPackSettings.GEO_ENABLED.get(settings);
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Arrays.asList(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.GEO, GeoUsageTransportAction.class),
            new ActionPlugin.ActionHandler<>(XPackInfoFeatureAction.GEO, GeoInfoTransportAction.class));
    }

    @Override
    public Map<String, Mapper.TypeParser> getMappers() {
        if (enabled == false) {
            return emptyMap();
        }
        Map<String, Mapper.TypeParser> mappers = new LinkedHashMap<>();
        mappers.put(GeometryFieldMapper.CONTENT_TYPE, new GeometryFieldMapper.TypeParser());
        return Collections.unmodifiableMap(mappers);
    }

    @Override
    public List<QuerySpec<?>> getQueries() {
        return singletonList(new QuerySpec<>(GeometryQueryBuilder.NAME, GeometryQueryBuilder::new, GeometryQueryBuilder::fromXContent));
    }
}
