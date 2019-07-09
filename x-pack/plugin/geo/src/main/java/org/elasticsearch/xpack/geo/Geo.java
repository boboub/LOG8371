/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.geo;

import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.common.inject.Module;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.ActionPlugin;
import org.elasticsearch.xpack.core.XPackPlugin;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.xpack.core.XPackSettings;

import java.util.Collection;
import java.util.Collections;
import java.util.List;

public class Geo extends Plugin implements ActionPlugin {
    protected final boolean enabled;

    public Geo(Settings settings) {
        this.enabled = XPackSettings.GEO_ENABLED.get(settings);
    }

    public Collection<Module> createGuiceModules() {
        return Collections.singletonList(b -> {
            XPackPlugin.bindFeatureSet(b, GeoFeatureSet.class);
        });
    }

    @Override
    public List<ActionHandler<? extends ActionRequest, ? extends ActionResponse>> getActions() {
        return Collections.singletonList(
            new ActionPlugin.ActionHandler<>(XPackUsageFeatureAction.GEO, GeoFeatureSet.UsageTransportAction.class));
    }
}
