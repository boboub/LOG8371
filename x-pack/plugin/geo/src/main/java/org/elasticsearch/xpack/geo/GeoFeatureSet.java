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
package org.elasticsearch.xpack.geo;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.protocol.xpack.XPackUsageRequest;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xpack.core.XPackFeatureSet;
import org.elasticsearch.xpack.core.XPackField;
import org.elasticsearch.xpack.core.XPackSettings;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureAction;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureResponse;
import org.elasticsearch.xpack.core.action.XPackUsageFeatureTransportAction;
import org.elasticsearch.xpack.core.geo.GeoFeatureSetUsage;

public class GeoFeatureSet implements XPackFeatureSet {
    private final boolean enabled;
    private final XPackLicenseState licenseState;

    @Inject
    public GeoFeatureSet(Settings settings, XPackLicenseState licenseState) {
        this.enabled = XPackSettings.GEO_ENABLED.get(settings);
        this.licenseState = licenseState;
    }

    @Override
    public String name() {
        return XPackField.GEO;
    }

    @Override
    public boolean available() {
        return licenseState != null && licenseState.isGeoAllowed();
    }

    @Override
    public boolean enabled() {
        return enabled;
    }

    public static class UsageTransportAction extends XPackUsageFeatureTransportAction {

        private final Settings settings;
        private final XPackLicenseState licenseState;

        @Inject
        public UsageTransportAction(TransportService transportService, ClusterService clusterService, ThreadPool threadPool,
                                    ActionFilters actionFilters, IndexNameExpressionResolver indexNameExpressionResolver,
                                    Settings settings, XPackLicenseState licenseState) {
            super(XPackUsageFeatureAction.GEO.name(), transportService, clusterService,
                threadPool, actionFilters, indexNameExpressionResolver);
            this.settings = settings;
            this.licenseState = licenseState;
        }

        @Override
        protected void masterOperation(XPackUsageRequest request, ClusterState state, ActionListener<XPackUsageFeatureResponse> listener) {
            GeoFeatureSetUsage usage =
                new GeoFeatureSetUsage(licenseState.isGeoAllowed(), XPackSettings.GEO_ENABLED.get(settings));
            listener.onResponse(new XPackUsageFeatureResponse(usage));
        }
    }
}
