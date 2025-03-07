/*
 * Copyright (c) 2021-2024 SigScalr, Inc.
 *
 * This file is part of SigLens Observability Solution
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

const QUERIES = {
    CPU_USAGE: `sum by (cluster) (
        max by (cluster, instance, cpu, core) (1 - rate(node_cpu_seconds_total{cluster=~".+", mode="idle"}[5m]))
    ) 
    / on (cluster) 
    sum by (cluster) (
        max by (cluster, node) (kube_node_status_capacity{cluster=~".+", resource="cpu"})
    )`,

    MEMORY_USAGE: `1 - (
        sum by (cluster) (
            max by (cluster, node) (
                label_replace(
                    windows_memory_available_bytes{cluster=~".+"}
                    OR
                    node_memory_MemAvailable_bytes{cluster=~".+"},
                    "node", "$1", "instance", "(.+)"
                )
            )
        ) 
        / on (cluster) 
        sum by (cluster) (
            max by (cluster, node) (
                kube_node_status_capacity{cluster=~".+", resource="memory"}
            )
        )
    )`,
};

//eslint-disable-next-line no-unused-vars
const MetricsUtils = {
    navigateToMetricsExplorer(query, dropdown) {
        const urlParams = new URLSearchParams(window.location.search);
        const startTime = urlParams.get('startEpoch') || 'now-1h';
        const endTime = urlParams.get('endEpoch') || 'now';

        const metricsQueryParamsData = {
            start: startTime,
            end: endTime,
            queries: [
                {
                    name: 'a',
                    query: query,
                    qlType: 'promql',
                    state: 'raw',
                },
            ],
            formulas: [],
        };

        const transformedMetricsQueryParams = JSON.stringify(metricsQueryParamsData);
        const encodedMetricsQueryParams = encodeURIComponent(transformedMetricsQueryParams);

        const newUrl = `metrics-explorer.html?queryString=${encodedMetricsQueryParams}`;
        window.open(newUrl, '_blank');

        if (dropdown) {
            $(dropdown).removeClass('active');
        }
    },
};

let dashboardComponents = {
    clusters: null,
    nodes: null,
    namespaces: null,
    workloads: null,
    pods: null,
    containers: null,
    containerImages: null,
    cpuUsage: null,
    memoryUsage: null,
};

function initializeUrlParameters() {
    const urlParams = new URLSearchParams(window.location.search);
    const currentUrl = new URL(window.location.href);
    let paramsChanged = false;

    const defaultParams = {
        startEpoch: 'now-1h',
        endEpoch: 'now',
        cluster: 'all',
        namespace: 'all',
    };

    Object.entries(defaultParams).forEach(([key, value]) => {
        if (!urlParams.has(key) || !urlParams.get(key)) {
            currentUrl.searchParams.set(key, value);
            paramsChanged = true;
        }
    });

    if (paramsChanged) {
        window.history.replaceState({}, '', currentUrl);
    }

    return {
        startTime: urlParams.get('startEpoch') || defaultParams.startEpoch,
        endTime: urlParams.get('endEpoch') || defaultParams.endEpoch,
        cluster: urlParams.get('cluster') || defaultParams.cluster,
        namespace: urlParams.get('namespace') || defaultParams.namespace,
    };
}

function updateUrlParams(startTime, endTime) {
    const currentUrl = new URL(window.location.href);
    currentUrl.searchParams.set('startEpoch', startTime);
    currentUrl.searchParams.set('endEpoch', endTime);
    window.history.pushState({}, '', currentUrl);
}

function updateUrlWithFilters(cluster, namespace) {
    const currentUrl = new URL(window.location.href);
    const params = currentUrl.searchParams;

    params.set('cluster', cluster.includes('All') ? 'all' : cluster.join('|'));
    params.set('namespace', namespace.includes('All') ? 'all' : namespace.join('|'));

    // Preserve existing time parameters
    const startTime = params.get('startEpoch') || 'now-1h';
    const endTime = params.get('endEpoch') || 'now';
    params.set('startEpoch', startTime);
    params.set('endEpoch', endTime);

    window.history.pushState({}, '', currentUrl);
}

function getFilterValuesFromUrl() {
    const params = new URLSearchParams(window.location.search);
    return {
        cluster: params.get('cluster') === 'all' ? ['All'] : (params.get('cluster') || 'all').split('|'),
        namespace: params.get('namespace') === 'all' ? ['All'] : (params.get('namespace') || 'all').split('|'),
    };
}

async function refreshDashboard() {
    const startTime = filterStartDate;
    const endTime = filterEndDate;

    updateUrlParams(startTime, endTime);

    try {
        // Fetch updated filter data
        const [clusterItems, namespaceItems] = await Promise.all([fetchFilterData('cluster', startTime, endTime), fetchFilterData('namespace', startTime, endTime)]);

        // Update filter dropdowns
        if (window.dashboardFilters) {
            const currentClusterValues = window.dashboardFilters.cluster.getSelectedValues();
            const currentNamespaceValues = window.dashboardFilters.namespace.getSelectedValues();

            window.dashboardFilters.cluster.options.items = clusterItems;
            window.dashboardFilters.namespace.options.items = namespaceItems;

            const validClusterValues = currentClusterValues.filter((value) => value === 'All' || clusterItems.includes(value));
            const validNamespaceValues = currentNamespaceValues.filter((value) => value === 'All' || namespaceItems.includes(value));

            // Default to 'All' if no valid selections remain
            const newClusterValues = validClusterValues.length > 0 ? validClusterValues : ['All'];
            const newNamespaceValues = validNamespaceValues.length > 0 ? validNamespaceValues : ['All'];

            window.dashboardFilters.cluster.init();
            window.dashboardFilters.namespace.init();

            window.dashboardFilters.cluster.setSelectedValues(newClusterValues);
            window.dashboardFilters.namespace.setSelectedValues(newNamespaceValues);

            updateUrlWithFilters(newClusterValues, newNamespaceValues);
        }

        Object.values(dashboardComponents).forEach((component) => {
            if (component && typeof component.fetchData === 'function') {
                component.fetchData();
            }
        });
    } catch (error) {
        console.error('Error refreshing dashboard:', error);
    }
}

async function fetchFilterData(type, startTime, endTime) {
    const query = type === 'cluster' ? 'kube_namespace_status_phase{cluster=~".+"}' : 'kube_namespace_status_phase{cluster=~".+", namespace=~".+"}';

    const requestData = {
        start: startTime,
        end: endTime,
        queries: [
            {
                name: 'a',
                query: query,
                qlType: 'promql',
                state: 'raw',
            },
        ],
        formulas: [
            {
                formula: 'a',
            },
        ],
    };

    try {
        const response = await $.ajax({
            method: 'post',
            url: 'metrics-explorer/api/v1/timeseries',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(requestData),
        });
        const values = new Set();
        if (response && response.series) {
            response.series.forEach((series) => {
                const match = type === 'cluster' ? series.match(/cluster:([^,}]+)/) : series.match(/namespace:([^,}]+)/);
                if (match && match[1]) {
                    values.add(match[1]);
                }
            });
        }
        return Array.from(values);
    } catch (error) {
        console.error('Error fetching filter data:', error);
    }
}

function setupRefreshHandlers() {
    // Listen for changes to the time range
    $('.range-item').on('click', function () {
        refreshDashboard();
    });

    // Handle custom range apply button
    $('#customrange-btn').on('click', function () {
        refreshDashboard();
    });

    // Handle manual refresh button
    $('.refresh-btn').on('click', function () {
        refreshDashboard();
    });

    // Handle auto-refresh interval selection
    let autoRefreshInterval;
    $('.refresh-range-item').on('click', function () {
        const refreshInterval = $(this).attr('id');

        if (autoRefreshInterval) {
            clearInterval(autoRefreshInterval);
        }

        if (refreshInterval !== '0') {
            const intervalMap = {
                '5m': 300000,
                '30m': 1800000,
                '1h': 3600000,
                '5h': 18000000,
                '1d': 86400000,
            };

            autoRefreshInterval = setInterval(() => {
                refreshDashboard();
            }, intervalMap[refreshInterval]);
        }

        $('.refresh-range-item').removeClass('active');
        $(this).addClass('active');
    });
}

$(document).ready(async () => {
    try {
        $('.theme-btn').on('click', function (e) {
            themePickerHandler(e);
            document.dispatchEvent(new Event('themeChanged')); //Update chart theme
        });

        const params = initializeUrlParameters();

        dashboardComponents.clusters = new BigNumberCard('clusters', 'Clusters', 0);
        dashboardComponents.nodes = new BigNumberCard('nodes', 'Nodes', 0);
        dashboardComponents.namespaces = new BigNumberCard('namespaces', 'Namespaces', 0);
        dashboardComponents.workloads = new BigNumberCard('workloads', 'Workloads', 0);
        dashboardComponents.pods = new BigNumberCard('pods', 'Pods', 0, false);
        dashboardComponents.containers = new BigNumberCard('containers', 'Containers', 0, false);
        dashboardComponents.containerImages = new ContainerImagesCard('container-images');
        dashboardComponents.cpuUsage = new ClusterUsageChart('cpu-usage', 'CPU Usage by Cluster', QUERIES.CPU_USAGE, 'cpu');
        dashboardComponents.memoryUsage = new ClusterUsageChart('memory-usage', 'Memory Usage by Cluster', QUERIES.MEMORY_USAGE, 'memory');

        initializeBreadcrumbs([
            { name: 'Infrastructure', url: 'infrastructure.html' },
            { name: 'Kubernetes', url: 'kubernetes-overview.html' },
        ]);

        const dashboardHeader = new DashboardHeader('header-container', {
            title: 'Kubernetes Overview',
            startTime: params.startTime,
            endTime: params.endTime,
        });
        dashboardHeader.render();

        const [clusterItems, namespaceItems] = await Promise.all([fetchFilterData('cluster', params.startTime, params.endTime), fetchFilterData('namespace', params.startTime, params.endTime)]);
        const urlFilters = getFilterValuesFromUrl();

        const clusterFilter = new SearchableDropdown(document.getElementById('filter-cluster'), {
            type: 'cluster',
            items: clusterItems,
            selectedValues: urlFilters.cluster,
            onChange: (values) => {
                const namespaceValues = namespaceFilter.getSelectedValues();
                updateUrlWithFilters(values, namespaceValues);
                refreshDashboard();
            },
        });

        const namespaceFilter = new SearchableDropdown(document.getElementById('filter-namespace'), {
            type: 'namespace',
            items: namespaceItems,
            selectedValues: urlFilters.namespace,
            onChange: (values) => {
                const clusterValues = clusterFilter.getSelectedValues();
                updateUrlWithFilters(clusterValues, values);
                refreshDashboard();
            },
        });

        window.dashboardFilters = {
            namespace: namespaceFilter,
            cluster: clusterFilter,
        };

        datePickerHandler(params.startTime, params.endTime, params.startTime);
        setupEventHandlers();
        setupRefreshHandlers();

        if ($('.refresh-range-item#5m').hasClass('active')) {
            $('.refresh-range-item#5m').trigger('click');
        }
    } catch (error) {
        console.error('Error initializing dashboard:', error);
    }
});
