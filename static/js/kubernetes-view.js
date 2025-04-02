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
class KubernetesView {
    constructor() {
        const urlParams = new URLSearchParams(window.location.search);
        this.type = urlParams.get('type') || 'clusters';
        this.startTime = urlParams.get('startEpoch') || 'now-1h';
        this.endTime = urlParams.get('endEpoch') || 'now';
        this.gridOptions = null;
        this.filters = {};
        this.currentFrom = 0;
        this.queryResults = {};

        this.initializeQueries();
        this.init();

        datePickerHandler(this.startTime, this.endTime, this.startTime);
        setupEventHandlers();
    }

    /* eslint-disable no-undef */
    initializeQueries() {
        const config = {
            timeRange: '1h',
            rateInterval: '5m',
        };

        switch (this.type) {
            case 'clusters':
                this.queries = getClusterMonitoringQueries(config);
                break;
            case 'namespaces':
                this.queries = getNamespaceMonitoringQueries(config);
                break;
            case 'nodes':
                this.queries = getNodeMonitoringQueries(config);
                break;
            case 'workloads':
                this.queries = getWorkloadMonitoringQueries(config);
                break;
            default:
                this.queries = getClusterMonitoringQueries(config);
        }
    }
    /* eslint-enable no-undef */

    init() {
        if (this.type === 'configuration') {
            this.initConfigurationView();
        } else if (this.type === 'events') {
            this.initEventsView();
        } else {
            this.initMainView();
        }
    }

    initConfigurationView() {
        $('.kubernetes-view-page').hide();
        $('.configuration-page').show();

        this.initBreadcrumb();
        const header = new DashboardHeader('config-header', {
            title: 'Configuration',
            showTimeRange: false,
            showRefresh: false,
        });
        header.render();

        const config = document.getElementById('config-container');
        new ConfigurationPage(config);
    }

    initEventsView() {
        $('.kubernetes-view-page').show();
        $('.configuration-page, .main-filter-container').hide();

        this.initBreadcrumb();
        this.initEventsHeader();
        this.initGrid();
        this.loadEventsData();
    }

    initEventsHeader() {
        const header = new DashboardHeader('kubernetes-header', {
            title: 'Events',
            startTime: this.startTime,
            endTime: this.endTime,
            showRefresh: false,
            showTimeRange: true,
        });
        header.render();

        $(document).on('click', '.range-item', () => {
            this.startTime = filterStartDate;
            this.endTime = filterEndDate;
            this.currentFrom = 0;
            this.loadEventsData();
        });

        $(document).on('click', '#customrange-btn', () => {
            this.startTime = filterStartDate;
            this.endTime = filterEndDate;
            this.currentFrom = 0;
            this.loadEventsData();
        });
    }

    async loadEventsData(append = false) {
        try {
            const param = {
                state: 'query',
                searchText: '*',
                startEpoch: this.startTime,
                endEpoch: this.endTime,
                indexName: 'k8s-events-sig',
                queryLanguage: 'Splunk QL',
                from: this.currentFrom,
            };

            const response = await $.ajax({
                method: 'post',
                url: 'api/search',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
                dataType: 'json',
                data: JSON.stringify(param),
            });

            if (response?.hits?.records) {
                const newData = response.hits.records;
                if (append) {
                    // Store scroll position
                    const gridBody = document.querySelector('.ag-body-viewport');
                    const scrollTop = gridBody.scrollTop;

                    // Update data
                    let currentData = [];
                    this.gridOptions.api.forEachNode((node) => {
                        currentData.push(node.data);
                    });
                    this.gridOptions.api.setRowData([...currentData, ...newData]);

                    // Restore scroll position
                    gridBody.scrollTop = scrollTop;
                } else {
                    this.gridOptions.api.setRowData(newData);
                }
            }
        } catch (error) {
            console.error('Error loading events data:', error);
        }
    }

    initMainView() {
        $('.kubernetes-view-page').show();
        $('.configuration-page').hide();

        this.initBreadcrumb();
        this.initHeader();
        this.initFilters();
        this.initGrid();
        this.loadData();
        new ResourceUsageComponent('resource-usage-container');
    }

    initHeader() {
        const header = new DashboardHeader('kubernetes-header', {
            title: `${this.type.charAt(0).toUpperCase() + this.type.slice(1)}`,
            startTime: this.startTime,
            endTime: this.endTime,
        });
        header.render();
    }

    getViewFilters() {
        const filterConfigs = {
            clusters: ['cluster'],
            namespaces: ['cluster', 'namespace'],
            nodes: ['cluster', 'node'],
            workloads: ['cluster', 'namespace', 'workload'],
            pods: ['cluster', 'namespace', 'pod'],
        };
        return filterConfigs[this.type] || ['cluster'];
    }

    async initFilters() {
        try {
            // Hide all filter containers first
            const filterIds = ['cluster', 'namespace', 'node', 'workload', 'pod'];
            filterIds.forEach((id) => {
                const container = document.getElementById(`filter-${id}`);
                if (container) {
                    container.style.display = 'none';
                }
            });

            // Get required filters for current view
            const requiredFilters = this.getViewFilters();

            // Initialize filters
            for (const filterType of requiredFilters) {
                const container = document.getElementById(`filter-${filterType}`);
                if (container) {
                    container.style.display = 'flex';
                    this.initializeFilter(filterType, container);
                }
            }
        } catch (error) {
            console.error('Error initializing filters:', error);
        }
    }

    initializeFilter(filterType, container) {
        // TODO: Implement retrieving actual data and display that in dropdown
        this.filters[filterType] = new SearchableDropdown(container, {
            type: filterType,
            items: [],
            selectedValues: ['All'],
            onChange: (selectedValues) => {
                console.log('onChange: ' + selectedValues);
            },
        });
    }

    initBreadcrumb() {
        const capitalizedType = this.type.charAt(0).toUpperCase() + this.type.slice(1);

        initializeBreadcrumbs([
            { name: 'Infrastructure', url: 'infrastructure.html' },
            { name: 'Kubernetes', url: 'kubernetes-overview.html' },
            { name: capitalizedType, url: this.type },
        ]);
    }

    initGrid() {
        const columnDefs = this.getColumnDefs();

        this.gridOptions = {
            columnDefs: columnDefs,
            rowData: [],
            rowHeight: 32,
            headerHeight: 26,
            defaultColDef: {
                sortable: true,
                filter: true,
                resizable: true,
                flex: 1,
                minWidth: 250,
                cellClass: 'align-center-grid',
                icons: {
                    sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
                    sortDescending: '<i class="fa fa-sort-alpha-down"/>',
                },
            },
            enableCellTextSelection: true,
            suppressCopyRowsToClipboard: true,
            onBodyScroll: (params) => {
                if (this.type === 'events') {
                    const lastRow = params.api.getLastDisplayedRow();
                    const totalRows = params.api.getModel().getRowCount();

                    if (lastRow >= totalRows - 10) {
                        this.currentFrom += 100;
                        this.loadEventsData(true);
                    }
                }
            },
        };

        const gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, this.gridOptions);
    }

    getColumnDefs() {
        if (this.type === 'events') {
            return [
                { field: 'severity_text', headerName: 'TYPE', cellRenderer: ExpandableJsonCellRenderer('events') },
                { field: 'attributes.k8s.event.name', headerName: 'NAME' },
                { field: 'attributes.k8s.event.reason', headerName: 'REASON' },
                { field: 'attributes.k8s.namespace.name', headerName: 'NAMESPACE' },
                { field: 'resource.attributes.k8s.object.kind', headerName: 'KIND' },
                { field: 'attributes.k8s.event.start_time', headerName: 'TIME' },
                { field: 'body', headerName: 'MESSAGE' },
            ];
        }

        const columnConfigs = {
            clusters: [
                { field: 'CLUSTER', headerName: 'CLUSTER' },
                { field: 'CPU_AVG', headerName: 'CPU AVG' },
                { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' },
                { field: 'CPU_MAX', headerName: 'CPU MAX' },
                { field: 'CPU_MAX_PERCENT', headerName: 'CPU MAX %' },
                { field: 'MEM_AVG', headerName: 'MEM AVG' },
                { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' },
                { field: 'MEM_MAX', headerName: 'MEM MAX' },
                { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' },
            ],
            namespaces: [
                { field: 'NAMESPACE', headerName: 'NAMESPACE' },
                { field: 'CLUSTER', headerName: 'CLUSTER' },
                { field: 'CPU_AVG', headerName: 'CPU AVG' },
                { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' },
                { field: 'CPU_MAX', headerName: 'CPU MAX' },
                { field: 'CPU_MAX_PERCENT', headerName: 'CPU MAX %' },
                { field: 'MEM_AVG', headerName: 'MEM AVG' },
                { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' },
                { field: 'MEM_MAX', headerName: 'MEM MAX' },
                { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' },
            ],
            workloads: [
                { field: 'WORKLOAD', headerName: 'WORKLOAD' },
                { field: 'TYPE', headerName: 'TYPE' },
                { field: 'NAMESPACE', headerName: 'NAMESPACE' },
                { field: 'CLUSTER', headerName: 'CLUSTER' },
                { field: 'CPU_AVG', headerName: 'CPU AVG' },
                { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' },
                { field: 'CPU_MAX', headerName: 'CPU MAX' },
                { field: 'CPU_MAX_PERCENT', headerName: 'CPU MAX %' },
                { field: 'MEM_AVG', headerName: 'MEM AVG' },
                { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' },
                { field: 'MEM_MAX', headerName: 'MEM MAX' },
                { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' },
            ],
            nodes: [
                { field: 'NODE', headerName: 'NODE' },
                { field: 'CLUSTER', headerName: 'CLUSTER' },
                { field: 'CPU_AVG', headerName: 'CPU AVG' },
                { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' },
                { field: 'CPU_MAX', headerName: 'CPU MAX' },
                { field: 'CPU_MAX_PERCENT', headerName: 'CPU MAX %' },
                { field: 'MEM_AVG', headerName: 'MEM AVG' },
                { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' },
                { field: 'MEM_MAX', headerName: 'MEM MAX' },
                { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' },
            ],
        };

        return columnConfigs[this.type] || [];
    }

    formatCPUValue(value) {
        if (typeof value !== 'number' || isNaN(value)) return 'N/A';
        return `${value.toFixed(2)} cores`;
    }

    formatMemoryValue(value) {
        if (typeof value !== 'number' || isNaN(value)) return 'N/A';

        // Convert bytes to appropriate unit
        const units = ['B', 'KB', 'MB', 'GB', 'TB'];
        let size = value;
        let unitIndex = 0;

        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }

        return `${size.toFixed(2)} ${units[unitIndex]}`;
    }

    formatPercentage(value) {
        if (typeof value !== 'number' || isNaN(value)) return 'N/A';
        return `${(value * 100).toFixed(2)}%`;
    }

    async loadData() {
        if (this.type === 'events') return;

        this.gridOptions.api.showLoadingOverlay();

        try {
            const results = await this.executeQueries(this.queries);
            this.queryResults = results;

            const rowData = this.processQueryResults();

            this.gridOptions.api.setRowData(rowData);

            this.setSummaryRow(rowData);

            this.gridOptions.api.hideOverlay();
        } catch (error) {
            console.error('Error loading data:', error);
            this.gridOptions.api.hideOverlay();
            this.gridOptions.api.showNoRowsOverlay();
        }
    }

    async executeQueries(queries) {
        const results = {};
        const promises = [];
        const currentTime = Math.floor(Date.now() / 1000);

        // Prepare all query requests
        Object.entries(queries).forEach(([queryName, query]) => {
            const request = this.executePromQLQuery(query, currentTime);
            promises.push(
                request
                    .then((response) => {
                        results[queryName] = response;
                    })
                    .catch((error) => {
                        console.error(`Error executing query ${queryName}:`, error);
                        results[queryName] = null;
                    })
            );
        });

        // Wait for all queries to complete
        await Promise.all(promises);
        return results;
    }

    async executePromQLQuery(query, timestamp) {
        return await $.ajax({
            url: '/promql/api/v1/query',
            type: 'GET',
            data: {
                time: timestamp,
                query: query,
            },
            headers: {
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
        });
    }

    processQueryResults() {
        switch (this.type) {
            case 'clusters':
                return this.processClusterResults();
            case 'nodes':
                return this.processNodeResults();
            case 'namespaces':
                return this.processNamespaceResults();
            case 'workloads':
                return this.processWorkloadResults();
            default:
                return [];
        }
    }

    COMMON_METRICS = [
        { query: 'CPU_USAGE_AVG', field: 'CPU_AVG', formatter: 'formatCPUValue' },
        { query: 'CPU_USAGE_AVG_PERCENT', field: 'CPU_AVG_PERCENT', formatter: 'formatPercentage' },
        { query: 'CPU_USAGE_MAX', field: 'CPU_MAX', formatter: 'formatCPUValue' },
        { query: 'CPU_USAGE_MAX_PERCENT', field: 'CPU_MAX_PERCENT', formatter: 'formatPercentage' },
        { query: 'MEMORY_USAGE_AVG', field: 'MEM_AVG', formatter: 'formatMemoryValue' },
        { query: 'MEMORY_USAGE_AVG_PERCENT', field: 'MEM_AVG_PERCENT', formatter: 'formatPercentage' },
        { query: 'MEMORY_USAGE_MAX', field: 'MEM_MAX', formatter: 'formatMemoryValue' },
        { query: 'MEMORY_USAGE_MAX_PERCENT', field: 'MEM_MAX_PERCENT', formatter: 'formatPercentage' },
    ];

    createEmptyMetricsObject() {
        return {
            CPU_AVG: 'No data',
            CPU_AVG_PERCENT: 'No data',
            CPU_MAX: 'No data',
            CPU_MAX_PERCENT: 'No data',
            MEM_AVG: 'No data',
            MEM_AVG_PERCENT: 'No data',
            MEM_MAX: 'No data',
            MEM_MAX_PERCENT: 'No data',
        };
    }

    processClusterResults() {
        const clusterData = {};

        this.COMMON_METRICS.forEach((metric) => {
            this.processClusterMetric(metric.query, clusterData, metric.field, this[metric.formatter]);
        });

        return Object.values(clusterData);
    }

    processClusterMetric(queryKey, clusterData, fieldName, formatter) {
        if (this.queryResults[queryKey]?.data?.result) {
            this.queryResults[queryKey].data.result.forEach((item) => {
                if (item.metric?.cluster && item.value) {
                    const cluster = item.metric.cluster;
                    const value = parseFloat(item.value[1]);

                    if (!clusterData[cluster]) {
                        clusterData[cluster] = {
                            CLUSTER: cluster,
                            ...this.createEmptyMetricsObject(),
                        };
                    }

                    if (!isNaN(value)) {
                        clusterData[cluster][fieldName] = formatter(value);
                    }
                }
            });
        }
    }

    processNamespaceResults() {
        const namespaceData = {};

        this.COMMON_METRICS.forEach((metric) => {
            this.processNamespaceMetric(metric.query, namespaceData, metric.field, this[metric.formatter]);
        });

        return Object.values(namespaceData);
    }

    processNamespaceMetric(queryKey, namespaceData, fieldName, formatter) {
        if (this.queryResults[queryKey]?.data?.result) {
            this.queryResults[queryKey].data.result.forEach((item) => {
                if (item.metric?.namespace && item.metric?.cluster && item.value) {
                    const namespace = item.metric.namespace;
                    const cluster = item.metric.cluster;
                    const key = `${cluster}:${namespace}`;
                    const value = parseFloat(item.value[1]);

                    if (!namespaceData[key]) {
                        namespaceData[key] = {
                            NAMESPACE: namespace,
                            CLUSTER: cluster,
                            ...this.createEmptyMetricsObject(),
                        };
                    }

                    if (!isNaN(value)) {
                        namespaceData[key][fieldName] = formatter(value);
                    }
                }
            });
        }
    }

    processWorkloadResults() {
        const workloadData = {};

        this.COMMON_METRICS.forEach((metric) => {
            this.processWorkloadMetric(metric.query, workloadData, metric.field, this[metric.formatter]);
        });

        return Object.values(workloadData);
    }

    processWorkloadMetric(queryKey, workloadData, fieldName, formatter) {
        if (this.queryResults[queryKey]?.data?.result) {
            this.queryResults[queryKey].data.result.forEach((item) => {
                if (item.metric && item.value) {
                    const workload = item.metric.workload || '';
                    const type = item.metric.workload_type || '';
                    const namespace = item.metric.namespace || '';
                    const cluster = item.metric.cluster || '';

                    if (workload && namespace && cluster) {
                        const key = `${cluster}:${namespace}:${workload}`;
                        const value = parseFloat(item.value[1]);

                        if (!workloadData[key]) {
                            workloadData[key] = {
                                WORKLOAD: workload,
                                TYPE: type,
                                NAMESPACE: namespace,
                                CLUSTER: cluster,
                                ...this.createEmptyMetricsObject(),
                            };
                        }

                        if (!isNaN(value)) {
                            workloadData[key][fieldName] = formatter(value);
                        }
                    }
                }
            });
        }
    }

    processNodeResults() {
        const nodeData = {};

        this.COMMON_METRICS.forEach((metric) => {
            this.processNodeMetric(metric.query, nodeData, metric.field, this[metric.formatter]);
        });

        return Object.values(nodeData);
    }

    processNodeMetric(queryKey, nodeData, fieldName, formatter) {
        if (this.queryResults[queryKey]?.data?.result) {
            this.queryResults[queryKey].data.result.forEach((item) => {
                if (item.metric?.node && item.metric?.cluster && item.value) {
                    const node = item.metric.node;
                    const cluster = item.metric.cluster;
                    const key = `${cluster}:${node}`;
                    const value = parseFloat(item.value[1]);

                    if (!nodeData[key]) {
                        nodeData[key] = {
                            NODE: node,
                            CLUSTER: cluster,
                            ...this.createEmptyMetricsObject(),
                        };
                    }

                    if (!isNaN(value)) {
                        nodeData[key][fieldName] = formatter(value);
                    }
                }
            });
        }
    }

    setSummaryRow(rowData) {
        if (!this.gridOptions || !rowData || rowData.length === 0) return;

        const columnFields = this.gridOptions.columnDefs.map((col) => col.field);
        const pinnedRow = {};

        pinnedRow[columnFields[0]] = 'Count';
        pinnedRow[columnFields[1]] = rowData.length.toString();

        for (let i = 2; i < columnFields.length; i++) {
            pinnedRow[columnFields[i]] = '';
        }

        this.gridOptions.api.setPinnedBottomRowData([pinnedRow]);
    }
}

$(document).ready(() => {
    $('.theme-btn').on('click', themePickerHandler);

    new KubernetesView();
});
