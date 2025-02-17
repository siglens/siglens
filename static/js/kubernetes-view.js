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
        this.init();
    }

    init() {
        if (this.type === 'configuration') {
            this.initConfigurationView();
        } else {
            this.initMainView();
        }
    }

    initConfigurationView() {
        $('.kubernetes-view-page').hide();
        $('.configuration-page').show();

        this.initBreadcrumb('config-breadcrumb');
        const header = new DashboardHeader('config-header', {
            title: 'Configuration',
            showTimeRange: false,
            showRefresh: false,
        });
        header.render();

        const config = document.getElementById('config-container');
        new ConfigurationPage(config);
    }

    initMainView() {
        $('.kubernetes-view-page').show();
        $('.configuration-page').hide();

        this.initBreadcrumb('kubernetes-breadcrumb');
        this.initHeader();
        this.initFilters();
        this.initGrid();
        this.loadData();
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

    initBreadcrumb(containerId) {
        const capitalizedType = this.type.charAt(0).toUpperCase() + this.type.slice(1);
        const breadcrumb = new Breadcrumb(containerId);

        breadcrumb.render([
            { label: 'Infrastructure', url: 'infrastructure', className: 'all-dashboards' },
            { label: 'Kubernetes', url: 'kubernetes-overview', className: 'all-dashboards' },
            { label: capitalizedType, url: this.type, className: 'myOrg-heading' },
        ]);
    }

    initGrid() {
        const columnDefs = this.getColumnDefs();

        this.gridOptions = {
            columnDefs: columnDefs,
            rowData: [],
            rowHeight: 34,
            headerHeight: 26,
            defaultColDef: {
                sortable: true,
                filter: true,
                resizable: true,
                cellClass: 'align-center-grid',
                icons: {
                    sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
                    sortDescending: '<i class="fa fa-sort-alpha-down"/>',
                },
            },
            enableCellTextSelection: true,
            suppressCopyRowsToClipboard: true,
        };

        const gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, this.gridOptions);
    }

    getColumnDefs() {
        const columnConfigs = {
            clusters: [{ field: 'CLUSTER' }, { field: 'PROVIDER' }, { field: 'NODES' }, { field: 'CPU_AVG', headerName: 'CPU AVG' }, { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' }, { field: 'MEM_AVG', headerName: 'MEM AVG' }, { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' }, { field: 'MEM_AVG', headerName: 'MEM AVG' }, { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' }, { field: 'MEM_MAX', headerName: 'MEM MAX' }, { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' }],
            namespaces: [{ field: 'NAMESPACE' }, { field: 'CLUSTER' }, { field: 'WORKLOADS' }, { field: 'CPU_AVG', headerName: 'CPU AVG' }, { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' }, { field: 'MEM_AVG', headerName: 'MEM AVG' }, { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' }, { field: 'MEM_AVG', headerName: 'MEM AVG' }, { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' }, { field: 'MEM_MAX', headerName: 'MEM MAX' }, { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' }],
            workloads: [{ field: 'WORKLOAD' }, { field: 'TYPE' }, { field: 'NAMESPACE' }, { field: 'CLUSTER' }, { field: 'PODS' }],
            nodes: [{ field: 'NODE' }, { field: 'CLUSTER' }, { field: 'CPU_AVG', headerName: 'CPU AVG' }, { field: 'CPU_AVG_PERCENT', headerName: 'CPU AVG %' }, { field: 'MEM_AVG', headerName: 'MEM AVG' }, { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' }, { field: 'MEM_AVG', headerName: 'MEM AVG' }, { field: 'MEM_AVG_PERCENT', headerName: 'MEM AVG %' }, { field: 'MEM_MAX', headerName: 'MEM MAX' }, { field: 'MEM_MAX_PERCENT', headerName: 'MEM MAX %' }],
        };

        return columnConfigs[this.type] || columnConfigs.clusters;
    }

    // TODO: Implement retrieving actual data and display that in table
    loadData() {
        const dummyData = {
            clusters: [
                {
                    CLUSTER: 'production-cluster',
                    PROVIDER: 'AWS',
                    NODES: '5',
                    CPU_AVG: '2.5 cores',
                    CPU_AVG_PERCENT: '62.5%',
                    MEM_AVG: '8.2 GB',
                    MEM_AVG_PERCENT: '75.3%',
                    MEM_MAX: '16 GB',
                    MEM_MAX_PERCENT: '92.1%',
                },
            ],
            namespaces: [
                {
                    NAMESPACE: 'default',
                    CLUSTER: 'production-cluster',
                    WORKLOADS: '12',
                    CPU_AVG: '1.8 cores',
                    CPU_AVG_PERCENT: '45.0%',
                    MEM_AVG: '4.5 GB',
                    MEM_AVG_PERCENT: '56.2%',
                    MEM_MAX: '8 GB',
                    MEM_MAX_PERCENT: '78.4%',
                },
            ],
            workloads: [
                {
                    WORKLOAD: 'nginx-deployment',
                    TYPE: 'Deployment',
                    NAMESPACE: 'default',
                    CLUSTER: 'production-cluster',
                    PODS: '3',
                },
            ],
            nodes: [
                {
                    NODE: 'worker-node-1',
                    CLUSTER: 'production-cluster',
                    CPU_AVG: '3.2 cores',
                    CPU_AVG_PERCENT: '80.0%',
                    MEM_AVG: '12.8 GB',
                    MEM_AVG_PERCENT: '80.0%',
                    MEM_MAX: '14.4 GB',
                    MEM_MAX_PERCENT: '90.0%',
                },
            ],
        };

        if (this.gridOptions && dummyData[this.type]) {
            const rowData = dummyData[this.type];
            this.gridOptions.api.setRowData(rowData);

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
}

$(document).ready(() => {
    $('.theme-btn').on('click', themePickerHandler);

    new KubernetesView();
});
