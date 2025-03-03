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
class ContainerImagesCard {
    constructor(containerId) {
        this.container = $(`#${containerId}`);
        this.gridOptions = null;
        this.loadingElement = null;
        this.init();
        this.fetchData();
    }

    init() {
        const template = `
            <div class="cluster-card">
                <div class="cluster-header">
                    <span class="header-title">Deployed Container Images (as of ${this.formatCurrentTime()})</span>
                    <div class="header-actions">
                        <div class="dropdown">
                            <button class="menu-button btn"></button>
                            <div class="dropdown-content">
                                <li class="explore-option">
                                    <span class="explore-icon"></span>
                                    Explore
                                </li>
                            </div>
                        </div>
                    </div>
                </div>
                <div class="position-relative">
                    <div id="imagesGrid" style="height: 400px; width: 100%;" class="ag-theme-alpine-dark"></div>
                    <div id="panel-loading" style="display: none;" class="image-loading"></div>
                    <div class="count-row">
                        <div class="label">Count</div>
                        <div class="total-count">-</div>
                    </div>
                </div>
            </div>
        `;

        this.container.html(template);
        this.loadingElement = this.container.find('#panel-loading');
        this.setupGrid();
        this.setupEventHandlers();
        this.addStyles();
    }

    setupGrid() {
        const columnDefs = [
            {
                field: 'imageSpec',
                headerName: 'IMAGE SPEC',
                flex: 1,
                sortable: true,
            },
            {
                field: 'containers',
                headerName: 'CONTAINERS',
                width: 120,
                sortable: true,
                cellRenderer: this.containersCellRenderer,
                type: 'numericColumn',
                cellStyle: { textAlign: 'right' },
            },
        ];

        this.gridOptions = {
            columnDefs: columnDefs,
            rowData: [],
            headerHeight: 40,
            rowHeight: 40,
            suppressMovableColumns: true,
            suppressColumnVirtualisation: true,
            defaultColDef: {
                resizable: true,
                cellClass: 'align-center-grid',
            },
            onGridReady: (params) => {
                params.api.sizeColumnsToFit();
            },
            getRowStyle: () => {
                return {
                    borderBottom: '1px solid var(--border-color)',
                    background: 'transparent',
                };
            },
        };

        const gridDiv = this.container.find('#imagesGrid')[0];
        new agGrid.Grid(gridDiv, this.gridOptions);
    }

    containersCellRenderer(params) {
        const maxValue = params.context.maxContainers;
        const percentage = (params.value / maxValue) * 100;
        return `
            <div style="position: relative; height: 100%; display: flex; align-items: center;">
                <div style="height: 16px; background: linear-gradient(90deg, rgb(87, 148, 242) 0%, rgb(184, 119, 217) 90%); border-radius: 3px;
                     width: ${percentage / 2}%; max-width: calc(100% - 40px); position: absolute; left: 0;"></div>
                <span style="position: absolute; right: 0;">${params.value}</span>
            </div>
        `;
    }

    formatCurrentTime() {
        const now = new Date();
        return now.toISOString().replace('T', ' ').substring(0, 19);
    }

    addStyles() {
        const styles = `
            <style>
                #imagesGrid {
                    flex: 1;
                }
                .count-row {
                    display: flex;
                    justify-content: space-between;
                    padding: 10px;
                    font-weight: bold;
                    border-top: 1px solid var(--border-color);
                }
                .ag-theme-alpine-dark {
                    --ag-background-color: transparent;
                    --ag-header-background-color: transparent;
                    --ag-odd-row-background-color: transparent;
                    --ag-header-column-separator-display: none;
                    --ag-row-hover-color: rgba(255, 255, 255, 0.1);
                }
                .ag-theme-alpine-dark .ag-root-wrapper {
                    border: none;
                }
                .ag-theme-alpine-dark .ag-header {
                    border-bottom: 1px solid var(--border-color);
                }
            </style>
        `;
        this.container.append(styles);
    }

    getQuery() {
        const urlParams = new URLSearchParams(window.location.search);
        const clusterFilter = urlParams.get('cluster') || 'all';
        const namespaceFilter = urlParams.get('namespace') || 'all';

        const clusterMatch = clusterFilter === 'all' ? '.+' : clusterFilter;
        const namespaceMatch = namespaceFilter === 'all' ? '.+' : namespaceFilter;

        return `sum by (image_spec) (kube_pod_container_info{cluster=~"${clusterMatch}", namespace=~"${namespaceMatch}"})`;
    }

    async fetchData() {
        this.showLoading();

        const urlParams = new URLSearchParams(window.location.search);
        const startTime = urlParams.get('startEpoch') || 'now-1h';
        const endTime = urlParams.get('endEpoch') || 'now';

        const query = this.getQuery();

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
            this.updateTable(response);
        } catch (error) {
            this.showError();
        } finally {
            this.hideLoading();
        }
    }

    updateTable(response) {
        if (!response || !response.series || response.series.length === 0) {
            this.showNoData();
            return;
        }

        const imageData = response.series.map((series, index) => {
            const match = series.match(/image_spec:([^,}]+)/);
            const imageSpec = match ? match[1] : 'unknown';
            const value = response.values[index]?.[0] || 0;
            return { imageSpec, containers: value };
        });

        const maxContainers = Math.max(...imageData.map((d) => d.containers));
        const totalCount = imageData.length;

        this.gridOptions.context = { maxContainers };

        this.gridOptions.api.setRowData(imageData);

        this.gridOptions.columnApi.applyColumnState({
            state: [{ colId: 'containers', sort: 'desc' }],
        });

        // Update total count
        this.container.find('.total-count').text(totalCount);
    }

    showError() {
        this.gridOptions.api.setRowData([]);
        this.gridOptions.api.showLoadingOverlay();
        this.container.find('.total-count').text('-');
    }

    showNoData() {
        this.gridOptions.api.setRowData([]);
        this.gridOptions.api.showNoRowsOverlay();
        this.container.find('.total-count').text('0');
    }

    setupEventHandlers() {
        const dropdown = this.container.find('.dropdown');
        const menuButton = dropdown.find('.menu-button');
        const exploreOption = this.container.find('.explore-option');

        menuButton.on('click', (e) => {
            e.stopPropagation();
            const currentDropdown = $(e.currentTarget).closest('.dropdown');
            $('.dropdown').not(currentDropdown).removeClass('active');
            currentDropdown.toggleClass('active');
        });

        $(document).on('click', () => {
            $('.dropdown').removeClass('active');
        });

        exploreOption.on('click', (e) => {
            e.stopPropagation();
            const query = this.getQuery();
            MetricsUtils.navigateToMetricsExplorer(query, dropdown);
        });
    }

    showLoading() {
        if (this.loadingElement) {
            this.loadingElement.show();
        }
    }

    hideLoading() {
        if (this.loadingElement) {
            this.loadingElement.hide();
        }
    }
}
