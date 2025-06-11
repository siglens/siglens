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

$(document).ready(function () {
    $('.theme-btn').on('click', themePickerHandler);
    updateGrids();
    renderQueryStatsTables();
    const savedViewMode = Cookies.get('query-view') || 'single';
    toggleViewMode(savedViewMode);
    setInterval(updateGrids, 5000); // Refresh every 5 seconds
});

$('#log-opt-multi-btn').on('click', () => toggleViewMode('multi'));
$('#log-opt-table-btn').on('click', () => toggleViewMode('table'));

const activeGridOptions = {
    columnDefs: [
        {
            field: 'queryText',
            headerName: 'Query',
            sortable: true,
            flex: 3,
        },
        {
            field: 'executionTimeMs',
            headerName: 'Execution Time (ms)',
            sortable: true,
            flex: 1,
            maxWidth: 300,
            valueFormatter: (params) => params.value.toLocaleString(),
        },
    ],
    defaultColDef: {
        resizable: true,
    },
    suppressDragLeaveHidesColumns: true,
};

const waitingGridOptions = {
    columnDefs: [
        {
            field: 'queryText',
            headerName: 'Query',
            sortable: true,
            flex: 3,
        },
        {
            field: 'waitingTimeMs',
            headerName: 'Waiting Time (ms)',
            sortable: true,
            flex: 1,
            maxWidth: 300,
            valueFormatter: (params) => params.value.toLocaleString(),
        },
    ],
    defaultColDef: {
        resizable: true,
    },
    suppressDragLeaveHidesColumns: true,
};

// eslint-disable-next-line no-unused-vars, no-undef
const activeGrid = new agGrid.Grid(document.querySelector('#active-queries'), activeGridOptions);
// eslint-disable-next-line no-unused-vars, no-undef
const waitingGrid = new agGrid.Grid(document.querySelector('#waiting-queries'), waitingGridOptions);

function updateGrids() {
    $.ajax({
        url: '/api/query-stats',
        method: 'GET',
        success: function (response) {
            activeGridOptions.api.setRowData(response.activeQueries);
            waitingGridOptions.api.setRowData(response.waitingQueries);
        },
        error: function () {
            showToast('Failed to fetch query stats', 'error');
        },
    });
}
function toggleViewMode(viewMode) {
    const grids = [activeGridOptions, waitingGridOptions];

    grids.forEach((gridOptions) => {
        gridOptions.columnDefs.forEach(function (colDef) {
            if (colDef.field === 'queryText') {
                if (viewMode === 'multi') {
                    colDef.cellStyle = { 'white-space': 'normal' };
                    colDef.autoHeight = true;
                    colDef.cellRenderer = function (params) {
                        const data = params.data || {};
                        return `<div style="white-space: pre-wrap;">${data.queryText}</div>`;
                    };
                } else {
                    colDef.cellStyle = { 'white-space': 'nowrap' };
                    colDef.autoHeight = false;
                    colDef.cellRenderer = null;
                }
            }
        });

        gridOptions.api.setColumnDefs(gridOptions.columnDefs);
        gridOptions.api.refreshCells({ force: true });
        gridOptions.api.redrawRows();
        gridOptions.api.sizeColumnsToFit();
    });

    if (viewMode === 'multi') {
        $('#log-opt-multi-btn').addClass('active');
        $('#log-opt-table-btn').removeClass('active');
    } else {
        $('#log-opt-table-btn').addClass('active');
        $('#log-opt-multi-btn').removeClass('active');
    }

    Cookies.set('query-view', viewMode, { expires: 365 });
}

function renderQueryStatsTables() {
    $.ajax({
        method: 'get',
        url: 'api/clusterStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    }).then(function (res) {
        processQueryStats(res);
    });
}

function processQueryStats(res) {
    _.forEach(res, (value, key) => {
        if (key === 'queryStats') {
            let table = $('#query-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');

                let formattedValue;
                if (k === 'Average Query Latency (since install)' || k === 'Average Query Latency (since restart)') {
                    const numericPart = parseFloat(v);
                    const avgLatency = Math.round(numericPart);
                    formattedValue = avgLatency.toLocaleString() + ' ms';
                } else {
                    const numericValue = parseInt(v, 10);
                    formattedValue = numericValue.toLocaleString();
                }
                tr.append('<td class="health-stats-value">' + formattedValue + '</td>');
                table.find('tbody').append(tr);
            });
        }
    });
}
