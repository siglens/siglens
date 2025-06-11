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

let stDate = 'now-1h';
let endDate = 'now';
let allMetrics = []; // To store all metrics initially
let metricsPagination;
let gridDiv = null;

$(document).ready(() => {
    datePickerHandler(stDate, endDate, stDate);
    setupEventHandlers();

    $('.range-item').on('click', fetchAllMetrics);
    $('#customrange-btn').on('dateRangeValid', fetchAllMetrics);

    //eslint-disable-next-line no-undef
    metricsPagination = createPagination('metrics-pagination', {
        pageSize: 50,
        pageSizeOptions: [25, 50, 100, 200],
        onPageChange: (page, pageSize) => {
            displayMetricsPaginated(page, pageSize);
        },
        onPageSizeChange: (pageSize) => {
            displayMetricsPaginated(1, pageSize);
        },
    });

    fetchAllMetrics();

    $('#metric-search-input').on('input', function () {
        filterMetrics();
    });
    $('.theme-btn').on('click', themePickerHandler);
});

function getTimeRange() {
    return {
        startEpoch: filterStartDate || 'now-1h',
        endEpoch: filterEndDate || 'now',
    };
}

function fetchAllMetrics() {
    const data = getTimeRange();
    const pl = {
        start: data.startEpoch,
        end: data.endEpoch,
    };
    $.ajax({
        method: 'post',
        url: 'metrics-explorer/api/v1/metric_names',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(pl),
    }).then(function (res) {
        if (res && res.metricNames && Array.isArray(res.metricNames)) {
            allMetrics = res.metricNames; // Store all metrics

            metricsPagination.updateState(allMetrics.length, 1);
            metricsPagination.show();
            displayMetricsPaginated(1, metricsPagination.getPageSize());
        } else {
            console.error('Invalid response format:', res);
        }
    });
}

function filterMetrics() {
    const searchTerm = $('#metric-search-input').val().trim().toLowerCase();
    const filteredMetrics = getFilteredMetrics(searchTerm);

    // Update pagination with filtered data
    metricsPagination.updateState(filteredMetrics.length, 1);
    displayMetricsPaginated(1, metricsPagination.getPageSize());
}

function getFilteredMetrics(searchTerm) {
    if (!searchTerm) return allMetrics;
    return allMetrics.filter((metric) => metric.toLowerCase().includes(searchTerm));
}

function displayMetricsPaginated(page, pageSize) {
    const searchTerm = $('#metric-search-input').val().trim().toLowerCase();
    const dataToShow = getFilteredMetrics(searchTerm);

    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const pageData = dataToShow.slice(startIndex, endIndex);

    displayData(pageData);
}

function displayData(metrics) {
    const metricRows = metrics.map((metric) => ({ metricName: metric }));

    if (gridDiv === null) {
        gridDiv = document.querySelector('#ag-grid');
        //eslint-disable-next-line no-undef
        new agGrid.Grid(gridDiv, gridOptions);
    }

    gridOptions.api.setRowData(metricRows);
    gridOptions.api.sizeColumnsToFit();
}

var gridOptions = {
    headerHeight: 26,
    rowHeight: 34,
    pagination: false,
    suppressDragLeaveHidesColumns: true,
    defaultColDef: {
        sortable: true,
        filter: false,
        resizable: false,
        cellStyle: { 'text-align': 'left' },
        cellClass: 'align-center-grid d-flex',
        minWidth: 120,
        animateRows: true,
    },
    icons: {
        sortAscending: '<i class="fa fa-sort-alpha-down"/>',
        sortDescending: '<i class="fa fa-sort-alpha-up"/>',
    },
    onGridReady: function (params) {
        params.api.sizeColumnsToFit();
    },
    columnDefs: [
        {
            headerName: 'Metric Name',
            field: 'metricName',
            sort: 'asc', // Initial sort order set to ascending
        },
    ],
    rowData: [],
};
