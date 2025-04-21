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

    let stDate = 'now-1h';
    let endDate = 'now';
    datePickerHandler(stDate, endDate, stDate);
    setupEventHandlers();

    fetchData('now-1h', 'now');

    $('.range-item').on('click', function () {
        fetchData(filterStartDate, filterEndDate);
    });

    $('#customrange-btn').on('dateRangeValid', function () {
        fetchData(filterStartDate / 1000, filterEndDate / 1000);
    });
});

var totalUniqueSeries = document.getElementById('totalUniqueSeries');

var tagKeysGridOptions = {
    columnDefs: [
        { headerName: 'Key', field: 'key' },
        { headerName: 'Number of Series', field: 'numSeries' },
    ],
    rowData: [],
    headerHeight: 26,
    rowHeight: 34,
    defaultColDef: {
        cellClass: 'align-center-grid',
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-down"/>',
            sortDescending: '<i class="fa fa-sort-alpha-up"/>',
        },
        sortable: true,
        filter: false,
        cellStyle: { 'text-align': 'left' },
        minWidth: 120,
        animateRows: true,
        readOnlyEdit: true,
        autoHeight: true,
        resizable: true,
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    onGridReady: function (params) {
        params.api.sizeColumnsToFit();
    },
};

var tagPairsGridOptions = {
    columnDefs: [
        { headerName: 'Key', field: 'key', filter: true },
        { headerName: 'Value', field: 'value', filter: true },
        { headerName: 'Number of Series', field: 'numSeries', filter: true },
    ],
    rowData: [],
    headerHeight: 26,
    rowHeight: 34,
    defaultColDef: {
        cellClass: 'align-center-grid',
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-down"/>',
            sortDescending: '<i class="fa fa-sort-alpha-up"/>',
        },
        sortable: true,
        filter: false,
        cellStyle: { 'text-align': 'left' },
        minWidth: 120,
        animateRows: true,
        readOnlyEdit: true,
        autoHeight: true,
        resizable: true,
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    onGridReady: function (params) {
        params.api.sizeColumnsToFit();
    },
};

var tagKeysValuesGridOptions = {
    columnDefs: [
        { headerName: 'Key', field: 'key' },
        { headerName: 'Number of Unique Values', field: 'numValues' },
    ],
    rowData: [],
    headerHeight: 26,
    rowHeight: 34,
    defaultColDef: {
        cellClass: 'align-center-grid',
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-down"/>',
            sortDescending: '<i class="fa fa-sort-alpha-up"/>',
        },
        sortable: true,
        filter: false,
        cellStyle: { 'text-align': 'left' },
        minWidth: 120,
        animateRows: true,
        readOnlyEdit: true,
        autoHeight: true,
        resizable: true,
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    onGridReady: function (params) {
        params.api.sizeColumnsToFit();
    },
};
//eslint-disable-next-line no-undef
new agGrid.Grid(document.getElementById('tagKeysGrid'), tagKeysGridOptions);
//eslint-disable-next-line no-undef
new agGrid.Grid(document.getElementById('tagPairsGrid'), tagPairsGridOptions);
//eslint-disable-next-line no-undef
new agGrid.Grid(document.getElementById('tagKeysValuesGrid'), tagKeysValuesGridOptions);

function fetchData(startEpoch, endEpoch) {
    $('body').css('cursor', 'wait');

    // Fetch total unique series
    fetch('metrics-explorer/api/v1/series-cardinality', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ startEpoch, endEpoch }),
    })
        .then((response) => response.json())
        .then((data) => {
            totalUniqueSeries.textContent = data.seriesCardinality.toLocaleString('en-US');
        })
        .catch((error) => {
            console.error('Error fetching series cardinality:', error);
        });

    // Fetch tag keys with most series
    fetch('metrics-explorer/api/v1/tag-keys-with-most-series', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ startEpoch, endEpoch, limit: 10 }),
    })
        .then((response) => response.json())
        .then((data) => {
            hideLoadingIcon('tagKeysGrid');
            tagKeysGridOptions.api.setRowData(data.tagKeys);
        })
        .catch((error) => {
            hideLoadingIcon('tagKeysGrid');
            console.error('Error fetching tag keys with most series:', error);
        });

    // Fetch tag pairs with most series
    fetch('metrics-explorer/api/v1/tag-pairs-with-most-series', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ startEpoch, endEpoch, limit: 10 }),
    })
        .then((response) => response.json())
        .then((data) => {
            hideLoadingIcon('tagPairsGrid');
            tagPairsGridOptions.api.setRowData(data.tagPairs);
        })
        .catch((error) => {
            hideLoadingIcon('tagPairsGrid');
            console.error('Error fetching tag pairs with most series:', error);
        });

    // Fetch tag keys with most unique values
    fetch('metrics-explorer/api/v1/tag-keys-with-most-values', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({ startEpoch, endEpoch, limit: 10 }),
    })
        .then((response) => response.json())
        .then((data) => {
            hideLoadingIcon('tagKeysValuesGrid');
            tagKeysValuesGridOptions.api.setRowData(data.tagKeys);

            $('body').css('cursor', 'default');
        })
        .catch((error) => {
            hideLoadingIcon('tagKeysValuesGrid');
            console.error('Error fetching tag keys with most values:', error);

            $('body').css('cursor', 'default');
        });
}

// Function to hide loading icon
function hideLoadingIcon(containerId) {
    $('#' + containerId)
        .find('.panel-loading')
        .remove();
}
