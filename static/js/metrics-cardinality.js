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
});

document.addEventListener('DOMContentLoaded', function () {
    var totalUniqueSeries = document.getElementById('totalUniqueSeries');
    var selectedTimeRange = document.getElementById('selected-time-range');

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
                tagKeysGridOptions.api.setRowData(data.tagKeys);
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
                tagPairsGridOptions.api.setRowData(data.tagPairs);
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
                tagKeysValuesGridOptions.api.setRowData(data.tagKeys);
            });
    }

    function updateSelectedTimeRange(element) {
        document.querySelectorAll('.range-item').forEach((item) => {
            item.classList.remove('active');
        });
        element.classList.add('active');
        selectedTimeRange.textContent = element.textContent;
    }

    document.querySelectorAll('.range-item').forEach((item) => {
        item.addEventListener('click', function () {
            var startEpoch = this.id;
            var endEpoch = 'now';
            fetchData(startEpoch, endEpoch);
            updateSelectedTimeRange(this);
        });
    });

    document.getElementById('customrange-btn').addEventListener('click', function () {
        var startEpoch = new Date(document.getElementById('date-start').value + 'T' + document.getElementById('time-start').value + ':00Z').getTime() / 1000;
        var endEpoch = new Date(document.getElementById('date-end').value + 'T' + document.getElementById('time-end').value + ':00Z').getTime() / 1000;
        fetchData(startEpoch, endEpoch);
        selectedTimeRange.textContent = 'Custom Range';
    });

    document.querySelectorAll('.range-item').forEach((item) => {
        item.classList.remove('active');
    });
    document.querySelector('.range-item#now-1h').classList.add('active');
    selectedTimeRange.textContent = '1 Hr';
    fetchData('now-1h', 'now');
});
