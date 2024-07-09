'use strict';

document.addEventListener('DOMContentLoaded', function () {
    var totalUniqueSeries = document.getElementById('totalUniqueSeries');
    var selectedTimeRange = document.getElementById('selected-time-range');

    var tagKeysGridOptions = {
        columnDefs: [
            { headerName: 'Key', field: 'key', filter: true, resizable: true },
            { headerName: 'Number of Series', field: 'numSeries', filter: true, resizable: true }
        ],
        rowData: [],
        rowHeight: 44,
        headerHeight: 32,
        defaultColDef: {
            cellClass: 'align-center-grid',

            resizable: true,
        },
        enableCellTextSelection: true,
        suppressScrollOnNewData: true,
        suppressAnimationFrame: true,
        onGridReady: function (params) {
            params.api.sizeColumnsToFit();
        }
    };

    var tagPairsGridOptions = {
        columnDefs: [
            { headerName: 'Key', field: 'key', filter: true, resizable: true },
            { headerName: 'Value', field: 'value', filter: true, resizable: true },
            { headerName: 'Number of Series', field: 'numSeries', filter: true, resizable: true }
        ],
        rowData: [],
        rowHeight: 44,
        headerHeight: 32,
        defaultColDef: {
            cellClass: 'align-center-grid',

            resizable: true,
        },
        enableCellTextSelection: true,
        suppressScrollOnNewData: true,
        suppressAnimationFrame: true,
        onGridReady: function (params) {
            params.api.sizeColumnsToFit();
        }
    };

    var tagKeysValuesGridOptions = {
        columnDefs: [
            { headerName: 'Key', field: 'key', filter: true, resizable: true },
            { headerName: 'Number of Unique Values', field: 'numValues', filter: true, resizable: true }
        ],
        rowData: [],
        rowHeight: 44,
        headerHeight: 32,
        defaultColDef: {
            cellClass: 'align-center-grid',

            resizable: true,
        },
        enableCellTextSelection: true,
        suppressScrollOnNewData: true,
        suppressAnimationFrame: true,
        onGridReady: function (params) {
            params.api.sizeColumnsToFit();
        }
    };

    new agGrid.Grid(document.getElementById('tagKeysGrid'), tagKeysGridOptions);
    new agGrid.Grid(document.getElementById('tagPairsGrid'), tagPairsGridOptions);
    new agGrid.Grid(document.getElementById('tagKeysValuesGrid'), tagKeysValuesGridOptions);

    function fetchData(startEpoch, endEpoch) {
        // Fetch total unique series
        fetch('http://localhost:5122/metrics-explorer/api/v1/series-cardinality', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ startEpoch, endEpoch })
        })
            .then(response => response.json())
            .then(data => {
                totalUniqueSeries.textContent = data.seriesCardinality;
            });

        // Fetch tag keys with most series
        fetch('http://localhost:5122/metrics-explorer/api/v1/tag-keys-with-most-series', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ startEpoch, endEpoch, limit: 10 })
        })
            .then(response => response.json())
            .then(data => {
                tagKeysGridOptions.api.setRowData(data.tagKeys);
            });

        // Fetch tag pairs with most series
        fetch('http://localhost:5122/metrics-explorer/api/v1/tag-pairs-with-most-series', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ startEpoch, endEpoch, limit: 10 })
        })
            .then(response => response.json())
            .then(data => {
                tagPairsGridOptions.api.setRowData(data.tagPairs);
            });

        // Fetch tag keys with most unique values
        fetch('http://localhost:5122/metrics-explorer/api/v1/tag-keys-with-most-values', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify({ startEpoch, endEpoch, limit: 10 })
        })
            .then(response => response.json())
            .then(data => {
                tagKeysValuesGridOptions.api.setRowData(data.tagKeys);
            });
    }

    function updateSelectedTimeRange(element) {
        document.querySelectorAll('.range-item').forEach(item => {
            item.classList.remove('active');
        });
        element.classList.add('active');
        selectedTimeRange.textContent = element.textContent;
    }

    document.querySelectorAll('.range-item').forEach(item => {
        item.addEventListener('click', function () {
            var startEpoch = this.id;
            var endEpoch = 'now';
            fetchData(startEpoch, endEpoch);
            updateSelectedTimeRange(this);
        });
    });

    document.getElementById('customrange-btn').addEventListener('click', function () {
        var startEpoch = document.getElementById('date-start').value + 'T' + document.getElementById('time-start').value + ':00Z';
        var endEpoch = document.getElementById('date-end').value + 'T' + document.getElementById('time-end').value + ':00Z';
        fetchData(startEpoch, endEpoch);
        selectedTimeRange.textContent = 'Custom Range';
    });

    // Initial data fetch for default time range
    fetchData('now-30d', 'now');
});
