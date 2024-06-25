let stDate = Cookies.get('startEpoch') || "now-3h";
let endDate = Cookies.get('endEpoch') || "now";
let allMetrics = [];  // To store all metrics initially

let gridDiv=null;
$(document).ready(() => {
    $('.inner-range #' + stDate).addClass('active');
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', isMetricsDatePickerHandler);
    let data = getTimeRange();
    fetchAllMetrics();  // Fetch all metrics on page load

    // Add event listener for the search input
    $('#metric-search-input').on('input', function() {
        filterMetrics();
    });
});

function isMetricsDatePickerHandler(evt) {
    evt.preventDefault();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), "now", $(this).attr('id'))
    fetchAllMetrics();
    $('#daterangepicker').hide();
}

function getTimeRange() {
    return {
        'startEpoch': filterStartDate || "now-1h",
        'endEpoch': filterEndDate || "now",
    };
}

function fetchAllMetrics() {
    const data = getTimeRange();
    const pl = {
        start: data.startEpoch,
        end: data.endEpoch,
    };
    $.ajax({
        method: "post",
        url: "metrics-explorer/api/v1/metric_names",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        crossDomain: true,
        dataType: "json",
        data: JSON.stringify(pl),
    }).then(function (res) {
        if (res && res.metricNames && Array.isArray(res.metricNames)) {
            allMetrics = res.metricNames;  // Store all metrics
            displaydata(allMetrics);
        } else {
            console.error('Invalid response format:', res);
        }
    });
}

function filterMetrics() {
    const searchTerm = $('#metric-search-input').val().trim().toLowerCase();
    const filteredMetrics = allMetrics.filter(metric => metric.toLowerCase().includes(searchTerm));
    displaydata(filteredMetrics);
}

function displaydata(metrics) {
    
    const metricRows = metrics.map(metric => ({ metricName: metric }));

    if (gridDiv === null) {
        gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, gridOptions);
    }

    gridOptions.api.setColumnDefs([{ headerName: "Metric Name", field: "metricName" }]);
    gridOptions.api.setRowData(metricRows);
    gridOptions.api.sizeColumnsToFit();
}

// AG Grid options with pagination and sorting icons enabled
var gridOptions = {
    headerHeight: 32,
    rowHeight:42,
    pagination: true,
    paginationAutoPageSize: true,
    defaultColDef: {
        sortable: true,
        filter: false,
        resizable:false,
        cellStyle: { 'text-align': "left" },
        minWidth: 120,
        animateRows: true,
        readOnlyEdit: true,
        autoHeight: true,
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-down"/>',
            sortDescending: '<i class="fa fa-sort-alpha-up"/>'
        },
    },
    onGridReady: function(params) {
        params.api.sizeColumnsToFit();
    },
    columnDefs: [
        { 
            headerName: "Metric Name", 
            field: "metricName",
            sort: 'asc' // Initial sort order set to ascending
        }
    ],
    rowData: []
};


