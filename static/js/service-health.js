/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

'use strict';

let redMetricsData ={
    "indexName":  "red-traces",
}
$(document).ready(() => {
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $(".theme-btn").on("click", themePickerHandler);

    let stDate = "now-15m";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', isServiceHealthDatePickerHandler);
    let data = getTimeRange();
    redMetricsData = {... redMetricsData, ... data};
    getAllServices();
    
    $('.search-input').on('input', filterServicesBySearch);
});

function isServiceHealthDatePickerHandler(evt) {
    evt.preventDefault();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), "now", $(this).attr('id'))
    getAllServices();
    $('#daterangepicker').hide();
}

function getTimeRange() {
    return {
        'startEpoch': filterStartDate || "now-1h",
        'endEpoch': filterEndDate || "now",
    };
}
let gridDiv = null;
let serviceRowData = [];
const columnDefs=[
    { headerName: "Service", field: "service"},
    { headerName: "Rate (Request per Second)", field: "rate"},
    { headerName: "Error (% of Rate)", field: "error"},
    { headerName: 'P50 (in ms)', field: 'p50' },
    { headerName: 'P90 (in ms)', field: 'p90' },
    { headerName: 'P99 (in ms)', field: 'p99' },
];

const gridOptions = {
    rowData: serviceRowData ,
    onRowClicked: onRowClicked,
    headerHeight:32,
    rowHeight: 42,
    defaultColDef: {
    cellClass: 'align-center-grid',
      resizable: true,
      sortable: true,
      animateRows: true,
      readOnlyEdit: true,
      autoHeight: true,
      icons: {
          sortAscending: '<i class="fa fa-sort-alpha-down"/>',
          sortDescending: '<i class="fa fa-sort-alpha-up"/>',
        },
    },
    columnDefs:columnDefs,
};

function filterServicesBySearch() {
    const searchValue = $('.search-input').val().toLowerCase();
    const filteredData = serviceRowData.filter(service => 
        service.service.toLowerCase().startsWith(searchValue)
    );
    gridOptions.api.setRowData(filteredData);
}

function processRedMetricsData(metricsData) {
    let latestMetrics = {};
    metricsData.forEach(metric => {
        const serviceName = metric.service;
        const metricTimestamp = metric.timestamp;

        if (!latestMetrics[serviceName] || latestMetrics[serviceName].timestamp < metricTimestamp) {
            latestMetrics[serviceName] = metric;
        }
    });

    return Object.values(latestMetrics);
}

function getAllServices(){
    data = getTimeRange();
    redMetricsData = {... redMetricsData, ... data}
    $.ajax({
        method: "POST",
        url: "api/search",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify(redMetricsData),
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        const processedData = processRedMetricsData(res.hits.records);
        displayServiceHealthTable(processedData);
    })
}

function displayServiceHealthTable(res){
    if (gridDiv === null) {
        gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, gridOptions);
    }
    gridOptions.api.setColumnDefs(columnDefs);
    let newRow = new Map()
    serviceRowData=[]
    $.each(res, function (key, value) {
        newRow.set("rowId", key);
        newRow.set("service", value.service);
        newRow.set("rate", (Math.abs(value.rate) % 1 === 0 ? Math.abs(value.rate) : Number(value.rate).toFixed(2)).toLocaleString("en-US"));
        newRow.set("error", (Math.abs(value.error_rate) % 1 === 0 ? Math.abs(value.error_rate) : Number(value.error_rate).toFixed(2)).toLocaleString("en-US"));
        newRow.set("p50", value.p50.toLocaleString("en-US"));
        newRow.set("p90", value.p90.toLocaleString("en-US"));
        newRow.set("p99", value.p99.toLocaleString("en-US"));

        serviceRowData = _.concat(serviceRowData, Object.fromEntries(newRow));
    })
    gridOptions.api.setRowData(serviceRowData);
    gridOptions.api.sizeColumnsToFit();
    gridOptions.columnApi.applyColumnState({
        state: [{ colId: 'error', sort: 'desc' }],
        defaultState: { sort: null },
    });
}

function onRowClicked(event) {
    const serviceName = event.data.service; 
    window.location.href = 'service-health-overview.html?service=' + encodeURIComponent(serviceName);
}


