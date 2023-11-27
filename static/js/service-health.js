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
const tenMinutesAgo = new Date(Date.now() - 10 * 60000).toISOString();
let redMetricsData ={
    "indexName":  "red-traces",
    "startEpoch": tenMinutesAgo,
    "endEpoch": new Date().toISOString()
   
}
$(document).ready(() => {
    displayNavbar();
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $(".theme-btn").on("click", themePickerHandler);
    getAllServices()
    
    $('.search-input').on('input', filterServicesBySearch);
});

let gridDiv = null;
let serviceRowData = [];
const columnDefs=[
    { headerName: "Service", field: "service"},
    { headerName: "Rate", field: "rate"},
    { headerName: "Error", field: "error"},
    { headerName: 'P50', field: 'p50' },
    { headerName: 'P90', field: 'p90' },
    { headerName: 'P99', field: 'p99' },
];

const gridOptions = {
    rowData: serviceRowData ,
    defaultColDef: {
      cellStyle: { 'text-align': "left" },
      resizable: true,
      sortable: true,
      animateRows: true,
      readOnlyEdit: true,
      autoHeight: true,
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
        newRow.set("rate", value.rate);
        newRow.set("error", value.error_rate);
        newRow.set("p50", value.p50);
        newRow.set("p90", value.p90);
        newRow.set("p99", value.p99);

        serviceRowData = _.concat(serviceRowData, Object.fromEntries(newRow));
    })
    gridOptions.api.setRowData(serviceRowData);
    gridOptions.api.sizeColumnsToFit();
}


