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

let EventCountChart;
 
$(document).ready(() => {
    $('#app-content-area').hide();
    setupEventHandlers();
    $('.theme-btn').on('click', themePickerHandler);
    $('.theme-btn').on('click', renderChart);
    $('#empty-response').empty();
    $('#empty-response').hide();

    let stDate = "now-7d";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', iStatsDatePickerHandler);

    // Make api call to get the cluster stats
    let data = getTimeRange();
    renderClusterStatsTables();
    renderChart();
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    {{ .Button1Function }}
});

function iStatsDatePickerHandler(evt) {
    evt.preventDefault();
    renderChart();
    $('#daterangepicker').hide();
}

function getTimeRange() {
    return {
        'startEpoch': filterStartDate || "now-7d",
        'endEpoch': filterEndDate || "now",
    };
}

function renderChart() {
    let endDate = filterEndDate || "now";
    let stDate = filterStartDate || "now-7d";
    let data = getTimeRange();

    $.ajax({
        method: 'post',
        url: 'api/clusterIngestStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(data)
    })
        .then((res)=> {
            $('#app-content-area').show();
            drawStatsChart(res,data)
        })
        .catch(showCStatsError);
}

function drawStatsChart(res,data) {
    let gridLineColor;
    let tickColor;
    if ($('body').attr('data-theme') == "light") {
        gridLineColor = "#DCDBDF";
        tickColor = "#160F29";
    }
    else {
        gridLineColor = "#383148";
        tickColor = "#FFFFFF"
    }
    var GBCountData = [];
    var EventCountData = [];
    _.forEach(res, (mvalue, key) => {
        if (key === "chartStats") {
            _.forEach(mvalue, (val, bucketKey) => 
            {
                var dataPointsPerMin;
                if(data.startEpoch==='now-24h'){
                    dataPointsPerMin = (val.MetricsCount/60);
                }
                else{
                    dataPointsPerMin = (val.MetricsCount/(60*24));
                }

                GBCountData.push({
                    x: bucketKey,
                    y: val.GBCount
                }),
                EventCountData.push({
                    x: bucketKey,
                    y: val.EventCount
                })
            })
            if (GBCountChart !== undefined) {
                GBCountChart.destroy();
            }
            if (EventCountChart !== undefined) {
                EventCountChart.destroy();
            }
            GBCountChart = renderGBCountChart(GBCountData,gridLineColor,tickColor);
            EventCountChart=renderEventCountChart(EventCountData,gridLineColor,tickColor);
        }
    })
}

function renderGBCountChart(GBCountData,gridLineColor,tickColor) {
    var GBCountChartCanvas = $("#GBCountChart").get(0).getContext("2d");
   
    GBCountChart = new Chart(GBCountChartCanvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Ingestion Volume',
                    data: GBCountData,
                    borderColor: ['rgb(99,71,217)'],
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 10,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },

            ]
        },
        options: {
            responsive: true,
            interaction: {
                intersect: false,
                mode: 'index',
              },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            let label = context.dataset.label || '';
                            if (context.parsed.y !== null ) {
                                let f = context.parsed.y;
                                if (context.parsed.y >=10){
                                    f = Number((context.parsed.y).toFixed()).toLocaleString("en-us")
                                    label += ' ' + (f) + ' GB';
                                }
                                else{
                                    label += ' ' + (f).toFixed(3) + ' GB';

                                }
                            }
                            return label;
                        }
                    },
                },
                legend: {
                    display: false
                },
            },
            scales: {
                y: {
                    ticks: {
                        callback: function (value, index, ticks) {
                            return (value).toFixed(3) + ' GB';
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Ingestion Volume'
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        callback: function (val, index, ticks) {
                            let value = this.getLabelForValue(val);
                            if (value && value.indexOf('T') > -1) {
                                let parts = value.split('T');
                                let xVal = "T" + parts[1];
                                return xVal;
                            } else {
                                if (value) {
                                    let parts = value.split('-');
                                    let xVal = parts[1] + "-" + parts[2];
                                    return xVal;
                                }
                            }
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Time Period'
                    },
                    grid: {
                        color: gridLineColor,
                    },
                }
            }
        }
    });
    return GBCountChart;
}

function renderEventCountChart(EventCountData,gridLineColor,tickColor){
    var EventCountCanvas = $("#EventCountChart").get(0).getContext("2d");

    EventCountChart = new Chart(EventCountCanvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Event Count',
                    data: EventCountData,
                    borderColor: ['rgb(99,71,217)'],
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 10,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },

            ]
        },
        options: {
            responsive: true,
            interaction: {
                intersect: false,
                mode: 'index',
              },
            plugins: {
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            let label = context.dataset.label || '';
                            if (context.parsed.y !== null ) {
                                label += ' ' + parseInt(context.parsed.y).toLocaleString();
                            }
                            return label;
                        }
                    },
                },
                legend: {
                    display: false
                 },
            },
            scales: {
                y: {
                    ticks: {
                        callback: function (value, index, ticks) {
                            return parseInt(value).toLocaleString();
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Event Count'
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        callback: function (val, index, ticks) {
                            let value = this.getLabelForValue(val);
                            if (value && value.indexOf('T') > -1) {
                                let parts = value.split('T');
                                let xVal = "T" + parts[1];
                                return xVal;
                            } else {
                                if (value) {
                                    let parts = value.split('-');
                                    let xVal = parts[1] + "-" + parts[2];
                                    return xVal;
                                }
                            }
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Time Period'
                    },
                    grid: {
                        color: gridLineColor,
                    },
                }
            }
        }
    });
    return EventCountChart;
}

function drawTotalStatsChart(res) {
    var totalIncomingVolume, totalIncomingVolumeMetrics;
    var totalStorageUsed;
    var totalStorageSaved;
    var totalStorageUsedMetrics;
    _.forEach(res, (mvalue, key) => {
        if (key === "ingestionStats") {
            
            _.forEach(mvalue, (v, k) => {
                if (k === 'Log Incoming Volume'){
                    totalIncomingVolume = v;
                }
                else if (k === 'Metrics Incoming Volume'){
                    totalIncomingVolumeMetrics = v;
                }
                else if (k === 'Log Storage Used'){
                    totalStorageUsed = v;
                }
                else if (k === 'Storage Saved'){
                    totalStorageSaved = v;
                }
                else if (k === 'Metrics Storage Used'){
                    totalStorageUsedMetrics = v;
                }
            });
            if (TotalVolumeChart !== undefined) {
                TotalVolumeChart.destroy();
            }

            TotalVolumeChart = renderTotalCharts(totalIncomingVolume, totalIncomingVolumeMetrics, totalStorageUsed, totalStorageUsedMetrics)
                return TotalVolumeChart
        }
    });

    let el = $('.storage-savings-container');
    el.append(`<div class="storage-savings-percent">${Math.round(totalStorageSaved * 10) / 10}%`);

    
}

function renderTotalCharts(totalIncomingVolume, totalIncomingVolumeMetrics, totalStorageUsed, totalStorageUsedMetrics) {
    var TotalVolumeChartCanvas = $("#TotalVolumeChart").get(0).getContext("2d");
    TotalVolumeChart = new Chart(TotalVolumeChartCanvas, {
        type: 'bar',
        data: {
            labels: ['Incoming Volume','Storage Used'],
            datasets: [
                {
                    
                    label: 'Logs' ,
                    data: [parseFloat(totalIncomingVolume),parseFloat(totalStorageUsed)],
                    backgroundColor: ['rgba(99, 72, 217)'],
                    borderWidth: 1,
                    categoryPercentage: 0.8,
                    barPercentage: 0.8,
                    
                },
                {
                    label:'Metrics' ,
                    data: [parseFloat(totalIncomingVolumeMetrics),parseFloat(totalStorageUsedMetrics)],
                    backgroundColor: ['rgb(255,1,255)'],
                    borderWidth: 1, 
                    categoryPercentage: 0.8,
                    barPercentage: 0.8,
                    
                },
            ]
        },
        options: {  
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top'
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            let label = context.dataset.label || '';
                            if (context.parsed.y !== null) {
                                label += ' ' + (context.parsed.y).toFixed(3) + ' GB';
                            }
                            return label;
                        }
                    },
                },
            },
            scales: {
                y: {
                    ticks: {
                        callback: function (value, index, ticks) {
                            return (value).toFixed(3) + ' GB';
                        }
                    },
               
                },
                x: {
                    ticks: {
                        callback: function (val, index, ticks) {
                            let value = this.getLabelForValue(val);
                            if (value && value.indexOf('T') > -1) {
                                let parts = value.split('T');
                                let xVal = "T" + parts[1];
                                return xVal;
                            } else {
                                if (value) {
                                    let parts = value.split('-');
                                    if (parts.length > 1) {
                                        let xVal = parts[1] + "-" + parts[2];
                                        return xVal;
                                    }
                                }
                            }
                        }
                    },
                    title: {
                        display: true,
                        text: ''
                    },
                 
                }
            }
        }
    });
    return TotalVolumeChart
}

function processClusterStats(res) {
    {{ .ClusterStatsSetUserRole }}
    _.forEach(res, (value, key) => {
        if (key === "ingestionStats") {
            let table = $('#ingestion-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');
                tr.append('<td class="health-stats-value">' + v + '</td>');
                table.find('tbody').append(tr);
            })
        }
        if (key === "metricsStats") {
            let table = $('#metrics-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');
                tr.append('<td class="health-stats-value">' + v + '</td>');
                table.find('tbody').append(tr);
            })
        }
        if (key === "queryStats") {
            let table = $('#query-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');
                if (k === "Average Latency") {
                    const numericPart = parseFloat(v); 
                    const avgLatency = Math.round(numericPart); 
                    tr.append('<td class="health-stats-value">' + avgLatency + ' ms</td>');
                }
                else 
                    tr.append('<td class="health-stats-value">' + v.toLocaleString() + '</td>');
                table.find("tbody").append(tr);
            });
        }
    })

    let columnOrder = [
        'Index Name',
        'Incoming Volume',
        'Event Count',
    ];

    {{ .ClusterStatsAdminView }}

    let indexdataTableColumns = columnOrder.map((columnName, index) => {
        let title = `<div class="grid"><div>${columnName}&nbsp;</div><div><i data-index="${index}"></i></div></div>`;
        return {
            title: title,
            name: columnName,
            visible: true,
            defaultContent: ``,
        };
    });

    const commonDataTablesConfig = {
        bPaginate: true,
        columns: indexdataTableColumns,
        autoWidth: false,
        colReorder: false,
        scrollX: false,
        deferRender: true,
        scrollY: 500,
        scrollCollapse: true,
        scroller: true,
        lengthChange: false,
        searching: false,
        order: [],
        columnDefs: [],
        data: []
    };
    
    let indexDataTable = $('#index-data-table').DataTable(commonDataTablesConfig);
    let metricsDataTable = $('#metrics-data-table').DataTable(commonDataTablesConfig);
    
    function displayIndexDataRows(res) {
        let totalIngestVolume = 0;
        let totalEventCount = 0;
        let totalValRow = [];
        totalValRow[0] = `Total`;
        totalValRow[1] = `${Number(`${totalIngestVolume >= 10 ? totalIngestVolume.toFixed().toLocaleString("en-US") : totalIngestVolume}`)} GB`;
        totalValRow[2] = `${totalEventCount.toLocaleString()}`;
        indexDataTable.row.add(totalValRow);
        if (res.indexStats && res.indexStats.length > 0) {
            res.indexStats.map((item) => {
                _.forEach(item, (v, k) => {
                    let currRow = [];
                    currRow[0] = k;
                    let l = parseFloat(v.ingestVolume)
                    currRow[1] = Number(`${l >= 10 ? l.toFixed().toLocaleString("en-US") : l}`) + '  GB';
                    currRow[2] = `${v.eventCount}`;
                    {{ .ClusterStatsAdminButton }}

                    totalIngestVolume += parseFloat(`${v.ingestVolume}`);
                    totalEventCount += parseInt(`${v.eventCount}`.replaceAll(',',''));

                    indexDataTable.row.add(currRow);
                });
            })
        }
        if (res.metricsStats) {
            let currRow = [];
            currRow[0] = `metrics`;
            let q = parseFloat(res.metricsStats["Incoming Volume"])
            currRow[1] = (Number(q >= 10 ? q.toFixed() : q)).toLocaleString("en-US") + '  GB';
            currRow[2] = `${res.metricsStats["Datapoints Count"]}`;
            metricsDataTable.row.add(currRow);

        }
      
        totalIngestVolume = Math.round(parseFloat(`${res.ingestionStats["Log Incoming Volume"]}`) * 1000)/1000
        totalValRow[1] = `${Number(`${totalIngestVolume >= 10 ? totalIngestVolume.toFixed().toLocaleString("en-US") : totalIngestVolume}`)} GB`;
        totalValRow[2] = `${totalEventCount.toLocaleString()}`;
        indexDataTable.draw();
        metricsDataTable.draw();
    }

    {{ if .ClusterStatsCallDisplayRows }}
        {{ .ClusterStatsCallDisplayRows }}
    {{ else }}
        setTimeout(() => {
            displayIndexDataRows(res);
        }, 0);
    {{ end }}

}



function renderClusterStatsTables() {
    {{ .ClusterStatsSetUserRole }}
    {{ .ClusterStatsExtraFunctions }}
    $.ajax({
        method: 'get',
        url: 'api/clusterStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        crossDomain: true,
        dataType: 'json',
    }).then(function (res) {
        $('#empty-response').empty();
        $('#empty-response').hide();
        drawTotalStatsChart(res);
        {{ .ClusterStatsExtraSetup }}
            processClusterStats(res);
        $('#app-content-area').show();
    }).catch(showCStatsError);
}

function showCStatsError(res) {
    if(res.status == 400) {
        $('#empty-response').html('Permission Denied');
        $('#empty-response').show();
        $('#app-content-area').hide();
    }
}
