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

let EventCountChart;
let TotalVolumeChartLogs;
let TotalVolumeChartMetrics;
let TotalVolumeChartTraces;
$(document).ready(() => {
    $('#app-content-area').hide();
    setupEventHandlers();
    $('.theme-btn').on('click', themePickerHandler);
    $('.theme-btn').on('click', renderChart);
    $('#empty-response').empty();
    $('#empty-response').hide();

    let stDate = 'now-7d';
    let endDate = 'now';
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', iStatsDatePickerHandler);

    // Make api call to get the cluster stats
    renderClusterStatsTables();
    renderChart();
    $('#cancel-del-index-btn, .usage-stats .popupOverlay').on('click', hidePopUpsOnUsageStats);
    $('.toast-close').on('click', removeToast);
    {{ .Button1Function }}
});

function iStatsDatePickerHandler(evt) {
    evt.preventDefault();
    renderChart();
    $('#daterangepicker').hide();
}

function getTimeRange() {
    return {
        startEpoch: filterStartDate || 'now-7d',
        endEpoch: filterEndDate || 'now',
    };
}

function renderChart() {
    let data = getTimeRange();

    $.ajax({
        method: 'post',
        url: 'api/clusterIngestStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(data),
    })
        .then((res) => {
            $('#app-content-area').show();
            return Promise.all([drawStatsChart(res, data, 'logs'), drawStatsChart(res, data, 'metrics'), drawStatsChart(res, data, 'trace')]);
        })
        .catch(showCStatsError);
}

function drawStatsChart(res, data, chartType) {
    return new Promise((resolve) => {
        let gridLineColor;
        let tickColor;
        if ($('html').attr('data-theme') == 'light') {
            gridLineColor = '#DCDBDF';
            tickColor = '#160F29';
        } else {
            gridLineColor = '#383148';
            tickColor = '#FFFFFF';
        }
        var GBCountData = [];
        var EventCountData = [];
        _.forEach(res, (mvalue, key) => {
            if (key === 'chartStats') {
                _.forEach(mvalue, (val, bucketKey) => {
                    if (chartType === 'logs') {
                        GBCountData.push({
                            x: bucketKey,
                            y: val.LogsGBCount,
                        }),
                            EventCountData.push({
                                x: bucketKey,
                                y: val.LogsEventCount,
                            });
                    } else if (chartType === 'metrics') {
                        GBCountData.push({
                            x: bucketKey,
                            y: val.MetricsGBCount,
                        }),
                            EventCountData.push({
                                x: bucketKey,
                                y: val.MetricsDatapointsCount,
                            });
                    } else if (chartType === 'trace') {
                        GBCountData.push({
                            x: bucketKey,
                            y: val.TraceGBCount,
                        }),
                            EventCountData.push({
                                x: bucketKey,
                                y: val.TraceSpanCount,
                            });
                    }
                });
                // Destroy only the relevant charts to prevent overwriting
                if (window[chartType + 'GBCountChart'] !== undefined) {
                    window[chartType + 'GBCountChart'].destroy();
                }
                if (window[chartType + 'EventCountChart'] !== undefined) {
                    window[chartType + 'EventCountChart'].destroy();
                }

                // Create charts and store in a global variable scoped by chart type
                window[chartType + 'GBCountChart'] = renderGBCountChart(GBCountData, gridLineColor, tickColor, chartType);
                window[chartType + 'EventCountChart'] = renderEventCountChart(EventCountData, gridLineColor, tickColor, chartType);
            }
        });
        resolve();
    });
}

function renderGBCountChart(GBCountData, gridLineColor, tickColor, chartType) {
    var GBCountChartCanvas = $('#GBCountChart-' + chartType)
        .get(0)
        .getContext('2d');

    var GBCountChart = new Chart(GBCountChartCanvas, {
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
            ],
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
                            if (context.parsed.y !== null) {
                                let f = context.parsed.y;
                                if (context.parsed.y >= 10) {
                                    f = Number(context.parsed.y.toFixed()).toLocaleString('en-us');
                                    label += ' ' + f + ' GB';
                                } else {
                                    label += ' ' + f.toFixed(3) + ' GB';
                                }
                            }
                            return label;
                        },
                    },
                },
                legend: {
                    display: false,
                },
            },
            scales: {
                y: {
                    ticks: {
                        callback: function (value, _index, _ticks) {
                            return value.toFixed(3) + ' GB';
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    type: 'linear',
                    display: true,
                    position: 'left',
                    title: {
                        display: true,
                        text: 'Ingestion Volume',
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        callback: function (val, _index, _ticks) {
                            let value = this.getLabelForValue(val);
                            if (value && value.indexOf('T') > -1) {
                                let parts = value.split('T');
                                let xVal = 'T' + parts[1];
                                return xVal;
                            } else {
                                if (value) {
                                    let parts = value.split('-');
                                    let xVal = parts[1] + '-' + parts[2];
                                    return xVal;
                                }
                            }
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Time Period',
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
            },
        },
    });
    return GBCountChart;
}

function renderEventCountChart(EventCountData, gridLineColor, tickColor, chartType) {
    var EventCountCanvas = $('#EventCountChart-' + chartType)
        .get(0)
        .getContext('2d');

    EventCountChart = new Chart(EventCountCanvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: chartType === 'metrics' ? 'Metrics datapoints count' : chartType === 'logs' ? 'Event Count' : 'Span Count',
                    data: EventCountData,
                    borderColor: ['rgb(99,71,217)'],
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 10,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },
            ],
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
                            if (context.parsed.y !== null) {
                                label += ' ' + parseInt(context.parsed.y).toLocaleString();
                            }
                            return label;
                        },
                    },
                },
                legend: {
                    display: false,
                },
            },
            scales: {
                y: {
                    ticks: {
                        callback: function (value, _index, _ticks) {
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
                        text: chartType === 'metrics' ? 'Metrics datapoints count' : chartType === 'logs' ? 'Event Count' : 'Span Count',
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        callback: function (val, _index, _ticks) {
                            let value = this.getLabelForValue(val);
                            if (value && value.indexOf('T') > -1) {
                                let parts = value.split('T');
                                let xVal = 'T' + parts[1];
                                return xVal;
                            } else {
                                if (value) {
                                    let parts = value.split('-');
                                    let xVal = parts[1] + '-' + parts[2];
                                    return xVal;
                                }
                            }
                        },
                        color: tickColor,
                    },
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Time Period',
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
            },
        },
    });
    return EventCountChart;
}

function drawTotalStatsChart(res) {
    var totalIncomingVolume, totalIncomingVolumeMetrics, totalIncomingVolumeTrace;
    var totalStorageUsed;
    var logStorageSaved, metricsStorageSaved, traceStorageSaved;
    var totalStorageUsedMetrics, totalStorageUsedTrace;
    _.forEach(res, (mvalue, key) => {
        if (key === 'ingestionStats') {
            _.forEach(mvalue, (v, k) => {
                if (k === 'Log Incoming Volume') {
                    totalIncomingVolume = v;
                } else if (k === 'Metrics Incoming Volume') {
                    totalIncomingVolumeMetrics = v;
                } else if (k === 'Log Storage Used') {
                    totalStorageUsed = v;
                } else if (k === 'Logs Storage Saved') {
                    logStorageSaved = v;
                } else if (k === 'Metrics Storage Saved') {
                    metricsStorageSaved = v;
                } else if (k === 'Metrics Storage Used') {
                    totalStorageUsedMetrics = v;
                }
            });
            if (TotalVolumeChartLogs !== undefined) {
                TotalVolumeChartLogs.destroy();
            }
            if (TotalVolumeChartMetrics !== undefined) {
                TotalVolumeChartMetrics.destroy();
            }

            TotalVolumeChartLogs = renderTotalCharts('Logs', totalIncomingVolume, totalStorageUsed);
            TotalVolumeChartMetrics = renderTotalCharts('Metrics', totalIncomingVolumeMetrics, totalStorageUsedMetrics);
        } else if (key === 'traceStats') {
            _.forEach(mvalue, (v, k) => {
                if (k === 'Trace Storage Saved') {
                    traceStorageSaved = v;
                } else if (k === 'Total Trace Volume') {
                    totalIncomingVolumeTrace = v;
                } else if (k === 'Trace Storage Used') {
                    totalStorageUsedTrace = v;
                }
            });
            if (TotalVolumeChartTraces !== undefined) {
                TotalVolumeChartTraces.destroy();
            }
            TotalVolumeChartTraces = renderTotalCharts('Traces', totalIncomingVolumeTrace, totalStorageUsedTrace);
        }
    });
    let elLogs = $('.logs-container .storage-savings-container');
    let elMetrics = $('.metrics-container .storage-savings-container');
    let elTraces = $('.traces-container .storage-savings-container');
    elLogs.append(`<div class="storage-savings-percent">${Math.round(logStorageSaved * 10) / 10}%`);
    elMetrics.append(`<div class="storage-savings-percent">${Math.round(metricsStorageSaved * 10) / 10}%`);
    elTraces.append(`<div class="storage-savings-percent">${Math.round(traceStorageSaved * 10) / 10}%`);
}

function renderTotalCharts(label, totalIncomingVolume, totalStorageUsed) {
    var TotalVolumeChartCanvas = $(`#TotalVolumeChart-${label.toLowerCase()}`).get(0).getContext('2d');
    var TotalVolumeChart = new Chart(TotalVolumeChartCanvas, {
        type: 'bar',
        data: {
            labels: ['Incoming Volume', 'Storage Used'],
            datasets: [
                {
                    label: label,
                    data: [parseFloat(totalIncomingVolume), parseFloat(totalStorageUsed)],
                    backgroundColor: ['rgba(147, 112, 219)', 'rgba(181, 126, 220, 1)'],
                    borderWidth: 1,
                    categoryPercentage: 0.8,
                    barPercentage: 0.8,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: true,
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            let label = context.dataset.label || '';
                            if (context.parsed.y !== null) {
                                label += ' ' + context.parsed.y.toFixed(3) + ' GB';
                            }
                            return label;
                        },
                    },
                },
            },
            scales: {
                y: {
                    ticks: {
                        callback: function (value, _index, _ticks) {
                            return value.toFixed(3) + ' GB';
                        },
                    },
                },
                x: {
                    ticks: {
                        callback: function (_val, index, _ticks) {
                            return ['Incoming Volume', 'Storage Used'][index];
                        },
                    },
                    title: {
                        display: true,
                        text: '',
                    },
                },
            },
        },
    });
    return TotalVolumeChart;
}

function processClusterStats(res) {
    {{ .ClusterStatsSetUserRole }}
    _.forEach(res, (value, key) => {
        if (key === 'ingestionStats') {
            let table = $('#ingestion-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');
                tr.append('<td class="health-stats-value">' + v + '</td>');
                table.find('tbody').append(tr);
            });
        }
        if (key === 'metricsStats') {
            let table = $('#metrics-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');
                tr.append('<td class="health-stats-value">' + v + '</td>');
                table.find('tbody').append(tr);
            });
        }
        if (key === 'queryStats') {
            let table = $('#query-table');
            _.forEach(value, (v, k) => {
                let tr = $('<tr>');
                tr.append('<td>' + k + '</td>');
                if (k === 'Average Latency') {
                    const numericPart = parseFloat(v);
                    const avgLatency = Math.round(numericPart);
                    tr.append('<td class="health-stats-value">' + avgLatency + ' ms</td>');
                } else tr.append('<td class="health-stats-value">' + v.toLocaleString() + '</td>');
                table.find('tbody').append(tr);
            });
        }
    });

    let indexColumnOrder = ['Index Name', 'Incoming Volume', 'Event Count', 'Segment Count', ''];
    let metricsColumnOrder = ['Index Name', 'Incoming Volume', 'Datapoint Count'];
    let traceColumnOrder = ['Index Name', 'Incoming Volume', 'Span Count', 'Segment Count'];

    let indexDataTableColumns = indexColumnOrder.map((columnName, index) => {
        let title = `<div class="grid"><div>${columnName}&nbsp;</div><div><i data-index="${index}"></i></div></div>`;
        return {
            title: title,
            name: columnName,
            visible: true,
            defaultContent: ``,
        };
    });

    let metricsDataTableColumns = metricsColumnOrder.map((columnName, index) => {
        let title = `<div class="grid"><div>${columnName}&nbsp;</div><div><i data-index="${index}"></i></div></div>`;
        return {
            title: title,
            name: columnName,
            visible: true,
            defaultContent: ``,
        };
    });

    let tracesDataTableColumns = traceColumnOrder.map((columnName, index) => {
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
        data: [],
    };

    let indexDataTable = $('#index-data-table').DataTable({
        ...commonDataTablesConfig,
        columns: indexDataTableColumns,
    });

    let metricsDataTable = $('#metrics-data-table').DataTable({
        ...commonDataTablesConfig,
        columns: metricsDataTableColumns,
    });

    let traceDataTable = $('#trace-data-table').DataTable({
        ...commonDataTablesConfig,
        columns: tracesDataTableColumns,
    });

    function displayIndexDataRows(res) {
        let totalIngestVolume = 0;
        let totalEventCount = 0;
        let totalLogSegmentCount = 0;
        let totalTraceSegmentCount = 0;
        let totalValRow = [];
        totalValRow[0] = `Total`;
        totalValRow[1] = `${Number(`${totalIngestVolume >= 10 ? totalIngestVolume.toFixed().toLocaleString('en-US') : totalIngestVolume}`)} GB`;
        totalValRow[2] = `${totalEventCount.toLocaleString()}`;
        indexDataTable.row.add(totalValRow);
        if (res.indexStats && res.indexStats.length > 0) {
            res.indexStats.map((item) => {
                _.forEach(item, (v, k) => {
                    let currRow = [];
                    currRow[0] = k;
                    let l = parseFloat(v.ingestVolume);
                    currRow[1] = Number(`${l >= 10 ? l.toFixed().toLocaleString('en-US') : l}`) + '  GB';
                    currRow[2] = `${v.eventCount}`;
                    currRow[3] = `${v.segmentCount}`;
                    currRow[4] = `<button class="btn-simple index-del-btn" id="index-del-btn-${k}"></button>`;

                    totalIngestVolume += parseFloat(`${v.ingestVolume}`);
                    totalEventCount += parseInt(`${v.eventCount}`.replaceAll(',', ''));
                    totalLogSegmentCount += parseInt(`${v.segmentCount}`.replaceAll(',', ''));

                    indexDataTable.row.add(currRow);
                });
            });
        }

        if (res.metricsStats) {
            let currRow = [];
            currRow[0] = `metrics`;
            let q = parseFloat(res.metricsStats['Incoming Volume']);
            currRow[1] = Number(q >= 10 ? q.toFixed() : q).toLocaleString('en-US') + '  GB';
            currRow[2] = `${res.metricsStats['Datapoints Count']}`;
            metricsDataTable.row.add(currRow);
        }

        let totalValRowTrace = [];
        totalValRowTrace[0] = `Total`;
        totalValRowTrace[1] = res.traceStats['Total Trace Volume'];
        totalValRowTrace[2] = res.traceStats['Trace Span Count'];
        traceDataTable.row.add(totalValRowTrace);
        if (res.traceIndexStats && res.traceIndexStats.length > 0) {
            res.traceIndexStats.map((item) => {
                _.forEach(item, (v, k) => {
                    let currRow = [];
                    currRow[0] = k;
                    let l = parseFloat(v.traceVolume);
                    currRow[1] = Number(`${l >= 10 ? l.toFixed().toLocaleString('en-US') : l}`) + '  GB';
                    currRow[2] = `${v.traceSpanCount}`;
                    currRow[3] = `${v.segmentCount}`;
                    totalTraceSegmentCount += parseInt(v.segmentCount);
                    traceDataTable.row.add(currRow);
                });
            });
        }
        totalValRowTrace[3] = totalTraceSegmentCount.toLocaleString(); 
        totalIngestVolume = Math.round(parseFloat(`${res.ingestionStats['Log Incoming Volume']}`) * 1000) / 1000;
        totalValRow[1] = `${Number(`${totalIngestVolume >= 10 ? totalIngestVolume.toFixed().toLocaleString('en-US') : totalIngestVolume}`)} GB`;
        totalValRow[2] = `${totalEventCount.toLocaleString()}`;
        totalValRow[3] = `${totalLogSegmentCount.toLocaleString()}`;
        indexDataTable.draw();
        metricsDataTable.draw();
        traceDataTable.draw();
    }
    let currRowIndex = null;

    setTimeout(() => {
        displayIndexDataRows(res);
        $('#index-data-table tbody').on('click', 'button', function () {
            currRowIndex = $(this).closest('tr').index();
        });
        let delBtns = $('#index-data-table tbody button');
        delBtns.each((i, btn) => {
            let indexName = $(btn).attr('id').split('index-del-btn-')[1];
            $(btn).on('click', () => showDelIndexPopup(indexName, currRowIndex));
        });
    }, 0);

    function showDelIndexPopup(indexName) {
        let allowDelete = false;
        $('#del-index-name-input').keyup((e) => confirmIndexDeletion(e, indexName, allowDelete));
        $('#del-index-btn').attr('disabled', true);
        $('#del-index-name-input').val('');
        $('.popupOverlay, .popupContent').addClass('active');
        $('#confirm-del-index-prompt').show();
        $('.del-org-prompt-text-container span').html(indexName);
    }

    function confirmIndexDeletion(e, indexName, allowDelete) {
        if (e) e.stopPropagation();
        if (indexName && indexName.trim() === 'traces') {
            allowDelete = false;
        }
        if ($('#del-index-name-input').val().trim() === 'delete ' + indexName) {
            $('#del-index-btn').attr('disabled', false);
            allowDelete = true;
        } else {
            $('#del-index-btn').attr('disabled', true);
            allowDelete = false;
        }
        if (allowDelete) {
            $('#del-index-btn').off('click');
            $('#del-index-btn').on('click', () => deleteIndex(e, indexName));
        } else {
            $('#del-index-btn').off('click');
        }
    }

    function deleteIndex(e, indexName) {
        if (e) e.stopPropagation();
        $.ajax({
            method: 'post',
            url: 'api/deleteIndex/' + indexName,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
        })
            .then(function (_res) {
                hidePopUpsOnUsageStats();
                indexDataTable.row(`:eq(${currRowIndex})`).remove().draw();
                showDeleteIndexToast('Index Deleted Successfully');
            })
            .catch((_err) => {
                hidePopUpsOnUsageStats();
                showDeleteIndexToast('Error Deleting Index');
            });
    }
}

function hidePopUpsOnUsageStats() {
    $('.popupOverlay, .popupContent').removeClass('active');
    $('#confirm-del-index-prompt').hide();
    $('#del-index-name-input').val('');
    $('#del-index-btn').attr('disabled', true);
    $('#del-index-btn').off('click');
}

function renderClusterStatsTables() {
    {{ .ClusterStatsSetUserRole }}
    {{ .ClusterStatsExtraFunctions }}
    $.ajax({
        method: 'get',
        url: 'api/clusterStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    })
        .then(function (res) {
            $('#empty-response').empty();
            $('#empty-response').hide();
            drawTotalStatsChart(res);
            {{ .ClusterStatsExtraSetup }}
            processClusterStats(res);
            $('#app-content-area').show();
        })
        .catch(showCStatsError);
}

function showCStatsError(res) {
    if (res.status == 400) {
        $('#empty-response').html('Permission Denied');
        $('#empty-response').show();
        $('#app-content-area').hide();
    }
}
