/*
 * Copyright (c) 2021-2025 SigScalr, Inc.
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

$(document).ready(() => {
    setupEventHandlers();
    $('.theme-btn').on('click', themePickerHandler);
    $('.theme-btn').on('click', () => {
        const { gridLineColor, tickColor } = getGraphGridColors();
        updateChartsTheme(gridLineColor, tickColor);
    });

    let stDate = 'now-7d';
    let endDate = 'now';
    datePickerHandler(stDate, endDate, stDate);

    $('.range-item').on('click', getClusterIngestStats);
    $('#customrange-btn').on('click', getClusterIngestStats);

    getClusterIngestStats();
    getClusterStats();

    $('.granularity-tabs .tab').click(function () {
        $('.granularity-tabs .tab').removeClass('active');
        $(this).addClass('active');
        getClusterIngestStats();
    });
});

function getClusterStats() {
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
        .then((res) => {
            displayTotal(res);
        })
        .catch((err) => {
            console.error('error:', err);
        });
}

function displayTotal(res) {
    const totalLogsVol = res.ingestionStats['Log Incoming Volume']; // Bytes
    const totalTracesVol = res.traceStats['Total Trace Volume']; // Bytes
    const totalDatapoints = res.metricsStats['Datapoints Count'];
    //eslint-disable-next-line no-undef
    const formattedLogsVol = formatByteSize(totalLogsVol);
    //eslint-disable-next-line no-undef
    const formattedTracesVol = formatByteSize(totalTracesVol);

    $('.logs-total').text(formattedLogsVol);
    $('.traces-total').text(formattedTracesVol);
    $('.datapoints-total').text(totalDatapoints);
}

function setupClusterStatsCharts(data) {
    if (!data || !data.chartStats) {
        console.error('No chart data available');
        return;
    }

    // Process the data for charts
    const dates = Object.keys(data.chartStats).sort();
    const processedData = {
        logs: {
            dates: dates,
            gbCount: dates.map((date) => data.chartStats[date].LogsBytesCount),
        },
        traces: {
            dates: dates,
            gbCount: dates.map((date) => data.chartStats[date].TraceBytesCount),
        },
        stacked: {
            dates: dates,
            activeSeriesCount: dates.map((date) => data.chartStats[date].ActiveSeriesCount),
            metricsDatapointsCount: dates.map((date) => data.chartStats[date].MetricsDatapointsCount),
        },
    };

    const { gridLineColor, tickColor } = getGraphGridColors();

    renderLogsVolumeChart(processedData.logs, gridLineColor, tickColor);
    renderTracesVolumeChart(processedData.traces, gridLineColor, tickColor);
    renderStackedChart(processedData.stacked, gridLineColor, tickColor);
}

function updateChartsTheme(gridLineColor, tickColor) {
    if (window.logsVolumeChart) {
        updateChartColors(window.logsVolumeChart, gridLineColor, tickColor);
    }

    if (window.tracesVolumeChart) {
        updateChartColors(window.tracesVolumeChart, gridLineColor, tickColor);
    }

    if (window.stackedChart) {
        updateChartColors(window.stackedChart, gridLineColor, tickColor);
    }
}

function updateChartColors(chart, gridLineColor, tickColor) {
    chart.options.scales.x.grid.color = gridLineColor;
    chart.options.scales.x.ticks.color = tickColor;

    chart.options.scales.y.grid.color = gridLineColor;
    chart.options.scales.y.ticks.color = tickColor;

    chart.update();
}

function getGraphGridColors() {
    const rootStyles = getComputedStyle(document.documentElement);
    let isDarkTheme = document.documentElement.getAttribute('data-theme') === 'dark';
    const gridLineColor = isDarkTheme ? rootStyles.getPropertyValue('--black-3') : rootStyles.getPropertyValue('--white-1');
    const tickColor = isDarkTheme ? rootStyles.getPropertyValue('--white-0') : rootStyles.getPropertyValue('--white-6');

    return { gridLineColor, tickColor };
}

// Common chart configuration function to avoid repetition
function createVolumeChart(chartId, data, options) {
    const ctx = document.getElementById(chartId).getContext('2d');
    const { chartType, color, gridLineColor, tickColor, title } = options;

    // Destroy existing chart instance if it exists
    if (window[chartType] && typeof window[chartType].destroy === 'function') {
        window[chartType].destroy();
    }

    // Format data properly
    const bytesData = data.dates.map((date, index) => ({
        x: date,
        y: data.gbCount[index],
    }));

    // Determine appropriate scale
    //eslint-disable-next-line no-undef
    const scale = determineUnit(bytesData);

    // Scale the data
    const scaledData = bytesData.map((point) => ({
        x: point.x,
        y: point.y / scale.divisor,
    }));

    // Create chart configuration
    const chartConfig = {
        type: 'bar',
        data: {
            labels: data.dates,
            datasets: [
                {
                    label: title,
                    data: scaledData,
                    backgroundColor: color,
                    borderRadius: 4,
                    barPercentage: 0.5,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        label: function (context) {
                            let label = context.dataset.label || '';
                            if (context.parsed.y !== null) {
                                let value = context.parsed.y;
                                if (value >= 10) {
                                    value = Number(value.toFixed()).toLocaleString('en-us');
                                    label += ' ' + value + ' ' + scale.unit;
                                } else {
                                    label += ' ' + value.toFixed(2) + ' ' + scale.unit;
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
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: scale.unit,
                    },
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                        callback: function (value) {
                            return value + ' ' + scale.unit;
                        },
                    },
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time Period',
                    },
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                        callback: function (val, _index, _ticks) {
                            let value = this.getLabelForValue(val);
                            return formatDateLabel(value);
                        },
                    },
                },
            },
        },
    };

    // Create and return chart
    window[chartType] = new Chart(ctx, chartConfig);
    return window[chartType];
}

// Simplified volume chart functions using the common approach
function renderLogsVolumeChart(data, gridLineColor, tickColor) {
    return createVolumeChart('logsVolumeChart', data, {
        chartType: 'logsVolumeChart',
        color: '#36A2EB',
        gridLineColor,
        tickColor,
        title: 'Logs Volume',
    });
}

function renderTracesVolumeChart(data, gridLineColor, tickColor) {
    return createVolumeChart('tracesVolumeChart', data, {
        chartType: 'tracesVolumeChart',
        color: '#4BC0C0',
        gridLineColor,
        tickColor,
        title: 'Traces Volume',
    });
}

// Render stacked chart for active series and datapoint count
function renderStackedChart(data, gridLineColor, tickColor) {
    const ctx = document.getElementById('stackedChart').getContext('2d');

    // Destroy existing chart instance if it exists
    if (window.stackedChart && typeof window.stackedChart.destroy === 'function') {
        window.stackedChart.destroy();
    }

    window.stackedChart = new Chart(ctx, {
        type: 'line',
        data: {
            labels: data.dates,
            datasets: [
                {
                    label: 'Active Series Count',
                    data: data.activeSeriesCount,
                    backgroundColor: 'rgba(255, 100, 132, 0.2)',
                    borderColor: '#FF6484',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    fill: false,
                },
                {
                    label: 'Metrics Datapoints Count',
                    data: data.metricsDatapointsCount,
                    backgroundColor: 'rgba(99, 71, 217, 0.2)',
                    borderColor: 'rgb(99, 71, 217)',
                    borderWidth: 2,
                    tension: 0.3,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    fill: false,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            interaction: {
                mode: 'index',
                intersect: false,
            },
            plugins: {
                tooltip: {
                    mode: 'index',
                    intersect: false,
                },
                legend: {
                    position: 'bottom',
                },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Count',
                    },
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                    },
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time Period',
                    },
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                        callback: function (val, _index, _ticks) {
                            let value = this.getLabelForValue(val);
                            return formatDateLabel(value);
                        },
                    },
                },
            },
        },
    });
}

function formatDateLabel(value) {
    if (!value) {
        return '';
    }
    // (YYYY-MM-DDThh)
    if (value.indexOf('T') > -1) {
        let parts = value.split('T');
        return 'T' + parts[1];
    }
    // (YYYY-MM)
    else if (value.split('-').length === 2) {
        return value;
    }
    // (YYYY-MM-DD)
    else {
        let parts = value.split('-');
        return parts[1] + '-' + parts[2];
    }
}

// Update the getClusterIngestStats function to use our charting functions
function getClusterIngestStats() {
    const selectedGranularity = $('.granularity-tabs .tab.active').data('tab');

    const requestBody = {
        startEpoch: filterStartDate,
        endEpoch: filterEndDate,
        granularity: selectedGranularity,
    };
    $.ajax({
        method: 'post',
        url: 'api/usageStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(requestBody),
    })
        .then((res) => {
            setupClusterStatsCharts(res);
        })
        .catch((err) => {
            console.error('error:', err);
        });
}
