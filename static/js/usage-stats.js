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

    $('.range-item').on('click', getUsageStats);
    $('#customrange-btn').on('dateRangeValid', getUsageStats);

    getUsageStats();

    $('.granularity-tabs .tab').click(function () {
        $('.granularity-tabs .tab').removeClass('active');
        $(this).addClass('active');
        getUsageStats();
    });

    $('#csv-block').on('click', downloadUsageStatsCSV);
});

function fetchUsageStatsData() {
    const selectedGranularity = $('.granularity-tabs .tab.active').data('tab');

    const requestBody = {
        startEpoch: filterStartDate,
        endEpoch: filterEndDate,
        granularity: selectedGranularity,
    };

    return $.ajax({
        method: 'post',
        url: 'api/usageStats',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(requestBody),
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

function setupUsageStatsCharts(data) {
    if (!data || !data.chartStats) {
        console.error('No chart data available');
        return;
    }

    const selectedGranularity = $('.granularity-tabs .tab.active').data('tab') || 'day';

    let chartGranularity;
    if (selectedGranularity === 'hourly') chartGranularity = 'hour';
    else if (selectedGranularity === 'daily') chartGranularity = 'day';
    else chartGranularity = 'month';

    // Process the data for charts
    const dates = Object.keys(data.chartStats).sort();
    const processedData = {
        logs: {
            dates: dates,
            gbCount: dates.map((date) => data.chartStats[date].LogsBytesCount),
            eventCount: dates.map((date) => data.chartStats[date].LogsEventCount || 0),
            granularity: chartGranularity,
        },
        traces: {
            dates: dates,
            gbCount: dates.map((date) => data.chartStats[date].TraceBytesCount),
            spanCount: dates.map((date) => data.chartStats[date].TraceSpanCount || 0),
            granularity: chartGranularity,
        },
        stacked: {
            dates: dates,
            activeSeriesCount: dates.map((date) => data.chartStats[date].ActiveSeriesCount),
            metricsDatapointsCount: dates.map((date) => data.chartStats[date].MetricsDatapointsCount),
            granularity: chartGranularity,
        },
    };

    const { gridLineColor, tickColor } = getGraphGridColors();

    renderLogsVolumeChart(processedData.logs, gridLineColor, tickColor);
    renderTracesVolumeChart(processedData.traces, gridLineColor, tickColor);
    renderStackedChart(processedData.stacked, gridLineColor, tickColor);
    displayTotal(data);
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

    if (chart.options.scales.y1) {
        chart.options.scales.y1.ticks.color = tickColor;
    }

    chart.update();
}

function determineUnit(data) {
    let maxBytes = 0;
    data.forEach((point) => {
        if (point.y > maxBytes) {
            maxBytes = point.y;
        }
    });

    if (maxBytes === 0) return { unit: 'Bytes', divisor: 1 };

    const k = 1024;
    const sizes = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];

    const i = Math.min(Math.floor(Math.log(maxBytes) / Math.log(k)), sizes.length - 1);

    return {
        unit: sizes[i],
        divisor: Math.pow(k, i),
    };
}

function createVolumeChart(chartId, data, options) {
    const ctx = document.getElementById(chartId).getContext('2d');
    const { chartType, color, lineColor, gridLineColor, tickColor, title, lineTitle, lineData } = options;

    // Destroy existing chart instance if it exists
    if (window[chartType] && typeof window[chartType].destroy === 'function') {
        window[chartType].destroy();
    }

    const chartData = data.dates.map((timestamp, index) => ({
        x: parseInt(timestamp) * 1000,
        y: data.gbCount[index],
    }));

    const scale = determineUnit(chartData.map((item) => ({ y: item.y })));

    const scaledData = chartData.map((item) => ({
        x: item.x,
        y: item.y / scale.divisor,
    }));

    const lineChartData = data.dates.map((timestamp, index) => ({
        x: parseInt(timestamp) * 1000,
        y: lineData[index],
    }));

    const xAxisConfig = configureTimeAxis(data, data.granularity);

    scaledData.sort((a, b) => a.x - b.x);
    lineChartData.sort((a, b) => a.x - b.x);

    const chartConfig = {
        type: 'bar',
        data: {
            datasets: [
                {
                    label: title,
                    data: scaledData,
                    backgroundColor: color,
                    borderRadius: 4,
                    barPercentage: 0.8,
                    categoryPercentage: 0.8,
                    barThickness: 'flex',
                    order: 1,
                    yAxisID: 'y',
                },
                {
                    label: lineTitle,
                    data: lineChartData,
                    type: 'line',
                    borderColor: lineColor,
                    backgroundColor: lineColor + '50',
                    borderWidth: 2,
                    pointRadius: 3,
                    pointHoverRadius: 6,
                    fill: false,
                    tension: 0.3,
                    order: 0,
                    yAxisID: 'y1',
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
                        title: function (tooltipItems) {
                            const timestamp = tooltipItems[0].parsed.x / 1000;
                            return formatTooltipTimestamp(timestamp, data.granularity);
                        },
                        label: function (context) {
                            const label = context.dataset.label || '';
                            let value = '';

                            if (context.datasetIndex === 0) {
                                // Volume data (bars)
                                value = context.parsed.y.toFixed(3) + ' ' + scale.unit;
                            } else if (context.datasetIndex === 1) {
                                // Count data (line)
                                value = context.parsed.y.toLocaleString();
                            }

                            return `${label}: ${value}`;
                        },
                    },
                },
                legend: {
                    display: true,
                    position: 'bottom',
                },
            },
            scales: {
                y: {
                    type: 'linear',
                    display: true,
                    position: 'left',
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
                y1: {
                    type: 'linear',
                    display: true,
                    position: 'right',
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Count',
                    },
                    grid: {
                        display: false,
                    },
                    ticks: {
                        color: tickColor,
                    },
                },
                x: {
                    ...xAxisConfig,
                    grid: {
                        display: true,
                        color: gridLineColor,
                        offset: data.granularity === 'hour' ? false : true,
                    },
                    ticks: {
                        ...xAxisConfig.ticks,
                        color: tickColor,
                    },
                    offset: data.granularity === 'hour' ? false : true,
                },
            },
        },
    };

    // Create and return chart
    window[chartType] = new Chart(ctx, chartConfig);
    return window[chartType];
}
function renderLogsVolumeChart(data, gridLineColor, tickColor) {
    return createVolumeChart('logsVolumeChart', data, {
        chartType: 'logsVolumeChart',
        color: '#36A2EB',
        lineColor: '#FF6484',
        gridLineColor,
        tickColor,
        title: 'Logs Volume',
        lineTitle: 'Event Count',
        lineData: data.dates.map((date, index) => data.eventCount[index]),
    });
}

function renderTracesVolumeChart(data, gridLineColor, tickColor) {
    return createVolumeChart('tracesVolumeChart', data, {
        chartType: 'tracesVolumeChart',
        color: '#4BC0C0',
        lineColor: '#FFCD56',
        gridLineColor,
        tickColor,
        title: 'Traces Volume',
        lineTitle: 'Span Count',
        lineData: data.dates.map((date, index) => data.spanCount[index]),
    });
}

function renderStackedChart(data, gridLineColor, tickColor) {
    const ctx = document.getElementById('stackedChart').getContext('2d');

    // Destroy existing chart instance if it exists
    if (window.stackedChart && typeof window.stackedChart.destroy === 'function') {
        window.stackedChart.destroy();
    }

    const timeSeriesData = data.dates.map((timestamp, index) => ({
        x: parseInt(timestamp) * 1000,
        y: data.activeSeriesCount[index],
    }));

    const metricsData = data.dates.map((timestamp, index) => ({
        x: parseInt(timestamp) * 1000,
        y: data.metricsDatapointsCount[index],
    }));

    const xAxisConfig = configureTimeAxis(data, data.granularity);

    window.stackedChart = new Chart(ctx, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Active Series Count',
                    data: timeSeriesData,
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
                    data: metricsData,
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
                    callbacks: {
                        title: function (tooltipItems) {
                            const timestamp = tooltipItems[0].parsed.x / 1000;
                            return formatTooltipTimestamp(timestamp, data.granularity);
                        },
                    },
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
                    ...xAxisConfig,
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        ...xAxisConfig.ticks,
                        color: tickColor,
                    },
                },
            },
        },
    });

    return window.stackedChart;
}

function getUsageStats() {
    fetchUsageStatsData()
        .then((res) => {
            setupUsageStatsCharts(res);
        })
        .catch((err) => {
            console.error('Error fetching usage stats:', err);
            showToast(`Error downloading usage stats:${err}`, 'error');
        });
}

function formatTooltipTimestamp(timestamp, granularity) {
    const date = new Date(timestamp * 1000);
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const month = monthNames[date.getMonth()];
    const day = date.getDate();
    const year = date.getFullYear();

    if (granularity === 'hour' || granularity === 'hourly') {
        const hours = date.getHours();
        const ampm = hours >= 12 ? 'PM' : 'AM';
        const hour12 = hours % 12 || 12;
        return `${month} ${day}, ${year} ${hour12}:00 ${ampm}`;
    } else {
        return `${month} ${day}, ${year}`;
    }
}

function configureTimeAxis(data, granularity) {
    const firstTimestamp = parseInt(data.dates[0]) * 1000;
    const lastTimestamp = parseInt(data.dates[data.dates.length - 1]) * 1000;
    const daysInRange = Math.ceil((lastTimestamp - firstTimestamp) / (1000 * 60 * 60 * 24));
    const hoursInRange = Math.ceil((lastTimestamp - firstTimestamp) / (1000 * 60 * 60));
    let unit, maxTicksLimit, stepSize;

    if (granularity === 'hour') {
        unit = 'hour';
        if (hoursInRange <= 24) {
            stepSize = 1;
            maxTicksLimit = hoursInRange + 1;
        } else if (daysInRange <= 4) {
            stepSize = 6;
            maxTicksLimit = daysInRange * 4 + 1;
        } else if (daysInRange <= 15) {
            stepSize = 12;
            maxTicksLimit = daysInRange * 2 + 1;
        } else {
            stepSize = 24;
            maxTicksLimit = Math.ceil(daysInRange / 2) + 1;
        }
    } else if (granularity === 'day') {
        unit = 'day';

        if (data.dates.length <= 15) {
            maxTicksLimit = data.dates.length;
        } else if (daysInRange <= 31) {
            maxTicksLimit = Math.min(14, Math.ceil(daysInRange / 2));
        } else if (daysInRange <= 90) {
            maxTicksLimit = Math.min(12, Math.ceil(daysInRange / 7));
        } else {
            maxTicksLimit = Math.min(15, Math.ceil(daysInRange / 14));
        }
    } else {
        unit = 'month';
        if (daysInRange <= 366) {
            maxTicksLimit = 12;
        } else {
            maxTicksLimit = Math.min(12, Math.ceil(daysInRange / 30));
        }
    }

    const timeOptions = {
        unit: unit,
        displayFormats: {
            hour: 'h aaa',
            day: 'MMM d',
            month: 'MMM yyyy',
        },
        tooltipFormat: 'MMM d, yyyy, h:mm aaa',
        bounds: 'ticks',
    };

    let offsetValue = true;
    if (granularity === 'hour') {
        timeOptions.round = 'hour';
        offsetValue = false;

        if (stepSize) {
            timeOptions.stepSize = stepSize;
        }
    }

    timeOptions.offset = offsetValue;

    if (granularity === 'day') {
        timeOptions.round = 'day';
    }

    const config = {
        type: 'time',
        time: timeOptions,
        border: {
            display: true,
        },
        title: {
            display: true,
            text: 'Time Period',
        },
        ticks: {
            maxRotation: 0,
            minRotation: 0,
            maxTicksLimit: maxTicksLimit,
            includeBounds: true,
            callback: function (value) {
                const date = new Date(value);
                const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
                const hours = date.getHours();
                const day = date.getDate();

                if (granularity === 'hour') {
                    if (hoursInRange <= 24) {
                        const hourIn12 = hours % 12 || 12;
                        const amPm = hours < 12 ? 'AM' : 'PM';

                        if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        } else {
                            return `${hourIn12}${amPm}`;
                        }
                    } else if (daysInRange <= 4) {
                        if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        } else if (hours === 6) {
                            return '6AM';
                        } else if (hours === 12) {
                            return '12PM';
                        } else if (hours === 18) {
                            return '6PM';
                        }
                    } else if (daysInRange <= 15) {
                        if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        } else if (hours === 12) {
                            return '12PM';
                        }
                    } else {
                        if (hours === 0 && day % 2 === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        } else if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        }
                    }
                    return null;
                } else if (granularity === 'day') {
                    return `${monthNames[date.getMonth()]} ${day}`;
                } else {
                    return `${monthNames[date.getMonth()]} ${date.getFullYear()}`;
                }
            },
            font: {
                weight: function (context) {
                    const value = context.tick.value;
                    const date = new Date(value);
                    const hours = date.getHours();

                    if (granularity === 'hour' && hours === 0) {
                        return 'bold';
                    }
                    return 'normal';
                },
            },
        },
        alignToPixels: true,
        offset: offsetValue,
    };

    return config;
}

function downloadUsageStatsCSV() {
    fetchUsageStatsData()
        .then((res) => {
            if (!res || !res.chartStats) {
                console.error('No chart data available for CSV download');
                return;
            }

            const csvContent = convertStatsToCSV(res.chartStats);
            const fileName = `usage_stats.csv`;

            downloadCSV(csvContent, fileName);
        })
        .catch((err) => {
            console.error('Error downloading usage stats:', err);
            showToast(`Error downloading usage stats:${err}`, 'error');
        });
}

function convertStatsToCSV(chartStats) {
    const timestamps = Object.keys(chartStats).sort((a, b) => parseInt(a) - parseInt(b));

    const logsUnit = determineUnit(timestamps.map((ts) => ({ y: chartStats[ts].LogsBytesCount || 0 })));
    const tracesUnit = determineUnit(timestamps.map((ts) => ({ y: chartStats[ts].TraceBytesCount || 0 })));

    const headers = ['Date', `Logs (${logsUnit.unit})`, 'Event Count', `Traces (${tracesUnit.unit})`, 'Span Count', 'Active Series', 'Metrics Datapoints'].join(',');

    const granularity = $('.granularity-tabs .tab.active').data('tab');

    const rows = timestamps.map((timestamp) => {
        const stats = chartStats[timestamp];
        const date = formatTooltipTimestamp(parseInt(timestamp), granularity);
        return [`"${date}"`, ((stats.LogsBytesCount || 0) / logsUnit.divisor).toFixed(2), stats.LogsEventCount || 0, ((stats.TraceBytesCount || 0) / tracesUnit.divisor).toFixed(2), stats.TraceSpanCount || 0, stats.ActiveSeriesCount || 0, stats.MetricsDatapointsCount || 0].join(',');
    });

    return headers + '\n' + rows.join('\n');
}

function downloadCSV(csvContent, fileName) {
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const link = document.createElement('a');

    const url = URL.createObjectURL(blob);
    link.setAttribute('href', url);
    link.setAttribute('download', fileName);
    link.style.display = 'none';
    document.body.appendChild(link);
    link.click();

    setTimeout(() => {
        document.body.removeChild(link);
        URL.revokeObjectURL(url);
    }, 100);
}
