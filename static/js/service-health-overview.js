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

let serviceName;
const charts = {
    rate: null,
    error: null,
    latencies: null,
};

const chartConfigs = {
    rate: {
        id: 'ServiceHealthChart',
        label: 'Rate',
        dataKey: 'rate',
        color: 'rgb(99,71,217)',
    },
    error: {
        id: 'ServiceHealthChartErr',
        label: 'Error Rate',
        dataKey: 'error_rate',
        color: 'rgb(99,71,217)',
    },
    latencies: {
        id: 'ServiceHealthChart2',
        datasets: [
            { label: 'P50 Latency', dataKey: 'p50', color: '#FF6484' },
            { label: 'P90 Latency', dataKey: 'p90', color: '#36A2EB' },
            { label: 'P99 Latency', dataKey: 'p99', color: '#4BC0C0' },
        ],
    },
};

let themeColors = {
    light: {
        gridLineColor: '#DCDBDF',
        tickColor: '#160F29',
    },
    dark: {
        gridLineColor: '#383148',
        tickColor: '#FFFFFF',
    },
};

$(document).ready(() => {
    serviceName = getParameterFromUrl('service');
    const startDate = getParameterFromUrl('startEpoch');
    const endDate = getParameterFromUrl('endEpoch');

    initializeBreadcrumbs([{ name: 'APM', url: './service-health.html' }, { name: 'Service Health', url: './service-health.html' }, { name: serviceName }]);

    $('.theme-btn').on('click', function () {
        themePickerHandler();
        getOneServiceOverview();
    });

    setupEventHandlers();
    datePickerHandler(startDate, endDate, startDate);
    $('.range-item, #customrange-btn').on('click', getOneServiceOverview);
    
    getOneServiceOverview();
});

function getTimeRange() {
    return {
        startEpoch: filterStartDate || 'now-1h',
        endEpoch: filterEndDate || 'now',
    };
}

function getParameterFromUrl(param) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(param);
}

function getOneServiceOverview() {
    console.log("getOneServiceOverview")
    const chartIds = Object.values(chartConfigs).map((config) => config.id || config.datasets?.[0]?.id);

    showLoadingIcons(chartIds);
    let data = getTimeRange();
    let requestBody = {
        indexName: 'red-traces',
        queryLanguage: 'Splunk QL',
        stDate: data.startEpoch,
        endDate: data.endEpoch,
        searchText: serviceName,
    };
    $.ajax({
        method: 'POST',
        url: 'api/search',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(requestBody),
        dataType: 'json',
        crossDomain: true,
    })
        .then(function (res) {
            const theme = $('html').attr('data-theme') || 'dark';
            const colors = themeColors[theme];

            Object.values(charts).forEach((chart) => {
                if (chart) chart.destroy();
            });

            hideLoadingIcons(chartIds);

            charts.rate = createSingleLineChart(chartConfigs.rate.id, res.hits.records, chartConfigs.rate.dataKey, chartConfigs.rate.label, chartConfigs.rate.color, colors, false);

            charts.error = createSingleLineChart(chartConfigs.error.id, res.hits.records, chartConfigs.error.dataKey, chartConfigs.error.label, chartConfigs.error.color, colors, false);

            charts.latencies = createMultiLineChart(chartConfigs.latencies.id, res.hits.records, chartConfigs.latencies.datasets, colors, true);
        })
        .catch((error) => {
            hideLoadingIcons(chartIds);
            console.error('Error fetching service health data:', error);
        });
}

function formatTimestamp(timestamp) {
    return new Date(timestamp).toISOString().slice(0, -5).replace('T', ' ');
}

function prepareChartData(records, dataKey) {
    const data = records.map((record) => ({
        x: formatTimestamp(record.timestamp),
        y: record[dataKey],
    }));

    return data.sort((a, b) => new Date(a.x) - new Date(b.x));
}

function createSingleLineChart(canvasId, records, dataKey, label, color, themeColors, showLegend = false) {
    const chartData = prepareChartData(records, dataKey);
    const canvas = $(`#${canvasId}`).get(0).getContext('2d');

    return new Chart(canvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: label,
                    data: chartData,
                    borderColor: color,
                    borderWidth: 2,
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 2,
                    pointBorderColor: color,
                    fill: false,
                },
            ],
        },
        options: createChartOptions(themeColors, showLegend),
    });
}

function createMultiLineChart(canvasId, records, datasets, themeColors, showLegend = true) {
    const canvas = $(`#${canvasId}`).get(0).getContext('2d');
    const chartDatasets = datasets.map((dataset) => {
        const data = prepareChartData(records, dataset.dataKey);
        return {
            label: dataset.label,
            data: data,
            borderColor: dataset.color,
            yAxisID: 'y',
            pointStyle: 'circle',
            pointRadius: 2,
            borderWidth: 2,
            pointBorderColor: dataset.color,
            fill: false,
        };
    });

    return new Chart(canvas, {
        type: 'line',
        data: { datasets: chartDatasets },
        options: createChartOptions(themeColors, showLegend),
    });
}

function createChartOptions(colors, showLegend) {
    return {
        responsive: true,
        maintainAspectRatio: false,
        interaction: {
            intersect: false,
            mode: 'index',
        },
        scales: {
            y: {
                beginAtZero: true,
                ticks: { color: colors.tickColor },
                grid: { color: colors.gridLineColor },
            },
            x: {
                ticks: { color: colors.tickColor },
                grid: { color: colors.gridLineColor },
            },
        },
        plugins: {
            legend: {
                display: showLegend,
                position: 'bottom',
                labels: {
                    boxHeight: 10,
                    padding: 20,
                },
            },
        },
    };
}

function showLoadingIcons(chartIds) {
    chartIds.forEach((chartId) => {
        const canvas = $('#' + chartId);
        const container = canvas.closest('.canvas-container');

        if (container.find('.panel-loading').length === 0) {
            container.prepend('<div class="panel-loading"></div>');
        } else {
            container.find('.panel-loading').show();
        }
    });
}

function hideLoadingIcons(chartIds) {
    chartIds.forEach((chartId) => {
        const canvas = $('#' + chartId);
        const container = canvas.closest('.canvas-container');
        container.find('.panel-loading').hide();
    });
}
