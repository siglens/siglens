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

let currentChartType = 'bar'; 

$('.visualization-type-options li').on('click', function() {
    $('.visualization-type-options li').removeClass('active');
    
    $(this).addClass('active');
    
    const selectedText = $(this).text();
    $('.visualization-type button span').text(selectedText);
    
    // Only call timeChart if the chart type actually changes
    const newChartType = (selectedText === 'Column Chart') ? 'bar' : 'line';
    
    if (newChartType !== currentChartType) {
        currentChartType = newChartType;
        timeChart(lastQType);
    }
});

//eslint-disable-next-line no-unused-vars
function timeChart(qtype) {
    // Check if measureInfo is defined and contains at least one item
    qtype = qtype || lastQType;
    if (isTimechart || qtype === 'aggs-query') {
        $('.column-chart').show();
        $('#hideGraph').hide();
    } else {
        $('.column-chart').hide();
        $('#hideGraph').show();
        return;
    }

    if (!measureInfo || measureInfo.length === 0) {
        return;
    }

    // Ensure all items in measureInfo have GroupByValues property before proceeding
    const hasGroupByValues = measureInfo.every((item) => item.GroupByValues);

    if (!hasGroupByValues) {
        return;
    }

    // Check if there are multiple group-by columns
    var multipleGroupBy = measureInfo[0].GroupByValues.length > 1;

    // Format x-axis labels (categories)
    var xData = measureInfo.map((item) => formatGroupByValues(item.GroupByValues, multipleGroupBy));

    var datasets = measureFunctions.map(function (measureFunction, index) {
        // Generate colors based on index (you can customize this)
        const hue = (index * 137) % 360; // Spread colors evenly
        const color = `hsl(${hue}, 70%, 60%)`;

        return {
            label: measureFunction,
            data: measureInfo.map(function (item) {
                return item.MeasureVal[measureFunction] || 0;
            }),
            backgroundColor: color,
            borderColor: color,
            borderWidth: 1,
        };
    });

    if (window.myBarChart) {
        window.myBarChart.destroy();
    }

    const columnChartEl = $('#columnChart');
    if (!columnChartEl.length) {
        console.error('Column chart element not found');
        return;
    }

    columnChartEl.empty();
    columnChartEl.html('<canvas width="100%" height="100%"></canvas>');

    const ctx = columnChartEl.find('canvas')[0].getContext('2d');
    const fontSize = measureInfo.length > 10 ? 10 : 12;
    const rotateLabels = measureInfo.length > 10 ? 45 : 0;

    window.myBarChart = new Chart(ctx, {
        type: currentChartType,
        data: {
            labels: xData,
            datasets: datasets,
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    ticks: {
                        font: {
                            size: fontSize,
                        },
                        maxRotation: rotateLabels,
                        minRotation: rotateLabels,
                    },
                },
                y: {
                    beginAtZero: true,
                },
            },
            plugins: {
                legend: {
                    position: 'right',
                    align: 'start',
                    labels: {
                        boxWidth: 12,
                        font: {
                            size: 12,
                        },
                    },
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return context.dataset.label + ': ' + context.raw;
                        },
                    },
                },
            },
        },
    });

    setupFormatPanel();
    updateTimeChartForOverlay();
}

function formatGroupByValues(groupByValues, multipleGroupBy) {
    if (multipleGroupBy) {
        return groupByValues.map(convertIfTimestamp).join(', ');
    } else {
        return convertIfTimestamp(groupByValues[0]);
    }
}

function convertTimestamp(timestampString) {
    var timestamp = parseInt(timestampString);
    var date = new Date(timestamp);

    var year = date.getFullYear();
    var month = ('0' + (date.getMonth() + 1)).slice(-2);
    var day = ('0' + date.getDate()).slice(-2);

    var hours = ('0' + date.getHours()).slice(-2);
    var minutes = ('0' + date.getMinutes()).slice(-2);
    var seconds = ('0' + date.getSeconds()).slice(-2);

    var readableDate = year + '-' + month + '-' + day + ' ' + hours + ':' + minutes + ':' + seconds;
    return readableDate;
}

function convertIfTimestamp(value) {
    const isTimestamp = !isNaN(value) && value.length === 13 && new Date(parseInt(value)).getTime() > 0;
    if (isTimestamp) {
        return convertTimestamp(value);
    }
    return value;
}

// Format Visualization
let chartOverlaySettings = {
    enabled: false,
    title: 'Conversion Rates',
    interval: 200,
    minValue: null,
    maxValue: 1000,
    abbreviations: false,
    metrics: [],
};

function setupFormatPanel() {
    // Toggle panel visibility
    $('.column-chart #formatBtn')
        .off('click')
        .on('click', function () {
            $('#formatPanel').toggle();
        });

    // Close panel when clicking close button
    $('#closeBtn').on('click', function () {
        $('#formatPanel').hide();
    });

    // Button group selection
    $('.button-group .group-btn').on('click', function () {
        $(this).siblings().removeClass('active');
        $(this).addClass('active');

        const settingType = $(this).closest('.form-row').find('.form-label').text().trim();
        const value = $(this).text().trim();

        if (settingType === 'View as Axis') {
            chartOverlaySettings.enabled = value === 'On';

            // Only apply chart overlay if it's enabled
            if (chartOverlaySettings.enabled) {
                applyChartOverlay();
            } else {
                // Otherwise, remove the secondary axis
                if (window.myBarChart && window.myBarChart.config) {
                    const chartConfig = window.myBarChart.config;
                    if (chartConfig.options.scales.y1) {
                        delete chartConfig.options.scales.y1;
                    }
                    chartConfig.data.datasets.forEach((dataset) => {
                        dataset.yAxisID = 'y';
                    });

                    window.myBarChart.update();
                }
            }
        } else if (settingType === 'Number Abbreviations') {
            chartOverlaySettings.abbreviations = value === 'On';
            if (chartOverlaySettings.enabled) {
                applyChartOverlay();
            }
        }
    });

    // Input field changes
    $('.form-input input').on('change', function () {
        const settingType = $(this).closest('.form-row').find('.form-label').text().trim();
        const value = $(this).val();

        switch (settingType) {
            case 'Interval':
                chartOverlaySettings.interval = parseInt(value) || 200;
                break;
            case 'Min Value':
                chartOverlaySettings.minValue = value === '' ? null : parseFloat(value);
                break;
            case 'Max Value':
                chartOverlaySettings.maxValue = value === '' ? 1000 : parseFloat(value);
                break;
            case 'Title':
                chartOverlaySettings.title = value;
                break;
        }

        if (chartOverlaySettings.enabled) {
            applyChartOverlay();
        }
    });

    // Metrics selection dropdown
    $('#addTagBtn')
        .off('click')
        .on('click', function (e) {
            e.stopPropagation();
            const dropdown = $('#tagDropdown');
            dropdown.toggleClass('show');

            if (dropdown.hasClass('show')) {
                populateTagDropdown();
            }
        });

    // Close dropdown when clicking outside
    $(document).on('click', function (e) {
        if (!$(e.target).closest('.tag-dropdown-container').length) {
            $('#tagDropdown').removeClass('show');
        }
    });

    // Select metric from dropdown
    $(document).on('click', '.tag-option', function () {
        if (!$(this).hasClass('disabled')) {
            const metric = $(this).data('value');
            addMetricTag(metric);
            $('#tagDropdown').removeClass('show');
        }
    });

    // Remove tag
    $(document).on('click', '.remove-tag', function (e) {
        e.stopPropagation();

        const metric = $(this).parent().data('value');
        removeMetricTag(metric);
    });

    $(document).on('click', function (e) {
        const formatPanel = $('#formatPanel');
        if (formatPanel.is(':visible') && !$(e.target).closest('#formatPanel').length && !$(e.target).closest('#formatBtn').length) {
            formatPanel.hide();
        }
    });

    updateSelectedTags();
}

function populateTagDropdown() {
    const dropdownContent = $('#tagDropdownContent');
    dropdownContent.empty();

    // Get all available metrics
    if (window.myBarChart && window.myBarChart.data && window.myBarChart.data.datasets) {
        // Filter out already selected metrics
        const availableMetrics = window.myBarChart.data.datasets.map((dataset) => dataset.label).filter((metric) => !chartOverlaySettings.metrics.includes(metric));

        if (availableMetrics.length === 0) {
            dropdownContent.append('<div class="tag-option disabled">No available metrics</div>');
            return;
        }

        // Add each available metric to dropdown
        availableMetrics.forEach((metric) => {
            dropdownContent.append(`
                <div class="tag-option" data-value="${metric}">${metric}</div>
            `);
        });
    }
}

function addMetricTag(metric) {
    if (!chartOverlaySettings.metrics.includes(metric)) {
        chartOverlaySettings.metrics.push(metric);
        updateSelectedTags();

        if (chartOverlaySettings.enabled) {
            applyChartOverlay();
        }
    }
}

function removeMetricTag(metric) {
    chartOverlaySettings.metrics = chartOverlaySettings.metrics.filter((m) => m !== metric);
    updateSelectedTags();

    if (chartOverlaySettings.enabled) {
        applyChartOverlay();
    }
}

function updateSelectedTags() {
    const tagsContainer = $('#selectedTags');
    tagsContainer.empty();

    chartOverlaySettings.metrics.forEach((metric) => {
        tagsContainer.append(`
            <div class="tag" data-value="${metric}">
                ${metric} <span class="remove-tag">×</span>
            </div>
        `);
    });
}

function applyChartOverlay() {
    if (!window.myBarChart) return;
    const chartConfig = window.myBarChart.config;

    // Only proceed if the overlay is enabled
    if (!chartOverlaySettings.enabled) return;

    // Secondary y-axis (y1)
    chartConfig.options.scales.y1 = {
        type: 'linear',
        position: 'right',
        beginAtZero: true,
        title: {
            display: true,
            text: chartOverlaySettings.title,
        },
        grid: {
            drawOnChartArea: false,
        },
    };

    // Apply min value if set
    if (chartOverlaySettings.minValue !== null) {
        chartConfig.options.scales.y1.min = chartOverlaySettings.minValue;
    }

    // Apply max value
    if (chartOverlaySettings.maxValue !== null) {
        chartConfig.options.scales.y1.max = chartOverlaySettings.maxValue;
    }

    // Apply interval
    if (chartOverlaySettings.interval && chartOverlaySettings.interval > 0) {
        chartConfig.options.scales.y1.ticks = chartConfig.options.scales.y1.ticks || {};
        chartConfig.options.scales.y1.ticks.stepSize = chartOverlaySettings.interval;
    }

    // Apply abbreviations
    if (chartOverlaySettings.abbreviations) {
        chartConfig.options.scales.y1.ticks = chartConfig.options.scales.y1.ticks || {};
        chartConfig.options.scales.y1.ticks.callback = function (value) {
            if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
            if (value >= 1000) return (value / 1000).toFixed(1) + 'K';
            return value;
        };
    }

    chartConfig.data.datasets.forEach((dataset) => {
        dataset.yAxisID = chartOverlaySettings.metrics.includes(dataset.label) ? 'y1' : 'y';
    });

    window.myBarChart.update();
}

function updateTimeChartForOverlay() {
    if (!window.myBarChart) return;

    // Primary y-axis
    const originalOptions = window.myBarChart.options;
    originalOptions.scales.y = {
        beginAtZero: true,
        title: {
            display: true,
            text: 'Primary Axis',
        },
    };

    if (chartOverlaySettings.enabled) {
        applyChartOverlay();
    }
}
