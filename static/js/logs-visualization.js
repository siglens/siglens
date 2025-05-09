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

let chartSettings = {
    lastActiveSection: 'x-axis',
    xAxis: {
        title: '',
        labelRotation: 0,
    },
    yAxis: {
        title: 'Primary Axis',
        interval: null,
        minValue: null,
        maxValue: null,
        abbreviations: false,
    },
    chartOverlay: {
        enabled: false,
        title: 'Conversion Rates',
        interval: null,
        minValue: null,
        maxValue: null,
        abbreviations: false,
        metrics: [],
    },
    legend: {
        show: true,
        position: 'right',
    },
};

$('.visualization-type-options li').on('click', function () {
    $('.visualization-type-options li').removeClass('active');

    $(this).addClass('active');

    const selectedText = $(this).text();
    $('.visualization-type button span').text(selectedText);

    const newChartType = selectedText === 'Bar Chart' ? 'bar' : 'line';

    if (newChartType !== currentChartType) {
        currentChartType = newChartType;

        if (window.myBarChart) {
            window.myBarChart.config.type = currentChartType;

            window.myBarChart.data.datasets.forEach(function (dataset, index) {
                let color;
                if (dataset.borderColor) {
                    color = dataset.borderColor;
                } else {
                    const colorIndex = index % globalColorArray.length;
                    color = globalColorArray[colorIndex];
                }

                if (currentChartType === 'line') {
                    dataset.type = 'line';
                    dataset.backgroundColor = color + '70';
                    dataset.borderWidth = 2;
                    dataset.fill = false;
                    dataset.tension = 0.1;
                    dataset.pointRadius = 3;
                    dataset.pointHoverRadius = 5;
                } else {
                    dataset.type = 'bar';
                    dataset.backgroundColor = color;
                    dataset.borderWidth = 1;
                    dataset.fill = true;
                    delete dataset.tension;
                    delete dataset.pointRadius;
                    delete dataset.pointHoverRadius;
                }
            });
            window.myBarChart.update();
            setupFormatPanel();
            updateChart();
        }
    }
});

function toggleChartVisibility(qtype, isTimechart) {
    qtype = qtype || lastQType;

    // Show chart for timechart or aggregation queries
    if (isTimechart || qtype === 'aggs-query') {
        $('.column-chart').show();
        $('#hideGraph').hide();
        return true; // Chart should be rendered
    } else {
        $('.column-chart').hide();
        $('#hideGraph').show();
        return false; // Chart should not be rendered
    }
}

function createTimeChart(measureInfo) {
    if (!measureInfo || measureInfo.length === 0) {
        return;
    }

    // Ensure all items in measureInfo have GroupByValues property
    const hasGroupByValues = measureInfo.every((item) => item.GroupByValues);
    if (!hasGroupByValues) {
        return;
    }

    // Check if there are multiple group-by columns
    var multipleGroupBy = measureInfo[0].GroupByValues.length > 1;

    // Format x-axis labels (categories)
    var xData = measureInfo.map((item) => formatGroupByValues(item.GroupByValues, multipleGroupBy));

    var datasets = measureFunctions.map(function (measureFunction, index) {
        const colorIndex = index % globalColorArray.length;
        const color = globalColorArray[colorIndex];

        return {
            label: measureFunction,
            data: measureInfo.map(function (item) {
                return item.MeasureVal[measureFunction] || 0;
            }),
            backgroundColor: currentChartType === 'line' ? color + '70' : color,
            borderColor: color,
            borderWidth: 1,
        };
    });

    if (window.myBarChart) {
        window.myBarChart.destroy();
    }

    const columnChartEl = $('#columnChart');
    if (!columnChartEl.length) {
        console.error('Bar chart element not found');
        return;
    }

    columnChartEl.empty();
    columnChartEl.html('<canvas width="100%" height="100%"></canvas>');

    const ctx = columnChartEl.find('canvas')[0].getContext('2d');
    const fontSize = measureInfo.length > 10 ? 10 : 12;

    const { gridLineColor, tickColor } = getGraphGridColors();

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
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
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
                        color: tickColor,
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
    updateChart();
}

//eslint-disable-next-line no-unused-vars
function timeChart(qtype, measureInfo, isTimechart) {
    const shouldRender = toggleChartVisibility(qtype, isTimechart);

    if (!shouldRender) {
        return;
    }

    createTimeChart(measureInfo);
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

function setupFormatPanel() {
    // Toggle panel visibility
    $('.column-chart #formatBtn')
        .off('click')
        .on('click', function () {
            $('#formatPanel').toggle();

            // Show the last active section when opening panel
            if ($('#formatPanel').is(':visible')) {
                $('.sidebar-item').removeClass('active');
                $(`.sidebar-item[data-section="${chartSettings.lastActiveSection}"]`).addClass('active');

                $('.content-section').hide();
                $(`#${chartSettings.lastActiveSection}-section`).show();

                updateFormValues();
            }
        });

    // Close panel when clicking close button
    $('#closeBtn').on('click', function () {
        $('#formatPanel').hide();
    });

    // Sidebar navigation
    $('.sidebar-item').on('click', function () {
        $('.sidebar-item').removeClass('active');
        $(this).addClass('active');

        const section = $(this).data('section');
        chartSettings.lastActiveSection = section;

        $('.content-section').hide();
        $(`#${section}-section`).show();
    });

    // Button group selection
    $('.button-group .group-btn').on('click', function () {
        $(this).siblings().removeClass('active');
        $(this).addClass('active');

        const section = $(this).closest('.content-section').attr('id').replace('-section', '');
        const settingType = $(this).closest('.form-row').find('.form-label').text().trim();
        const value = $(this).text().trim();

        handleButtonGroupChange(section, settingType, value);
        updateChart();
    });

    // Rotation button
    $('.rotation-btn')
        .off('click')
        .on('click', function () {
            $('.rotation-btn').removeClass('active');
            $(this).addClass('active');

            const rotation = parseInt($(this).data('rotation'), 10);

            chartSettings.xAxis.labelRotation = rotation;
            updateChart();
        });

    // Input field changes
    $('.form-input input').on('change', function () {
        const section = $(this).closest('.content-section').attr('id').replace('-section', '');
        const settingType = $(this).closest('.form-row').find('.form-label').text().trim();
        const value = $(this).val();

        handleInputChange(section, settingType, value);
        updateChart();
    });

    // Select field changes
    $('.form-input select, .legends-position-options li').on('click', function () {
        const section = $(this).closest('.content-section').attr('id').replace('-section', '');
        let settingType, value;

        if ($(this).hasClass('legends-position-options') || $(this).parent().hasClass('legends-position-options')) {
            settingType = 'Position';
            value = $(this).text().trim().toLowerCase();

            $(this).closest('.legends-position').find('button span').text($(this).text().trim());

            $(this).siblings().removeClass('active');
            $(this).addClass('active');
        } else {
            settingType = $(this).closest('.form-row').find('.form-label').text().trim();
            value = $(this).val();
        }

        handleSelectChange(section, settingType, value);
        updateChart();
    });

    // Legend position dropdown handling
    $('.legends-position-options li').on('click', function () {
        const position = $(this).text().trim().toLowerCase();
        $('.legends-position button span').text($(this).text().trim());

        $(this).siblings().removeClass('active');
        $(this).addClass('active');

        chartSettings.legend.position = position;
        updateChart();
    });

    // Metrics selection for chart overlay
    setupMetricsSelection();

    // Handle clicking outside the panel to close it
    $(document).on('click', function (e) {
        const formatPanel = $('#formatPanel');
        if (formatPanel.is(':visible') && !$(e.target).closest('#formatPanel').length && !$(e.target).closest('#formatBtn').length) {
            formatPanel.hide();
        }
    });

    // Initialize the form with current settings values
    updateFormValues();
}

function updateFormValues() {
    // X-Axis settings
    $('input[name="x-axis-title"]').val(chartSettings.xAxis.title);
    $(`.rotation-btn[data-rotation="${chartSettings.xAxis.labelRotation}"]`).addClass('active').siblings().removeClass('active');

    // Y-Axis settings
    $('input[name="y-axis-title"]').val(chartSettings.yAxis.title);
    $('input[name="y-axis-interval"]').val(chartSettings.yAxis.interval || '');
    $('input[name="y-axis-interval"]').attr('placeholder', 'Auto');
    $('input[name="y-axis-min"]').val(chartSettings.yAxis.minValue || '');
    $('input[name="y-axis-min"]').attr('placeholder', 'Auto');
    $('input[name="y-axis-max"]').val(chartSettings.yAxis.maxValue || '');
    $('input[name="y-axis-max"]').attr('placeholder', 'Auto');
    // Set button groups
    $('#y-axis-section .form-row').each(function () {
        const label = $(this).find('.form-label').text().trim();
        if (label === 'Number Abbreviations') {
            const btnValue = chartSettings.yAxis.abbreviations ? 'On' : 'Off';
            $(this).find(`.group-btn:contains('${btnValue}')`).addClass('active').siblings().removeClass('active');
        }
    });

    // Chart overlay settings
    $('input[name="overlay-title"]').val(chartSettings.chartOverlay.title);
    $('input[name="overlay-interval"]').val(chartSettings.chartOverlay.interval || '');
    $('input[name="overlay-interval"]').attr('placeholder', 'Auto');
    $('input[name="overlay-min"]').val(chartSettings.chartOverlay.minValue || '');
    $('input[name="overlay-min"]').attr('placeholder', 'Auto');
    $('input[name="overlay-max"]').val(chartSettings.chartOverlay.maxValue || '');
    $('input[name="overlay-max"]').attr('placeholder', 'Auto');

    $('#chart-overlay-section .form-row').each(function () {
        const label = $(this).find('.form-label').text().trim();
        if (label === 'View as Axis') {
            const btnValue = chartSettings.chartOverlay.enabled ? 'On' : 'Off';
            $(this).find(`.group-btn:contains('${btnValue}')`).addClass('active').siblings().removeClass('active');
        } else if (label === 'Number Abbreviations') {
            const btnValue = chartSettings.chartOverlay.abbreviations ? 'On' : 'Off';
            $(this).find(`.group-btn:contains('${btnValue}')`).addClass('active').siblings().removeClass('active');
        }
    });

    // Legend settings
    $('#legend-section .form-row').each(function () {
        const label = $(this).find('.form-label').text().trim();
        if (label === 'Show Legend') {
            const btnValue = chartSettings.legend.show ? 'Yes' : 'No';
            $(this).find(`.group-btn:contains('${btnValue}')`).addClass('active').siblings().removeClass('active');
        }
    });

    // Set legend position in dropdown
    const capitalizedPosition = chartSettings.legend.position.charAt(0).toUpperCase() + chartSettings.legend.position.slice(1);
    $('.legends-position button span').text(capitalizedPosition);
    $(`.legends-position-options li:contains('${capitalizedPosition}')`).addClass('active').siblings().removeClass('active');
}

function handleButtonGroupChange(section, settingType, value) {
    if (section === 'chart-overlay') {
        if (settingType === 'View as Axis') {
            chartSettings.chartOverlay.enabled = value === 'On';
        } else if (settingType === 'Number Abbreviations') {
            chartSettings.chartOverlay.abbreviations = value === 'On';
        }
    } else if (section === 'y-axis') {
        if (settingType === 'Number Abbreviations') {
            chartSettings.yAxis.abbreviations = value === 'On';
        }
    } else if (section === 'legend') {
        if (settingType === 'Show Legend') {
            chartSettings.legend.show = value === 'Yes';
        }
    }
}

function handleInputChange(section, settingType, value) {
    if (section === 'x-axis') {
        if (settingType === 'Title') {
            chartSettings.xAxis.title = value;
        }
    } else if (section === 'y-axis') {
        switch (settingType) {
            case 'Title':
                chartSettings.yAxis.title = value;
                break;
            case 'Interval':
                chartSettings.yAxis.interval = value === '' ? null : parseInt(value);
                break;
            case 'Min Value':
                chartSettings.yAxis.minValue = value === '' ? null : parseFloat(value);
                break;
            case 'Max Value':
                chartSettings.yAxis.maxValue = value === '' ? null : parseFloat(value);
                break;
        }
    } else if (section === 'chart-overlay') {
        switch (settingType) {
            case 'Title':
                chartSettings.chartOverlay.title = value;
                break;
            case 'Interval':
                chartSettings.chartOverlay.interval = value === '' ? null : parseInt(value);
                break;
            case 'Min Value':
                chartSettings.chartOverlay.minValue = value === '' ? null : parseFloat(value);
                break;
            case 'Max Value':
                chartSettings.chartOverlay.maxValue = value === '' ? null : parseFloat(value);
                break;
        }
    }
}

function handleSelectChange(section, settingType, value) {
    if (section === 'legend') {
        if (settingType === 'Position') {
            chartSettings.legend.position = value.toLowerCase();
        }
    }
}

function setupMetricsSelection() {
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
            updateChart();
        }
    });

    // Remove tag
    $(document).on('click', '.remove-tag', function (e) {
        e.stopPropagation();

        const metric = $(this).parent().data('value');
        removeMetricTag(metric);
        updateChart();
    });

    updateSelectedTags();
}

function populateTagDropdown() {
    const dropdownContent = $('#tagDropdownContent');
    dropdownContent.empty();

    // Get all available metrics
    if (window.myBarChart && window.myBarChart.data && window.myBarChart.data.datasets) {
        // Filter out already selected metrics
        const availableMetrics = window.myBarChart.data.datasets.map((dataset) => dataset.label).filter((metric) => !chartSettings.chartOverlay.metrics.includes(metric));

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
    if (!chartSettings.chartOverlay.metrics.includes(metric)) {
        chartSettings.chartOverlay.metrics.push(metric);
        updateSelectedTags();
    }
}

function removeMetricTag(metric) {
    chartSettings.chartOverlay.metrics = chartSettings.chartOverlay.metrics.filter((m) => m !== metric);
    updateSelectedTags();
}

function updateSelectedTags() {
    const tagsContainer = $('#selectedTags');
    tagsContainer.empty();

    chartSettings.chartOverlay.metrics.forEach((metric) => {
        tagsContainer.append(`
            <div class="tag" data-value="${metric}">
                ${metric} <span class="remove-tag">×</span>
            </div>
        `);
    });
}

function updateChart() {
    if (!window.myBarChart) return;
    const chartConfig = window.myBarChart.config;

    // Apply X-Axis settings
    chartConfig.options.scales.x = chartConfig.options.scales.x || {};
    chartConfig.options.scales.x.title = chartConfig.options.scales.x.title || {};

    if (chartSettings.xAxis.title && chartSettings.xAxis.title.trim() !== '') {
        chartConfig.options.scales.x.title.display = true;
        chartConfig.options.scales.x.title.text = chartSettings.xAxis.title;
    } else {
        chartConfig.options.scales.x.title.display = false;
    }

    // Apply label rotation
    chartConfig.options.scales.x.ticks = chartConfig.options.scales.x.ticks || {};
    chartConfig.options.scales.x.ticks.maxRotation = chartSettings.xAxis.labelRotation;
    chartConfig.options.scales.x.ticks.minRotation = chartSettings.xAxis.labelRotation;

    // Apply Y-Axis settings
    chartConfig.options.scales.y = chartConfig.options.scales.y || {};
    chartConfig.options.scales.y.beginAtZero = true;
    chartConfig.options.scales.y.title = chartConfig.options.scales.y.title || {};
    chartConfig.options.scales.y.title.display = true;
    chartConfig.options.scales.y.title.text = chartSettings.yAxis.title;

    // Apply Y-Axis min/max values - Only apply if not null
    if (chartSettings.yAxis.minValue !== null) {
        chartConfig.options.scales.y.min = chartSettings.yAxis.minValue;
    } else {
        delete chartConfig.options.scales.y.min;
    }

    if (chartSettings.yAxis.maxValue !== null) {
        chartConfig.options.scales.y.max = chartSettings.yAxis.maxValue;
    } else {
        delete chartConfig.options.scales.y.max;
    }

    // Apply Y-Axis interval - Only apply if not null
    if (chartSettings.yAxis.interval && chartSettings.yAxis.interval > 0) {
        chartConfig.options.scales.y.ticks = chartConfig.options.scales.y.ticks || {};
        chartConfig.options.scales.y.ticks.stepSize = chartSettings.yAxis.interval;
    } else {
        if (chartConfig.options.scales.y.ticks) {
            delete chartConfig.options.scales.y.ticks.stepSize;
        }
    }

    // Apply Y-Axis number abbreviations
    if (chartSettings.yAxis.abbreviations) {
        chartConfig.options.scales.y.ticks = chartConfig.options.scales.y.ticks || {};
        chartConfig.options.scales.y.ticks.callback = function (value) {
            if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
            if (value >= 1000) return (value / 1000).toFixed(1) + 'K';
            return value;
        };
    } else {
        if (chartConfig.options.scales.y.ticks && chartConfig.options.scales.y.ticks.callback) {
            delete chartConfig.options.scales.y.ticks.callback;
        }
    }

    // Apply Chart Overlay settings
    if (chartSettings.chartOverlay.enabled) {
        applyChartOverlay(chartConfig);
    } else {
        // Remove secondary axis if it exists
        if (chartConfig.options.scales.y1) {
            delete chartConfig.options.scales.y1;
        }

        // Reset all datasets to the primary y-axis
        chartConfig.data.datasets.forEach((dataset) => {
            dataset.yAxisID = 'y';
            dataset.type = currentChartType;
            delete dataset.order;
        });
    }

    // Apply Legend settings - Fixed to properly set position
    chartConfig.options.plugins = chartConfig.options.plugins || {};
    chartConfig.options.plugins.legend = chartConfig.options.plugins.legend || {};
    chartConfig.options.plugins.legend.display = chartSettings.legend.show;
    chartConfig.options.plugins.legend.position = chartSettings.legend.position;

    if (currentChartType === 'line') {
        // Configure global line chart options
        chartConfig.options.elements = chartConfig.options.elements || {};
        chartConfig.options.elements.point = chartConfig.options.elements.point || {};
        chartConfig.options.elements.line = chartConfig.options.elements.line || {};

        // Apply these properties to individual datasets
        chartConfig.data.datasets.forEach((dataset) => {
            if (dataset.type === 'line' || currentChartType === 'line') {
                dataset.pointRadius = 3;
                dataset.pointHoverRadius = 5;
                dataset.pointHoverBorderWidth = 2;
                dataset.pointHoverBackgroundColor = dataset.borderColor + '70';
                dataset.pointHoverBorderColor = dataset.borderColor;
                dataset.borderWidth = 2;
                dataset.tension = 0.1;
                dataset.fill = false;
            }
        });
    }
    // Update the chart
    window.myBarChart.update();
}

function applyChartOverlay(chartConfig) {
    const { gridLineColor, tickColor } = getGraphGridColors();

    // Secondary y-axis (y1)
    chartConfig.options.scales.y1 = {
        type: 'linear',
        position: 'right',
        beginAtZero: true,
        title: {
            display: true,
            text: chartSettings.chartOverlay.title,
        },
        grid: {
            drawOnChartArea: false,
            color: gridLineColor,
        },
        ticks: {
            color: tickColor,
        },
    };

    // Apply min value if set
    if (chartSettings.chartOverlay.minValue !== null) {
        chartConfig.options.scales.y1.min = chartSettings.chartOverlay.minValue;
    }

    // Apply max value
    if (chartSettings.chartOverlay.maxValue !== null) {
        chartConfig.options.scales.y1.max = chartSettings.chartOverlay.maxValue;
    }

    // Apply interval
    if (chartSettings.chartOverlay.interval && chartSettings.chartOverlay.interval > 0) {
        chartConfig.options.scales.y1.ticks = chartConfig.options.scales.y1.ticks || {};
        chartConfig.options.scales.y1.ticks.stepSize = chartSettings.chartOverlay.interval;
    }

    // Apply abbreviations
    if (chartSettings.chartOverlay.abbreviations) {
        chartConfig.options.scales.y1.ticks = chartConfig.options.scales.y1.ticks || {};
        chartConfig.options.scales.y1.ticks.callback = function (value) {
            if (value >= 1000000) return (value / 1000000).toFixed(1) + 'M';
            if (value >= 1000) return (value / 1000).toFixed(1) + 'K';
            return value;
        };
    }

    // Set all primary datasets
    chartConfig.data.datasets.forEach((dataset) => {
        if (!chartSettings.chartOverlay.metrics.includes(dataset.label)) {
            dataset.yAxisID = 'y';
            dataset.type = currentChartType;
            dataset.order = 1;
        }
    });

    // Set overlay metrics to secondary axis
    chartConfig.data.datasets.forEach((dataset) => {
        if (chartSettings.chartOverlay.metrics.includes(dataset.label)) {
            dataset.yAxisID = 'y1';
            dataset.type = 'line'; // Always set overlay metrics to line type
            dataset.fill = false;
            dataset.order = 0;

            dataset.pointRadius = 3;
            dataset.pointHoverRadius = 5;
            dataset.pointHoverBorderWidth = 2;
            dataset.pointBackgroundColor = dataset.borderColor + '70';
            dataset.pointHoverBorderColor = dataset.borderColor;
            dataset.borderWidth = 2;
            dataset.tension = 0.1;
            dataset.fill = false;
        }
    });
}

//eslint-disable-next-line no-unused-vars
function updateTimeChartTheme() {
    if (!window.myBarChart) return;

    const { gridLineColor, tickColor } = getGraphGridColors();

    window.myBarChart.options.scales.x.grid.color = gridLineColor;
    window.myBarChart.options.scales.x.ticks.color = tickColor;

    window.myBarChart.options.scales.y.grid.color = gridLineColor;
    window.myBarChart.options.scales.y.ticks.color = tickColor;

    if (window.myBarChart.options.scales.y1) {
        window.myBarChart.options.scales.y1.grid.color = gridLineColor;
        window.myBarChart.options.scales.y1.ticks.color = tickColor;
    }

    window.myBarChart.options.plugins.legend.labels.color = tickColor;

    window.myBarChart.update();
}
