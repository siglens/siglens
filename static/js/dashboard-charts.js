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
function loadBarOptions(xAxisData, yAxisData, chart) {
    const { gridLineColor, tickColor } = getGraphGridColors();

    const datasets = yAxisData.map((dataset, index) => {
        return {
            label: dataset.name,
            data: dataset.data,
            backgroundColor: globalColorArray[index % globalColorArray.length],
            borderColor: globalColorArray[index % globalColorArray.length],
            borderWidth: 1,
            barPercentage: 0.6,
            categoryPercentage: 0.8,
        };
    });
    const chartType = chart === 'Line Chart' ? 'line' : 'bar';
    return {
        type: chartType,
        data: {
            labels: xAxisData,
            datasets: datasets,
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'top',
                    labels: {
                        color: tickColor,
                        font: {
                            size: 12,
                        },
                    },
                },
            },
            scales: {
                x: {
                    ticks: {
                        color: tickColor,
                        maxRotation: xAxisData.length > 5 ? 30 : 0,
                        autoSkip: true,
                        autoSkipPadding: 10,
                    },
                    grid: {
                        display: false,
                    },
                },
                y: {
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
            },
        },
    };
}

function loadPieOptions(xAxisData, yAxisData) {
    let pieDataMapList = [];
    // loop
    for (let i = 0; i < xAxisData.length; i++) {
        let mapVal1 = yAxisData[i];
        let mapVal2 = xAxisData[i];
        let pieDataMap = {};
        pieDataMap['value'] = mapVal1;
        pieDataMap['name'] = mapVal2;
        pieDataMapList.push(pieDataMap);
    }

    const { _, tickColor } = getGraphGridColors();

    let pieOptions = {
        tooltip: {
            trigger: 'item',
            formatter: '{a} <br/>{b} : {c} ({d}%)',
        },
        legend: {
            orient: 'vertical',
            left: 'left',
            data: xAxisData,
            textStyle: {
                color: tickColor,
                borderColor: tickColor,
            },
        },
        series: [
            {
                name: 'Pie Chart',
                type: 'pie',
                radius: '50%',
                center: ['50%', '50%'],
                data: pieDataMapList,
                itemStyle: {
                    emphasis: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)',
                    },
                },
                label: {
                    color: tickColor,
                },
            },
        ],
    };

    return pieOptions;
}

//eslint-disable-next-line no-unused-vars
function renderBarChart(columns, res, panelId, chartType, dataType, panelIndex) {
    $('.panelDisplay #panelLogResultsGrid').hide();
    $('.panelDisplay #empty-response').empty();
    $('.panelDisplay #corner-popup').hide();
    $('.panelDisplay #empty-response').hide();
    $(`.panelDisplay .big-number-display-container`).empty();
    $(`.panelDisplay .big-number-display-container`).hide();
    $('.panelDisplay .panEdit-panel').empty();

    let bigNumVal = null;
    let hits = res.measure;
    if (columns.length == 1) {
        bigNumVal = hits[0].MeasureVal[columns[0]];
    }

    let panelChartEl, barChart;
    if (panelId == -1) {
        panelChartEl = document.querySelector(`.panelDisplay .panEdit-panel`);
    } else if (chartType !== 'number') {
        let panelChartSelector = $(`#panel${panelId} .panEdit-panel`);
        panelChartSelector.css('width', '100%').css('height', '100%');
        panelChartEl = document.querySelector(`#panel${panelId} .panEdit-panel`);
    }

    if (bigNumVal != null) {
        chartType = 'number';
    }

    /* eslint-disable */
    switch (chartType) {
        case 'Bar Chart':
        case 'Line Chart':
            $(`.panelDisplay .big-number-display-container`).hide();

            var multipleGroupBy = hits[0].GroupByValues.length > 1;
            let measureFunctions = res.measureFunctions;

            var seriesData = measureFunctions.map(function (measureFunction) {
                return {
                    name: measureFunction,
                    data: hits.map(function (item) {
                        return item.MeasureVal[measureFunction] || 0;
                    }),
                };
            });

            let xData = hits.map((item) => {
                let groupByValue = formatGroupByValues(item.GroupByValues, multipleGroupBy);

                // If groupByValue is null, set it to "NULL" or any other default label
                if (groupByValue === null || groupByValue === undefined || groupByValue === '') {
                    groupByValue = 'NULL'; // or "Unknown", "N/A", etc.
                }

                return groupByValue;
            });

            $(panelChartEl).html('<canvas class="bar-chart-canvas"></canvas>');
            const canvasEl = $(panelChartEl).find('canvas')[0];
            const ctx = canvasEl.getContext('2d');

            if (barChart) {
                barChart.destroy();
            }

            var barConfig = loadBarOptions(xData, seriesData, chartType);

            barChart = new Chart(ctx, barConfig);

            let panel = panelId === -1 ? currentPanel : localPanels[panelIndex];
            if (panel && panel.formatSettings) {
                window.myBarChart = barChart;

                chartSettings = JSON.parse(JSON.stringify(panel.formatSettings));

                currentChartType = chartType === 'Line Chart' ? 'line' : 'bar';

                updateChart();
            }

            if (panelId !== -1 && seriesData.length > 5) {
                // Disable legend display
                barConfig.options.plugins.legend.display = false;
            }
            break;
        case 'Pie Chart':
            $(`.panelDisplay .big-number-display-container`).hide();

            let xAxisData = [];
            let yAxisData = [];
            // loop through the hits and create the data for the bar chart
            for (let i = 0; i < hits.length; i++) {
                let hit = hits[i];
                let xAxisValue = hit.GroupByValues[0];
                let yAxisValue;
                let measureVal = hit.MeasureVal;
                yAxisValue = Object.values(measureVal)[0];

                if (xAxisValue === null || xAxisValue === undefined || xAxisValue === '') {
                    xAxisValue = 'NULL'; // or "Unknown", "N/A", etc.
                }

                xAxisData.push(xAxisValue);
                yAxisData.push(yAxisValue);
            }

            var pieOptions = loadPieOptions(xAxisData, yAxisData);

            if (panelId !== -1 && xAxisData.length > 5) {
                // Disable legend display for pie chart
                pieOptions.legend.show = false;
            }

            let panelChart = echarts.init(panelChartEl);
            panelChart.setOption(pieOptions);
            break;
        case 'number':
            displayBigNumber(bigNumVal, panelId, dataType, panelIndex);
            break;
    }
    /* eslint-enable */
    $(`#panel${panelId} .panel-body #panel-loading`).hide();

    // Return the appropriate chart instance
    return chartType === 'Bar Chart' ? barChart : panelChart;
}

let mapIndexToAbbrev = new Map([
    ['', ''],
    ['none', ''],
    ['percent(0-100)', '%'],
    ['bytes', 'B'],
    ['kB', 'KB'],
    ['MB', 'MB'],
    ['GB', 'GB'],
    ['TB', 'TB'],
    ['PB', 'PB'],
    ['EB', 'EB'],
    ['ZB', 'ZB'],
    ['YB', 'YB'],
    ['counts/sec', 'c/s'],
    ['writes/sec', 'wr/s'],
    ['reads/sec', 'rd/s'],
    ['requests/sec', 'req/s'],
    ['ops/sec', 'ops/s'],
    ['hertz(1/s)', 'Hz'],
    ['nanoseconds(ns)', 'ns'],
    ['microsecond(µs)', 'µs'],
    ['milliseconds(ms)', 'ms'],
    ['seconds(s)', 's'],
    ['minutes(m)', 'm'],
    ['hours(h)', 'h'],
    ['days(d)', 'd'],
    ['packets/sec', 'p/s'],
    ['bytes/sec', 'B/s'],
    ['bits/sec', 'b/s'],
    ['kilobytes/sec', 'KB/s'],
    ['kilobits/sec', 'Kb/s'],
    ['megabytes/sec', 'MB/s'],
    ['megabits/sec', 'Mb/s'],
    ['gigabytes/sec', 'GB/s'],
    ['gigabits/sec', 'Gb/s'],
    ['terabytes/sec', 'TB/s'],
    ['terabits/sec', 'Tb/s'],
    ['petabytes/sec', 'PB/s'],
    ['petabits/sec', 'Pb/s'],
]);

function addSuffix(number) {
    let suffix = '';
    if (number >= 1e24) {
        suffix = 'Y';
        number /= 1e24;
    } else if (number >= 1e21) {
        suffix = 'Z';
        number /= 1e21;
    } else if (number >= 1e18) {
        suffix = 'E';
        number /= 1e18;
    } else if (number >= 1e15) {
        suffix = 'P';
        number /= 1e15;
    } else if (number >= 1e12) {
        suffix = 'T';
        number /= 1e12;
    } else if (number >= 1e9) {
        suffix = 'G';
        number /= 1e9;
    } else if (number >= 1e6) {
        suffix = 'M';
        number /= 1e6;
    }

    return [parseFloat(number).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }), suffix];
}

function findSmallestGreaterOne(number) {
    const hours = number / 3600; // Convert seconds to hours
    const minutes = number / 60; // Convert seconds to minutes
    const years = number / (3600 * 24 * 365); // Convert seconds to years

    let smallest = Infinity;
    let suffix = '';

    if (hours > 1 && hours < smallest) {
        smallest = hours;
        suffix = 'h';
    }
    if (minutes > 1 && minutes < smallest) {
        smallest = minutes;
        suffix = 'm';
    }
    if (years > 1 && years < smallest) {
        smallest = years;
        suffix = 'year';
    }

    if (smallest === Infinity) {
        smallest = number;
        suffix = 's';
    }

    return [parseFloat(smallest).toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 }), suffix];
}

//eslint-disable-next-line no-unused-vars
function displayBigNumber(value, panelId, dataType, panelIndex) {
    if (panelId === -1) {
        $('.panelDisplay .panEdit-panel').hide();
        $(`.panelDisplay .big-number-display-container`).show();
        $(`.panelDisplay .big-number-display-container`).empty();
    } else {
        $(`#panel${panelId} .panEdit-panel`).hide();
        $(`#panel${panelId} .big-number-display-container`).show();
        $(`#panel${panelId} .big-number-display-container`).empty();
    }

    let panelChartEl;

    if (panelId == -1) {
        panelChartEl = $('.panelDisplay .big-number-display-container');
    } else {
        panelChartEl = $(`#panel${panelId} .big-number-display-container`);
        $(`#panel${panelId} .panEdit-panel`).hide();
        panelChartEl = $(`#panel${panelId} .big-number-display-container`);
        panelChartEl.css('width', '100%').css('height', '100%');
    }

    if (!value) {
        panelChartEl.append(`<div class="big-number">NA</div> `);
    } else {
        if (dataType != null && dataType != undefined && dataType != '') {
            var bigNum = [];
            let dataTypeAbbrev = mapIndexToAbbrev.get(dataType);
            let number = typeof value === 'string' ? parseFloat(value.replace(/,/g, '')) : parseFloat(value);
            let dataTypeAbbrevCap = dataTypeAbbrev.substring(0, 2).toUpperCase();
            if (['KB', 'MB', 'GB', 'TB', 'PB', 'EB', 'ZB', 'YB'].includes(dataTypeAbbrevCap)) {
                dataTypeAbbrev = dataTypeAbbrev.substring(1);
                if (dataTypeAbbrevCap === 'KB') {
                    number = number * 1000;
                } else if (dataTypeAbbrevCap === 'MB') {
                    number = number * 1e6;
                } else if (dataTypeAbbrevCap === 'GB') {
                    number = number * 1e9;
                } else if (dataTypeAbbrevCap === 'TB') {
                    number = number * 1e12;
                } else if (dataTypeAbbrevCap === 'PB') {
                    number = number * 1e15;
                } else if (dataTypeAbbrevCap === 'EB') {
                    number *= 1e18;
                } else if (dataTypeAbbrevCap === 'ZB') {
                    number *= 1e21;
                } else if (dataTypeAbbrevCap === 'YB') {
                    number *= 1e24;
                }
                bigNum = addSuffix(number);
            } else if (['ns', 'µs', 'ms', 'd', 'm', 's', 'h'].includes(dataTypeAbbrev)) {
                if (dataTypeAbbrev === 'ns') {
                    number = number / 1e9;
                } else if (dataTypeAbbrev === 'µs') {
                    number = number / 1e6;
                } else if (dataTypeAbbrev === 'ms') {
                    number = number / 1e3;
                } else if (dataTypeAbbrev === 'd') {
                    number = number * 24 * 60 * 60;
                } else if (dataTypeAbbrev === 'm') {
                    number = number * 60;
                } else if (dataTypeAbbrev === 'h') {
                    number = number * 3600;
                }
                dataTypeAbbrev = '';
                bigNum = findSmallestGreaterOne(number);
            } else if (dataTypeAbbrev === '' || dataTypeAbbrev === '%') {
                bigNum[1] = '';
                bigNum[0] = parseFloat(number).toLocaleString('en-US');
            } else {
                bigNum = addSuffix(number);
            }
            panelChartEl.append(`<div class="big-number">${bigNum[0]} </div> <div class="unit">${bigNum[1] + dataTypeAbbrev} </div> `);
        } else {
            const formattedValue = !isNaN(parseFloat(value)) ? parseFloat(value).toLocaleString('en-US') : value;
            panelChartEl.append(`<div class="big-number">${formattedValue} </div> `);
        }
    }

    if (panelId === -1) {
        let parentWidth = $('.panelDisplay').width();
        let numWidth = $('.panelDisplay .big-number-display-container').width();
        if (numWidth > parentWidth) {
            $('.big-number').css('font-size', '5.5em');
            $('.unit').css('font-size', '55px');
        }
    }
    if (panelId !== -1) {
        var newSize = $('#' + panelId).width() / 8;
        $('#' + panelId)
            .find('.big-number, .unit')
            .css('font-size', newSize + 'px');
    }
}

window.addEventListener('resize', function (_event) {
    if ($('.panelEditor-container').css('display') !== 'none' && panelChart) {
        panelChart.resize();
    }
});
