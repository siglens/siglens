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

var lineChart;
function loadBarOptions(xAxisData, yAxisData) {
    // colors for dark & light modes
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let gridLineDarkThemeColor = rootStyles.getPropertyValue('--black-3');
    let gridLineLightThemeColor = rootStyles.getPropertyValue('--white-3');
    let labelDarkThemeColor = rootStyles.getPropertyValue('--white-0');
    let labelLightThemeColor = rootStyles.getPropertyValue('--black-1');

    let legendData = [];
    let seriesData = [];
    let colorList = ['#6347D9', '#FF8700'];

    yAxisData.forEach((dataset, index) => {
        legendData.push(dataset.name); // Add dataset names to legend
        seriesData.push({
            name: dataset.name,
            type: 'bar',
            data: dataset.data,
            barWidth: 10,
            itemStyle: {
                color: colorList[index % colorList.length],
            },
            barCategoryGap: '10%', // space between bars
            barGap: '15%',
            tooltip: {
                trigger: 'axis',
                axisPointer: {
                    type: 'shadow',
                },
                formatter: function (params) {
                    return params[0].name + ': ' + params[0].value;
                },
            },
            emphasis: {
                itemStyle: {
                    color: colorList[index % colorList.length],
                },
                label: {
                    show: false,
                    position: 'top',
                    formatter: function (params) {
                        return params.value;
                    },
                },
            },
        });
    });

    let barOptions = {
        legend: {
            data: legendData,
            textStyle: {
                color: function () {
                    return $('html').attr('data-theme') == 'dark' ? labelDarkThemeColor : labelLightThemeColor;
                },
            },
        },
        xAxis: {
            type: 'category',
            data: xAxisData,
            axisLine: {
                lineStyle: {
                    color: function () {
                        return $('html').attr('data-theme') == 'dark' ? gridLineDarkThemeColor : gridLineLightThemeColor;
                    },
                },
            },
            axisLabel: {
                interval: 0,
                rotate: 45,
                margin: 10,
                color: function () {
                    return $('html').attr('data-theme') == 'dark' ? labelDarkThemeColor : labelLightThemeColor;
                },
            },
        },
        yAxis: {
            type: 'value',
            axisLabel: {
                color: function () {
                    return $('html').attr('data-theme') == 'dark' ? labelDarkThemeColor : labelLightThemeColor;
                },
            },
            splitLine: {
                lineStyle: {
                    lineStyle: {
                        color: gridLineDarkThemeColor,
                    },
                },
            },
        },
        series: seriesData,
    };
    if ($('html').attr('data-theme') == 'dark') {
        barOptions.xAxis.axisLine.lineStyle.color = gridLineDarkThemeColor;
        barOptions.yAxis.splitLine.lineStyle.color = gridLineDarkThemeColor;
    } else {
        barOptions.xAxis.axisLine.lineStyle.color = gridLineLightThemeColor;
        barOptions.yAxis.splitLine.lineStyle.color = gridLineLightThemeColor;
    }
    return barOptions;
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
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let labelDarkThemeColor = rootStyles.getPropertyValue('--white-0');
    let labelLightThemeColor = rootStyles.getPropertyValue('--black-1');

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
                color: labelDarkThemeColor,
                borderColor: labelDarkThemeColor,
            },
        },
        series: [
            {
                name: 'Pie Chart',
                type: 'pie',
                radius: '55%',
                center: ['50%', '60%'],
                data: pieDataMapList,
                itemStyle: {
                    emphasis: {
                        shadowBlur: 10,
                        shadowOffsetX: 0,
                        shadowColor: 'rgba(0, 0, 0, 0.5)',
                    },
                },
                label: {
                    color: labelDarkThemeColor,
                },
            },
        ],
    };

    if ($('html').attr('data-theme') == 'dark') {
        pieOptions.series[0].label.color = labelDarkThemeColor;
        pieOptions.legend.textStyle.borderColor = labelDarkThemeColor;
        pieOptions.legend.textStyle.borderColor = labelDarkThemeColor;
    } else {
        pieOptions.series[0].label.color = labelLightThemeColor;
        pieOptions.legend.textStyle.color = labelLightThemeColor;
        pieOptions.legend.textStyle.borderColor = labelDarkThemeColor;
    }

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

    let panelChart;
    if (panelId == -1) {
        let panelChartEl = document.querySelector(`.panelDisplay .panEdit-panel`);
        panelChart = echarts.init(panelChartEl);
    } else if (chartType !== 'number') {
        let panelChartEl = $(`#panel${panelId} .panEdit-panel`);
        panelChartEl.css('width', '100%').css('height', '100%');
        panelChart = echarts.init(document.querySelector(`#panel${panelId} .panEdit-panel`));
    }

    if (bigNumVal != null) {
        chartType = 'number';
    }
    /* eslint-disable */
    switch (chartType) {
        case 'Bar Chart':
            $(`.panelDisplay .big-number-display-container`).hide();

            // Determine if multiple group by values are used
            var multipleGroupBy = hits[0].GroupByValues.length > 1;
            let measureFunctions = res.measureFunctions;

            // Prepare series data for the bar chart
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
            var barOptions = loadBarOptions(xData, seriesData);
            panelChart.setOption(barOptions);
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
            panelChart.setOption(pieOptions);
            break;
        case 'number':
            displayBigNumber(bigNumVal, panelId, dataType, panelIndex);
            break;
    }
    /* eslint-enable */
    $(`#panel${panelId} .panel-body #panel-loading`).hide();

    return panelChart;
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

    return [number.toFixed(2), suffix];
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

    return [smallest.toFixed(2), suffix];
}

//eslint-disable-next-line no-unused-vars
function displayBigNumber(value, panelId, dataType, panelIndex) {
    let panelChartEl;

    // First get the container reference
    if (panelId == -1) {
        panelChartEl = $('.panelDisplay .big-number-display-container');
        $('.panelDisplay .panEdit-panel').hide();
    } else {
        panelChartEl = $(`#panel${panelId} .big-number-display-container`);
        $(`#panel${panelId} .panEdit-panel`).hide();
    }

    // Clear existing content and ensure container is ready
    panelChartEl.empty().show();

    // Set container dimensions
    if (panelId !== -1) {
        panelChartEl.css('width', '100%').css('height', '100%');
    }

    // Handle the value display
    if (!value || value === 'undefined' || value === 'null') {
        panelChartEl.append(`<div class="big-number">NA</div>`);
        return;
    }

    if (dataType != null && dataType !== undefined && dataType !== '') {
        var bigNum = [];
        let dataTypeAbbrev = mapIndexToAbbrev.get(dataType);
        let number = typeof value === 'string' ? parseFloat(value.replace(/,/g, '')) : parseFloat(value);

        if (!isNaN(number)) {
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
                bigNum[0] = number;
            } else {
                bigNum = addSuffix(number);
            }
            panelChartEl.append(`<div class="big-number">${bigNum[0]}</div> <div class="unit">${bigNum[1]}${dataTypeAbbrev}</div>`);
        } else {
            panelChartEl.append(`<div class="big-number">NA</div>`);
        }
    } else {
        // For values without a data type, just display the raw value
        panelChartEl.append(`<div class="big-number">${value}</div>`);
    }

    // Handle font sizing
    if (panelId === -1) {
        let parentWidth = $('.panelDisplay').width();
        let numWidth = panelChartEl.width();
        if (numWidth > parentWidth) {
            $('.big-number').css('font-size', '5.5em');
            $('.unit').css('font-size', '55px');
        }
    } else {
        var newSize = $('#' + panelId).width() / 8;
        $('#' + panelId)
            .find('.big-number, .unit')
            .css('font-size', newSize + 'px');
    }
}

//eslint-disable-next-line no-unused-vars
function createColorsArray() {
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let colorArray = [];
    for (let i = 1; i <= 20; i++) {
        colorArray.push(rootStyles.getPropertyValue(`--graph-line-color-${i}`));
    }
    return colorArray;
}

//eslint-disable-next-line no-unused-vars
function renderLineChart(seriesData, panelId) {
    let classic = ['#a3cafd', '#5795e4', '#d7c3fa', '#7462d8', '#f7d048', '#fbf09e'];
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let gridLineDarkThemeColor = rootStyles.getPropertyValue('--black-3');
    let gridLineLightThemeColor = rootStyles.getPropertyValue('--white-3');
    let tickDarkThemeColor = rootStyles.getPropertyValue('--white-0');
    let tickLightThemeColor = rootStyles.getPropertyValue('--white-6');
    var labels, datasets;
    var panelChartEl;
    // Extract labels and datasets from seriesData
    if (seriesData.length > 0) {
        labels = Object.keys(seriesData[0].values);
        datasets = seriesData.map(function (series, index) {
            return {
                label: series.seriesName,
                data: Object.values(series.values),
                borderColor: classic[index % classic.length],
                backgroundColor: classic[index % classic.length] + 70,
                borderWidth: 2,
                fill: false,
            };
        });
    } else {
        labels = [];
        datasets = [];
    }

    var chartData = {
        labels: labels,
        datasets: datasets,
    };

    const config = {
        type: 'line',
        data: chartData,
        options: {
            maintainAspectRatio: false,
            responsive: true,
            title: {
                display: true,
            },
            plugins: {
                legend: {
                    position: 'bottom',
                    align: 'start',
                    labels: {
                        boxWidth: 10,
                        boxHeight: 2,
                        fontSize: 10,
                    },
                },
            },
            scales: {
                x: {
                    grid: {
                        display: false,
                    },
                    ticks: {
                        color: function () {
                            return $('html').attr('data-theme') == 'dark' ? tickDarkThemeColor : tickLightThemeColor;
                        },
                    },
                },
                y: {
                    grid: {
                        color: function () {
                            return $('html').attr('data-theme') == 'dark' ? gridLineDarkThemeColor : gridLineLightThemeColor;
                        },
                    },
                    ticks: {
                        color: function () {
                            return $('html').attr('data-theme') == 'dark' ? tickDarkThemeColor : tickLightThemeColor;
                        },
                    },
                },
            },
        },
    };
    if (panelId == -1) {
        panelChartEl = $(`.panelDisplay .panEdit-panel`);
    } else {
        panelChartEl = $(`#panel${panelId} .panEdit-panel`);
        panelChartEl.css('width', '100%').css('height', '100%');
    }

    let can = `<canvas class="line-chart-canvas" ></canvas>`;
    panelChartEl.append(can);
    var lineCanvas = panelChartEl.find('canvas')[0].getContext('2d');
    lineChart = new Chart(lineCanvas, config);
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
    if (panelId === -1) {
        panelChartEl.append(`<div class="lineChartLegend"></div>`);
    }

    return lineChart;
}

window.addEventListener('resize', function (_event) {
    if ($('.panelEditor-container').css('display') !== 'none' && panelChart) {
        panelChart.resize();
    }
});
