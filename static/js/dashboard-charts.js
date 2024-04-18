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
var pieOptions, barOptions;

function loadBarOptions(xAxisData, yAxisData) {
	// colors for dark & light modes
	let root = document.querySelector(':root');
	let rootStyles = getComputedStyle(root);
	let gridLineDarkThemeColor = rootStyles.getPropertyValue('--black-3');
	let gridLineLightThemeColor = rootStyles.getPropertyValue('--white-3');
	let labelDarkThemeColor = rootStyles.getPropertyValue('--white-0');
	let labelLightThemeColor = rootStyles.getPropertyValue('--black-1')

	barOptions = {
		xAxis: {
			type: 'category',
			data: xAxisData,
			axisLine: {
				lineStyle: {
					color: gridLineDarkThemeColor,
				}
			},
			axisLabel: {
				interval: 0, // Set this to 0 to display all labels
				rotate: 45, // You can adjust this value to rotate the labels
				margin: 10, // You can adjust this value to add or reduce spacing between the labels and the axis line
				color: function () {
					return $('html').attr('data-theme') == 'dark' ? labelDarkThemeColor : labelLightThemeColor;
				}
			}
		},
		yAxis: {
			type: 'value',
			axisLabel: {
				color: function () {
					return $('html').attr('data-theme') == 'dark' ? labelDarkThemeColor : labelLightThemeColor;
				}
			},
			splitLine: {
				lineStyle: {
					color: gridLineDarkThemeColor,
				},
			},
		},
		series: [
			{
				data: yAxisData,
				type: 'bar',
				barWidth: 10,
				itemStyle: {
					// color: '#6347D9' // You can set any color code here
					color: function (params) {
						var colorList = ['#6347D9', '#FF8700']; // Define an array of colors
						return colorList[params.dataIndex % colorList.length]; // Use the modulus operator to alternate between colors
					}
				},
				barCategoryGap: '10%', // You can adjust this value to add space between bars
				barGap: '15%',
				tooltip: {
					trigger: 'axis', // Set the trigger type for the tooltip
					axisPointer: { // Set the type of pointer that shows up when hovering over the bars
						type: 'shadow' // 'line' or 'shadow' for vertical or horizontal lines, respectively
					},
					formatter: function (params) { // Add a formatter function to show the value of the bar in the tooltip
						return params[0].name + ': ' + params[0].value;
					}
				},
				emphasis: {
					itemStyle: {
						color: '#FFC107' // Set the color of the bars when hovering over them
					},
					label: {
						show: true,
						position: 'top',
						formatter: function (params) { // Add a formatter function to show the value of the bar on hover
							return params.value;
						}
					}
				}
			},

		]
	}
	if ($('html').attr('data-theme') == "dark") {
		barOptions.xAxis.axisLine.lineStyle.color = gridLineDarkThemeColor;
		barOptions.yAxis.splitLine.lineStyle.color = gridLineDarkThemeColor;
	} else {
		barOptions.xAxis.axisLine.lineStyle.color = gridLineLightThemeColor;
		barOptions.yAxis.splitLine.lineStyle.color = gridLineLightThemeColor;
	}
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
	let labelLightThemeColor = rootStyles.getPropertyValue('--black-1')

	pieOptions = {
		xAxis: {
			show: false
		},
		tooltip: {
			trigger: 'item',
			formatter: "{a} <br/>{b} : {c} ({d}%)"
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
						shadowColor: 'rgba(0, 0, 0, 0.5)'
					}
				},
				label: {
					color: labelDarkThemeColor,
				}
			}
		]
	}
	if ($('html').attr('data-theme') == "dark") {
		pieOptions.series[0].label.color = labelDarkThemeColor;
		pieOptions.legend.textStyle.borderColor = labelDarkThemeColor;
		pieOptions.legend.textStyle.borderColor = labelDarkThemeColor;
	} else {
		pieOptions.series[0].label.color = labelLightThemeColor;
		pieOptions.legend.textStyle.color = labelLightThemeColor;
		pieOptions.legend.textStyle.borderColor = labelDarkThemeColor;
	}

}
function renderBarChart(columns, hits, panelId, chartType, dataType, panelIndex) {
	$(".panelDisplay #panelLogResultsGrid").hide();
	$(".panelDisplay #empty-response").empty();
	$('.panelDisplay #corner-popup').hide();
	$(".panelDisplay #empty-response").hide();
	$(`.panelDisplay .big-number-display-container`).empty();
	$(`.panelDisplay .big-number-display-container`).hide();
	$('.panelDisplay .panEdit-panel').empty();
	let bigNumVal = null;
	let xAxisData = [];
	let yAxisData = [];

	if (columns.length == 1) {
		bigNumVal = hits[0].MeasureVal[columns[0]];
	}

	// loop through the hits and create the data for the bar chart
	for (let i = 0; i < hits.length; i++) {
		let hit = hits[i];

		let xAxisValue = hit.GroupByValues[0];
		let yAxisValue;
		let measureVal = hit.MeasureVal;
		yAxisValue = measureVal[columns[1]]
		xAxisData.push(xAxisValue);
		yAxisData.push(yAxisValue);
	}

	let panelChart;
	if (panelId == -1) {
		let panelChartEl = document.querySelector(`.panelDisplay .panEdit-panel`);
		panelChart = echarts.init(panelChartEl);
	} else if(chartType !== 'number'){
		let panelChartEl = $(`#panel${panelId} .panEdit-panel`);
		panelChartEl.css("width", "100%").css("height", "100%");
		panelChart = echarts.init(document.querySelector(`#panel${panelId} .panEdit-panel`));
	}

	if (bigNumVal != null) {
		chartType = 'number';
	}

	switch (chartType) {
		case 'Bar Chart':
			$(`.panelDisplay .big-number-display-container`).hide();
			loadBarOptions(xAxisData, yAxisData);
			panelChart.setOption(barOptions);
			break;
		case 'Pie Chart':
			$(`.panelDisplay .big-number-display-container`).hide();
			loadPieOptions(xAxisData, yAxisData);
			panelChart.setOption(pieOptions);
			break;
		case 'number':
			displayBigNumber(bigNumVal, panelId, dataType, panelIndex);
	}
	$(`#panel${panelId} .panel-body #panel-loading`).hide();

	return panelChart;
}
let mapIndexToAbbrev = new Map([
	["", ""],
	["none", ""],
	["percent(0-100)", "%"],
	["bytes", "B"],
	["kB", "KB"],
	["MB", "MB"],
	["GB", "GB"],
	["TB", "TB"],
	["PB", "PB"],
	["EB", "EB"],
	["ZB", "ZB"],
	["YB", "YB"],
	["counts/sec", "c/s"],
	["writes/sec", "wr/s"],
	["reads/sec", "rd/s"],
	["requests/sec", "req/s"],
	["ops/sec", "ops/s"],
	["hertz(1/s)", "Hz"],
	["nanoseconds(ns)", "ns"],
	["microsecond(µs)", "µs"],
	["milliseconds(ms)", "ms"],
	["seconds(s)", "s"],
	["minutes(m)", "m"],
	["hours(h)", "h"],
	["days(d)", "d"],
	["packets/sec", "p/s"],
	["bytes/sec", "B/s"],
	["bits/sec", "b/s"],
	["kilobytes/sec", "KB/s"],
	["kilobits/sec", "Kb/s"],
	["megabytes/sec", "MB/s"],
	["megabits/sec", "Mb/s"],
	["gigabytes/sec", "GB/s"],
	["gigabits/sec", "Gb/s"],
	["terabytes/sec", "TB/s"],
	["terabits/sec", "Tb/s"],
	["petabytes/sec", "PB/s"],
	["petabits/sec", "Pb/s"],
])

function addSuffix(number) {
	let suffix = '';
	if (number >= 1e24) {
		suffix = "Y";
		number /= 1e24;
	} else if (number >= 1e21) {
		suffix = "Z";
		number /= 1e21;
	} else if (number >= 1e18) {
	  suffix = "E";
	  number /= 1e18;
	} else if (number >= 1e15) {
	  suffix = "P";
	  number /= 1e15;
	} else if (number >= 1e12) {
	  suffix = "T";
	  number /= 1e12;
	} else if (number >= 1e9) {
	  suffix = "G";
	  number /= 1e9;
	} else if (number >= 1e6) {
	  suffix = "M";
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

  return [smallest.toFixed(2),suffix];
}

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
		panelChartEl.css("width", "100%").css("height", "100%");
	}

	if (!value) {
		panelChartEl.append(`<div class="big-number">NA</div> `);
	} else {
		if (dataType != null && dataType != undefined && dataType != ''){
			var bigNum = [];
			let dataTypeAbbrev = mapIndexToAbbrev.get(dataType);
			let number = parseFloat(value.replace(/,/g, ''));
			let dataTypeAbbrevCap = dataTypeAbbrev.substring(0,2).toUpperCase()
			if (["KB",'MB',"GB","TB","PB","EB","ZB","YB"].includes(dataTypeAbbrevCap)){
				dataTypeAbbrev = dataTypeAbbrev.substring(1)
				if(dataTypeAbbrevCap === "KB"){
					number = number * 1000;
				}else if (dataTypeAbbrevCap === "MB"){
					number = number * 1e6;
				} else if(dataTypeAbbrevCap === "GB"){
					number = number * 1e9;
				} else if(dataTypeAbbrevCap === "TB"){
					number = number * 1e12;
				} else if(dataTypeAbbrevCap === "PB"){
					number = number * 1e15;
				} else if (dataTypeAbbrevCap === "EB") {
					number *= 1e18;
				} else if (dataTypeAbbrevCap === "ZB") {
					number *= 1e21;
				} else if (dataTypeAbbrevCap === "YB") {
					number *= 1e24;
				}
				bigNum = addSuffix(number);

			} else if(['ns',"µs", "ms", "d","m","s","h"].includes(dataTypeAbbrev)){				
				if (dataTypeAbbrev === "ns"){
					number = number / 1e9;
				} else if (dataTypeAbbrev === "µs"){
					number = number / 1e6;
				}else if (dataTypeAbbrev === "ms"){
					number = number / 1e3;
				}else if (dataTypeAbbrev === "d"){
					number = number * 24 * 60 * 60
				}else if (dataTypeAbbrev === "m"){
					number = number * 60
				}else if (dataTypeAbbrev === "s"){
					number = number 
				}else if (dataTypeAbbrev === "h"){
					number = number * 3600;
				}
				dataTypeAbbrev = ""
				bigNum = findSmallestGreaterOne(number);		
			} else if (dataTypeAbbrev === "" || dataTypeAbbrev === "%"){
				bigNum[1]  = ""
				bigNum[0] = number
			} else{
				bigNum = addSuffix(number);		
			}	
			panelChartEl.append(`<div class="big-number">${bigNum[0]} </div> <div class="unit">${bigNum[1]+dataTypeAbbrev} </div> `);
		}else{
			panelChartEl.append(`<div class="big-number">${value} </div> `);
		}
	}


	if(panelId === -1) {
		let parentWidth = $(".panelDisplay").width();
		let numWidth = $(".panelDisplay .big-number-display-container").width();
		if (numWidth > parentWidth){
			$(".big-number").css("font-size","5.5em");
			$(".unit").css("font-size","55px");
		}
	}
	if(panelId !== -1) {
		resizePanelFontSize(panelIndex, panelId);
	}
}

function createColorsArray() {
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let colorArray = [];
    for (let i = 1; i <= 20; i++) {
        colorArray.push(rootStyles.getPropertyValue(`--graph-line-color-${i}`));
    }
    return colorArray;
}

function renderLineChart(seriesArray, metricsDatasets, labels, panelId, chartType, flag) {
	const colors = createColorsArray();
    let gridLineColor;
    let tickColor;
	let root = document.querySelector(':root');
	let rootStyles = getComputedStyle(root);
	let gridLineDarkThemeColor = rootStyles.getPropertyValue('--black-3');
	let gridLineLightThemeColor = rootStyles.getPropertyValue('--white-3');
	let tickDarkThemeColor = rootStyles.getPropertyValue('--white-0');
	let tickLightThemeColor = rootStyles.getPropertyValue('--white-6');

	if ($('html').attr('data-theme') == "light") {
        gridLineColor = gridLineLightThemeColor;
        tickColor = tickLightThemeColor;
    }
    else {
        gridLineColor = gridLineDarkThemeColor;
        tickColor = tickDarkThemeColor;
    }
	let datasets = [];
    let data = {};

    datasets = seriesArray.map((o, i) => ({
        label: o.seriesName,
        data: Object.values(o.values),
        backgroundColor: colors[i],
        borderColor: colors[i],
        color: gridLineColor,
        borderWidth: 2,
        fill: false,
    }));
	data = {
        labels: labels,
        datasets: datasets
    }

	const config = {
        type: 'line',
        data,
        options: {
            maintainAspectRatio: false,
            responsive: true,
            title: {
                display: true,
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    enabled: true,
                }
            },
            scales: {
                x: {
                    grid: {
                        color: function () {
							return $('html').attr('data-theme') == 'dark' ? gridLineDarkThemeColor : gridLineLightThemeColor;
						},
                    },
                    ticks: {
                        color: function () {
							return $('html').attr('data-theme') == 'dark' ? tickDarkThemeColor : tickLightThemeColor;
                    }
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
            }
        }
	}
    }
	if (panelId == -1) {
		var panelChartEl = $(`.panelDisplay .panEdit-panel`);
	} else {
		var panelChartEl = $(`#panel${panelId} .panEdit-panel`);
		panelChartEl.css("width", "100%").css("height", "100%");
	}

	if (flag === -1) {
		let can = `<canvas class="line-chart-canvas" ></canvas>`
		panelChartEl.append(can)
		var lineCanvas = (panelChartEl).find('canvas')[0].getContext('2d');
        lineChart = new Chart(lineCanvas, config);
		$(`#panel${panelId} .panel-body #panel-loading`).hide();
		if (panelId === -1){
			panelChartEl.append(`<div class="lineChartLegend"></div>`)
			displayLegendsForLineChart(seriesArray, labels, colors,metricsDatasets,panelId, chartType);
		}
    }

    const bgColor = [];
    const bColor = data.datasets.map(color => {
        bgColor.push(color.backgroundColor);
        return color.borderColor;
    })

    if (flag == -2) {

        lineChart.config.data.datasets.map((dataset, index) => {
            dataset.backgroundColor = bgColor[index];
            dataset.borderColor = bColor[index];
        })
        lineChart.update();
    }

    if(flag >= 0) {
        const chartDataObject = lineChart.config.data.datasets.map(dataset => {
            dataset.borderColor = "rgba(0,0,0,0)";
            dataset.backgroundColor = "rgba(0,0,0,0)";
        })

        lineChart.config.data.datasets[flag].borderColor = bColor[flag];
        lineChart.config.data.datasets[flag].backgroundColor = bgColor[flag];
        lineChart.update();
    }

	return lineChart;
};

function displayLegendsForLineChart(seriesArray, labels, colors,metricsDatasets,panelId, chartType) {
    $.each(seriesArray, function (k, v) {
		const htmlString = `<div class="legend-element-line" id="legend-line-${k}"><span class="legend-colors-line" style="background-color:` + colors[k] + `"></span>` + v.seriesName + `</div>`;
        $('.lineChartLegend').append(htmlString);
	});

    let prev = null;

    const legends = document.querySelectorAll('.legend-element-line');
    $.each(legends, function (i, legend) {
        legend.addEventListener('click', (e) => {
                let currSelectedEl = parseInt((e.target.id).slice(12));
                if(prev == null) {
                    e.target.classList.add("selected");
                    prev = currSelectedEl;
                    renderLineChart(seriesArray, metricsDatasets, labels, panelId, chartType, currSelectedEl);
                } else if (prev == currSelectedEl) {
                    e.target.classList.remove("selected");
                    prev = null;
                    renderLineChart(seriesArray, metricsDatasets, labels, panelId, chartType, -2);
                } else {
                    let prevEl = document.getElementById(`legend-line-${prev}`);
                    prevEl.classList.remove("selected");
                    e.target.classList.add("selected");
                    prev = currSelectedEl;
                    renderLineChart(seriesArray, metricsDatasets, labels, panelId, chartType, currSelectedEl);                  
                }
            }
        )
    })
}

window.addEventListener('resize', function (event) {
    if ($('.panelEditor-container').css('display') !== 'none' && panelChart){
		panelChart.resize();
    }
});