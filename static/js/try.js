function initializeChart(canvas, seriesData, queryName, chartType) {
    var ctx = canvas[0].getContext('2d');
    let chartData = prepareChartData(seriesData, chartDataCollection, queryName);
    const { gridLineColor, tickColor } = getGraphGridColors();
    var selectedPalette = colorPalette[selectedTheme] || colorPalette.Classic;

    // Calculate max value from data
    const maxDataValue = Math.max(...chartData.datasets.flatMap((d) => Object.values(d.data).filter((v) => v !== null)));
    const maxYTick = maxDataValue * 1.2;

    const thresholdValue = parseFloat($('#threshold-value').val()) || 0;
    const conditionType = $('#alert-condition span').text();

    const visibleThreshold = Math.min(thresholdValue, maxYTick);

    // Get threshold value only if we're in alert mode
    let annotationConfig = {};
    if (isAlertScreen) {
        let operator = '≥';
        let boxConfig = {};

        if (conditionType === 'Is above') {
            operator = '>';
            boxConfig = {
                type: 'box',
                yMin: visibleThreshold,
                yMax: maxYTick,
                backgroundColor: 'rgb(255, 218, 224, 0.8)',
                borderWidth: 0,
            };
        } else if (conditionType === 'Is below') {
            operator = '<';
            boxConfig = {
                type: 'box',
                yMin: 0,
                yMax: visibleThreshold,
                backgroundColor: 'rgb(255, 218, 224, 0.8)',
                borderWidth: 0,
            };
        } else {
            operator = conditionType === 'Equal to' ? '=' : '≠';
        }

        annotationConfig = {
            annotation: {
                drawTime: 'beforeDatasetsDraw',
                annotations: {
                    ...(Object.keys(boxConfig).length > 0 && { thresholdBox: boxConfig }),
                    thresholdLine: {
                        type: 'line',
                        scaleID: 'y',
                        value: visibleThreshold,
                        borderColor: 'rgb(255, 107, 107)',
                        borderWidth: 2,
                        borderDash: [5, 5],
                        label: {
                            display: true,
                            content: `y ${operator} ${thresholdValue}`,
                            position: 'start',
                            backgroundColor: 'rgb(255, 107, 107)',
                            color: '#fff',
                            padding: {
                                x: 6,
                                y: 4,
                            },
                            font: {
                                size: 12,
                            },
                            z: 100,
                        },
                    },
                },
            },
        };
    }

    var legendContainer = $('<div class="legend-container"></div>');
    canvas.parent().append(legendContainer);

    // Variables to track active tooltip state
    let activeTooltip = {
        datasetIndex: -1,
        pointIndex: -1,
        distance: Infinity
    };

    let lastUpdateTime = 0;
    const throttleDelay = 20;

    // Create crosshair plugin
    const crosshairPlugin = {
        id: 'crosshair',
        beforeDraw: (chart) => {
            if (!chart.crosshair) return;

            const { ctx, chartArea: {top, bottom, left, right} } = chart;
            const { x, y } = chart.crosshair;

            if (x >= left && x <= right && y >= top && y <= bottom) {
                ctx.save();

                // Draw new crosshair lines
                ctx.beginPath();
                ctx.setLineDash([5, 5]);
                ctx.lineWidth = 1;
                ctx.strokeStyle = 'rgba(102, 102, 102, 0.8)';
                ctx.moveTo(x, top);
                ctx.lineTo(x, bottom);
                ctx.stroke();

                ctx.beginPath();
                ctx.setLineDash([5, 5]);
                ctx.lineWidth = 1;
                ctx.strokeStyle = 'rgba(102, 102, 102, 0.9)';
                ctx.moveTo(left, y);
                ctx.lineTo(right, y);
                ctx.stroke();

                ctx.restore();
            }
        }
    };

    const horizontalProximityThreshold = 15; // X-axis (horizontal) proximity in pixels
    const verticalProximityThreshold = 10;    // Y-axis (vertical) proximity in pixels

    const strictProximityPlugin = {
        id: 'strictProximity',
        beforeEvent: (chart, args) => {
            const event = args.event;
            if (event.type !== 'mousemove') return;

            const { x, y } = event;
            const { chartArea } = chart;
            const currentTime = Date.now();

            chart.crosshair = { x, y };

            if (currentTime - lastUpdateTime < throttleDelay) {
                chart.draw();
                return;
            }

            lastUpdateTime = currentTime;

            if (x < chartArea.left || x > chartArea.right || y < chartArea.top || y > chartArea.bottom) {
                chart.tooltip.setActiveElements([]);
                chart.update('none');
                activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
                return;
            }

            let nearestPoint = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
            let foundPointInProximity = false;

            chart.data.datasets.forEach((dataset, datasetIndex) => {
                if (!dataset.data || dataset.hidden) return;

                const meta = chart.getDatasetMeta(datasetIndex);
                if (!meta.visible) return;

                meta.data.forEach((element, index) => {
                    if (!element || typeof element.getCenterPoint !== 'function') return;

                    try {
                        const centerPoint = element.getCenterPoint();

                        const dx = Math.abs(centerPoint.x - x);
                        const dy = Math.abs(centerPoint.y - y);

                        if (dx <= horizontalProximityThreshold && dy <= verticalProximityThreshold) {
                            foundPointInProximity = true;

                            const weightedDistance = Math.sqrt(dx * dx + dy * dy);

                            if (weightedDistance < nearestPoint.distance) {
                                nearestPoint = {
                                    datasetIndex,
                                    pointIndex: index,
                                    distance: weightedDistance
                                };
                            }
                        }
                    } catch (error) {
                        console.log("Error processing data point:", error);
                    }
                });
            });

            let needsUpdate = false;

            if (foundPointInProximity && nearestPoint.datasetIndex !== -1) {
                if (nearestPoint.datasetIndex !== activeTooltip.datasetIndex ||
                    nearestPoint.pointIndex !== activeTooltip.pointIndex) {

                    activeTooltip = nearestPoint;

                    chart.tooltip.setActiveElements([{
                        datasetIndex: nearestPoint.datasetIndex,
                        index: nearestPoint.pointIndex
                    }]);

                    needsUpdate = true;
                }
            } else if (activeTooltip.datasetIndex !== -1) {
                chart.tooltip.setActiveElements([]);
                activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
                needsUpdate = true;
            }

            if (needsUpdate) {
                chart.update('none');
            } else {
                chart.draw();
            }
        }
    };

    var lineChart = new Chart(ctx, {
        type: chartType === 'Area chart' ? 'line' : chartType === 'Bar chart' ? 'bar' : 'line',
        data: chartData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    enabled: true,
                    position: 'nearest',
                    events: ['mousemove'],
                    mode: 'nearest',
                    intersect: true,
                    callbacks: {
                        title: function (tooltipItems) {
                            const date = new Date(tooltipItems[0].parsed.x);
                            const formattedDate = date.toLocaleString('default', { month: 'short', day: 'numeric' }) + ', ' + date.toLocaleTimeString();
                            return formattedDate;
                        },
                        label: function (tooltipItem) {
                            return `${tooltipItem.dataset.label}: ${tooltipItem.formattedValue}`;
                        },
                    },
                },
                ...annotationConfig,
                crosshair: {}
            },
            scales: {
                x: {
                    type: 'time',
                    display: true,
                    title: {
                        display: true,
                        text: '',
                    },
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                        callback: xaxisFomatter,
                        autoSkip: false,
                        major: {
                            enabled: true,
                        },
                        font: (context) => {
                            if (context.tick && context.tick.major) {
                                return {
                                    weight: 'bold',
                                };
                            }
                            return {
                                weight: 'normal',
                            };
                        },
                    },
                    time: {
                        unit: timeUnit.includes('day') ? 'day' : timeUnit.includes('hour') ? 'hour' : timeUnit.includes('minute') ? 'minute' : timeUnit,
                        tooltipFormat: 'MMM d, HH:mm:ss',
                        displayFormats: {
                            minute: 'HH:mm',
                            hour: 'HH:mm',
                            day: 'MMM d',
                            month: 'MMM YYYY',
                        },
                    },
                },
                y: {
                    display: true,
                    title: {
                        display: false,
                    },
                    grid: {
                        drawTicks: true,
                        color: function (context) {
                            const maxValue = Math.max(...context.chart.scales.y.ticks.map((t) => t.value));
                            // Hide the top grid line
                            if (context.tick.value === maxValue) return 'rgba(0, 0, 0, 0)';
                            return gridLineColor;
                        },
                    },
                    border: {
                        color: 'rgba(0, 0, 0, 0)',
                    },
                    ticks: {
                        color: tickColor,
                        callback: function (value, index, values) {
                            // Hide label for the maximum tick value
                            if (index === values.length - 1) return '';
                            return value;
                        },
                    },
                    suggestedMin: 0,
                    suggestedMax: maxYTick,
                },
            },
            spanGaps: true,
            interaction: {
                mode: 'nearest',
                axis: 'xy',
                intersect: false,
            },
        },
        plugins: [crosshairPlugin, strictProximityPlugin]
    });

     // Add mouseout event listener to clear crosshair and tooltip
    canvas[0].addEventListener('mouseout', (event) => {
        if (!event.relatedTarget || !canvas[0].contains(event.relatedTarget)) {
            lineChart.crosshair = null;
            lineChart.tooltip.setActiveElements([]);
            activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
            lineChart.draw();
        }
    });

    // Update threshold line if threshold value or condition is changed
    if (isAlertScreen) {
        $('#threshold-value').on('input', updateChartThresholds);
        $('.alert-condition-options li').on('click', updateChartThresholds);
    }

    chartData.datasets.forEach(function (dataset, index) {
        dataset.borderColor = selectedPalette[index % selectedPalette.length];
        dataset.backgroundColor = selectedPalette[index % selectedPalette.length] + '70';
        dataset.borderDash = selectedLineStyle === 'Dash' ? [5, 5] : selectedLineStyle === 'Dotted' ? [1, 3] : [];
        dataset.borderWidth = selectedStroke === 'Thin' ? 1 : selectedStroke === 'Thick' ? 3 : 2;
    });

    // Modify the fill property based on the chart type after chart initialization
    if (chartType === 'Area chart') {
        lineChart.config.data.datasets.forEach(function (dataset) {
            dataset.fill = true;
        });
    } else {
        lineChart.config.data.datasets.forEach(function (dataset) {
            dataset.fill = false;
        });
    }

    generateCustomLegend(lineChart, legendContainer[0]);

    lineChart.update();
    return lineChart;
}

function mergeGraphs(chartType, panelId = -1) {
    var mergedCtx;
    var colorIndex = 0;
    var mergedCanvas, legendContainer;
    if (isDashboardScreen) {
        // For dashboard page
        if (currentPanel) {
            const data = getMetricsQData();
            currentPanel.queryData = data;
        }
        var panelChartEl;
        if (panelId === -1) {
            panelChartEl = $(`.panelDisplay .panEdit-panel`);
            panelChartEl.empty(); // Clear any existing content

            var mergedGraphDiv = $('<div class="merged-graph"></div>');
            panelChartEl.append(mergedGraphDiv);

            mergedCanvas = $('<canvas></canvas>');
            legendContainer = $('<div class="legend-container"></div>');
            mergedGraphDiv.append(mergedCanvas);
            mergedGraphDiv.append(legendContainer);
        } else {
            panelChartEl = $(`#panel${panelId} .panEdit-panel`);
            panelChartEl.css('width', '100%').css('height', '100%');

            panelChartEl.empty(); // Clear any existing content
            mergedCanvas = $('<canvas></canvas>');
            panelChartEl.append(mergedCanvas);
        }
        mergedCtx = mergedCanvas[0].getContext('2d');
    } else {
        // For metrics explorer page
        var visualizationContainer = $(`
            <div class="merged-graph-name"></div>
            <div class="merged-graph"></div>`);

        $('#merged-graph-container').empty().append(visualizationContainer);

        mergedCanvas = $('<canvas></canvas>');
        legendContainer = $('<div class="legend-container"></div>');

        $('.merged-graph').empty().append(mergedCanvas).append(legendContainer);
        mergedCtx = mergedCanvas[0].getContext('2d');
    }

    var mergedData = {
        labels: [],
        datasets: [],
    };
    var graphNames = [];

    // Loop through chartDataCollection to merge datasets
    for (var queryName in chartDataCollection) {
        if (Object.prototype.hasOwnProperty.call(chartDataCollection, queryName)) {
            // Merge datasets for the current query
            var datasets = chartDataCollection[queryName].datasets;
            graphNames.push(`${datasets[0]?.label}`);

            datasets.forEach(function (dataset) {
                // Calculate color for the dataset
                let datasetColor = colorPalette[selectedTheme][colorIndex % colorPalette[selectedTheme].length];

                mergedData.datasets.push({
                    label: dataset.label,
                    data: dataset.data,
                    borderColor: datasetColor,
                    borderWidth: dataset.borderWidth,
                    backgroundColor: datasetColor + '70', // opacity
                    fill: chartType === 'Area chart' ? true : false,
                    borderDash: selectedLineStyle === 'Dash' ? [5, 5] : selectedLineStyle === 'Dotted' ? [1, 3] : [],
                });

                colorIndex++;
            });
            // Update labels (same for all graphs)
            mergedData.labels = chartDataCollection[queryName].labels;
        }
    }
    $('.merged-graph-name').html(graphNames.join(', '));
    const { gridLineColor, tickColor } = getGraphGridColors();

    // Variables to track active tooltip state
    let activeTooltip = {
        datasetIndex: -1,
        pointIndex: -1,
        distance: Infinity
    };

    let lastUpdateTime = 0;
    const throttleDelay = 20;

    // Create crosshair plugin - matching implementation from first code
    const crosshairPlugin = {
        id: 'crosshair',
        beforeDraw: (chart) => {
            if (!chart.crosshair) return;

            const { ctx, chartArea: {top, bottom, left, right} } = chart;
            const { x, y } = chart.crosshair;

            if (x >= left && x <= right && y >= top && y <= bottom) {
                ctx.save();

                // Draw new crosshair lines
                ctx.beginPath();
                ctx.setLineDash([5, 5]);
                ctx.lineWidth = 1;
                ctx.strokeStyle = 'rgba(102, 102, 102, 0.8)';
                ctx.moveTo(x, top);
                ctx.lineTo(x, bottom);
                ctx.stroke();

                ctx.beginPath();
                ctx.setLineDash([5, 5]);
                ctx.lineWidth = 1;
                ctx.strokeStyle = 'rgba(102, 102, 102, 0.9)';
                ctx.moveTo(left, y);
                ctx.lineTo(right, y);
                ctx.stroke();

                ctx.restore();
            }
        }
    };

    const horizontalProximityThreshold = 15; // X-axis (horizontal) proximity in pixels
    const verticalProximityThreshold = 10;    // Y-axis (vertical) proximity in pixels

    // Implement the strict proximity plugin matching the first code
    const strictProximityPlugin = {
        id: 'strictProximity',
        beforeEvent: (chart, args) => {
            const event = args.event;
            if (event.type !== 'mousemove') return;

            const { x, y } = event;
            const { chartArea } = chart;
            const currentTime = Date.now();

            chart.crosshair = { x, y };

            if (currentTime - lastUpdateTime < throttleDelay) {
                chart.draw();
                return;
            }

            lastUpdateTime = currentTime;

            if (x < chartArea.left || x > chartArea.right || y < chartArea.top || y > chartArea.bottom) {
                chart.tooltip.setActiveElements([]);
                chart.update('none');
                activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
                return;
            }

            let nearestPoint = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
            let foundPointInProximity = false;

            chart.data.datasets.forEach((dataset, datasetIndex) => {
                if (!dataset.data || dataset.hidden) return;

                const meta = chart.getDatasetMeta(datasetIndex);
                if (!meta.visible) return;

                meta.data.forEach((element, index) => {
                    if (!element || typeof element.getCenterPoint !== 'function') return;

                    try {
                        const centerPoint = element.getCenterPoint();

                        const dx = Math.abs(centerPoint.x - x);
                        const dy = Math.abs(centerPoint.y - y);

                        if (dx <= horizontalProximityThreshold && dy <= verticalProximityThreshold) {
                            foundPointInProximity = true;

                            const weightedDistance = Math.sqrt(dx * dx + dy * dy);

                            if (weightedDistance < nearestPoint.distance) {
                                nearestPoint = {
                                    datasetIndex,
                                    pointIndex: index,
                                    distance: weightedDistance
                                };
                            }
                        }
                    } catch (error) {
                        console.log("Error processing data point:", error);
                    }
                });
            });

            let needsUpdate = false;

            if (foundPointInProximity && nearestPoint.datasetIndex !== -1) {
                if (nearestPoint.datasetIndex !== activeTooltip.datasetIndex ||
                    nearestPoint.pointIndex !== activeTooltip.pointIndex) {

                    activeTooltip = nearestPoint;

                    chart.tooltip.setActiveElements([{
                        datasetIndex: nearestPoint.datasetIndex,
                        index: nearestPoint.pointIndex
                    }]);

                    needsUpdate = true;
                }
            } else if (activeTooltip.datasetIndex !== -1) {
                chart.tooltip.setActiveElements([]);
                activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
                needsUpdate = true;
            }

            if (needsUpdate) {
                chart.update('none');
            } else {
                chart.draw();
            }
        }
    };


    var mergedLineChart = new Chart(mergedCtx, {
        type: chartType === 'Area chart' ? 'line' : chartType === 'Bar chart' ? 'bar' : 'line',
        data: mergedData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    enabled: true,
                    position: 'nearest',
                    events: ['mousemove'],
                    mode: 'nearest',
                    intersect: true,
                    callbacks: {
                        title: function (tooltipItems) {
                            // Display formatted timestamp in the title
                            const date = new Date(tooltipItems[0].parsed.x);
                            const formattedDate = date.toLocaleString('default', { month: 'short', day: 'numeric' }) + ', ' + date.toLocaleTimeString();
                            return formattedDate;
                        },
                        label: function (tooltipItem) {
                            // Display dataset label and value
                            return `${tooltipItem.dataset.label}: ${tooltipItem.formattedValue}`;
                        },
                    },
                },
            },
            scales: {
                x: {
                    type: 'time',
                    display: true,
                    title: {
                        display: true,
                        text: '',
                    },
                    grid: {
                        display: true,
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                        callback: xaxisFomatter,
                        autoSkip: false,
                        major: {
                            enabled: true,
                        },
                        font: (context) => {
                            if (context.tick && context.tick.major) {
                                return {
                                    weight: 'bold',
                                };
                            }
                            return {
                                weight: 'normal',
                            };
                        },
                    },
                    time: {
                        unit: timeUnit.includes('day') ? 'day' : timeUnit.includes('hour') ? 'hour' : timeUnit.includes('minute') ? 'minute' : timeUnit,
                        tooltipFormat: 'MMM d, HH:mm:ss',
                        displayFormats: {
                            minute: 'HH:mm',
                            hour: 'HH:mm',
                            day: 'MMM d',
                            month: 'MMM YYYY',
                        },
                    },
                },
                y: {
                    display: true,
                    title: {
                        display: false,
                    },
                    border: {
                        color: 'rgba(0, 0, 0, 0)',
                    },
                    grid: { color: gridLineColor },
                    ticks: { color: tickColor },
                },
            },
            spanGaps: true,
            interaction: {
                mode: 'nearest',
                axis: 'xy',
                intersect: false,
            },
        },
        plugins: [crosshairPlugin, strictProximityPlugin]
    });

    // Add mouseout event listener to clear crosshair and tooltip
    if (isDashboardScreen) {
        panelChartEl.find('canvas')[0].addEventListener('mouseout', () => {
            mergedLineChart.crosshair = null;
            mergedLineChart.tooltip.setActiveElements([]);
            activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
            mergedLineChart.update();
        });
    } else {
        mergedCanvas[0].addEventListener('mouseout', () => {
            mergedLineChart.crosshair = null;
            mergedLineChart.tooltip.setActiveElements([]);
            activeTooltip = { datasetIndex: -1, pointIndex: -1, distance: Infinity };
            mergedLineChart.update();
        });
    }

    // Only generate and display legend for panelId == -1 or metrics explorer
    if (!isDashboardScreen || panelId === -1) {
        var legendContainerEl = isDashboardScreen ? $(`.panelDisplay .panEdit-panel .merged-graph .legend-container`) : $('.merged-graph .legend-container');
        generateCustomLegend(mergedLineChart, legendContainerEl[0]);
    }

    mergedGraph = mergedLineChart;
    updateDownloadButtons();
}