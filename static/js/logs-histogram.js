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
//eslint-disable-next-line no-unused-vars
const HistogramState = {
    currentHistogram: null,
    originalData: null,
    currentGranularity: 'day',
    isDragging: false,
    isZoomed: false,
    originalStartTime: null,
    originalEndTime: null,
    canvas: null,
    eventListeners: {},
};

// Custom plugin to draw dashed left and right borders for drag selection
const customDragBorderPlugin = {
    id: 'customDragBorder',
    afterDraw: (chart) => {
        const zoomPlugin = chart.$zoom;
        if (!zoomPlugin || !zoomPlugin._active || !zoomPlugin._active.length) {
            return;
        }

        const { ctx, chartArea } = chart;
        const zoomState = zoomPlugin._active[0];
        const { startX, endX } = zoomState;

        if (startX !== undefined && endX !== undefined) {
            const leftX = Math.min(startX, endX);
            const rightX = Math.max(startX, endX);
            const top = chartArea.top;
            const bottom = chartArea.bottom;

            ctx.save();
            ctx.beginPath();
            ctx.setLineDash([3, 3]);
            ctx.strokeStyle = 'rgba(0, 0, 255, 0.8)';
            ctx.lineWidth = 1;

            ctx.moveTo(leftX, top);
            ctx.lineTo(leftX, bottom);
            ctx.moveTo(rightX, top);
            ctx.lineTo(rightX, bottom);

            ctx.stroke();
            ctx.restore();
        }
    }
};

function determineGranularity(startTime, endTime) {
    const durationMs = endTime - startTime;
    const MAX_BARS = 50;
    const MIN_BARS = 10;

    const calculateMaxBars = (intervalMs) => {
        const totalBars = Math.ceil(durationMs / intervalMs);
        return Math.max(MIN_BARS, Math.min(MAX_BARS, totalBars));
    };

    if (durationMs <= 15 * 60 * 1000) {
        return { granularity: 'second', intervalMs: 10 * 1000, maxBars: calculateMaxBars(10 * 1000) };
    } else if (durationMs <= 60 * 60 * 1000) {
        return { granularity: 'minute', intervalMs: 60 * 1000, maxBars: calculateMaxBars(60 * 1000) };
    } else if (durationMs <= 4 * 60 * 60 * 1000) {
        return { granularity: 'minute', intervalMs: 5 * 60 * 1000, maxBars: calculateMaxBars(5 * 60 * 1000) };
    } else if (durationMs <= 24 * 60 * 60 * 1000) {
        return { granularity: 'minute', intervalMs: 30 * 60 * 1000, maxBars: calculateMaxBars(30 * 60 * 1000) };
    } else if (durationMs <= 7 * 24 * 60 * 60 * 1000) {
        return { granularity: 'hour', intervalMs: 60 * 60 * 1000, maxBars: calculateMaxBars(60 * 60 * 1000) };
    } else if (durationMs <= 180 * 24 * 60 * 60 * 1000) {
        return { granularity: 'day', intervalMs: 24 * 60 * 60 * 1000, maxBars: calculateMaxBars(24 * 60 * 60 * 1000) };
    } else {
        return { granularity: 'month', intervalMs: 30 * 24 * 60 * 60 * 1000, maxBars: calculateMaxBars(30 * 24 * 60 * 60 * 1000) };
    }
}

function formatTooltipTimestamp(timestamp, granularity) {
    const date = new Date(timestamp);
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const month = monthNames[date.getMonth()];
    const day = date.getDate();
    const year = date.getFullYear();
    const hours = date.getHours();
    const minutes = ('0' + date.getMinutes()).slice(-2);
    const seconds = ('0' + date.getSeconds()).slice(-2);
    const ampm = hours >= 12 ? 'PM' : 'AM';
    const hour12 = hours % 12 || 12;

    let result = `${month} ${day}, ${year}`;
    if (granularity === 'second') {
        return `${result} ${hour12}:${minutes}:${seconds} ${ampm}`;
    } else if (granularity === 'minute') {
        return `${result} ${hour12}:${minutes} ${ampm}`;
    } else if (granularity === 'hour') {
        return `${result} ${hour12}:00 ${ampm}`;
    } else {
        return result;
    }
}

function configureTimeAxis(startTime, endTime, intervalMs, granularity, maxBars) {
    const daysInRange = Math.ceil((endTime - startTime) / (1000 * 60 * 60 * 24));
    const hoursInRange = Math.ceil((endTime - startTime) / (1000 * 60 * 60));
    const minutesInRange = Math.ceil((endTime - startTime) / (1000 * 60));
    let unit, maxTicksLimit;

    if (granularity === 'second') {
        unit = 'second';
        maxTicksLimit = Math.min(maxBars, Math.ceil((endTime - startTime) / intervalMs) + 1);
    } else if (granularity === 'minute') {
        unit = 'minute';
        maxTicksLimit = Math.min(maxBars, Math.ceil((endTime - startTime) / intervalMs) + 1);
    } else if (granularity === 'hour') {
        unit = 'hour';
        maxTicksLimit = Math.min(maxBars, Math.ceil((endTime - startTime) / intervalMs) + 1);
    } else if (granularity === 'day') {
        unit = 'day';
        maxTicksLimit = Math.min(maxBars, Math.ceil((endTime - startTime) / intervalMs) + 1);
    } else {
        unit = 'month';
        maxTicksLimit = Math.min(maxBars, Math.ceil((endTime - startTime) / intervalMs) + 1);
    }

    const timeOptions = {
        unit: unit,
        displayFormats: {
            second: 'h:mm:ss a',
            minute: 'h:mm a',
            hour: 'h a',
            day: 'MMM d',
            month: 'MMM yyyy',
        },
        tooltipFormat: 'MMM d, yyyy, h:mm:ss a',
        bounds: 'ticks',
    };

    let offsetValue = true;
    if (granularity === 'second') {
        timeOptions.round = 'second';
        offsetValue = false;
        timeOptions.stepSize = intervalMs / 1000;
    } else if (granularity === 'minute') {
        timeOptions.round = 'minute';
        offsetValue = false;
        timeOptions.stepSize = intervalMs / (60 * 1000);
    } else if (granularity === 'hour') {
        timeOptions.round = 'hour';
        offsetValue = false;
        timeOptions.stepSize = intervalMs / (60 * 60 * 1000);
    } else if (granularity === 'day') {
        timeOptions.round = 'day';
        timeOptions.stepSize = intervalMs / (24 * 60 * 60 * 1000);
    }

    timeOptions.offset = offsetValue;

    let paddingMs;
    switch (granularity) {
        case 'second':
            paddingMs = 10 * 1000;
            break;
        case 'minute':
            paddingMs = 60 * 1000;
            break;
        case 'hour':
            paddingMs = 60 * 60 * 1000;
            break;
        case 'day':
            paddingMs = 12 * 60 * 60 * 1000;
            break;
        case 'month':
            paddingMs = 5 * 24 * 60 * 60 * 1000;
            break;
        default:
            paddingMs = 12 * 60 * 60 * 1000;
    }

    const config = {
        type: 'time',
        time: timeOptions,
        min: startTime - paddingMs,
        max: endTime + paddingMs,
        title: {
            display: true,
            text: `Timestamp (Interval - ${granularity})`,
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
                const minutes = date.getMinutes();
                const seconds = date.getSeconds();
                const day = date.getDate();

                if (granularity === 'second') {
                    if (seconds === 0) {
                        return `${monthNames[date.getMonth()]} ${day} ${hours % 12 || 12}:${('0' + minutes).slice(-2)} ${hours < 12 ? 'AM' : 'PM'}`;
                    } else {
                        return `${hours % 12 || 12}:${('0' + seconds).slice(-2)} ${hours < 12 ? 'AM' : 'PM'}`;
                    }
                } else if (granularity === 'minute') {
                    if (hoursInRange <= 24 || minutesInRange <= 60) {
                        const hourIn12 = hours % 12 || 12;
                        const amPm = hours < 12 ? 'AM' : 'PM';
                        if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day} ${hourIn12}:${('0' + minutes).slice(-2)} ${amPm}`;
                        } else {
                            return `${hourIn12}:${('0' + minutes).slice(-2)} ${amPm}`;
                        }
                    }
                    else if (minutes === 0) {
                        return `${monthNames[date.getMonth()]} ${day} ${hours % 12 || 12}:00 ${hours < 12 ? 'AM' : 'PM'}`;
                    } else {
                        return `${hours % 12 || 12}:${('0' + minutes).slice(-2)} ${hours < 12 ? 'AM' : 'PM'}`;
                    }
                } else if (granularity === 'hour') {
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
                        } else if (hours % (intervalMs / (60 * 60 * 1000)) === 0) {
                            const hourIn12 = hours % 12 || 12;
                            const amPm = hours < 12 ? 'AM' : 'PM';
                            return `${hourIn12}${amPm}`;
                        }
                    } else {
                        if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        } else if (hours === 12) {
                            return '12PM';
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
                    const minutes = date.getMinutes();
                    const seconds = date.getSeconds();
                    if ((granularity === 'second' && seconds === 0) ||
                        (granularity === 'minute' && minutes === 0) ||
                        (granularity === 'minute' && hours === 0) ||
                        (granularity === 'hour' && hours === 0)) {
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

function triggerZoomSearch(startTime, endTime) {
    const histoContainer = $('#histogram-container');
    histoContainer.html('<div class="loading-message">Loading new data...</div>');

    //eslint-disable-next-line no-undef
    isSearchButtonTriggered = true;
    //eslint-disable-next-line no-undef
    if (!isHistogramViewActive) {
        //eslint-disable-next-line no-undef
        hasNewSearchWhileHistogramClosed = true;
    }

    const data = getSearchFilter(false, false, true, false);
    data.startEpoch = Math.floor(startTime);
    data.endEpoch = Math.ceil(endTime);
    //eslint-disable-next-line no-undef
    isZoomSearch=true;

    Cookies.set('startEpoch', data.startEpoch);
    Cookies.set('endEpoch', data.endEpoch);

    resetDashboard();
    logsRowData = [];
    accumulatedRecords = [];
    lastColumnsOrder = [];
    totalLoadedRecords = 0;
    wsState = 'query';
    initialSearchData = data;

    doSearch(data).finally(() => {
        //eslint-disable-next-line no-undef
        isSearchButtonTriggered = false;
    });
}

function renderHistogram(timechartData) {
    const histoContainer = $('#histogram-container');

    if (!histoContainer.length) {
        console.error('Histogram container not found');
        return;
    }

    if (!timechartData || !Array.isArray(timechartData.measure) || timechartData.measure.length === 0) {
        histoContainer.html('<div class="error-message">No histogram data available</div>');
        return;
    }

    if (!HistogramState.originalData) {
        HistogramState.originalData = JSON.parse(JSON.stringify(timechartData));
    }

    let dataToRender = timechartData;

    let timestamps = dataToRender.measure.map(item => {
        if (!item.GroupByValues || !item.GroupByValues[0]) {
            console.warn('Missing GroupByValues in measure:', item);
            return null;
        }
        try {
            return parseInt(item.GroupByValues[0]);
        } catch (e) {
            console.error('Error parsing timestamp:', item.GroupByValues[0], e);
            return null;
        }
    }).filter(ts => ts !== null);

    let startTime = Math.min(...timestamps);
    let endTime = Math.max(...timestamps);

    if (!HistogramState.isZoomed) {
        HistogramState.originalStartTime = startTime;
        HistogramState.originalEndTime = endTime;
    }

    const { granularity, intervalMs, maxBars } = determineGranularity(startTime, endTime);
    HistogramState.currentGranularity = granularity;

    let chartData = dataToRender.measure.map(item => {
        try {
            const timestamp = parseInt(item.GroupByValues[0]);
            return {
                x: timestamp,
                y: item.MeasureVal['count(*)'] || 0,
                originalTimestamps: [timestamp]
            };
        } catch (e) {
            console.error('Error processing measure for chart:', item, e);
            return null;
        }
    }).filter(item => item !== null).sort((a, b) => a.x - b.x);

    if (!chartData.length) {
        console.log('No data available to render');
        histoContainer.html('<div class="error-message">No data available for the selected range</div>');
        return;
    }

    const { gridLineColor, tickColor } = typeof getGraphGridColors === 'function'
        ? getGraphGridColors()
        : { gridLineColor: '#e0e0e0', tickColor: '#666' };

    const xAxisConfig = configureTimeAxis(startTime, endTime, intervalMs, granularity, maxBars);

    if (HistogramState.currentHistogram) {
        HistogramState.currentHistogram.destroy();
        HistogramState.currentHistogram = null;
    }

    histoContainer.empty();
    histoContainer.html('<canvas width="100%" height="100%"></canvas>');

    HistogramState.canvas = histoContainer.find('canvas')[0];
    const ctx = HistogramState.canvas.getContext('2d');

    //eslint-disable-next-line no-undef
    if (typeof ChartZoom !== 'undefined') {
        //eslint-disable-next-line no-undef
        Chart.register(ChartZoom);
        Chart.register(customDragBorderPlugin);
    }

    HistogramState.currentHistogram = new Chart(ctx, {
        type: 'bar',
        data: {
            datasets: [{
                label: 'Count',
                data: chartData,
                backgroundColor: globalColorArray && globalColorArray[0] ? globalColorArray[0] + '70' : 'rgba(0, 123, 255, 0.5)',
                borderColor: globalColorArray && globalColorArray[0] ? globalColorArray[0] : 'rgba(0, 123, 255, 1)',
                borderRadius: 4,
                borderWidth: 1,
                barPercentage: 0.8,
                categoryPercentage: 0.8,
                barThickness: 'flex',
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: {
                padding: {
                    left: 10,
                    right: 10,
                },
            },
            scales: {
                x: {
                    ...xAxisConfig,
                    grid: {
                        color: gridLineColor,
                    },
                    ticks: {
                        ...xAxisConfig.ticks,
                        color: tickColor,
                    },
                },
                y: {
                    title: {
                        display: false,
                    },
                    beginAtZero: true,
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    }
                }
            },
            plugins: {
                legend: {
                    display: false,
                },
                tooltip: {
                    mode: 'index',
                    intersect: false,
                    callbacks: {
                        title: function (tooltipItems) {
                            const dataPoint = tooltipItems[0].raw;
                            const timestamp = dataPoint.originalTimestamps[0] || dataPoint.x;
                            return formatTooltipTimestamp(timestamp, granularity);
                        },
                        label: function (context) {
                            const count = context.parsed.y;
                            return `${context.dataset.label}: ${count}`;
                        },
                    }
                },
                zoom: {
                    zoom: {
                        wheel: {
                            enabled: false,
                        },
                        pinch: {
                            enabled: false,
                        },
                        drag: {
                            enabled: true,
                            backgroundColor: 'rgba(0, 0, 255, 0.1)',
                            borderWidth: 0,
                        },
                        mode: 'x',
                        onZoomComplete: ({ chart }) => {
                            const xScale = chart.scales.x;
                            const min = xScale.min;
                            const max = xScale.max;

                            if (min >= HistogramState.originalStartTime && max <= HistogramState.originalEndTime) {
                                HistogramState.isZoomed = true;
                                triggerZoomSearch(min, max);
                            }
                        }
                    },
                    limits: {
                        x: {
                            min: 'original',
                            max: 'original',
                        }
                    }
                }
            },
            onHover: function (event, elements, chart) {
                const canvas = chart.canvas;
                if (event.native.buttons === 1) {
                    canvas.style.cursor = 'crosshair';
                    HistogramState.isDragging = true;
                } else if (HistogramState.isDragging) {
                    canvas.style.cursor = 'crosshair';
                } else {
                    canvas.style.cursor = 'default';
                }
            }
        }
    });

    // Add double-click to reset zoom
    const handleDoubleClick = () => {
        if (HistogramState.currentHistogram && HistogramState.originalData) {
            HistogramState.currentGranularity = 'day';
            HistogramState.isZoomed = false;
            HistogramState.currentHistogram.resetZoom();
            // Trigger search with original time range
            triggerZoomSearch(HistogramState.originalStartTime, HistogramState.originalEndTime);
        }
    };

    // Remove existing event listeners
    const eventTypes = ['dblclick'];
    eventTypes.forEach(eventType => {
        if (HistogramState.eventListeners[eventType]) {
            HistogramState.canvas.removeEventListener(eventType, HistogramState.eventListeners[eventType]);
        }
    });

    // Add double-click listener
    HistogramState.canvas.addEventListener('dblclick', handleDoubleClick);
    HistogramState.eventListeners = {
        dblclick: handleDoubleClick
    };

    addZoomHelper();
}

//eslint-disable-next-line no-unused-vars
function updateHistogramTheme() {
    if (!HistogramState.currentHistogram) return;

    const { gridLineColor, tickColor } = typeof getGraphGridColors === 'function'
        ? getGraphGridColors()
        : { gridLineColor: '#e0e0e0', tickColor: '#666' };

    HistogramState.currentHistogram.options.scales.x.grid.color = gridLineColor;
    HistogramState.currentHistogram.options.scales.x.ticks.color = tickColor;
    HistogramState.currentHistogram.options.scales.x.title.color = tickColor;

    HistogramState.currentHistogram.options.scales.y.grid.color = gridLineColor;
    HistogramState.currentHistogram.options.scales.y.ticks.color = tickColor;
    HistogramState.currentHistogram.options.scales.y.title.color = tickColor;

    HistogramState.currentHistogram.options.plugins.legend.labels.color = tickColor;

    HistogramState.currentHistogram.update();
}

function addZoomHelper() {
    const helpText = document.createElement('div');
    helpText.className = 'zoom-helper';
    helpText.style.cssText = `
        position: absolute;
        color: var(--text-color);
        bottom: 5px;
        right: 10px;
        font-size: 10px;
        opacity: 0.7;
        transition: opacity 0.3s ease;
        pointer-events: none;
    `;
    helpText.textContent = HistogramState.isZoomed
        ? 'Double-click to reset zoom'
        : 'Drag to zoom and Double-click to reset zoom';

    $('#histogram-container').append(helpText);
}

$(document).ready(function() {
    $('#histogram-toggle-btn').on('click', function() {
        $(this).toggleClass('active');
        $('.histo-container').toggle();

        if ($(this).hasClass('active')) {
            //eslint-disable-next-line no-undef
            isHistogramViewActive = true;
            if ($('#histogram-container').is(':visible')) {
                //eslint-disable-next-line no-undef
                if (hasNewSearchWhileHistogramClosed) {
                    $('#histogram-container').html('<div class="info-message">Hit search button to see histogram view</div>');
                    //eslint-disable-next-line no-undef
                    hasNewSearchWhileHistogramClosed = false;
                    //eslint-disable-next-line no-undef
                } else if (hasRenderedHistogramOnce && timechartComplete) {
                    //eslint-disable-next-line no-undef
                    renderHistogram(timechartComplete);
                    addZoomHelper();
                } else {
                    $('#histogram-container').html('<div class="info-message">Hit search button to see histogram view</div>');
                }
            }
        } else {
            //eslint-disable-next-line no-undef
            isHistogramViewActive = false;
        }
    });

    const emptyResponse = document.getElementById('empty-response');
    if (emptyResponse) {
        const observer = new MutationObserver((mutations) => {
            mutations.forEach((mutation) => {
                if (mutation.attributeName === 'style') {
                    const isVisible = $(emptyResponse).is(':visible');
                    if (isVisible) {
                        $('.histo-container').hide();
                        $('#histogram-toggle-btn').removeClass('active');
                        //eslint-disable-next-line no-undef
                        isHistogramViewActive = false;
                        //eslint-disable-next-line no-undef
                        timechartComplete = null;
                        //eslint-disable-next-line no-undef
                        hasRenderedHistogramOnce = false;
                        HistogramState.isZoomed = false;
                        HistogramState.originalStartTime = null;
                        HistogramState.originalEndTime = null;
                        if (HistogramState.currentHistogram) {
                            HistogramState.currentHistogram.destroy();
                            HistogramState.currentHistogram = null;
                        }
                    }
                }
            });
        });
        observer.observe(emptyResponse, {
            attributes: true,
            attributeFilter: ['style'],
        });
        if ($(emptyResponse).is(':visible')) {
            $('.histo-container').hide();
            $('#histogram-toggle-btn').removeClass('active');
            //eslint-disable-next-line no-undef
            isHistogramViewActive = false;
            //eslint-disable-next-line no-undef
            timechartComplete = null;
            //eslint-disable-next-line no-undef
            hasRenderedHistogramOnce = false;
            HistogramState.isZoomed = false;
            HistogramState.originalStartTime = null;
            HistogramState.originalEndTime = null;
            if (HistogramState.currentHistogram) {
                HistogramState.currentHistogram.destroy();
                HistogramState.currentHistogram = null;
            }
        }
    }
});