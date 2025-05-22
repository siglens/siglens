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
    canvas: null,
    isDragging: false,
    isZoomed: false,
    eventListeners: {},
    originalStartTime: null,
    originalEndTime: null
};

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

    const granularityOptions = [
        { duration: 15 * 60 * 1000, granularity: 'second', intervalMs: 10 * 1000 },
        { duration: 60 * 60 * 1000, granularity: 'minute', intervalMs: 60 * 1000 },
        { duration: 4 * 60 * 60 * 1000, granularity: 'minute', intervalMs: 5 * 60 * 1000 },
        { duration: 24 * 60 * 60 * 1000, granularity: 'minute', intervalMs: 30 * 60 * 1000 },
        { duration: 7 * 24 * 60 * 60 * 1000, granularity: 'hour', intervalMs: 60 * 60 * 1000 },
        { duration: 180 * 24 * 60 * 60 * 1000, granularity: 'day', intervalMs: 24 * 60 * 60 * 1000 },
        { duration: Infinity, granularity: 'month', intervalMs: 30 * 24 * 60 * 60 * 1000 },
    ];

    const { granularity, intervalMs } = granularityOptions.find(opt => durationMs <= opt.duration);
    return { granularity, intervalMs };
}

function parseTimestamp(timestamp) {
    const date = new Date(timestamp);
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    return {
        month: monthNames[date.getMonth()],
        day: date.getDate(),
        year: date.getFullYear(),
        hours: date.getHours(),
        minutes: ('0' + date.getMinutes()).slice(-2),
        seconds: ('0' + date.getSeconds()).slice(-2),
        hour12: date.getHours() % 12 || 12,
        ampm: date.getHours() >= 12 ? 'PM' : 'AM'
    };
}

function formatTooltipTimestamp(timestamp, granularity) {
    const { month, day, year, hour12, minutes, seconds, ampm } = parseTimestamp(timestamp);
    const base = `${month} ${day}, ${year}`;
    switch (granularity) {
        case 'second': return `${base} ${hour12}:${minutes}:${seconds} ${ampm}`;
        case 'minute': return `${base} ${hour12}:${minutes} ${ampm}`;
        case 'hour': return `${base} ${hour12}:00 ${ampm}`;
        default: return base;
    }
}

function formatXTicks(timestamp, granularity, startTime, isFirstTickOfDay, daysInRange) {
    const { month, day, year, hours, minutes, seconds, hour12, ampm } = parseTimestamp(timestamp);

    const startDate = new Date(startTime);
    const isNewYear = granularity === 'day' && year !== startDate.getFullYear();

    const showMonthDate = (granularity === 'second' || granularity === 'minute' || granularity === 'hour') && isFirstTickOfDay;

    switch (granularity) {
        case 'second':
            if (showMonthDate) {
                return `${month} ${day}`;
            }
            return `${hour12}:${minutes}:${seconds} ${ampm}`;
        case 'minute':
            if (showMonthDate) {
                return `${month} ${day}`;
            }
            return `${hour12}:${minutes} ${ampm}`;
        case 'hour':
            if (showMonthDate) {
                return `${month} ${day}`;
            }
            if (daysInRange <= 1) { 
                return `${hour12}${ampm}`;
            } else if (daysInRange <= 4) {
                if (hours === 6) return '6AM';
                if (hours === 12) return '12PM';
                if (hours === 18) return '6PM';
            } else if (daysInRange <= 15) {
                if (hours === 12) return '12PM';
            }
            return null; 
        case 'day':
            if (isNewYear) {
                return `${month} ${day}, ${year}`;
            }
            return `${month} ${day}`;
        case 'month':
            return `${month} ${year}`;
        default:
            return `${month} ${day}`;
    }
}

function msToReadable(intervalMs) {
    const units = [
        { name: 'month', ms: 30 * 24 * 60 * 60 * 1000 },
        { name: 'day', ms: 24 * 60 * 60 * 1000 },
        { name: 'hour', ms: 60 * 60 * 1000 },
        { name: 'minute', ms: 60 * 1000 },
        { name: 'second', ms: 1000 },
    ];

    const { name, ms } = units.find(unit => intervalMs >= unit.ms) || units[units.length - 1];
    const value = Math.round((intervalMs / ms) * 10) / 10;
    return `${value} ${name}${value !== 1 ? 's' : ''}`;
}

function configureTimeAxis(startTime, endTime, intervalMs, granularity) {
    const durationMs = endTime - startTime;
    const daysInRange = Math.ceil(durationMs / (1000 * 60 * 60 * 24));
    const hoursInRange = durationMs / (1000 * 60 * 60);

    const paddingMs = Math.min(
        {
            second: 30 * 1000,
            minute: 5 * 60 * 1000,
            hour: 30 * 60 * 1000,
            day: 12 * 60 * 60 * 1000,
            month: 5 * 24 * 60 * 60 * 1000,
        }[granularity] || durationMs * 0.05,
        durationMs * 0.05
    );

    let adjustedStartTime = startTime;
    let adjustedEndTime = endTime;
    if (granularity === 'second') {
        const startDate = new Date(startTime);
        const endDate = new Date(endTime);
        adjustedStartTime += (30 - (startDate.getSeconds() % 30)) % 30 * 1000;
        adjustedEndTime += (30 - (endDate.getSeconds() % 30)) % 30 * 1000;
    }
    const adjustedDurationMs = adjustedEndTime - adjustedStartTime;

    const timeOptions = {
        unit: granularity,
        displayFormats: {
            second: 'h:mm:ss a',
            minute: 'h:mm a',
            hour: 'h a',
            day: 'MMM d',
            month: 'MMM yyyy',
        },
        tooltipFormat: 'MMM d, yyyy, h:mm:ss a',
        bounds: 'ticks',
        round: granularity,
        offset: granularity !== 'second' && granularity !== 'minute' && granularity !== 'hour',
    };

    let stepSize, maxTicksLimit;
    let lastDay = null; // Track the last day to identify the first tick of a new day

    if (granularity === 'second') {
        if (adjustedDurationMs <= 5 * 60 * 1000) {
            timeOptions.unit = 'second';
            stepSize = 30;
            maxTicksLimit = Math.ceil(adjustedDurationMs / (30 * 1000)) + 1;
        } else if (adjustedDurationMs <= 15 * 60 * 1000) {
            timeOptions.unit = 'minute';
            stepSize = 1;
            maxTicksLimit = Math.ceil(adjustedDurationMs / (60 * 1000)) + 1;
        } else {
            timeOptions.unit = 'minute';
            stepSize = 2;
            maxTicksLimit = Math.ceil(adjustedDurationMs / (2 * 60 * 1000)) + 1;
        }
    } else if (granularity === 'minute') {
        if (durationMs <= 60 * 60 * 1000) {
            stepSize = 5;
            maxTicksLimit = Math.ceil(durationMs / (5 * 60 * 1000)) + 1;
        } else if (durationMs <= 4 * 60 * 60 * 1000) {
            stepSize = 15;
            maxTicksLimit = Math.ceil(durationMs / (15 * 60 * 1000)) + 1;
        } else if (durationMs <= 24 * 60 * 60 * 1000) {
            timeOptions.unit = 'hour';
            stepSize = 2;
            maxTicksLimit = Math.ceil(durationMs / (2 * 60 * 60 * 1000)) + 1;
        } else {
            stepSize = Math.ceil(intervalMs / (60 * 1000));
            maxTicksLimit = Math.ceil(durationMs / (stepSize * 60 * 1000)) + 1;
        }
    } else if (granularity === 'hour') {
        stepSize = 1; 
        maxTicksLimit = Math.ceil(durationMs / (60 * 60 * 1000)) + 1; 
        timeOptions.unit = 'hour';
        timeOptions.stepSize = stepSize;
        timeOptions.ticks = {
            source: 'auto',
            autoSkip: false,
            callback: (value) => {
                const date = new Date(value);
                const hours = date.getHours();
                if (hoursInRange <= 24) {
                    // Show every hour
                    return value;
                } else if (daysInRange <= 4) {
                    // Show at 00:00, 06:00, 12:00, 18:00
                    if (hours % 6 === 0) {
                        return value;
                    }
                } else if (daysInRange <= 15) {
                    // Show at 00:00, 12:00
                    if (hours % 12 === 0) {
                        return value;
                    }
                } else {
                    // Show at 00:00 every day (or every other day if needed)
                    if (hours === 0) {
                        return value;
                    }
                }
                return null;
            }
        };
    } else if (granularity === 'day') {
        stepSize = Math.ceil(intervalMs / (24 * 60 * 60 * 1000));
        maxTicksLimit = Math.ceil(durationMs / (stepSize * 24 * 60 * 60 * 1000)) + 1;
    } else {
        timeOptions.unit = 'month';
        stepSize = Math.ceil(intervalMs / (30 * 24 * 60 * 60 * 1000));
        maxTicksLimit = Math.ceil(durationMs / (stepSize * 30 * 24 * 60 * 60 * 1000)) + 1;
    }

    return {
        type: 'time',
        time: timeOptions,
        min: (granularity === 'second' ? adjustedStartTime : startTime) - paddingMs,
        max: (granularity === 'second' ? adjustedEndTime : endTime) + paddingMs,
        title: {
            display: true,
            text: `Timestamp (Interval: ${msToReadable(intervalMs)})`,
        },
        ticks: {
            maxRotation: 45,
            minRotation: 0,
            maxTicksLimit,
            callback: (value) => {
                const date = new Date(value);
                const hours = date.getHours();
                const minutes = date.getMinutes();
                const seconds = date.getSeconds();
                const currentDay = new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime();
                const isFirstTickOfDay = lastDay !== null && currentDay !== lastDay;
                lastDay = currentDay; 

                if (granularity === 'second') {
                    if ((seconds % 30 === 0 && adjustedDurationMs <= 5 * 60 * 1000) || 
                        (seconds % 60 === 0 && adjustedDurationMs <= 15 * 60 * 1000)) {
                        return formatXTicks(value, 'second', startTime, isFirstTickOfDay, daysInRange);
                    }
                } else if (granularity === 'minute') {
                    if ((durationMs <= 60 * 60 * 1000 && minutes % 5 === 0) ||
                        (durationMs <= 4 * 60 * 60 * 1000 && minutes % 15 === 0) ||
                        (durationMs <= 24 * 60 * 60 * 1000 && hours % 2 === 0 && minutes === 0)) {
                        return formatXTicks(value, 'minute', startTime, isFirstTickOfDay, daysInRange);
                    }
                } else if (granularity === 'hour') {
                    if (hoursInRange <= 24) {
                        return formatXTicks(value, 'hour', startTime, isFirstTickOfDay, daysInRange);
                    } else if (daysInRange <= 4) {
                        if (hours % 6 === 0) {
                            return formatXTicks(value, 'hour', startTime, isFirstTickOfDay, daysInRange);
                        }
                    } else if (daysInRange <= 15) {
                        if (hours % 12 === 0) {
                            return formatXTicks(value, 'hour', startTime, isFirstTickOfDay, daysInRange);
                        }
                    } else {
                        if (hours === 0) {
                            return formatXTicks(value, 'hour', startTime, isFirstTickOfDay, daysInRange);
                        }
                    }
                } else {
                    return formatXTicks(value, granularity, startTime, isFirstTickOfDay, daysInRange);
                }
                return null;
            },
            font: {
                weight: ({ tick }) => {
                    const date = new Date(tick.value);
                    if ((granularity === 'second' && date.getMinutes() === 0) ||
                        (granularity === 'minute' && date.getHours() === 0) ||
                        (granularity === 'hour' && date.getHours() === 0)) {
                        return 'bold';
                    }
                    return 'normal';
                },
            },
        },
    };
}

function triggerZoomSearch(startTime, endTime) {
    const data = getSearchFilter(true, false); 
    data.startEpoch = Math.floor(startTime);
    data.endEpoch = Math.floor(endTime);
    data.runTimechart = true;
    console.log('Search data:', data);

    updateDatePickerAndUrl(data.startEpoch, data.endEpoch);

    resetDashboard();
    logsRowData = [];
    accumulatedRecords = [];
    lastColumnsOrder = [];
    totalLoadedRecords = 0;

    $('#hits-summary').html(`
        <div><b>Processing query</b></div>
        <div>Searching for matching records...</div>
        <div></div>
    `);
    $('#pagination-container').hide();

    isSearchButtonTriggered = true;
    if (!isHistogramViewActive) {
        hasSearchSinceHistogramClosed = true;
    }

    wsState = 'query';
    initialSearchData = data;

    doSearch(data);
}

function updateDatePickerAndUrl(startEpoch, endEpoch) {
    datePickerHandler(startEpoch, endEpoch, 'custom');
    loadCustomDateTimeFromEpoch(startEpoch, endEpoch);

    addQSParm('startEpoch', startEpoch);
    addQSParm('endEpoch', endEpoch);
    window.history.pushState({ path: myUrl }, '', myUrl);
}

function renderHistogram(timechartData) {
    const histoContainer = $('#histogram-container');

    if (!histoContainer.length) {
        console.error('Histogram container not found');
        return;
    }

    if (!timechartData || !Array.isArray(timechartData.measure) || timechartData.measure.length === 0) {
        histoContainer.html('<div class="info-message">Histogram view only available for filter queries. Please modify your query to see histogram data.</div>');
        return;
    }

    if (!HistogramState.originalData && isSearchButtonTriggered) {
        HistogramState.originalData = JSON.parse(JSON.stringify(timechartData));
        // Only set original time range if not already set
        if (!HistogramState.originalStartTime || !HistogramState.originalEndTime) {
            let timestamps = timechartData.measure.map(item => {
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

            HistogramState.originalStartTime = Math.min(...timestamps);
            HistogramState.originalEndTime = Math.max(...timestamps);
        }
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

    const { granularity, intervalMs } = determineGranularity(startTime, endTime);
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

    const { gridLineColor, tickColor } = typeof getGraphGridColors === 'function'
        ? getGraphGridColors()
        : { gridLineColor: '#e0e0e0', tickColor: '#666' };

    const xAxisConfig = configureTimeAxis(startTime, endTime, intervalMs, granularity);

    if (HistogramState.currentHistogram) {
        HistogramState.currentHistogram.destroy();
        HistogramState.currentHistogram = null;
    }

    histoContainer.empty();
    histoContainer.html('<canvas width="100%" height="100%"></canvas><div class="resize-handle"></div>');
    histoContainer.css('height', '180px');//default height

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
                            min: HistogramState.originalStartTime,
                            max: HistogramState.originalEndTime,
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

    const handleDoubleClick = () => {
        if (HistogramState.currentHistogram && HistogramState.originalData) {
            HistogramState.currentGranularity = 'day';
            HistogramState.isZoomed = false;
            HistogramState.currentHistogram.resetZoom();
            triggerZoomSearch(HistogramState.originalStartTime, HistogramState.originalEndTime);
        }
    };

    if (HistogramState.eventListeners.dblclick) {
        HistogramState.canvas.removeEventListener('dblclick', HistogramState.eventListeners.dblclick);
    }

    HistogramState.canvas.addEventListener('dblclick', handleDoubleClick);
    HistogramState.eventListeners.dblclick = handleDoubleClick;

    const resizeHandle = histoContainer.find('.resize-handle')[0];
    let isResizing = false;

    resizeHandle.addEventListener('mousedown', (e) => {
        isResizing = true;
        resizeHandle.classList.add('active');
        e.preventDefault(); 
    });

    document.addEventListener('mousemove', (e) => {
        if (!isResizing) return;

        const container = histoContainer[0];
        const newHeight = e.clientY - container.getBoundingClientRect().top;
        const minHeight = 100; 
        const maxHeight = 600; 

        if (newHeight >= minHeight && newHeight <= maxHeight) {
            container.style.height = `${newHeight}px`;
        }
    });

    document.addEventListener('mouseup', () => {
        if (isResizing) {
            isResizing = false;
            resizeHandle.classList.remove('active');
        }
    });

    const resizeObserver = new ResizeObserver(() => {
        if (HistogramState.currentHistogram) {
            HistogramState.currentHistogram.resize();
        }
    });
    resizeObserver.observe(histoContainer[0]);

    addZoomHelper();
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

//eslint-disable-next-line no-unused-vars
function checkAndRestoreHistogramVisibility() {
    if (isHistogramViewActive && isSearchButtonTriggered &&
        !$('#empty-response').is(':visible') && !$('#corner-popup').is(':visible')) {
        $('.histo-container').show();
        
        if (timechartComplete) {
            renderHistogram(timechartComplete);
        }
    }
}

$(document).ready(function() {
    $('#histogram-toggle-btn').on('click', function() {
        $(this).toggleClass('active');
        const isActive = $(this).hasClass('active');
        isHistogramViewActive = isActive;

        if (isActive) {
            $('.histo-container').show();
            if (hasSearchSinceHistogramClosed) {
                $('#histogram-container').html('<div class="info-message">Hit search button to see histogram view</div>');
                hasSearchSinceHistogramClosed = false; 
            } else if (timechartComplete && HistogramState.currentHistogram) {
                // Show cached histogram if no new search and data exists
                $('.histo-container').show();
            } else if (timechartComplete) {
                // Render histogram if data exists but no chart is cached
                renderHistogram(timechartComplete);
            } else {
                // Default message when no data is available
                $('#histogram-container').html('<div class="info-message">Hit search button to see histogram view</div>');
            }
        } else {
            $('.histo-container').hide();
        }
    });
});