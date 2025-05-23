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
    isDragging: false,
    currentStartTime: null,
    currentEndTime: null,
    lastSearchStartTime: null,
    lastSearchEndTime: null,
    currentHeight: null,
    isZoomTriggered: false,
    isResetting: false
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

function getGranularityLabel(granularity) {
    if (granularity === 10 * 1000) return '10-second';
    if (granularity === 60 * 1000) return '1-minute';
    if (granularity === 5 * 60 * 1000) return '5-minute';
    if (granularity === 30 * 60 * 1000) return '30-minute';
    if (granularity === 60 * 60 * 1000) return '1-hour';
    if (granularity === 24 * 60 * 60 * 1000) return '1-day';
    return '1-month';
}

function getTimeAxisTitle(granularity, duration) {
    const interval = granularity === 'second' ? 10 * 1000 : 
                    granularity === 'minute' && duration <= 60 * 60 * 1000 ? 60 * 1000 : 
                    granularity === 'minute' && duration <= 4 * 60 * 60 * 1000 ? 5 * 60 * 1000 : 
                    granularity === 'minute' ? 30 * 60 * 1000 : 
                    granularity === 'hour' ? 60 * 60 * 1000 : 
                    granularity === 'day' ? 24 * 60 * 60 * 1000 : 
                    30 * 24 * 60 * 60 * 1000; 
    return `Time (Interval- ${getGranularityLabel(interval)})`;
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

function configureTimeAxis() {
    const duration = HistogramState.currentEndTime - HistogramState.currentStartTime;
    const daysInRange = Math.ceil(duration / (1000 * 60 * 60 * 24));
    const hoursInRange = Math.ceil(duration / (1000 * 60 * 60));
    let granularity, timeFormat, unit, stepSize, maxTicksLimit;

    if (duration <= 5 * 60 * 1000) { // ≤5 minutes
        granularity = 'second';
        timeFormat = 'HH:mm:ss';
        unit = 'second';
        stepSize = 10;
        maxTicksLimit = Math.ceil(duration / (10 * 1000)) + 1;
    } else if (duration <= 15 * 60 * 1000) { // >5 minutes and ≤15 minutes
        granularity = 'second';
        timeFormat = 'HH:mm:ss';
        unit = 'second';
        stepSize = 30;
        maxTicksLimit = Math.ceil(duration / (30 * 1000)) + 1;
    } else if (duration <= 60 * 60 * 1000) { // ≤1 hour
        granularity = 'minute';
        timeFormat = 'HH:mm';
        unit = 'minute';
        stepSize = 5;
        maxTicksLimit = Math.ceil(duration / (5 * 60 * 1000)) + 1;
    } else if (duration <= 4 * 60 * 60 * 1000) { // ≤4 hours
        granularity = 'minute';
        timeFormat = 'HH:mm';
        unit = 'minute';
        stepSize = 15;
        maxTicksLimit = Math.ceil(duration / (15 * 60 * 1000)) + 1;
    } else if (duration <= 24 * 60 * 60 * 1000) { // ≤1 day
        granularity = 'minute';
        timeFormat = 'HH:mm';
        unit = 'hour';
        stepSize = 2;
        maxTicksLimit = Math.ceil(duration / (2 * 60 * 60 * 1000)) + 1;
    } else if (duration <= 7 * 24 * 60 * 60 * 1000) { // ≤7 days
        granularity = 'hour';
        timeFormat = 'HH:mm';
        unit = 'hour';
        if (hoursInRange <= 24) {
            stepSize = 1;
            maxTicksLimit = hoursInRange + 1;
        } else if (daysInRange <= 4) {
            stepSize = 6;
            maxTicksLimit = daysInRange * 4 + 1;
        } else {
            stepSize = 12;
            maxTicksLimit = daysInRange * 2 + 1;
        }
    } else if (duration <= 180 * 24 * 60 * 60 * 1000) { // ≤180 days
        granularity = 'day';
        timeFormat = 'MM/dd/yyyy';
        unit = 'day';
        if (daysInRange <= 15) {
            maxTicksLimit = daysInRange;
        } else if (daysInRange <= 31) {
            maxTicksLimit = Math.min(14, Math.ceil(daysInRange / 2));
        } else if (daysInRange <= 90) {
            maxTicksLimit = Math.min(12, Math.ceil(daysInRange / 7));
        } else {
            maxTicksLimit = Math.min(15, Math.ceil(daysInRange / 14));
        }
    } else { // >180 days
        granularity = 'month';
        //eslint-disable-next-line no-unused-vars
        timeFormat = 'MM/yyyy';
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
            second: 'h:mm:ss a',
            minute: 'h:mm a',
            hour: 'h a',
            day: 'MMM d',
            month: 'MMM yyyy'
        },
        bounds: 'ticks'
    };

    if (granularity === 'hour') {
        timeOptions.round = 'hour';
        timeOptions.stepSize = stepSize;
        timeOptions.offset = false;
    } else if (granularity === 'day') {
        timeOptions.round = 'day';
        timeOptions.offset = true;
    } else if (granularity === 'second') {
        timeOptions.round = 'second';
        timeOptions.stepSize = stepSize;
        timeOptions.offset = true;
    } else {
        timeOptions.offset = true;
    }

    let lastDay = null; // Track the last day for day change detection

    return {
        type: 'time',
        time: timeOptions,
        title: {
            display: true,
            text: getTimeAxisTitle(granularity, duration),
        },
        ticks: {
            source: 'auto',
            autoSkip: false,
            maxTicksLimit: maxTicksLimit,
            maxRotation: 0,
            minRotation: 0,
            callback: function(value) {
                const date = new Date(value);
                const hours = date.getHours();
                const minutes = date.getMinutes();
                const seconds = date.getSeconds();

                const currentDay = new Date(date.getFullYear(), date.getMonth(), date.getDate()).getTime();
                const isFirstTickOfDay = lastDay !== null && currentDay !== lastDay;
                lastDay = currentDay;

                if (granularity === 'second') {
                    if ((duration <= 5 * 60 * 1000 && seconds % 10 === 0) || // 10-second intervals
                        (duration <= 15 * 60 * 1000 && seconds % 30 === 0)) { // 30-second intervals
                        return formatXTicks(value, 'second', HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                    }
                } else if (granularity === 'minute') {
                    if ((duration <= 60 * 60 * 1000 && minutes % 5 === 0) ||
                        (duration <= 4 * 60 * 60 * 1000 && minutes % 15 === 0) ||
                        (duration <= 24 * 60 * 60 * 1000 && hours % 2 === 0 && minutes === 0)) {
                        return formatXTicks(value, 'minute', HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                    }
                } else if (granularity === 'hour') {
                    if (hoursInRange <= 24) {
                        return formatXTicks(value, 'hour', HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                    } else if (daysInRange <= 4) {
                        if (hours % 6 === 0) {
                            return formatXTicks(value, 'hour', HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                        }
                    } else if (daysInRange <= 15) {
                        if (hours % 12 === 0) {
                            return formatXTicks(value, 'hour', HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                        }
                    } else {
                        if (hours === 0) {
                            return formatXTicks(value, 'hour', HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                        }
                    }
                } else {
                    return formatXTicks(value, granularity, HistogramState.currentStartTime, isFirstTickOfDay, daysInRange);
                }
                return null;
            },
            font: {
                weight: function(context) {
                    const date = new Date(context.tick.value);
                    if ((granularity === 'second' || granularity === 'minute' || granularity === 'hour') &&
                        date.getHours() === 0 && date.getMinutes() === 0 && date.getSeconds() === 0) {
                        return 'bold';
                    }
                    return 'normal';
                }
            }
        }
    };
}

function formatTooltipTimestamp(timestamp) {
    const duration = HistogramState.currentEndTime - HistogramState.currentStartTime;
    const { month, day, year, hour12, minutes, seconds, ampm } = parseTimestamp(timestamp);
    const base = `${month} ${day}, ${year}`;

    if (duration <= 15 * 60 * 1000) {
        return `${base} ${hour12}:${minutes}:${seconds} ${ampm}`;
    } else if (duration <= 24 * 60 * 60 * 1000) {
        return `${base} ${hour12}:${minutes} ${ampm}`;
    } else if (duration <= 7 * 24 * 60 * 60 * 1000) {
        return `${base} ${hour12}:00 ${ampm}`;
    } else if (duration <= 180 * 24 * 60 * 60 * 1000) {
        return base;
    } else {
        return `${month} ${year}`;
    }
}

function triggerZoomSearch(startTime, endTime, isZoomTriggered = true) {
    const data = getSearchFilter(true, false);
    data.startEpoch = Math.floor(startTime);
    data.endEpoch = Math.floor(endTime);
    data.runTimechart = true;

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

    HistogramState.isZoomTriggered = isZoomTriggered;

    wsState = 'query';
    initialSearchData = data;

    doSearch(data).finally(() => {
        HistogramState.isZoomTriggered = false;
    });
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

    if (isSearchButtonTriggered) {
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

        HistogramState.currentStartTime = Math.min(...timestamps);
        HistogramState.currentEndTime = Math.max(...timestamps);

        if (!HistogramState.isZoomTriggered) {
            HistogramState.lastSearchStartTime = HistogramState.currentStartTime;
            HistogramState.lastSearchEndTime = HistogramState.currentEndTime;
        }
    }

    const chartData = timechartData.measure.map(item => ({
        x: parseInt(item.GroupByValues[0]),
        y: item.MeasureVal["count(*)"],
        originalTimestamps: [parseInt(item.GroupByValues[0])]
    }));

    const { gridLineColor, tickColor } = typeof getGraphGridColors === 'function'
        ? getGraphGridColors()
        : { gridLineColor: '#e0e0e0', tickColor: '#666' };

    const xAxisConfig = configureTimeAxis();

    if (HistogramState.currentHistogram) {
        HistogramState.currentHistogram.destroy();
        HistogramState.currentHistogram = null;
    }

    histoContainer.empty();
    histoContainer.html('<canvas width="100%" height="100%"></canvas><div class="resize-handle"></div>');

    const canvas = histoContainer.find('canvas')[0];
    const ctx = canvas.getContext('2d');

    //eslint-disable-next-line no-undef
    Chart.register(ChartZoom);
    Chart.register(customDragBorderPlugin);

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
                barPercentage:0.8,
                categoryPercentage: 0.8
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
                        display: true,
                        text: 'Event Count',
                        color: tickColor
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
                            return formatTooltipTimestamp(timestamp);
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
                            if (HistogramState.isResetting) {
                                return;
                            }
                            const xScale = chart.scales.x;
                            const min = xScale.min;
                            const max = xScale.max;

                            triggerZoomSearch(min, max, true);
                            HistogramState.currentStartTime = min;
                            HistogramState.currentEndTime = max;
                        }
                    },
                    limits: {
                        x: {
                            min: HistogramState.lastSearchStartTime,
                            max: HistogramState.lastSearchEndTime,
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
        if (HistogramState.currentHistogram) {
            const xScale = HistogramState.currentHistogram.scales.x;
            const isZoomed = Math.abs(xScale.min - HistogramState.lastSearchStartTime) > 1000 ||
                            Math.abs(xScale.max - HistogramState.lastSearchEndTime) > 1000;
            if (isZoomed) {
                HistogramState.isResetting = true;
                HistogramState.currentHistogram.resetZoom();
                triggerZoomSearch(HistogramState.lastSearchStartTime, HistogramState.lastSearchEndTime, false);
                HistogramState.isResetting = false;
                const helper = document.querySelector('.zoom-helper');
                if (helper) {
                    helper.textContent = 'Drag to zoom and Double-click to reset zoom';
                }
            }
        }
    };

    canvas.removeEventListener('dblclick', handleDoubleClick);
    canvas.addEventListener('dblclick', handleDoubleClick);

    const resizeHandle = histoContainer.find('.resize-handle')[0];
    let isResizing = false;

    if (!HistogramState.currentHeight) {
        const currentHeightPx = histoContainer.css('height');
        const parsedHeight = parseInt(currentHeightPx, 10);
        const maxHeight = Math.floor(window.innerHeight * 0.5);
        HistogramState.currentHeight = !isNaN(parsedHeight) && parsedHeight >= 100 && parsedHeight <= maxHeight 
            ? parsedHeight 
            : Math.min(180, maxHeight);
    }

    histoContainer.css({
        'height': `${HistogramState.currentHeight}px`,
        '--histogram-height': `${HistogramState.currentHeight}px`
    });

    resizeHandle.addEventListener('mousedown', (e) => {
        isResizing = true;
        resizeHandle.classList.add('active');
        e.preventDefault();
    });

    const handleMouseMove = (e) => {
        if (!isResizing) return;
        
        const container = histoContainer[0];
        const newHeight = e.clientY - container.getBoundingClientRect().top;
        const minHeight = 100;
        const maxHeight = Math.floor(window.innerHeight * 0.5);

        if (newHeight >= minHeight && newHeight <= maxHeight) {
            container.style.height = `${newHeight}px`;
            container.style.setProperty('--histogram-height', `${newHeight}px`);
            HistogramState.currentHeight = newHeight;
            
            if (HistogramState.currentHistogram) {
                HistogramState.currentHistogram.resize();
            }
        }
    };

    const handleMouseUp = () => {
        if (isResizing) {
            isResizing = false;
            resizeHandle.classList.remove('active');
        }
    };

    document.addEventListener('mousemove', handleMouseMove);
    document.addEventListener('mouseup', handleMouseUp);

    HistogramState.cleanupResize = () => {
        document.removeEventListener('mousemove', handleMouseMove);
        document.removeEventListener('mouseup', handleMouseUp);
    };

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
    helpText.textContent = 'Drag to zoom and Double-click to reset zoom';

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