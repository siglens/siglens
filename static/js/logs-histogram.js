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
    dragStartX: 0,
    dragEndX: 0,
    selectionOverlay: null,
    canvas: null,
    eventListeners: {}, 
};

const GRANULARITY_LEVELS = ['month', 'week', 'day', 'hour', 'minute', 'second'];

function determineGranularity(timestamps) {
    if (!Array.isArray(timestamps) || timestamps.length < 2) return 'day';

    const parsedTimestamps = timestamps
        .map(ts => {
            try {
                const date = new Date(convertIfTimestamp(ts));
                return isNaN(date.getTime()) ? null : date.getTime();
            } catch (e) {
                console.error('Invalid timestamp:', ts, e);
                return null;
            }
        })
        .filter(ts => ts !== null);

    if (parsedTimestamps.length < 2) return 'day';

    const sortedTimestamps = parsedTimestamps.sort((a, b) => a - b);
    const minDiff = Math.min(...sortedTimestamps.slice(1).map((val, idx) => val - sortedTimestamps[idx]));

    if (isNaN(minDiff) || minDiff <= 0) return 'day'; // Fallback for invalid differences

    if (minDiff >= 30 * 24 * 60 * 60 * 1000) return 'month';
    if (minDiff >= 7 * 24 * 60 * 60 * 1000) return 'week';
    if (minDiff >= 24 * 60 * 60 * 1000) return 'day';
    if (minDiff >= 60 * 60 * 1000) return 'hour';
    if (minDiff >= 60 * 1000) return 'minute';
    return 'second';
}

function formatTimestampForGranularity(timestamp, granularity) {
    try {
        const date = new Date(timestamp);
        if (isNaN(date.getTime())) throw new Error('Invalid date');

        const year = date.getFullYear();
        const month = ('0' + (date.getMonth() + 1)).slice(-2);
        const day = ('0' + date.getDate()).slice(-2);
        const hours = ('0' + date.getHours()).slice(-2);
        const minutes = ('0' + date.getMinutes()).slice(-2);
        const seconds = ('0' + date.getSeconds()).slice(-2);

        switch (granularity) {
            case 'month':
                return `${year}-${month}`;
            case 'week':
            case 'day':
                return `${year}-${month}-${day}`;
            case 'hour':
                return `${hours}:${minutes}`;
            case 'minute':
                return `${hours}:${minutes}`;
            case 'second':
                return `${hours}:${minutes}:${seconds}`;
            default:
                return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
        }
    } catch (e) {
        console.error('Error formatting timestamp:', timestamp, e);
        return 'Invalid Timestamp';
    }
}

function filterDataByRange(data, startTime, endTime, newGranularity) {
    if (!data || !Array.isArray(data.measure)) return null;

    const filteredMeasures = data.measure.filter(item => {
        if (!item.GroupByValues || !item.GroupByValues[0]) return false;
        try {
            const timestamp = new Date(convertIfTimestamp(item.GroupByValues[0])).getTime();
            return timestamp >= startTime && timestamp <= endTime;
        } catch (e) {
            console.error('Error parsing timestamp in filter:', item.GroupByValues[0], e);
            return false;
        }
    });

    if (filteredMeasures.length === 0) {
        return null;
    }

    return {
        ...data,
        measure: filteredMeasures
    };
}

function renderHistogram(timechartData, zoomRange = null) {
    const histoContainer = $('#histogram-container');
    if (!histoContainer.length) {
        console.error('Histogram container not found');
        return;
    }

    // Validate input data
    if (!timechartData || !Array.isArray(timechartData.measure) || timechartData.measure.length === 0) {
        histoContainer.hide();
        histoContainer.html('<div class="error-message">No histogram data available</div>');
        return;
    }

    // Store original data if not already stored
    if (!HistogramState.originalData) {
        HistogramState.originalData = JSON.parse(JSON.stringify(timechartData));
    }

    let dataToRender = timechartData;
    let timestamps = dataToRender.measure.map(item => {
        if (!item.GroupByValues || !item.GroupByValues[0]) {
            console.warn('Missing GroupByValues in measure:', item);
            return null;
        }
        return convertIfTimestamp(item.GroupByValues[0]);
    }).filter(ts => ts !== null);

    let counts = dataToRender.measure.map((item, idx) => {
        if (!item.MeasureVal || !('count(*)' in item.MeasureVal)) {
            console.warn('Missing count(*) in measure:', item);
            return 0;
        }
        return item.MeasureVal['count(*)'] || 0;
    });

    if (zoomRange) {
        if (zoomRange.start < 0 || zoomRange.end >= timestamps.length || zoomRange.start >= zoomRange.end) {
            console.warn('Invalid zoom range:', zoomRange);
            return;
        }

        const startTime = new Date(timestamps[zoomRange.start]).getTime();
        const endTime = new Date(timestamps[zoomRange.end]).getTime();
        const timeDiff = (endTime - startTime) / 1000;

        let newGranularityIndex = GRANULARITY_LEVELS.indexOf(HistogramState.currentGranularity);
        if (timeDiff >= 30 * 24 * 60 * 60) newGranularityIndex = Math.max(0, newGranularityIndex - 1);
        else if (timeDiff >= 7 * 24 * 60 * 60) newGranularityIndex = Math.max(1, newGranularityIndex - 1);
        else if (timeDiff >= 24 * 60 * 60) newGranularityIndex = Math.max(2, newGranularityIndex - 1);
        else if (timeDiff >= 60 * 60) newGranularityIndex = Math.max(3, newGranularityIndex - 1);
        else if (timeDiff >= 60) newGranularityIndex = Math.max(4, newGranularityIndex - 1);
        else newGranularityIndex = Math.max(5, newGranularityIndex - 1);

        HistogramState.currentGranularity = GRANULARITY_LEVELS[newGranularityIndex];
        const filteredData = filterDataByRange(dataToRender, startTime, endTime, HistogramState.currentGranularity);

        if (!filteredData) {
            console.log('No data available at this granularity');
            histoContainer.html('<div class="error-message">No data available for the selected range</div>');
            return;
        }

        dataToRender = filteredData;
        timestamps = dataToRender.measure.map(item => convertIfTimestamp(item.GroupByValues[0]));
        counts = dataToRender.measure.map(item => item.MeasureVal['count(*)'] || 0);
    }

    const granularity = determineGranularity(timestamps);
    HistogramState.currentGranularity = granularity;

    const formattedTimestamps = timestamps.map(ts => formatTimestampForGranularity(ts, granularity));

    if (HistogramState.currentHistogram) {
        HistogramState.currentHistogram.destroy();
        HistogramState.currentHistogram = null;
    }

    histoContainer.empty();
    histoContainer.html('<canvas width="100%" height="100%"></canvas>');

    HistogramState.canvas = histoContainer.find('canvas')[0];
    const ctx = HistogramState.canvas.getContext('2d');
    const fontSize = formattedTimestamps.length > 10 ? 10 : 12;

    // Fallback for getGraphGridColors
    const { gridLineColor, tickColor } = typeof getGraphGridColors === 'function'
        ? getGraphGridColors()
        : { gridLineColor: '#e0e0e0', tickColor: '#666' };

    const barCount = formattedTimestamps.length;
    const rotationThreshold = 20;
    const labelSkipThreshold = 50;
    const shouldRotate = barCount > rotationThreshold;
    const skipInterval = barCount > labelSkipThreshold ? Math.ceil(barCount / 10) : 1;

    HistogramState.currentHistogram = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: formattedTimestamps,
            datasets: [{
                label: 'Count',
                data: counts,
                backgroundColor: globalColorArray && globalColorArray[0] ? globalColorArray[0] + '70' : 'rgba(0, 123, 255, 0.5)',
                borderColor: globalColorArray && globalColorArray[0] ? globalColorArray[0] : 'rgba(0, 123, 255, 1)',
                borderWidth: 1,
                barPercentage: 1.0,
                categoryPercentage: 1.0
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: 'Timestamp',
                        color: tickColor
                    },
                    ticks: {
                        font: {
                            size: fontSize
                        },
                        color: tickColor,
                        maxRotation: shouldRotate ? 45 : 0,
                        minRotation: shouldRotate ? 45 : 0,
                        callback: function(value, index) {
                            return index % skipInterval === 0 ? formattedTimestamps[index] : '';
                        }
                    },
                    grid: {
                        color: gridLineColor
                    }
                },
                y: {
                    title: {
                        display: true,
                        text: 'Count',
                        color: tickColor
                    },
                    beginAtZero: true,
                    ticks: {
                        color: tickColor
                    },
                    grid: {
                        color: gridLineColor
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        boxWidth: 12,
                        font: {
                            size: 12
                        },
                        color: tickColor
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function(context) {
                            const index = context.dataIndex;
                            const originalTimestamp = dataToRender.measure[index].GroupByValues[0];
                            const fullTimestamp = convertIfTimestamp(originalTimestamp);
                            return `${context.dataset.label}: ${context.raw} | Time: ${fullTimestamp}`;
                        }
                    }
                }
            }
        }
    });

    if ($('#histogram-toggle-btn').hasClass('active')) {
        $('#histogram-container').show();
    }

    // Remove existing event listeners
    const eventTypes = ['mousedown', 'mousemove', 'mouseup', 'mouseleave', 'dblclick'];
    eventTypes.forEach(eventType => {
        if (HistogramState.eventListeners[eventType]) {
            HistogramState.canvas.removeEventListener(eventType, HistogramState.eventListeners[eventType]);
        }
    });

    // Add new event listeners
    const handleMouseDown = (e) => {
        const rect = HistogramState.canvas.getBoundingClientRect();
        HistogramState.dragStartX = e.clientX - rect.left;
        HistogramState.isDragging = true;

        if (!HistogramState.selectionOverlay) {
            HistogramState.selectionOverlay = document.createElement('canvas');
            HistogramState.selectionOverlay.style.position = 'absolute';
            HistogramState.selectionOverlay.style.left = '0';
            HistogramState.selectionOverlay.style.top = '0';
            HistogramState.selectionOverlay.width = rect.width;
            HistogramState.selectionOverlay.height = rect.height;
            HistogramState.selectionOverlay.style.pointerEvents = 'none';
            histoContainer[0].appendChild(HistogramState.selectionOverlay);
        }
    };

    const handleMouseMove = (e) => {
        if (!HistogramState.isDragging) return;

        const rect = HistogramState.canvas.getBoundingClientRect();
        HistogramState.dragEndX = e.clientX - rect.left;

        const overlayCtx = HistogramState.selectionOverlay.getContext('2d');
        overlayCtx.clearRect(0, 0, HistogramState.selectionOverlay.width, HistogramState.selectionOverlay.height);

        const chartArea = HistogramState.currentHistogram.chartArea;
        const top = chartArea.top;
        const height = chartArea.bottom - chartArea.top;

        const startX = Math.min(HistogramState.dragStartX, HistogramState.dragEndX);
        const width = Math.abs(HistogramState.dragEndX - HistogramState.dragStartX);

        overlayCtx.fillStyle = 'rgba(0, 0, 255, 0.1)';
        overlayCtx.fillRect(startX, top, width, height);

        overlayCtx.beginPath();
        overlayCtx.setLineDash([3, 3]);
        overlayCtx.strokeStyle = 'rgba(0, 0, 255, 0.8)';
        overlayCtx.lineWidth = 1;

        overlayCtx.moveTo(startX, top);
        overlayCtx.lineTo(startX, top + height);

        overlayCtx.moveTo(startX + width, top);
        overlayCtx.lineTo(startX + width, top + height);

        overlayCtx.stroke();
    };

    const handleMouseUp = () => {
        if (!HistogramState.isDragging) return;
        HistogramState.isDragging = false;

        if (HistogramState.selectionOverlay) {
            HistogramState.selectionOverlay.remove();
            HistogramState.selectionOverlay = null;
        }

        const xScale = HistogramState.currentHistogram.scales.x;
        const startIdx = Math.round(xScale.getValueForPixel(Math.min(HistogramState.dragStartX, HistogramState.dragEndX)));
        const endIdx = Math.round(xScale.getValueForPixel(Math.max(HistogramState.dragStartX, HistogramState.dragEndX)));

        if (startIdx >= 0 && endIdx >= 0 && startIdx < endIdx) {
            renderHistogram(timechartData, { start: startIdx, end: endIdx });
        }
    };

    const handleMouseLeave = () => {
        if (HistogramState.isDragging && HistogramState.selectionOverlay) {
            HistogramState.isDragging = false;
            HistogramState.selectionOverlay.remove();
            HistogramState.selectionOverlay = null;
        }
    };

    const handleDoubleClick = () => {
        if (HistogramState.originalData) {
            HistogramState.currentGranularity = 'day';
            renderHistogram(HistogramState.originalData);
            updateZoomIndicator();
        }
    };

    HistogramState.canvas.addEventListener('mousedown', handleMouseDown);
    HistogramState.canvas.addEventListener('mousemove', handleMouseMove);
    HistogramState.canvas.addEventListener('mouseup', handleMouseUp);
    HistogramState.canvas.addEventListener('mouseleave', handleMouseLeave);
    HistogramState.canvas.addEventListener('dblclick', handleDoubleClick);

    HistogramState.eventListeners = {
        mousedown: handleMouseDown,
        mousemove: handleMouseMove,
        mouseup: handleMouseUp,
        mouseleave: handleMouseLeave,
        dblclick: handleDoubleClick
    };

    updateZoomIndicator();
    addZoomHelper();
}

function convertTimestamp(timestampString) {
    try {
        const timestamp = parseInt(timestampString);
        if (isNaN(timestamp)) throw new Error('Invalid timestamp value');
        const date = new Date(timestamp);

        const year = date.getFullYear();
        const month = ('0' + (date.getMonth() + 1)).slice(-2);
        const day = ('0' + date.getDate()).slice(-2);
        const hours = ('0' + date.getHours()).slice(-2);
        const minutes = ('0' + date.getMinutes()).slice(-2);
        const seconds = ('0' + date.getSeconds()).slice(-2);

        return `${year}-${month}-${day} ${hours}:${minutes}:${seconds}`;
    } catch (e) {
        console.error('Error converting timestamp:', timestampString, e);
        return 'Invalid Timestamp';
    }
}

function convertIfTimestamp(value) {
    const isTimestamp = !isNaN(value) && String(value).length === 13 && new Date(parseInt(value)).getTime() > 0;
    if (isTimestamp) {
        return convertTimestamp(value);
    }
    return value;
}

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
    helpText.textContent = 'Drag to zoom and Double-click to reset zoom';

    $('#histogram-container').append(helpText);
}

function updateZoomIndicator() {
    let zoomIndicator = $('#zoom-level-indicator');

    if (zoomIndicator.length === 0) {
        zoomIndicator = $('<div id="zoom-level-indicator"></div>');
        zoomIndicator.css({
            position: 'absolute',
            top: '5px',
            left: '10px',
            fontSize: '11px',
            color: 'var(--text-color)',
            padding: '2px 6px',
            borderRadius: '3px',
            pointerEvents: 'none'
        });
        $('#histogram-container').append(zoomIndicator);
    }

    const granularityDisplay = {
        'month': 'Monthly view',
        'week': 'Weekly view',
        'day': 'Daily view',
        'hour': 'Hourly view',
        'minute': 'Minute view',
        'second': 'Second view'
    };

    zoomIndicator.text(granularityDisplay[HistogramState.currentGranularity] || 'Timeline view');
}

$(document).ready(function() {
    $('#histogram-toggle-btn').on('click', function() {
        $(this).toggleClass('active');
        $('.histo-container').toggle();
    
        if ($(this).hasClass('active')) {
            //eslint-disable-next-line no-undef
            isHistogramViewActive = true;
            //eslint-disable-next-line no-undef
            if ($('#histogram-container').is(':visible') && !HistogramState.currentHistogram && window.timechartComplete) {
                //eslint-disable-next-line no-undef
                renderHistogram(window.timechartComplete);
                addZoomHelper();
                updateZoomIndicator();
            } else {
                const searchFilter = window.getSearchFilter(false, false);
                window.doSearch(searchFilter).catch(error => {
                    console.error('Error running search with histogram:', error);
                    window.showError('Failed to load histogram: ' + error);
                });
            }
        } else {
            //eslint-disable-next-line no-undef
            window.isHistogramViewActive = false;
        }
    });
});
