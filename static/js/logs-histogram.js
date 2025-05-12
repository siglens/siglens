<<<<<<< HEAD
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
let currentHistogram = null;
let originalData = null; 
let currentGranularity = 'day'; 
const GRANULARITY_LEVELS = ['month', 'week', 'day', 'hour', 'minute', 'second'];

let isDragging = false;
let dragStartX = 0;
let dragEndX = 0;
let selectionOverlay = null;

function determineGranularity(timestamps) {
    if (timestamps.length < 2) return 'day'; 

    const parsedTimestamps = timestamps.map(ts => new Date(convertIfTimestamp(ts)).getTime());
    const minDiff = Math.min(...parsedTimestamps.slice(1).map((val, idx) => val - parsedTimestamps[idx]));

    if (minDiff >= 30 * 24 * 60 * 60 * 1000) return 'month'; 
    if (minDiff >= 7 * 24 * 60 * 60 * 1000) return 'week';
    if (minDiff >= 24 * 60 * 60 * 1000) return 'day'; 
    if (minDiff >= 60 * 60 * 1000) return 'hour'; 
    if (minDiff >= 60 * 1000) return 'minute'; 
    return 'second'; 
}

function formatTimestampForGranularity(timestamp, granularity) {
    const date = new Date(timestamp);
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
}

function filterDataByRange(data, startTime, endTime, newGranularity) {
    const filteredMeasures = data.measure.filter(item => {
        const timestamp = new Date(convertIfTimestamp(item.GroupByValues[0])).getTime();
        return timestamp >= startTime && timestamp <= endTime;
    });
y
    if (filteredMeasures.length === 0) {
        return null;
    }

    return {
        ...data,
        measure: filteredMeasures
    };
}

// eslint-disable-next-line no-unused-vars
function renderHistogram(timechartData, zoomRange = null) {
    if (!timechartData || !timechartData.measure || timechartData.measure.length === 0) {
        $('#histogram-container').hide();
        return;
    }

    if (!originalData) {
        originalData = JSON.parse(JSON.stringify(timechartData));
    }

    let dataToRender = timechartData;
    let timestamps = dataToRender.measure.map(item => convertIfTimestamp(item.GroupByValues[0]));
    let counts = dataToRender.measure.map(item => item.MeasureVal['count(*)'] || 0);


    if (zoomRange) {
        const startTime = new Date(timestamps[zoomRange.start]).getTime();
        const endTime = new Date(timestamps[zoomRange.end]).getTime();
        const timeDiff = (endTime - startTime) / 1000; 

        let newGranularityIndex = GRANULARITY_LEVELS.indexOf(currentGranularity);
        if (timeDiff >= 30 * 24 * 60 * 60) newGranularityIndex = Math.max(0, newGranularityIndex - 1); // Month
        else if (timeDiff >= 7 * 24 * 60 * 60) newGranularityIndex = Math.max(1, newGranularityIndex - 1); // Week
        else if (timeDiff >= 24 * 60 * 60) newGranularityIndex = Math.max(2, newGranularityIndex - 1); // Day
        else if (timeDiff >= 60 * 60) newGranularityIndex = Math.max(3, newGranularityIndex - 1); // Hour
        else if (timeDiff >= 60) newGranularityIndex = Math.max(4, newGranularityIndex - 1); // Minute
        else newGranularityIndex = Math.max(5, newGranularityIndex - 1); // Second

        currentGranularity = GRANULARITY_LEVELS[newGranularityIndex];
        const filteredData = filterDataByRange(dataToRender, startTime, endTime, currentGranularity);

        if (!filteredData) {
            console.log('No data available at this granularity');
            return;
        }

        dataToRender = filteredData;
        timestamps = dataToRender.measure.map(item => convertIfTimestamp(item.GroupByValues[0]));
        counts = dataToRender.measure.map(item => item.MeasureVal['count(*)'] || 0);
    }

    const granularity = determineGranularity(timestamps);
    currentGranularity = granularity;

    const formattedTimestamps = timestamps.map(ts => formatTimestampForGranularity(ts, granularity));

    if (currentHistogram) {
        currentHistogram.destroy();
    }

    const histoContainer = $('#histogram-container');
    if (!histoContainer.length) {
        console.error('Histogram container not found');
        return;
    }

    histoContainer.empty();
    histoContainer.html('<canvas width="100%" height="100%"></canvas>');

    const canvas = histoContainer.find('canvas')[0];
    const ctx = canvas.getContext('2d');
    const fontSize = formattedTimestamps.length > 10 ? 10 : 12;

    const { gridLineColor, tickColor } = getGraphGridColors();

    const barCount = formattedTimestamps.length;
    const rotationThreshold = 20; 
    const labelSkipThreshold = 50; 
    const shouldRotate = barCount > rotationThreshold;
    const skipInterval = barCount > labelSkipThreshold ? Math.ceil(barCount / 10) : 1; 

    currentHistogram = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: formattedTimestamps,
            datasets: [{
                label: 'Count',
                data: counts,
                backgroundColor: globalColorArray[0] + '70',
                borderColor: globalColorArray[0],
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

    canvas.addEventListener('mousedown', (e) => {
        const rect = canvas.getBoundingClientRect();
        dragStartX = e.clientX - rect.left;
        isDragging = true;

        if (!selectionOverlay) {
            selectionOverlay = document.createElement('canvas');
            selectionOverlay.style.position = 'absolute';
            selectionOverlay.style.left = '0';
            selectionOverlay.style.top = '0';
            selectionOverlay.width = rect.width;
            selectionOverlay.height = rect.height;
            selectionOverlay.style.pointerEvents = 'none';
            histoContainer[0].appendChild(selectionOverlay);
        }
    });

    canvas.addEventListener('mousemove', (e) => {
        if (!isDragging) return;

        const rect = canvas.getBoundingClientRect();
        dragEndX = e.clientX - rect.left;

        const overlayCtx = selectionOverlay.getContext('2d');
        overlayCtx.clearRect(0, 0, selectionOverlay.width, selectionOverlay.height);
        
        const chartArea = currentHistogram.chartArea;
        const top = chartArea.top;
        const height = chartArea.bottom - chartArea.top;

        const startX = Math.min(dragStartX, dragEndX);
        const width = Math.abs(dragEndX - dragStartX);

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
  });

    canvas.addEventListener('mouseup', () => {
        if (!isDragging) return;
        isDragging = false;

        if (selectionOverlay) {
            selectionOverlay.remove();
            selectionOverlay = null;
        }

        const xScale = currentHistogram.scales.x;
        const startIdx = Math.round(xScale.getValueForPixel(Math.min(dragStartX, dragEndX)));
        const endIdx = Math.round(xScale.getValueForPixel(Math.max(dragStartX, dragEndX)));

        if (startIdx >= 0 && endIdx >= 0 && startIdx < endIdx) {
            renderHistogram(timechartData, { start: startIdx, end: endIdx });
        }
    });

    canvas.addEventListener('mouseleave', () => {
        if (isDragging && selectionOverlay) {
            isDragging = false;
            selectionOverlay.remove();
            selectionOverlay = null;
        }
    });

    canvas.addEventListener('dblclick', () => {
        if (originalData) {
            currentGranularity = 'day';
            renderHistogram(originalData);
            
            if (typeof updateZoomIndicator === 'function') {
                updateZoomIndicator();
            }
        }
    });
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

// eslint-disable-next-line no-unused-vars
function updateHistogramTheme() {
    if (!currentHistogram) return;

    const { gridLineColor, tickColor } = getGraphGridColors();

    currentHistogram.options.scales.x.grid.color = gridLineColor;
    currentHistogram.options.scales.x.ticks.color = tickColor;
    currentHistogram.options.scales.x.title.color = tickColor;

    currentHistogram.options.scales.y.grid.color = gridLineColor;
    currentHistogram.options.scales.y.ticks.color = tickColor;
    currentHistogram.options.scales.y.title.color = tickColor;

    currentHistogram.options.plugins.legend.labels.color = tickColor;

    currentHistogram.update();
}

function addZoomHelper() {
    const helpText = document.createElement('div');
    helpText.className = 'zoom-helper';
    helpText.style.cssText = `
        position: absolute;
        bottom: 5px;
        right: 10px;
        font-size: 11px;
        color: #666;
        padding: 4px 8px;
        background-color: rgba(255,255,255,0.7);
        border-radius: 3px;
        pointer-events: none;
        opacity: 0;
        transition: opacity 0.3s ease;
    `;
    helpText.textContent = 'Drag to zoom • Double-click to reset';
    
    $('#histogram-container').append(helpText);
    
    $('#histogram-container').one('mouseenter', function() {
        helpText.style.opacity = '1';
        setTimeout(() => {
            helpText.style.opacity = '0';
        }, 3000);
    });
    
    canvas.addEventListener('mouseup', () => {
        if (isDragging) {
            helpText.style.opacity = '1';
            setTimeout(() => {
                helpText.style.opacity = '0';
            }, 2000);
        }
    });
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
            color: '#666',
            padding: '2px 6px',
            backgroundColor: 'rgba(255,255,255,0.7)',
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
    
    zoomIndicator.text(granularityDisplay[currentGranularity] || 'Timeline view');
}

$(document).ready(function() {
    $('#histogram-toggle-btn').on('click', function() {
        $(this).toggleClass('active');
        $('#histogram-container').toggle();

        if ($('#histogram-container').is(':visible') && !currentHistogram && timechartComplete) {
            renderHistogram(timechartComplete);
            addZoomHelper();
            updateZoomIndicator();
        }
    });
});