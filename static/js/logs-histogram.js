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
    currentGranularity: { type: 'day', interval: 1 },
    canvas: null,
};

function determineGranularity(timestamps, startTime, endTime) {
    if (!Array.isArray(timestamps) || timestamps.length < 2) return { type: 'day', interval: 1 };

    const duration = endTime - startTime;

    if (duration >= 180 * 24 * 60 * 60 * 1000) return { type: 'month', interval: 1 };
    if (duration >= 7 * 24 * 60 * 60 * 1000) return { type: 'week', interval: 1 };
    if (duration >= 24 * 60 * 60 * 1000) return { type: 'day', interval: 1 };
    if (duration >= 60 * 60 * 1000) return { type: 'hour', interval: 1 };
    if (duration >= 60 * 1000) return { type: 'minute', interval: 1 };
    return { type: 'second', interval: 1 };
}

function formatTimestampForGranularity(timestamp, granularityType) {
    try {
        const date = new Date(timestamp);
        if (isNaN(date.getTime())) throw new Error('Invalid date');

        const year = date.getFullYear();
        const month = ('0' + (date.getMonth() + 1)).slice(-2);
        const day = ('0' + date.getDate()).slice(-2);
        const hours = ('0' + date.getHours()).slice(-2);
        const minutes = ('0' + date.getMinutes()).slice(-2);
        const seconds = ('0' + date.getSeconds()).slice(-2);

        switch (granularityType) {
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

    const existingTimestamps = new Set();
    filteredMeasures.forEach(item => {
        if (item.GroupByValues && item.GroupByValues[0]) {
            existingTimestamps.add(new Date(convertIfTimestamp(item.GroupByValues[0])).getTime());
        }
    });

    let interval;
    const { type, interval: intervalCount } = newGranularity;
    switch (type) {
        case 'month':
            interval = intervalCount * 30 * 24 * 60 * 60 * 1000;
            break;
        case 'week':
            interval = intervalCount * 7 * 24 * 60 * 60 * 1000;
            break;
        case 'day':
            interval = intervalCount * 24 * 60 * 60 * 1000;
            break;
        case 'hour':
            interval = intervalCount * 60 * 60 * 1000;
            break;
        case 'minute':
            interval = intervalCount * 60 * 1000;
            break;
        case 'second':
            interval = intervalCount * 1000;
            break;
        default:
            interval = 24 * 60 * 60 * 1000;
    }

    const allTimestamps = [];
    let currentTime = startTime;
    while (currentTime <= endTime) {
        allTimestamps.push(currentTime);
        currentTime += interval;
    }

    const completeMeasures = [];
    allTimestamps.forEach(timestamp => {
        if (!existingTimestamps.has(timestamp)) {
            completeMeasures.push({
                GroupByValues: [new Date(timestamp).toISOString()],
                MeasureVal: { 'count(*)': 0 }
            });
        }
    });

    const finalMeasures = [...filteredMeasures, ...completeMeasures].sort((a, b) => {
        const timeA = new Date(convertIfTimestamp(a.GroupByValues[0])).getTime();
        const timeB = new Date(convertIfTimestamp(b.GroupByValues[0])).getTime();
        return timeA - timeB;
    });

    if (finalMeasures.length === 0) {
        return null;
    }

    return {
        ...data,
        measure: finalMeasures
    };
}

function renderHistogram(timechartData) {
    const histoContainer = $('#histogram-container');
    const parentContainer = $('.histo-container');
    const emptyResponse = $('#empty-response');
    if (!histoContainer.length) {
        console.error('Histogram container not found');
        parentContainer.hide();
        return;
    }

    if (emptyResponse.is(':visible')) {
        parentContainer.hide();
        $('#histogram-toggle-btn').removeClass('active');
        return;
    }

    if (!timechartData || !Array.isArray(timechartData.measure) || timechartData.measure.length === 0) {
        histoContainer.hide();
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
        return convertIfTimestamp(item.GroupByValues[0]);
    }).filter(ts => ts !== null);

    let startTime = Math.min(...timestamps.map(ts => new Date(ts).getTime()));
    let endTime = Math.max(...timestamps.map(ts => new Date(ts).getTime()));

    if (!isFinite(startTime) || !isFinite(endTime)) {
        const now = new Date();
        endTime = now.getTime();
        startTime = endTime - 24 * 60 * 60 * 1000;
    }

    const granularity = determineGranularity(timestamps, startTime, endTime);
    HistogramState.currentGranularity = granularity;
    const filteredData = filterDataByRange(dataToRender, startTime, endTime, granularity);

    if (!filteredData) {
        console.log('No data available at this granularity');
        histoContainer.html('<div class="error-message">No data available for the selected range</div>');
        return;
    }

    dataToRender = filteredData;
    timestamps = dataToRender.measure.map(item => convertIfTimestamp(item.GroupByValues[0]));
    const counts = dataToRender.measure.map(item => item.MeasureVal['count(*)'] || 0);

    const formattedTimestamps = timestamps.map(ts => formatTimestampForGranularity(ts, granularity.type));

    if (HistogramState.currentHistogram) {
        HistogramState.currentHistogram.destroy();
        HistogramState.currentHistogram = null;
    }

    histoContainer.empty();
    histoContainer.html('<canvas width="100%" height="100%"></canvas>');

    HistogramState.canvas = histoContainer.find('canvas')[0];
    const ctx = HistogramState.canvas.getContext('2d');
    const fontSize = formattedTimestamps.length > 10 ? 10 : 12;

    const { gridLineColor, tickColor } = typeof getGraphGridColors === 'function'
        ? getGraphGridColors()
        : { gridLineColor: '#e0e0e0', tickColor: '#666' };

    const barCount = formattedTimestamps.length;
    const rotationThreshold = 20;
    const labelSkipThreshold = 50;
    const shouldRotate = barCount > rotationThreshold;
    const skipInterval = barCount > labelSkipThreshold ? Math.ceil(barCount / 10) : 1;

    const barPercentage = formattedTimestamps.length > 40 ? 0.95 : 0.85;
    const categoryPercentage = formattedTimestamps.length > 40 ? 0.95 : 0.85;

    let zoomIndicatorText;
    const { type, interval } = HistogramState.currentGranularity;
    if (interval === 1) {
        zoomIndicatorText = `granularity - ${type}`;
    } else {
        const unit = type === 'week' || type === 'month' ? type : `${type}s`;
        zoomIndicatorText = `granularity - ${interval}${unit}`;
    }
    const xAxisLabel = `Timestamp (${zoomIndicatorText})`;

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
                barPercentage: barPercentage,
                categoryPercentage: categoryPercentage
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                x: {
                    title: {
                        display: true,
                        text: xAxisLabel,
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
                        display: false
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
                    display: false
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
        parentContainer.show();
    }
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

    HistogramState.currentHistogram.update();
}

$(document).ready(function() {
    $('#histogram-toggle-btn').on('click', function() {
        $(this).toggleClass('active');
        $('.histo-container').toggle();
        //eslint-disable-next-line no-undef
        isHistogramViewActive = $(this).hasClass('active');

        //eslint-disable-next-line no-undef
        if (isHistogramViewActive) {
            //eslint-disable-next-line no-undef
            if (timechartComplete) {
                //eslint-disable-next-line no-undef
                renderHistogram(timechartComplete);
            } else {
                const searchFilter = window.getSearchFilter(false, false);
                window.doSearch(searchFilter).catch(error => {
                    console.error('Error running search with histogram:', error);
                    window.showError('Failed to load histogram: ' + error);
                });
            }
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
                    }
                }
            });
        });
        observer.observe(emptyResponse, {
            attributes: true,
            attributeFilter: ['style']
        });
        if ($(emptyResponse).is(':visible')) {
            $('.histo-container').hide();
            $('#histogram-toggle-btn').removeClass('active');
        }
    }
});