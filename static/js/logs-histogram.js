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

function parseRelativeTime(timeStr, now) {
    if (timeStr === 'now') {
        return now.getTime();
    }
    const regex = /^now-(\d+)([smhd])$/;
    const match = timeStr.match(regex);
    if (!match) {
        throw new Error(`Invalid relative time format: ${timeStr}`);
    }
    const value = parseInt(match[1]);
    const unit = match[2];
    let milliseconds;
    switch (unit) {
        case 's':
            milliseconds = value * 1000;
            break;
        case 'm':
            milliseconds = value * 60 * 1000;
            break;
        case 'h':
            milliseconds = value * 60 * 60 * 1000;
            break;
        case 'd':
            milliseconds = value * 24 * 60 * 60 * 1000;
            break;
        default:
            throw new Error(`Unknown time unit: ${unit}`);
    }
    return now.getTime() - milliseconds;
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

function configureHistogramTimeAxis(data, granularityType) {
    const firstTimestamp = new Date(data.measure[0].GroupByValues[0]).getTime();
    const lastTimestamp = new Date(data.measure[data.measure.length - 1].GroupByValues[0]).getTime();
    const duration = lastTimestamp - firstTimestamp;
    
    let unit, maxTicksLimit, stepSize;
    let displayFormats = {};
    let tooltipFormat = 'MMM d, yyyy, h:mm aaa';

    switch (granularityType) {
        case 'month':
            unit = 'month';
            maxTicksLimit = duration <= 366 * 24 * 60 * 60 * 1000 ? 12 : Math.min(12, Math.ceil(duration / (30 * 24 * 60 * 60 * 1000)));
            displayFormats.month = 'MMM yyyy';
            tooltipFormat = 'MMM yyyy';
            break;
        case 'week':
            unit = 'week';
            maxTicksLimit = Math.min(12, Math.ceil(duration / (7 * 24 * 60 * 60 * 1000)));
            displayFormats.week = 'MMM d, yyyy';
            tooltipFormat = 'MMM d, yyyy';
            break;
        case 'day':
            unit = 'day';
            if (duration <= 15 * 24 * 60 * 60 * 1000) {
                maxTicksLimit = Math.min(14, Math.ceil(duration / (24 * 60 * 60 * 1000)));
            } else if (duration <= 90 * 24 * 60 * 60 * 1000) {
                maxTicksLimit = Math.min(12, Math.ceil(duration / (7 * 24 * 60 * 60 * 1000)));
            } else {
                maxTicksLimit = Math.min(15, Math.ceil(duration / (14 * 24 * 60 * 60 * 1000)));
            }
            displayFormats.day = 'MMM d';
            tooltipFormat = 'MMM d, yyyy';
            break;
        case 'hour':
            unit = 'hour';
            const hoursInRange = Math.ceil(duration / (60 * 60 * 1000));
            if (hoursInRange <= 24) {
                stepSize = 1;
                maxTicksLimit = hoursInRange + 1;
            } else if (duration <= 4 * 24 * 60 * 60 * 1000) {
                stepSize = 6;
                maxTicksLimit = Math.ceil(hoursInRange / 6);
            } else if (duration <= 15 * 24 * 60 * 60 * 1000) {
                stepSize = 12;
                maxTicksLimit = Math.ceil(hoursInRange / 12);
            } else {
                stepSize = 24;
                maxTicksLimit = Math.ceil(hoursInRange / 24);
            }
            displayFormats.hour = 'h aaa';
            tooltipFormat = 'MMM d, yyyy, h aaa';
            break;
        case 'minute':
            unit = 'minute';
            const minutesInRange = Math.ceil(duration / (60 * 1000));
            if (minutesInRange <= 60) {
                stepSize = 5;
                maxTicksLimit = Math.ceil(minutesInRange / 5);
            } else {
                stepSize = 15;
                maxTicksLimit = Math.ceil(minutesInRange / 15);
            }
            displayFormats.minute = 'h:mm aaa';
            tooltipFormat = 'MMM d, yyyy, h:mm aaa';
            break;
        case 'second':
            unit = 'second';
            stepSize = 10;
            maxTicksLimit = Math.ceil(duration / (10 * 1000));
            displayFormats.second = 'h:mm:ss aaa';
            tooltipFormat = 'MMM d, yyyy, h:mm:ss aaa';
            break;
        default:
            unit = 'day';
            maxTicksLimit = 12;
            displayFormats.day = 'MMM d';
    }

    const timeOptions = {
        unit: unit,
        displayFormats: displayFormats,
        tooltipFormat: tooltipFormat,
        bounds: 'ticks',
    };

    if (stepSize) {
        timeOptions.stepSize = stepSize;
    }

    const offsetValue = !['hour', 'minute', 'second'].includes(granularityType);
    timeOptions.offset = offsetValue;

    const config = {
        type: 'time',
        time: timeOptions,
        title: {
            display: true,
            text: 'Time Period',
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
                const day = date.getDate();

                switch (granularityType) {
                    case 'month':
                        return `${monthNames[date.getMonth()]} ${date.getFullYear()}`;
                    case 'week':
                        return `${monthNames[date.getMonth()]} ${day}`;
                    case 'day':
                        return `${monthNames[date.getMonth()]} ${day}`;
                    case 'hour':
                        if (hoursInRange <= 24) {
                            const hourIn12 = hours % 12 || 12;
                            const amPm = hours < 12 ? 'AM' : 'PM';
                            if (hours === 0) {
                                return `${monthNames[date.getMonth()]} ${day}`;
                            }
                            return `${hourIn12}${amPm}`;
                        } else if (duration <= 4 * 24 * 60 * 60 * 1000) {
                            if (hours === 0) {
                                return `${monthNames[date.getMonth()]} ${day}`;
                            } else if (hours === 6) {
                                return '6AM';
                            } else if (hours === 12) {
                                return '12PM';
                            } else if (hours === 18) {
                                return '6PM';
                            }
                        } else if (duration <= 15 * 24 * 60 * 60 * 1000) {
                            if (hours === 0) {
                                return `${monthNames[date.getMonth()]} ${day}`;
                            } else if (hours === 12) {
                                return '12PM';
                            }
                        } else {
                            if (hours === 0) {
                                return `${monthNames[date.getMonth()]} ${day}`;
                            }
                        }
                        return null;
                    case 'minute':
                        if (minutes === 0) {
                            const hourIn12 = hours % 12 || 12;
                            const amPm = hours < 12 ? 'AM' : 'PM';
                            return `${hourIn12}${amPm}`;
                        }
                        return null;
                    case 'second':
                        if (date.getSeconds() === 0) {
                            const hourIn12 = hours % 12 || 12;
                            const amPm = hours < 12 ? 'AM' : 'PM';
                            return `${hourIn12}:${minutes < 10 ? '0' + minutes : minutes}${amPm}`;
                        }
                        return null;
                    default:
                        return `${monthNames[date.getMonth()]} ${day}`;
                }
            },
            font: {
                weight: function (context) {
                    const value = context.tick.value;
                    const date = new Date(value);
                    
                    if (granularityType === 'hour' && date.getHours() === 0) {
                        return 'bold';
                    }
                    if (granularityType === 'day' && date.getDate() === 1) {
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

    // Resolve startEpoch and endEpoch to absolute timestamps
    const now = new Date(); // Current time: May 15, 2025, 16:52 IST
    let startTime, endTime;

    try {
        if (typeof timechartData.startEpoch === 'string') {
            startTime = parseRelativeTime(timechartData.startEpoch, now);
        } else {
            startTime = parseInt(timechartData.startEpoch);
        }
        if (typeof timechartData.endEpoch === 'string') {
            endTime = parseRelativeTime(timechartData.endEpoch, now);
        } else {
            endTime = parseInt(timechartData.endEpoch);
        }

        if (isNaN(startTime) || isNaN(endTime)) {
            throw new Error('Invalid startEpoch or endEpoch');
        }
    } catch (e) {
        console.error('Error parsing startEpoch or endEpoch:', e);
        // Fallback to a default range if parsing fails
        endTime = now.getTime();
        startTime = endTime - 24 * 60 * 60 * 1000; // Last 24 hours
    }

    const timestamps = dataToRender.measure.map(item => {
        if (!item.GroupByValues || !item.GroupByValues[0]) {
            console.warn('Missing GroupByValues in measure:', item);
            return null;
        }
        return convertIfTimestamp(item.GroupByValues[0]);
    }).filter(ts => ts !== null);

    const granularity = determineGranularity(timestamps, startTime, endTime);
    HistogramState.currentGranularity = granularity;

    const endDate = new Date(endTime);
    let adjustedEndTime = endTime;
    switch (granularity.type) {
        case 'month':
            adjustedEndTime = new Date(endDate.getFullYear(), endDate.getMonth() + 1, 0, 23, 59, 59, 999).getTime();
            break;
        case 'week':
            const dayOfWeek = endDate.getDay();
            const daysToEndOfWeek = 6 - dayOfWeek;
            adjustedEndTime = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate() + daysToEndOfWeek, 23, 59, 59, 999).getTime();
            break;
        case 'day':
            adjustedEndTime = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate(), 23, 59, 59, 999).getTime();
            break;
        case 'hour':
            adjustedEndTime = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate(), endDate.getHours(), 59, 59, 999).getTime();
            break;
        case 'minute':
            adjustedEndTime = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate(), endDate.getHours(), endDate.getMinutes(), 59, 999).getTime();
            break;
        case 'second':
            break;
        default:
            adjustedEndTime = new Date(endDate.getFullYear(), endDate.getMonth(), endDate.getDate(), 23, 59, 59, 999).getTime();
    }
    endTime = adjustedEndTime;

    const filteredData = filterDataByRange(dataToRender, startTime, endTime, granularity);

    if (!filteredData) {
        console.log('No data available at this granularity');
        histoContainer.html('<div class="error-message">No data available for the selected range</div>');
        return;
    }

    dataToRender = filteredData;
    const updatedTimestamps = dataToRender.measure.map(item => convertIfTimestamp(item.GroupByValues[0]));
    const counts = dataToRender.measure.map(item => item.MeasureVal['count(*)'] || 0);

    const formattedTimestamps = updatedTimestamps.map(ts => formatTimestampForGranularity(ts, granularity.type));

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
                    ...configureHistogramTimeAxis(dataToRender, granularity.type),
                    title: {
                        display: true,
                        text: xAxisLabel,
                        color: tickColor
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
                tooltip: {
                    callbacks: {
                        title: function(context) {
                            const timestamp = context[0].parsed.x;
                            return formatTooltipTimestamp(timestamp / 1000, granularity.type);
                        },
                        label: function(context) {
                            return `${context.dataset.label}: ${context.raw}`;
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

function formatTooltipTimestamp(timestamp, granularity) {
    const date = new Date(timestamp * 1000);
    const monthNames = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
    const month = monthNames[date.getMonth()];
    const day = date.getDate();
    const year = date.getFullYear();
    const hours = date.getHours();
    const minutes = date.getMinutes();
    const ampm = hours >= 12 ? 'PM' : 'AM';
    const hour12 = hours % 12 || 12;

    switch (granularity) {
        case 'month':
            return `${month} ${year}`;
        case 'week':
        case 'day':
            return `${month} ${day}, ${year}`;
        case 'hour':
            return `${month} ${day}, ${year} ${hour12}:00 ${ampm}`;
        case 'minute':
            return `${month} ${day}, ${year} ${hour12}:${minutes < 10 ? '0' + minutes : minutes} ${ampm}`;
        case 'second':
            const seconds = date.getSeconds();
            return `${month} ${day}, ${year} ${hour12}:${minutes < 10 ? '0' + minutes : minutes}:${seconds < 10 ? '0' + seconds : seconds} ${ampm}`;
        default:
            return `${month} ${day}, ${year}`;
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