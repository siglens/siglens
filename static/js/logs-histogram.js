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

function formatAxisTitle(intervalMs, unit) {
    function msToReadable(ms) {
        const seconds = ms / 1000;
        const minutes = seconds / 60;
        const hours = minutes / 60;
        const days = hours / 24;
        const months = days / 30; 

        if (months >= 1) {
            const value = Math.round(months * 10) / 10;
            return `${value} month${value !== 1 ? 's' : ''}`;
        } else if (days >= 1) {
            const value = Math.round(days * 10) / 10;
            return `${value} day${value !== 1 ? 's' : ''}`;
        } else if (hours >= 1) {
            const value = Math.round(hours * 10) / 10;
            return `${value} hour${value !== 1 ? 's' : ''}`;
        } else if (minutes >= 1) {
            const value = Math.round(minutes * 10) / 10;
            return `${value} minute${value !== 1 ? 's' : ''}`;
        } else {
            const value = Math.round(seconds * 10) / 10;
            return `${value} second${value !== 1 ? 's' : ''}`;
        }
    }

    const readableInterval = msToReadable(intervalMs);
    const capitalizedUnit = unit.charAt(0).toUpperCase() + unit.slice(1);

    return `Timestamp (Interval: ${readableInterval}, Unit: ${capitalizedUnit})`;
}

function configureTimeAxis(startTime, endTime, intervalMs, granularity, maxBars) {
    const durationMs = endTime - startTime;
    const daysInRange = Math.ceil(durationMs / (1000 * 60 * 60 * 24));
    const hoursInRange = Math.ceil(durationMs / (1000 * 60 * 60));

    let unit, maxTicksLimit, stepSize;

    let adjustedStartTime = startTime;
    let adjustedEndTime = endTime;
    
    if (granularity === 'second') {
        const startDate = new Date(startTime);
        const startSeconds = startDate.getSeconds();
        const startAdjustment = startSeconds % 30 === 0 ? 0 : (30 - (startSeconds % 30));
        adjustedStartTime = startTime + startAdjustment * 1000;

        const endDate = new Date(endTime);
        const endSeconds = endDate.getSeconds();
        const endAdjustment = endSeconds % 30 === 0 ? 0 : (30 - (endSeconds % 30));
        adjustedEndTime = endTime + endAdjustment * 1000;
    }


    const adjustedDurationMs = adjustedEndTime - adjustedStartTime;

    // Set tick intervals based on granularity and time range
    if (granularity === 'second') {
        unit = 'second';
        if (adjustedDurationMs <= 5 * 60 * 1000) {
            // 5 min range: 30-sec ticks
            stepSize = 30; // 30 seconds
            maxTicksLimit = Math.ceil(adjustedDurationMs / (30 * 1000)) + 1;
            maxTicksLimit = Math.min(maxTicksLimit, 11); // Cap at 11 ticks
        } else if (adjustedDurationMs <= 15 * 60 * 1000) {
            // 15 min range: 60-sec (1-min) ticks
            unit = 'minute';
            stepSize = 1; // 1 minute
            maxTicksLimit = Math.ceil(adjustedDurationMs / (60 * 1000)) + 1;
        } else {
            unit = 'minute';
            stepSize = 2; // 2 minutes
            maxTicksLimit = Math.ceil(adjustedDurationMs / (2 * 60 * 1000)) + 1;
        }
        maxTicksLimit = Math.min(maxBars, maxTicksLimit);
    } else if (granularity === 'minute') {
        unit = 'minute';
        if (durationMs <= 60 * 60 * 1000 && intervalMs === 60 * 1000) {
            // 1 hr range: 5-min ticks
            stepSize = 5; // 5 minutes
            maxTicksLimit = Math.ceil(durationMs / (5 * 60 * 1000)) + 1;
        } else if (durationMs <= 4 * 60 * 60 * 1000 && intervalMs === 5 * 60 * 1000) {
            // ≤ 4 hr range: 15-min ticks
            stepSize = 15; // 15 minutes
            maxTicksLimit = Math.ceil(durationMs / (15 * 60 * 1000)) + 1;
        } else if (durationMs <= 24 * 60 * 60 * 1000 && intervalMs === 30 * 60 * 1000) {
            // ≤ 1 day range: 2-hr ticks
            unit = 'hour';
            stepSize = 2; // 2 hours
            maxTicksLimit = Math.ceil(durationMs / (2 * 60 * 60 * 1000)) + 1;
        } else {
            stepSize = Math.max(1, Math.ceil(intervalMs / (60 * 1000)));
            maxTicksLimit = Math.ceil(durationMs / (stepSize * 60 * 1000)) + 1;
        }
        maxTicksLimit = Math.min(maxBars, maxTicksLimit);
    } else if (granularity === 'hour') {
        unit = 'hour';
        stepSize = Math.max(1, Math.ceil(intervalMs / (60 * 60 * 1000)));
        maxTicksLimit = Math.min(maxBars, Math.ceil(durationMs / (stepSize * 60 * 60 * 1000)) + 1);
    } else if (granularity === 'day') {
        unit = 'day';
        stepSize = Math.max(1, Math.ceil(intervalMs / (24 * 60 * 60 * 1000)));
        maxTicksLimit = Math.min(maxBars, Math.ceil(durationMs / (stepSize * 24 * 60 * 60 * 1000)) + 1);
    } else {
        unit = 'month';
        stepSize = Math.max(1, Math.ceil(intervalMs / (30 * 24 * 60 * 60 * 1000)));
        maxTicksLimit = Math.min(maxBars, Math.ceil(durationMs / (stepSize * 30 * 24 * 60 * 60 * 1000)) + 1);
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

    let offsetValue = granularity !== 'second' && granularity !== 'minute' && granularity !== 'hour';
    
    if (granularity === 'second') {
        timeOptions.round = 'second';
        timeOptions.min = adjustedStartTime;
        timeOptions.max = adjustedEndTime;
        timeOptions.stepSize = stepSize;
    } else if (granularity === 'minute') {
        timeOptions.round = 'minute';
        timeOptions.stepSize = stepSize;
    } else if (granularity === 'hour') {
        timeOptions.round = 'hour';
        timeOptions.stepSize = stepSize;
    } else if (granularity === 'day') {
        timeOptions.round = 'day';
        timeOptions.stepSize = stepSize;
    } else {
        timeOptions.round = 'month';
        timeOptions.stepSize = stepSize;
    }

    timeOptions.offset = offsetValue;

    let paddingMs;
    switch (granularity) {
        case 'second':
            paddingMs = Math.min(30 * 1000, durationMs * 0.05); // 30 seconds or 5% of duration
            break;
        case 'minute':
            paddingMs = Math.min(5 * 60 * 1000, durationMs * 0.05); // 5 minutes or 5% of duration
            break;
        case 'hour':
            paddingMs = Math.min(30 * 60 * 1000, durationMs * 0.05); // 30 minutes or 5% of duration
            break;
        case 'day':
            paddingMs = Math.min(12 * 60 * 60 * 1000, durationMs * 0.05); // 12 hours or 5% of duration
            break;
        case 'month':
            paddingMs = Math.min(5 * 24 * 60 * 60 * 1000, durationMs * 0.05); // 5 days or 5% of duration
            break;
        default:
            paddingMs = durationMs * 0.05; // 5% of duration
    }

    const startDate = new Date(startTime);
    const endDate = new Date(endTime);
    const spansDifferentDays = startDate.getDate() !== endDate.getDate() || 
                              startDate.getMonth() !== endDate.getMonth() || 
                              startDate.getFullYear() !== endDate.getFullYear();
    
    const spansDifferentHours = startDate.getHours() !== endDate.getHours() || spansDifferentDays;
    const axisTitle = formatAxisTitle(intervalMs, unit);

    const config = {
        type: 'time',
        time: timeOptions,
        min: (granularity === 'second' ? adjustedStartTime : startTime) - paddingMs,
        max: (granularity === 'second' ? adjustedEndTime : endTime) + paddingMs,
        title: {
            display: true,
            text: axisTitle,
        },
        ticks: {
            maxRotation: 45, 
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

                const formatTime = (h, m, s, showHours = true) => {
                    const hourIn12 = h % 12 || 12;
                    const amPm = h < 12 ? 'AM' : 'PM';
                    const minuteStr = ('0' + m).slice(-2);
                    const secondStr = ('0' + s).slice(-2);
                    
                    if (showHours) {
                        return `${hourIn12}:${minuteStr}:${secondStr} ${amPm}`;
                    } else {
                        return `${minuteStr}:${secondStr}`;
                    }
                };

                const formatDate = (m, d) => `${monthNames[m]} ${d}`;

                if (granularity === 'second') {
                    if (adjustedDurationMs <= 5 * 60 * 1000) {
                        // For 30-sec ticks
                        if (seconds % 30 === 0) {
                            if (seconds === 0) {
                                if (spansDifferentDays) {
                                    return `${formatDate(date.getMonth(), day)} ${formatTime(hours, minutes, seconds)}`;
                                } else if (spansDifferentHours) {
                                    return formatTime(hours, minutes, seconds);
                                } else {
                                    return formatTime(hours, minutes, seconds, false);
                                }
                            } else {
                                if (spansDifferentHours) {
                                    return formatTime(hours, minutes, seconds);
                                } else {
                                    return formatTime(hours, minutes, seconds, false);
                                }
                            }
                        }
                    } else if (adjustedDurationMs <= 15 * 60 * 1000) {
                        // For 60-sec ticks
                        if (seconds % 60 === 0) {
                            if (seconds === 0) {
                                if (spansDifferentDays) {
                                    return `${formatDate(date.getMonth(), day)} ${formatTime(hours, minutes, seconds)}`;
                                } else if (spansDifferentHours) {
                                    return formatTime(hours, minutes, seconds);
                                } else {
                                    return formatTime(hours, minutes, seconds, false);
                                }
                            } else {
                                if (spansDifferentHours) {
                                    return formatTime(hours, minutes, seconds);
                                } else {
                                    return formatTime(hours, minutes, seconds, false);
                                }
                            }
                        }
                    }
                    return null;
                } else if (granularity === 'minute') {
                    if (durationMs <= 60 * 60 * 1000) {
                        // 5-min ticks for ≤ 1 hr
                        if (minutes % 5 === 0) {
                            if (spansDifferentDays && hours === 0) {
                                return `${formatDate(date.getMonth(), day)} ${formatTime(hours, minutes, 0, true).slice(0, -3)}`;
                            } else {
                                return formatTime(hours, minutes, 0, true).slice(0, -3);
                            }
                        }
                    } else if (durationMs <= 4 * 60 * 60 * 1000) {
                        // 15-min ticks for ≤ 4 hr
                        if (minutes % 15 === 0) {
                            if (spansDifferentDays && hours === 0) {
                                return `${formatDate(date.getMonth(), day)} ${formatTime(hours, minutes, 0, true).slice(0, -3)}`;
                            } else {
                                return formatTime(hours, minutes, 0, true).slice(0, -3);
                            }
                        }
                    } else if (durationMs <= 24 * 60 * 60 * 1000) {
                        // 2-hr ticks for ≤ 1 day
                        if (hours % 2 === 0 && minutes === 0) {
                            if (hours === 0) {
                                return formatDate(date.getMonth(), day);
                            } else {
                                return `${hours % 12 || 12}:00 ${hours < 12 ? 'AM' : 'PM'}`;
                            }
                        }
                    }
                    return null;
                } else if (granularity === 'hour') {
                    if (hoursInRange <= 24) {
                        const hourIn12 = hours % 12 || 12;
                        const amPm = hours < 12 ? 'AM' : 'PM';
                        if (hours === 0) {
                            return formatDate(date.getMonth(), day);
                        } else {
                            return `${hourIn12}${amPm}`;
                        }
                    } else if (daysInRange <= 4) {
                        if (hours === 0) {
                            return `${monthNames[date.getMonth()]} ${day}`;
                        } else if (hours === 6) {
                            return '6AM';
                        } else if (hours === 12) {
                            return '12PM';
                        } else if (hours === 18) {
                            return '6PM';
                        }
                    } else {
                        if (hours === 0) {
                            return formatDate(date.getMonth(), day);
                        } else if (hours === 12) {
                            return '12PM';
                        }
                    }
                    return null;
                } else if (granularity === 'day') {
                    return formatDate(date.getMonth(), day);
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
                    if (
                        (granularity === 'second' && minutes === 0) ||
                        (granularity === 'minute' && hours === 0) ||
                        (granularity === 'hour' && hours === 0) 
                    ) {
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

    addQSParm('startEpoch', data.startEpoch);
    addQSParm('endEpoch', data.endEpoch);
    window.history.pushState({ path: myUrl }, '', myUrl);

    datePickerHandler(data.startEpoch, data.endEpoch, 'custom');
    loadCustomDateTimeFromEpoch(data.startEpoch, data.endEpoch);

    filterStartDate = data.startEpoch;
    filterEndDate = data.endEpoch;
    displayStart = new Date(data.startEpoch);
    displayEnd = new Date(data.endEpoch);

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
            triggerZoomSearch(HistogramState.originalStartTime, HistogramState.originalEndTime);
        }
    };

    const eventTypes = ['dblclick'];
    eventTypes.forEach(eventType => {
        if (HistogramState.eventListeners[eventType]) {
            HistogramState.canvas.removeEventListener(eventType, HistogramState.eventListeners[eventType]);
        }
    });

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
        }
    }
});