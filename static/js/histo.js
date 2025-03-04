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
$(document).ready(function () {
    let selectionStart = null;
    let selectionEnd = null;
    let isDragging = false;
    let originalTimestamps = [];
    let originalData = [];
    let isInitialState = true;
    let initialStartEpoch = null;
    let initialEndEpoch = null;
    let lastQueryTime = 0;
    let socket = window.sharedSocket; // Reference to shared socket

    function initializeHistogram() {
        if (!socket || socket.readyState === WebSocket.CLOSED) {
            console.warn('No active WebSocket or socket closed. Attempting to reinitialize.');

            if (window.sharedSocket && window.sharedSocket.readyState === WebSocket.OPEN) {
                socket = window.sharedSocket;
            } else {
                // Fallback: Reopen socket with the last known range (if available) or default
                const startEpoch = initialStartEpoch || moment().subtract(30, 'days').valueOf();
                const endEpoch = initialEndEpoch || moment().valueOf();
                socket = new WebSocket((location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/api/search/ws');
                socket.onopen = function () {
                    console.log('Reopened WebSocket for histogram.');
                    submitTimeRangeQuery(startEpoch, endEpoch); // Initial fetch with fallback range
                };
            }
        }

        socket.onmessage = function (event) {
            const jsonEvent = JSON.parse(event.data);
            console.log('WebSocket message received in histo.js:', jsonEvent.state, JSON.stringify(jsonEvent, null, 2));

            // Capture initial startEpoch and endEpoch from the first message
            if (!initialStartEpoch || !initialEndEpoch) {
                initialStartEpoch = jsonEvent.startEpoch || moment().subtract(30, 'days').valueOf();
                initialEndEpoch = jsonEvent.endEpoch || moment().valueOf();
                console.log('Captured initial range in histo.js:', initialStartEpoch, initialEndEpoch);
                if (originalData.length === 0 && jsonEvent.state === 'QUERY_UPDATE') {
                    const updateBuckets = jsonEvent.timechartUpdate?.measure;
                    if (updateBuckets) {
                        const buckets = updateBuckets.map(item => ({
                            timestamp: parseInt(item.GroupByValues[0], 10),
                            count: item.MeasureVal["count(*)"]
                        }));
                        updateHistogram(buckets);
                        updateZoomInfo(buckets[0].timestamp, buckets[buckets.length - 1].timestamp, determineOptimalBucketSize(buckets[0].timestamp, buckets[buckets.length - 1].timestamp));
                    }
                }
            }

            switch (jsonEvent.state) {
                case 'QUERY_UPDATE':
                    const updateBuckets = jsonEvent.timechartUpdate?.measure;
                    if (updateBuckets) {
                        const buckets = updateBuckets.map(item => ({
                            timestamp: parseInt(item.GroupByValues[0], 10),
                            count: item.MeasureVal["count(*)"]
                        }));
                        isInitialState = false;
                        updateHistogram(buckets);
                        updateZoomInfo(buckets[0].timestamp, buckets[buckets.length - 1].timestamp, determineOptimalBucketSize(buckets[0].timestamp, buckets[buckets.length - 1].timestamp));
                    } else {
                        console.warn('No measure data in QUERY_UPDATE timechartUpdate');
                    }
                    break;
                case 'COMPLETE':
                    const completeBuckets = jsonEvent.timechartComplete?.measure;
                    if (completeBuckets) {
                        const buckets = completeBuckets.map(item => ({
                            timestamp: parseInt(item.GroupByValues[0], 10),
                            count: item.MeasureVal["count(*)"]
                        }));
                        if (originalTimestamps.length === 0) isInitialState = true;
                        else isInitialState = false;
                        updateHistogram(buckets);
                        originalTimestamps = buckets.map(b => b.timestamp);
                        originalData = buckets.map(b => b.count);
                        updateZoomInfo(buckets[0].timestamp, buckets[buckets.length - 1].timestamp, determineOptimalBucketSize(buckets[0].timestamp, buckets[buckets.length - 1].timestamp));
                    } else {
                        console.warn('No measure data in COMPLETE timechartComplete');
                    }
                    break;
                case 'ERROR':
                    console.error(`Server error: ${jsonEvent.message || 'Unknown error'}`);
                    break;
                case 'TIMEOUT':
                    console.warn('Query timed out');
                    break;
                case 'CANCELLED':
                    console.warn('Query cancelled');
                    break;
                default:
                    console.log(`Unhandled state: ${jsonEvent.state}`);
            }
        };

        socket.onclose = function (event) {
            console.log(`WebSocket closed in histo.js, code=${event.code}, reason=${event.reason}`);
            // Attempt to reinitialize if search.js might reopen
            socket = null; // Reset socket reference
            setTimeout(initializeHistogram, 1000); // Retry after 1 second
        };

        socket.onerror = function (error) {
            console.error('WebSocket error in histo.js:', error);
        };
    }

    $('#toggle-btn').on('click', function () {
        const $histoContainer = $('.histo-container');
        const isVisible = $histoContainer.hasClass('visible');

        if (!isVisible) {
            $histoContainer.addClass('visible');
            initializeHistogram(); // Initialize or reinitialize with shared socket
            if (originalData.length > 0) {
                updateHistogram(originalData.map((count, i) => ({ timestamp: originalTimestamps[i], count })));
            } else if (initialStartEpoch && initialEndEpoch) {
                submitTimeRangeQuery(initialStartEpoch, initialEndEpoch);
            }
        } else {
            $histoContainer.removeClass('visible');
        }
    });


    function throttle(func, limit) {
        let inThrottle;
        return function (...args) {
            if (!inThrottle) {
                func.apply(this, args);
                inThrottle = true;
                setTimeout(() => inThrottle = false, limit);
            }
        };
    }

    $(document).on('mousedown', '#histogram', function (e) {
        const chart = window.myChart;
        if (!chart) return;
        const rect = this.getBoundingClientRect();
        const x = e.clientX - rect.left;
        const xValue = chart.scales.x.getValueForPixel(x);
        if (xValue) {
            selectionStart = xValue;
            isDragging = true;
            updateSelectionArea(chart, selectionStart, selectionStart);
        }
    });

    $('#histogram').on('mousemove', throttle(function (e) {
        const chart = window.myChart;
        if (!isDragging || !chart) return;
        const rect = this.getBoundingClientRect();
        const x = Math.max(rect.left, Math.min(e.clientX, rect.right)) - rect.left;
        const xValue = chart.scales.x.getValueForPixel(x);
        if (xValue) {
            selectionEnd = xValue;
            updateSelectionArea(chart, selectionStart, selectionEnd);
        }
    }, 60));

    $(document).on('mouseup', function () {
        if (isDragging && selectionStart && selectionEnd) {
            const start = Math.min(selectionStart, selectionEnd);
            const end = Math.max(selectionStart, selectionEnd);
            const now = Date.now();
            if (now - lastQueryTime > 1000) { // Debounce: 1 second minimum between queries
                submitTimeRangeQuery(start, end);
                lastQueryTime = now;
            }
        }
        isDragging = false;
    });

    $(document).on('dblclick', '#histogram', function () {
        const chart = window.myChart;
        if (!chart) return;

        chart.options.plugins.annotation.annotations = {};
        chart.update();

        if (originalTimestamps.length > 0) {
            submitTimeRangeQuery(originalTimestamps[0], originalTimestamps[originalTimestamps.length - 1]);
        } else if (initialStartEpoch && initialEndEpoch) {
            submitTimeRangeQuery(initialStartEpoch, initialEndEpoch);
        } else {
            submitTimeRangeQuery(moment().subtract(30, 'days').valueOf(), moment().valueOf());
        }
        selectionStart = null;
        selectionEnd = null;
        $('#zoom-info').remove();
        isInitialState = true;
    });

    function submitTimeRangeQuery(startTime, endTime) {
        if (!socket || socket.readyState !== WebSocket.OPEN) {
            console.warn('WebSocket not open, skipping query');
            return;
        }

        const bucketSize = determineOptimalBucketSize(startTime, endTime);
        const queryData = {
            searchText: "*",
            startEpoch: startTime,
            endEpoch: endTime,
            runTimechart: true,
            queryLanguage: "Splunk QL",
            state: "query",
            bucketSize: bucketSize
        };
        socket.send(JSON.stringify(queryData));
        updateZoomInfo(startTime, endTime, bucketSize, true);
    }

    function updateHistogram(buckets) {
        if (!buckets || buckets.length === 0) {
            console.warn("No buckets to render.");
            return;
        }

        const ctx = document.getElementById('histogram')?.getContext('2d');
        if (!ctx) {
            console.error("Canvas context not found.");
            return;
        }

        const timeFormat = getTimeFormatForRange(buckets[0].timestamp, buckets[buckets.length - 1].timestamp);
        const labels = formatDynamicLabels(buckets, timeFormat);
        const data = buckets.map(bucket => bucket.count);
        const tooltipTimestamps = buckets.map(bucket => moment(bucket.timestamp).format(timeFormat.tooltipFormat));

        if (window.myChart) {
            window.myChart.data.labels = labels;
            window.myChart.data.datasets[0].data = data;
            window.myChart.fullTimestamps = tooltipTimestamps;
            window.myChart.options.scales.x.title.text = `Time (${timeFormat.granularity})`;
            window.myChart.update();
            return;
        }

        window.myChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: labels,
                datasets: [{
                    label: 'Log Count',
                    data: data,
                    backgroundColor: 'rgba(75, 192, 192, 0.7)',
                    borderColor: 'rgba(75, 192, 192, 1)',
                    borderWidth: 1
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                scales: {
                    x: {
                        title: { display: true, text: `Time (${timeFormat.granularity})` },
                        grid: { color: 'rgba(200, 200, 200, 0.3)' }
                    },
                    y: {
                        beginAtZero: true,
                        title: { display: true, text: 'Count of Logs' },
                        grid: { color: 'rgba(200, 200, 200, 0.3)' }
                    }
                },
                plugins: {
                    legend: { display: true, position: 'top' },
                    tooltip: {
                        callbacks: {
                            label: context => `${context.parsed.y} logs`,
                            title: tooltipItems => window.myChart.fullTimestamps[tooltipItems[0].dataIndex]
                        }
                    },
                    annotation: { annotations: {} }
                }
            }
        });
        window.myChart.fullTimestamps = tooltipTimestamps;
    }

    function updateSelectionArea(chart, start, end) {
        chart.options.plugins.annotation.annotations = {
            selection: {
                type: 'box',
                xMin: Math.min(start, end),
                xMax: Math.max(start, end),
                yMin: 0,
                yMax: 'max',
                backgroundColor: 'rgba(173, 216, 230, 0.3)',
                borderColor: 'rgba(0, 123, 255, 0.8)',
                borderWidth: 1
            }
        };
        chart.update('none');
    }

    function getTimeFormatForRange(startTime, endTime) {
        const rangeMs = endTime - startTime;
        const rangeDays = rangeMs / (1000 * 60 * 60 * 24);
        const rangeHours = rangeMs / (1000 * 60 * 60);

        if (isInitialState) {
            if (rangeDays > 60) {
                return { format: 'MMM Do', tooltipFormat: 'YYYY-MM-DD', granularity: 'week', intervalDays: 7 };
            } else if (rangeDays > 14) {
                return { format: 'MMM Do', tooltipFormat: 'YYYY-MM-DD', granularity: 'day', intervalDays: 4 };
            } else {
                return { format: 'MMM Do', tooltipFormat: 'YYYY-MM-DD', granularity: 'day', intervalDays: 1 };
            }
        } else if (rangeHours <= 2) {
            return { format: 'HH:mm:ss', tooltipFormat: 'YYYY-MM-DD HH:mm:ss', granularity: 'second' };
        } else if (rangeHours <= 24) {
            return { format: 'HH', tooltipFormat: 'YYYY-MM-DD HH:mm', granularity: 'hour' };
        } else if (rangeHours <= 168) {
            return { format: 'MMM Do HH:mm', tooltipFormat: 'YYYY-MM-DD HH:mm', granularity: 'hour' };
        } else {
            return { format: 'MMM Do', tooltipFormat: 'YYYY-MM-DD', granularity: 'day' };
        }
    }

    function formatDynamicLabels(buckets, timeFormat) {
        if (!isInitialState) {
            const labels = [];
            let lastDay = null;
            buckets.forEach(bucket => {
                const m = moment(bucket.timestamp);
                const currentDay = m.format('YYYY-MM-DD');
                if (timeFormat.granularity === 'hour') {
                    if (lastDay === null || currentDay !== lastDay) {
                        labels.push(m.format('MMM Do HH'));
                        lastDay = currentDay;
                    } else {
                        labels.push(m.format('HH'));
                    }
                } else if (timeFormat.granularity === 'second') {
                    if (lastDay === null || currentDay !== lastDay) {
                        labels.push(m.format('MMM Do HH:mm:ss'));
                        lastDay = currentDay;
                    } else {
                        labels.push(m.format('HH:mm:ss'));
                    }
                } else {
                    labels.push(m.format(timeFormat.format));
                }
            });
            return labels;
        }

        const labels = [];
        const intervalDays = timeFormat.intervalDays || 1;
        let nextLabelTime = moment(buckets[0].timestamp).startOf('day');
        buckets.forEach(bucket => {
            const m = moment(bucket.timestamp);
            if (m.isSameOrAfter(nextLabelTime, 'day')) {
                labels.push(m.format('MMM Do'));
                nextLabelTime.add(intervalDays, 'days');
            } else {
                labels.push('');
            }
        });
        return labels;
    }

    function determineOptimalBucketSize(startTime, endTime) {
        const rangeMs = endTime - startTime;
        const rangeHours = rangeMs / (1000 * 60 * 60);

        if (rangeHours <= 2) return '1s';
        else if (rangeHours <= 24) return '1m';
        else if (rangeHours <= 168) return '1h';
        else return '1d';
    }

    function updateZoomInfo(startTime, endTime, granularity, loading = false) {
        let $zoomInfo = $('#zoom-info');
        if ($zoomInfo.length === 0) {
            $zoomInfo = $('<div id="zoom-info" style="text-align: center; margin-top: 5px; font-size: 12px;"></div>');
            $('.my-chart-container').after($zoomInfo);
        }
        const start = moment(startTime).format('YYYY-MM-DD HH:mm:ss');
        const end = moment(endTime).format('YYYY-MM-DD HH:mm:ss');
        $zoomInfo.html(`<strong>Range:</strong> ${start} to ${end}<br><strong>Granularity:</strong> ${granularity}${loading ? ' (Loading...)' : ''}`);
    }
});