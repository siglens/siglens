
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
    let reconnectAttempts = 0;
    const MAX_RECONNECT_ATTEMPTS = 5;
    let reconnectTimeout = null;
    let isQueryComplete = false; // Flag to track completion
    let baselineMaxCount = null; // Store initial maximum count, validated

    function wsURL(path) {
        var protocol = location.protocol === 'https:' ? 'wss://' : 'ws://';
        var url = protocol + location.host;
        return url + path;
    }

    function parseTimeRange() {
        const queryParams = new URLSearchParams(window.location.search);
        let stDate = queryParams.get('startEpoch') || Cookies.get('startEpoch');
        let endDate = queryParams.get('endEpoch') || Cookies.get('endEpoch');

        const now = moment().valueOf();

        if (!stDate || !endDate) {
            stDate = moment().subtract(30, 'days').valueOf();
            endDate = now;
        } else {
            stDate = typeof stDate === 'string' ? parseInt(stDate) : stDate;
            endDate = typeof endDate === 'string' ? parseInt(endDate) : endDate;

            if (typeof stDate === 'string' && stDate.match(/^now-\d+[hmhdw]$/)) {
                const match = stDate.match(/now-(\d+)([hmhdw])/);
                if (match) {
                    const value = parseInt(match[1]);
                    const unit = match[2];
                    stDate = moment().subtract(value, unit).valueOf();
                    endDate = now;
                }
            } else if (typeof endDate === 'string' && endDate.match(/^now-\d+[hmhdw]$/)) {
                const match = endDate.match(/now-(\d+)([hmhdw])/);
                if (match) {
                    const value = parseInt(match[1]);
                    const unit = match[2];
                    endDate = moment().subtract(value, unit).valueOf();
                    stDate = now;
                }
            }

            const customFrom = $('#custom-search-from').val();
            const customTo = $('#custom-search-to').val();
            if (customFrom && customTo) {
                stDate = moment(customFrom, 'DD-MM-YYYY HH:mm:ss').valueOf();
                endDate = moment(customTo, 'DD-MM-YYYY HH:mm:ss').valueOf();
            } else if (isNaN(stDate) || isNaN(endDate)) {
                stDate = moment().subtract(30, 'days').valueOf();
                endDate = now;
            }
        }

        return { startEpoch: stDate, endDate: endDate };
    }

    function initializeHistogram() {
        if (!socket || socket.readyState === WebSocket.CLOSED) {
            console.warn('No active WebSocket or socket closed. Attempting to reinitialize.');

            if (window.sharedSocket && window.sharedSocket.readyState === WebSocket.OPEN) {
                socket = window.sharedSocket;
            } else if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS && !isQueryComplete) {
                reconnectAttempts++;
                const { startEpoch, endDate } = parseTimeRange();
                socket = new WebSocket(wsURL('/api/search/ws'));
                socket.onopen = function () {
                    console.log('Reopened WebSocket for histogram.');
                    submitTimeRangeQuery(startEpoch, endDate);
                    reconnectAttempts = 0; // Reset on successful connection
                };
            } else {
                console.error('Max reconnection attempts reached or query complete. Please refresh the page.');
                return;
            }
        }

        socket.onmessage = function (event) {
            const jsonEvent = JSON.parse(event.data);
            console.log('WebSocket message received in histo.js:', jsonEvent.state, JSON.stringify(jsonEvent, null, 2));

            if (!initialStartEpoch || !initialEndEpoch) {
                initialStartEpoch = jsonEvent.startEpoch || moment().subtract(30, 'days').valueOf();
                initialEndEpoch = jsonEvent.endEpoch || moment().valueOf();
                console.log('Captured initial range in histo.js:', initialStartEpoch, initialEndEpoch);
            }

            switch (jsonEvent.state) {
                case 'RUNNING':
                    console.log('Query is running, progress tracking...');
                    break;

                case 'QUERY_UPDATE': {
                    const updateBuckets = jsonEvent.timechartUpdate?.measure;
                    if (updateBuckets && Array.isArray(updateBuckets)) {
                        const buckets = updateBuckets.map(item => ({
                            timestamp: parseInt(item.GroupByValues[0], 10),
                            count: item.MeasureVal["count(*)"] || 0
                        }));
                        if (buckets.length > 0 && !baselineMaxCount) {
                            const initialMax = Math.max(...buckets.map(b => b.count || 0));
                            if (initialMax <= 200) { // Reasonable upper limit
                                baselineMaxCount = initialMax;
                                console.log('Initialized baselineMaxCount:', baselineMaxCount);
                            } else {
                                console.warn('Initial max count too high, skipping baseline:', initialMax);
                            }
                        }
                        if (buckets.length > 0) {
                            isInitialState = false;
                            updateHistogram(buckets);
                            updateZoomInfo(buckets[0].timestamp, buckets[buckets.length - 1].timestamp, determineOptimalBucketSize(buckets[0].timestamp, buckets[buckets.length - 1].timestamp));
                        } else {
                            console.warn('Empty buckets in QUERY_UPDATE');
                        }
                    } else {
                        console.warn('Invalid or missing timechart update data:', jsonEvent.timechartUpdate);
                    }
                    break;
                }

                case 'COMPLETE': {
                    const completeBuckets = jsonEvent.timechartComplete?.measure;
                    if (completeBuckets && Array.isArray(completeBuckets)) {
                        const buckets = completeBuckets.map(item => ({
                            timestamp: parseInt(item.GroupByValues[0], 10),
                            count: item.MeasureVal["count(*)"] || 0
                        }));
                        if (buckets.length > 0) {
                            if (!baselineMaxCount) {
                                const initialMax = Math.max(...buckets.map(b => b.count || 0));
                                if (initialMax <= 200) {
                                    baselineMaxCount = initialMax;
                                    console.log('Set baselineMaxCount from COMPLETE:', baselineMaxCount);
                                } else {
                                    console.warn('Initial max count too high, skipping baseline:', initialMax);
                                }
                            }
                            isInitialState = false;
                            updateHistogram(buckets);
                            originalTimestamps = buckets.map(b => b.timestamp);
                            originalData = buckets.map(b => b.count);
                            updateZoomInfo(buckets[0].timestamp, buckets[buckets.length - 1].timestamp, determineOptimalBucketSize(buckets[0].timestamp, buckets[buckets.length - 1].timestamp));
                        } else {
                            console.warn('Empty buckets in COMPLETE');
                        }
                    } else {
                        console.warn('Invalid or missing timechart complete data:', jsonEvent.timechartComplete);
                    }
                    isQueryComplete = true; // Mark query as complete
                    break;
                }

                case 'ERROR':
                    console.error('WebSocket Error:', jsonEvent.message);
                    break;

                default:
                    console.warn(`Unhandled WebSocket state: ${jsonEvent.state}`);
            }
        };

        socket.onclose = function (event) {
            console.log(`WebSocket closed, code=${event.code}, reason=${event.reason}`);
            socket = null;
            if (window.sharedSocket && window.sharedSocket.readyState === WebSocket.OPEN) {
                socket = window.sharedSocket;
            } else if (reconnectAttempts < MAX_RECONNECT_ATTEMPTS && !isQueryComplete) {
                reconnectTimeout = setTimeout(() => {
                    console.log('Attempting to reconnect...');
                    initializeHistogram();
                }, 2000);
            } else {
                console.error('Max reconnection attempts reached or query complete. Please refresh the page.');
            }
            return () => clearTimeout(reconnectTimeout); // Cleanup function
        };

        socket.onerror = function (error) {
            console.error('WebSocket error in histo.js:', error);
            if (reconnectTimeout) clearTimeout(reconnectTimeout); // Cleanup on error
        };
    }

    $('#histo-toggle-btn').on('click', function () {
        const $histoContainer = $('.histo-container');
        const isVisible = $histoContainer.hasClass('visible');

        if (!isVisible) {
            $histoContainer.addClass('visible');
            initializeHistogram();
            if (originalData.length > 0) {
                updateHistogram(originalData.map((count, i) => ({ timestamp: originalTimestamps[i], count })));
            } else if (initialStartEpoch && initialEndEpoch) {
                submitTimeRangeQuery(initialStartEpoch, initialEndEpoch);
            }
        } else {
            $histoContainer.removeClass('visible');
            isQueryComplete = false; // Reset for next toggle
            if (socket) {
                socket.close();
                socket = null;
            }
            if (window.myChart) {
                window.myChart.destroy();
                window.myChart = null;
            }
            baselineMaxCount = null; // Reset baseline for next query
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
            const end = Math.max(start, selectionEnd); // Corrected to use start
            const now = Date.now();
            if (now - lastQueryTime > 1000) {
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
        console.log('Submitting Query:', queryData);
        socket.send(JSON.stringify(queryData));
        updateZoomInfo(startTime, endTime, bucketSize, true);
    }

    function updateHistogram(buckets) {
        if (!buckets || buckets.length === 0) {
            console.warn("No buckets to render.");
            if (window.myChart) {
                window.myChart.data.datasets[0].data = [];
                window.myChart.update();
            }
            return;
        }

        const ctx = document.getElementById('histogram')?.getContext('2d');
        if (!ctx) {
            console.error("Canvas context not found. Ensure <canvas id='histogram'> is present and visible.");
            return;
        }

        // Filter outliers and calculate effective max count
        const counts = buckets.map(bucket => bucket.count || 0);
        console.log('Raw Buckets:', buckets); // Debug raw buckets
        const validCounts = counts.filter(count => count <= 200); // Filter out extreme outliers
        const currentMaxCount = validCounts.length > 0 ? Math.max(...validCounts) : 0;
        if (!baselineMaxCount && currentMaxCount > 0 && currentMaxCount <= 200) {
            baselineMaxCount = currentMaxCount;
            console.log('Initialized baselineMaxCount:', baselineMaxCount);
        }
        const effectiveMaxCount = baselineMaxCount !== null ? Math.max(baselineMaxCount, currentMaxCount) : currentMaxCount;
        console.log('Bucket Counts:', counts, 'Valid Counts:', validCounts, 'Current Max Count:', currentMaxCount, 'Effective Max Count:', effectiveMaxCount);

        let yMin = 0;
        let yMax, tickStep;
        if (effectiveMaxCount <= 10) {
            yMax = 10;
            tickStep = 1;
        } else if (effectiveMaxCount <= 20) {
            yMax = 20;
            tickStep = 2;
        } else if (effectiveMaxCount <= 50) {
            yMax = 50;
            tickStep = 5;
        } else if (effectiveMaxCount <= 100) {
            yMax = 100;
            tickStep = 10;
        } else if (effectiveMaxCount <= 200) {
            yMax = 200;
            tickStep = 20;
        } else {
            yMax = Math.ceil(effectiveMaxCount / 100) * 100;
            tickStep = Math.ceil(yMax / 10);
        }

        if (yMax > 10000 && effectiveMaxCount < 1000) yMax = 1000;
        console.log(`Dynamic Y-Axis: min=${yMin}, max=${yMax}, step=${tickStep}, effectiveMaxCount=${effectiveMaxCount}, buckets.length=${buckets.length}`);

        const timeFormat = getTimeFormatForRange(buckets[0].timestamp, buckets[buckets.length - 1].timestamp);
        const labels = formatDynamicLabels(buckets, timeFormat);
        const data = buckets.map(bucket => bucket.count || 0);
        const tooltipTimestamps = buckets.map(bucket => moment(bucket.timestamp).format(timeFormat.tooltipFormat));

        const scaleChanged = window.myChart && (window.myChart.options.scales.y.max !== yMax || window.myChart.options.scales.y.min !== yMin);
        if (window.myChart && scaleChanged) {
            window.myChart.destroy();
            window.myChart = null;
        }

        if (!window.myChart) {
            window.myChart = new Chart(ctx, {
                type: 'bar',
                data: {
                    labels: labels,
                    datasets: [{
                        label: 'Log Count',
                        data: data,
                        backgroundColor: 'rgba(75, 192, 192, 1)',
                        borderColor: 'rgba(0, 131, 131, 0.79)',
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
                            min: yMin,
                            max: yMax,
                            ticks: {
                                stepSize: tickStep,
                                beginAtZero: true
                            },
                            title: { display: true, text: 'Count of Logs' },
                            grid: { color: 'rgba(200, 200, 200, 0.3)' },
                            suggestedMin: yMin,
                            suggestedMax: yMax
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
        } else {
            window.myChart.data.labels = labels;
            window.myChart.data.datasets[0].data = data;
            window.myChart.options.scales.y.min = yMin;
            window.myChart.options.scales.y.max = yMax;
            window.myChart.options.scales.y.ticks.stepSize = tickStep;
            window.myChart.options.scales.y.suggestedMin = yMin;
            window.myChart.options.scales.y.suggestedMax = yMax;
            window.myChart.fullTimestamps = tooltipTimestamps;
            window.myChart.options.scales.x.title.text = `Time (${timeFormat.granularity})`;
            window.myChart.update();
        }
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
            return { format: 'HH:mm', tooltipFormat: 'YYYY-MM-DD HH:mm', granularity: 'hour' };
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
                        labels.push(m.format('HH:mm'));
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
            $zoomInfo = $('<div id="zoom-info" style="position: absolute; top: 0; right: 0; text-align: center; margin: 2px; font-size: 10px; padding: 2px; "></div>');
            $('.my-chart-container').after($zoomInfo);
        }
        const start = moment(startTime).format('YYYY-MM-DD HH:mm:ss');
        const end = moment(endTime).format('YYYY-MM-DD HH:mm:ss');
        $zoomInfo.html(`<strong>Range:</strong> ${start} to ${end}<br><strong>Granularity:</strong> ${granularity}${loading ? ' (Loading...)' : ''}`);
    }
});