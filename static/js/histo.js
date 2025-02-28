$(document).ready(function () {
    let socket = null;
    let wsConnected = false;
    let selectionStart = null;
    let selectionEnd = null;
    let isDragging = false;
    let originalData = [];
    let originalTimestamps = [];

    // Initialize WebSocket connection
    function initializeWebSocket() {
        if (wsConnected) return;
        const wsURL = (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/api/search/ws';
        socket = new WebSocket(wsURL);

        socket.onopen = function (_e) {
            wsConnected = true;
            console.log('WebSocket connection established for timechart.');
            // Send initial query with runTimechart enabled
            const initialData = {
                searchText: "*",
                startEpoch: moment().subtract(30, 'days').valueOf(),
                endEpoch: moment().valueOf(),
                runTimechart: true,
                queryLanguage: "Splunk QL",
                state: 'RUNNING'
            };
            socket.send(JSON.stringify(initialData));
        };

        socket.onmessage = function (event) {
            let jsonEvent = JSON.parse(event.data);
            let eventType = jsonEvent.state;
            console.log(`WebSocket message received: ${eventType}`, jsonEvent);

            switch (eventType) {
                case 'QUERY_UPDATE':
                    if (jsonEvent.TimechartUpdate) {
                        $('#timechart-state').data('timechart-data', jsonEvent.TimechartUpdate);
                        const buckets = jsonEvent.TimechartUpdate.buckets || [];
                        console.log("TimechartUpdate received, bucket count:", buckets.length,
                               "first timestamp:", buckets[0]?.timestamp,
                               "last timestamp:", buckets[buckets.length - 1]?.timestamp,
                               "buckets:", JSON.stringify(buckets));
                    }
                    break;
                case 'COMPLETE':
                    if (jsonEvent.TimechartComplete) {
                        $('#timechart-state').data('timechart-data', jsonEvent.TimechartComplete);
                        const buckets = jsonEvent.TimechartComplete.buckets || [];
                        console.log("TimechartComplete received, bucket count:", buckets.length,
                               "first timestamp:", buckets[0]?.timestamp,
                               "last timestamp:", buckets[buckets.length - 1]?.timestamp,
                               "buckets:", JSON.stringify(buckets));

                        // If histogram is visible, update it with the new data
                        if ($('#histogram-container').is(':visible')) {
                            updateHistogram(buckets);
                        }
                    }
                    break;
                case 'CANCELLED':
                case 'TIMEOUT':
                case 'ERROR':
                    console.warn(`Timechart error: ${eventType} - ${JSON.stringify(jsonEvent)}`);
                    break;
                default:
                    console.log(`Unknown state received from server: ` + JSON.stringify(jsonEvent));
            }
        };

        socket.onclose = function (event) {
            wsConnected = false;
            console.log(`WebSocket connection closed, code=${event.code} reason=${event.reason}`);
            if (event.code !== 1000) { // Not a clean close
                setTimeout(initializeWebSocket, 2000); // Retry after 2 seconds
            }
        };

        socket.onerror = function (error) {
            console.error('WebSocket error:', error);
            wsConnected = false;
            setTimeout(initializeWebSocket, 2000); // Retry after 2 seconds
        };
    }

    // Initialize WebSocket on page load
    initializeWebSocket();

    // Handle toggle click to show/hide histogram
    $('#toggle-btn').on('click', toggleHistogram);

    // Add this function to determine appropriate time format based on range duration
    function getTimeFormatForRange(startTime, endTime) {
        const rangeMs = endTime - startTime;
        const rangeHours = rangeMs / (1000 * 60 * 60);

        // Define thresholds for different time formats
        if (rangeHours <= 2) {
            return {
                format: 'HH:mm:ss', // Show hours:minutes:seconds for small ranges (≤ 2 hours)
                tooltipFormat: 'MMM Do, YYYY @ HH:mm:ss.SSS',
                granularity: 'second'
            };
        } else if (rangeHours <= 24) {
            return {
                format: 'HH:mm', // Show hours:minutes for medium ranges (≤ 24 hours)
                tooltipFormat: 'MMM Do, YYYY @ HH:mm:ss',
                granularity: 'minute'
            };
        } else if (rangeHours <= 168) { // 7 days
            return {
                format: 'MMM Do, HH:mm', // Show day + hour for ≤ 7 days
                tooltipFormat: 'MMM Do, YYYY @ HH:mm',
                granularity: 'hour'
            };
        } else if (rangeHours <= 720) { // 30 days
            return {
                format: 'MMM Do', // Show month and day for ≤ 30 days
                tooltipFormat: 'MMM Do, YYYY',
                granularity: 'day'
            };
        } else {
            return {
                format: 'YYYY-MM-DD', // Default format for large ranges
                tooltipFormat: 'MMM Do, YYYY',
                granularity: 'day'
            };
        }
    }

    // Function to determine optimal bucket size based on time range
    function determineOptimalBucketSize(startEpoch, endEpoch) {
        const rangeMs = endEpoch - startEpoch;
        const rangeHours = rangeMs / (1000 * 60 * 60);

        // Example logic for bucket size determination
        if (rangeHours <= 2) {
            return '1s'; // 1 second buckets for small ranges
        } else if (rangeHours <= 24) {
            return '1m'; // 1 minute buckets
        } else if (rangeHours <= 168) {
            return '1h'; // 1 hour buckets
        } else if (rangeHours <= 720) {
            return '1d'; // 1 day buckets
        } else {
            return '1d'; // Default
        }
    }

    // Function to display information about current zoom level
    function updateZoomInfo(startTime, endTime, granularity) {
        // Create or update an info box to show current zoom level
        let $zoomInfo = $('#zoom-info');
        if ($zoomInfo.length === 0) {
            $zoomInfo = $('<div id="zoom-info" style="position: absolute; top: 5px; right: 5px; background: rgba(255,255,255,0.8); padding: 5px; border-radius: 4px; font-size: 12px;"></div>');
            $('#histogram-container').append($zoomInfo);
        }

        const start = moment(startTime).format('YYYY-MM-DD HH:mm:ss');
        const end = moment(endTime).format('YYYY-MM-DD HH:mm:ss');
        $zoomInfo.html(`<strong>Time Range:</strong> ${start} to ${end}<br><strong>Granularity:</strong> ${granularity}`);
    }

    // Modified submitTimeRangeQuery to include granularity
    function submitTimeRangeQuery(startTime, endTime) {
        if (!wsConnected || !socket) {
            console.error('WebSocket not connected, cannot submit query');
            return;
        }

        // Determine appropriate time granularity based on range
        const timeFormat = getTimeFormatForRange(startTime, endTime);
        const bucketSize = determineOptimalBucketSize(startTime, endTime);

        console.log(`Submitting time range query: ${startTime} to ${endTime} with granularity: ${timeFormat.granularity}, bucket size: ${bucketSize}`);

        // Update zoom info display
        updateZoomInfo(startTime, endTime, timeFormat.granularity);

        const queryData = {
            searchText: "*",
            startEpoch: startTime,
            endEpoch: endTime,
            runTimechart: true,
            queryLanguage: "Splunk QL",
            state: 'RUNNING',
            timeGranularity: timeFormat.granularity,
            bucketSize: bucketSize
        };
        socket.send(JSON.stringify(queryData));
    }

    // Handle mouse events for time selection
    $(document).on('mousedown', '#histogram', function(e) {
        const chart = window.myChart;
        if (!chart) return;

        const rect = this.getBoundingClientRect();
        const x = e.clientX - rect.left;

        // Get the x value (timestamp) at the cursor position
        const xValue = chart.scales.x.getValueForPixel(x);

        if (xValue) {
            selectionStart = xValue;
            selectionEnd = null;
            isDragging = true;

            // Create initial selection area
            updateSelectionArea(chart, selectionStart, selectionStart);

            // Store timestamps for each data point if not already stored
            if (originalTimestamps.length === 0) {
                originalTimestamps = chart.data.labels.map(label => {
                    // Convert the formatted label back to timestamp if needed
                    if (typeof label === 'string') {
                        return moment(label, 'YYYY-MM-DD').valueOf();
                    }
                    return label;
                });
            }

            // Store the original data if not already stored
            if (originalData.length === 0) {
                originalData = [...chart.data.datasets[0].data];
            }
        }
    });

    $(document).on('mousemove', function(e) {
        const chart = window.myChart;
        if (!isDragging || !chart) return;

        const canvas = document.getElementById('histogram');
        if (!canvas) return;

        const rect = canvas.getBoundingClientRect();
        const x = Math.max(rect.left, Math.min(e.clientX, rect.right)) - rect.left;

        // Get the x value at the cursor position
        const xValue = chart.scales.x.getValueForPixel(x);

        if (xValue) {
            selectionEnd = xValue;
            updateSelectionArea(chart, selectionStart, selectionEnd);
        }
    });

    $(document).on('mouseup', function() {
        if (isDragging && selectionStart && selectionEnd) {
            // Make the selection in chronological order
            const start = Math.min(selectionStart, selectionEnd);
            const end = Math.max(selectionStart, selectionEnd);

            // Convert to epoch timestamps if they're not already
            const startEpoch = typeof start === 'number' ? start : moment(start).valueOf();
            const endEpoch = typeof end === 'number' ? end : moment(end).valueOf();

            console.log(`Selection complete: ${moment(startEpoch).format('YYYY-MM-DD HH:mm:ss')} to ${moment(endEpoch).format('YYYY-MM-DD HH:mm:ss')}`);

            // Submit query with selected time range
            submitTimeRangeQuery(startEpoch, endEpoch);
        }

        isDragging = false;
    });

    // Double click to reset the selection and query the full time range
    $(document).on('dblclick', '#histogram', function() {
        const chart = window.myChart;
        if (!chart) return;

        // Clear selection area
        if (chart.options.plugins.annotation && chart.options.plugins.annotation.annotations) {
            chart.options.plugins.annotation.annotations = {};
            chart.update();
        }

        // Clear zoom info
        $('#zoom-info').remove();

        // Reset to original data range
        if (originalTimestamps.length > 0 && originalTimestamps.length === originalData.length) {
            const startEpoch = originalTimestamps[0];
            const endEpoch = originalTimestamps[originalTimestamps.length - 1];
            submitTimeRangeQuery(startEpoch, endEpoch);
        } else {
            // Fallback to a wide date range if original data isn't available
            submitTimeRangeQuery(moment().subtract(30, 'days').valueOf(), moment().valueOf());
        }

        selectionStart = null;
        selectionEnd = null;
    });

    function updateSelectionArea(chart, start, end) {
        // Get pixel positions for the selection points
        const startPx = chart.scales.x.getPixelForValue(start);
        const endPx = chart.scales.x.getPixelForValue(end);

        // Calculate selection width and position
        const left = Math.min(startPx, endPx);
        const width = Math.abs(endPx - startPx);

        // Ensure annotations plugin is available
        if (!chart.options.plugins.annotation) {
            chart.options.plugins.annotation = {
                annotations: {}
            };
        }

        // Update or create the selection box annotation
        chart.options.plugins.annotation.annotations.selection = {
            type: 'box',
            xMin: Math.min(start, end),
            xMax: Math.max(start, end),
            yMin: 0,
            yMax: 'max',
            backgroundColor: 'rgba(173, 216, 230, 0.3)',
            borderColor: 'rgba(0, 123, 255, 0.8)',
            borderWidth: 1
        };

        // Add vertical lines at selection boundaries
        chart.options.plugins.annotation.annotations.leftLine = {
            type: 'line',
            xMin: Math.min(start, end),
            xMax: Math.min(start, end),
            yMin: 0,
            yMax: 'max',
            borderColor: 'rgba(0, 123, 255, 0.8)',
            borderWidth: 1,
            borderDash: [5, 5]
        };

        chart.options.plugins.annotation.annotations.rightLine = {
            type: 'line',
            xMin: Math.max(start, end),
            xMax: Math.max(start, end),
            yMin: 0,
            yMax: 'max',
            borderColor: 'rgba(0, 123, 255, 0.8)',
            borderWidth: 1,
            borderDash: [5, 5]
        };

        chart.update();
    }
});

function checkAndRenderHistogram() {
    const $histogramContainer = $('#histogram-container');
    const timechartData = $('#timechart-state').data('timechart-data') || {};

    if (timechartData && timechartData.buckets && timechartData.buckets.length > 0) {
        console.log("Timechart data found, rendering histogram with", timechartData.buckets.length, "buckets");
        updateHistogram(timechartData.buckets);
    } else {
        console.log("No timechart data available yet, will check again...");
        // Check if container is still visible before setting another timeout
        if ($histogramContainer.is(':visible')) {
            setTimeout(checkAndRenderHistogram, 500); // Check every 500ms
        }
    }
}

function toggleHistogram() {
    const $histogramContainer = $('#histogram-container');
    const $showTable = $('#showTable');
    const $tabsChart = $('#tabs-chart');
    const isHidden = $histogramContainer.is(':hidden');

    console.log('Toggling histogram, isHidden:', isHidden);

    if (isHidden) {

        $histogramContainer.show();

        // Move table down
        if ($showTable.is(':visible')) {
            $showTable.css('transform', 'translateY(220px)');
        }

        // Move chart down if visible
        if ($tabsChart.is(':visible')) {
            $tabsChart.css('transform', 'translateY(220px)');
        }

        // If chart exists but has no data, or if chart doesn't exist, try to render
        if (!window.myChart || window.myChart.data.datasets[0].data.length === 0) {
            checkAndRenderHistogram();
        }
    } else {

        $histogramContainer.hide();

        $showTable.css('transform', 'translateY(0)');
        $tabsChart.css('transform', 'translateY(0)');

    }
}

function updateHistogram(buckets) {
    if (!buckets || buckets.length === 0) {
        console.warn("No buckets available for histogram.");
        return;
    }

    const ctx = document.getElementById('histogram').getContext('2d');

    // Get time range to determine appropriate formatting
    const firstTimestamp = moment(buckets[0].timestamp);
    const lastTimestamp = moment(buckets[buckets.length - 1].timestamp);
    const timeFormat = getTimeFormatForRange(firstTimestamp.valueOf(), lastTimestamp.valueOf());

    console.log(`Using time format: ${timeFormat.format} for range: ${firstTimestamp.format('YYYY-MM-DD HH:mm:ss')} to ${lastTimestamp.format('YYYY-MM-DD HH:mm:ss')}`);

    // Store the full timestamps and use appropriate date format for display
    const timestamps = buckets.map(bucket => moment(bucket.timestamp));
    // Format x-axis labels according to selected time range
    const dateLabels = timestamps.map(time => time.format(timeFormat.format));
    const data = buckets.map(bucket => bucket.count);

    // Store full timestamp formatted strings for tooltips with appropriate detail
    const fullTimestamps = timestamps.map(time => time.format(timeFormat.tooltipFormat));

    // If chart exists, update its data and axis formatting
    if (window.myChart) {
        console.log("Updating existing chart with new data and time format...");
        window.myChart.data.labels = dateLabels;
        window.myChart.data.datasets[0].data = data;
        // Store the full timestamps for tooltips
        window.myChart.fullTimestamps = fullTimestamps;

        // Update x-axis title to indicate current time granularity
        window.myChart.options.scales.x.title.text = `Time (${timeFormat.granularity})`;

        window.myChart.update();

        // Update zoom info if we have a time range selection
        if (buckets.length > 0) {
            updateZoomInfo(firstTimestamp.valueOf(), lastTimestamp.valueOf(), timeFormat.granularity);
        }

        return;
    }

    // Check if Chart.js annotations plugin exists, load it if needed
    if (!Chart.annotation) {
        console.warn("Chart.js annotation plugin not loaded. Selection areas may not display correctly.");
    }

    console.log("Creating new chart instance...");
    window.myChart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: dateLabels,
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
                    title: {
                        display: true,
                        text: `Time (${timeFormat.granularity})` // Dynamic title based on granularity
                    },
                    grid: {
                        display: true,
                        color: 'rgba(200, 200, 200, 0.3)'
                    }
                },
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Count of Logs'
                    },
                    grid: {
                        display: true,
                        color: 'rgba(200, 200, 200, 0.3)'
                    }
                }
            },
            plugins: {
                legend: {
                    display: true,
                    position: 'top',
                    labels: {
                        boxWidth: 10,
                        padding: 5
                    }
                },
                tooltip: {
                    callbacks: {
                        label: function (context) {
                            return `${context.parsed.y} logs`;
                        },
                        title: function(tooltipItems) {
                            // Use the full timestamp for tooltip instead of just the date
                            const dataIndex = tooltipItems[0].dataIndex;
                            return window.myChart.fullTimestamps[dataIndex];
                        }
                    }
                },
                // Initialize empty annotations object that will be populated during selection
                annotation: {
                    annotations: {}
                }
            },
            // Allow interactions with the chart
            interaction: {
                mode: 'nearest',
                intersect: false
            },
            animation: {
                duration: 300
            }
        }
    });

    // Store the full timestamps for tooltip use
    window.myChart.fullTimestamps = fullTimestamps;

    // Store the original data for reset functionality
    window.originalBuckets = [...buckets];

    // Add zoom info
    if (buckets.length > 0) {
        updateZoomInfo(firstTimestamp.valueOf(), lastTimestamp.valueOf(), timeFormat.granularity);
    }

    console.log("Histogram initialized with", buckets.length, "data points at granularity:", timeFormat.granularity);
}

// Helper function to determine time format based on range
function getTimeFormatForRange(startTime, endTime) {
    const rangeMs = endTime - startTime;
    const rangeHours = rangeMs / (1000 * 60 * 60);

    // Define thresholds for different time formats
    if (rangeHours <= 2) {
        return {
            format: 'HH:mm:ss', // Show hours:minutes:seconds for small ranges (≤ 2 hours)
            tooltipFormat: 'MMM Do, YYYY @ HH:mm:ss.SSS',
            granularity: 'second'
        };
    } else if (rangeHours <= 24) {
        return {
            format: 'HH:mm', // Show hours:minutes for medium ranges (≤ 24 hours)
            tooltipFormat: 'MMM Do, YYYY @ HH:mm:ss',
            granularity: 'minute'
        };
    } else if (rangeHours <= 168) {
        return {
            format: 'MMM Do, HH:mm', // Show day + hour for ≤ 7 days
            tooltipFormat: 'MMM Do, YYYY @ HH:mm',
            granularity: 'hour'
        };
    } else if (rangeHours <= 720) { 
        return {
            format: 'MMM Do', // Show month and day for ≤ 30 days
            tooltipFormat: 'MMM Do, YYYY',
            granularity: 'day'
        };
    } else {
        return {
            format: 'YYYY-MM-DD', // Default format for large ranges
            tooltipFormat: 'MMM Do, YYYY',
            granularity: 'day'
        };
    }
}