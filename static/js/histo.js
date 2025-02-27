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

    function submitTimeRangeQuery(startTime, endTime) {
        if (!wsConnected || !socket) {
            console.error('WebSocket not connected, cannot submit query');
            return;
        }

        console.log(`Submitting time range query: ${startTime} to ${endTime}`);
        const queryData = {
            searchText: "*",
            startEpoch: startTime,
            endEpoch: endTime,
            runTimechart: true,
            queryLanguage: "Splunk QL",
            state: 'RUNNING'
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
                        return moment(label, 'MMM Do, YYYY @ HH:mm:ss').valueOf();
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
    const runTimechart = $('#timechart-state').data('run-timechart') || false;
    const timechartData = $('#timechart-state').data('timechart-data') || {};
    if (runTimechart && timechartData && timechartData.buckets) {
        updateHistogram(timechartData.buckets);
    } else {
        setTimeout(checkAndRenderHistogram, 500); // Check every 500ms
    }
}

function toggleHistogram() {
    const $histogramContainer = $('#histogram-container');
    const isHidden = $histogramContainer.is(':hidden');

    console.log('Toggling histogram, isHidden:', isHidden);
    if (isHidden) {
        $histogramContainer.show();
        checkAndRenderHistogram();
    } else {
        $histogramContainer.hide();
        if (window.myChart) {
            window.myChart.destroy();
            console.log('Histogram chart destroyed');
        }
    }
}

function updateHistogram(buckets) {
    const ctx = document.getElementById('histogram').getContext('2d');
    if (window.myChart) {
        window.myChart.destroy();
    }

    if (!buckets || buckets.length === 0) {
        console.warn("No buckets available for histogram.");
        return;
    }

    // Convert timestamps to moment objects for better handling
    const timestamps = buckets.map(bucket => moment(bucket.timestamp));
    const labels = timestamps.map(time => time.format('MMM Do, YYYY @ HH:mm:ss'));
    const data = buckets.map(bucket => bucket.count);

    // Check if Chart.js annotations plugin exists, load it if needed
    if (!Chart.annotation) {
        console.warn("Chart.js annotation plugin not loaded. Selection areas may not display correctly.");
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
                    title: {
                        display: true,
                        text: 'Time'
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
                            return tooltipItems[0].label;
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

    // Store the original data for reset functionality
    window.originalBuckets = [...buckets];

    console.log("Histogram initialized with", buckets.length, "data points");
}