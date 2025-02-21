const verticalLinePlugin = {
    id: 'verticalLine',
    afterDraw: (chart) => {
        if (chart.dragStart !== undefined && chart.dragEnd !== undefined) {
            const ctx = chart.ctx;
            const xAxis = chart.scales.x;
            const yAxis = chart.scales.y;

            // Draw selection area
            ctx.save();
            ctx.fillStyle = 'rgba(75, 192, 192, 0.2)';
            ctx.fillRect(
                chart.dragStart,
                yAxis.top,
                chart.dragEnd - chart.dragStart,
                yAxis.bottom - yAxis.top
            );

            // Draw vertical lines
            ctx.beginPath();
            ctx.strokeStyle = 'rgb(75, 192, 192)';
            ctx.lineWidth = 2;
            ctx.setLineDash([5, 5]);

            ctx.moveTo(chart.dragStart, yAxis.top);
            ctx.lineTo(chart.dragStart, yAxis.bottom);
            ctx.moveTo(chart.dragEnd, yAxis.top);
            ctx.lineTo(chart.dragEnd, yAxis.bottom);
            ctx.stroke();
            ctx.restore();
        }
    }
};

// sample log data generator
function generateSampleData(start, end, interval) {
    const data = [];
    let current = moment(start);

    // Generation function
    const basePattern = [];
    for (let i = 0; i < 24; i++) {
        // Create a daily pattern with peak hours
        let baseCount = 200;
        // Morning peak (8-11 AM)
        if (i >= 8 && i <= 11) baseCount = 1500;
        // Afternoon peak (2-5 PM)
        if (i >= 14 && i <= 17) baseCount = 1200;
        // Night valley (11 PM-5 AM)
        if (i >= 23 || i <= 5) baseCount = 100;
        basePattern.push(baseCount);
    }

    while (current.isBefore(end)) {
        const hour = current.hour();
        const baseCount = basePattern[hour];


        const variation = baseCount * 0.3;
        const count = Math.floor(baseCount + (Math.random() * variation * 2 - variation));


        const spike = Math.random() < 0.05 ? Math.floor(baseCount * 2) : 0;

        data.push({
            timestamp: current.valueOf(),
            count: count + spike
        });

        current.add(1, interval);
    }
    return data;
}

// Initialize chart
let chart;
function initChart() {
    const ctx = document.getElementById('histogram').getContext('2d');
    chart = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [{
                label: 'Log Count',
                data: [],
                backgroundColor: 'rgb(75, 192, 192)',
                borderColor: 'rgb(75, 192, 192)',
                borderWidth: 1
            }]
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                verticalLinePlugin: true,
            },
            scales: {
                y: {
                    beginAtZero: true,
                    title: {
                        display: true,
                        text: 'Count of Logs'
                    },
                    ticks: {
                        stepSize: 500,
                        callback: function(value) {
                            return value.toLocaleString();
                        }
                    },
                    //the max is always a multiple of 500
                    afterDataLimits: (scale) => {
                        scale.max = Math.ceil(scale.max / 500) * 500;
                    }
                },
                x: {
                    title: {
                        display: true,
                        text: 'Time'
                    }
                }
            }
        },
        plugins: [verticalLinePlugin]
    });

    // Add mouse event listeners
    let isDragging = false;

    $('#histogram').on('mousedown', function(e) {
        isDragging = true;
        const rect = chart.canvas.getBoundingClientRect();
        chart.dragStart = e.clientX - rect.left;
        chart.dragEnd = chart.dragStart;
        chart.update();
    });

    $(document).on('mousemove', function(e) {
        if (isDragging) {
            const rect = chart.canvas.getBoundingClientRect();
            chart.dragEnd = Math.min(
                Math.max(e.clientX - rect.left, 0),
                chart.width
            );
            chart.update();
        }
    });

    $(document).on('mouseup', function() {
        if (isDragging) {
            isDragging = false;

            // Convert pixel positions to data indices
            const xScale = chart.scales.x;
            const startIndex = Math.round(xScale.getValueForPixel(chart.dragStart));
            const endIndex = Math.round(xScale.getValueForPixel(chart.dragEnd));

            // Update date inputs
            const startDate = moment(chart.data.labels[startIndex]);
            const endDate = moment(chart.data.labels[endIndex]);

            $('#startDate').val(startDate.format('YYYY-MM-DDTHH:mm'));
            $('#endDate').val(endDate.format('YYYY-MM-DDTHH:mm'));

            // Update chart with new range
            updateChart(startDate, endDate, determineTimeframe(startDate, endDate));
        }
    });
}

function determineTimeframe(start, end) {
    const diff = end.diff(start, 'hours');
    if (diff > 168) return 'day';  // > 1 week
    if (diff > 24) return 'hour';   // > 1 day
    if (diff > 1) return 'minute';  // > 1 hour
    return 'second';
}

function updateChart(start, end, timeframe) {
    const data = generateSampleData(start, end, timeframe);
    const labels = data.map(d =>
        moment(d.timestamp).format(
            timeframe === 'day' ? 'YYYY-MM-DD' :
            timeframe === 'hour' ? 'HH:mm' :
            timeframe === 'minute' ? 'HH:mm:ss' :
            'HH:mm:ss.SSS'
        )
    );
    chart.data.labels = labels;
    chart.data.datasets[0].data = data.map(d => d.count);
    chart.dragStart = undefined;
    chart.dragEnd = undefined;
    chart.update();
}

$(document).ready(function() {
    const end = moment();
    const start = moment().subtract(7, 'days');

    $('#startDate').val(start.format('YYYY-MM-DDTHH:mm'));
    $('#endDate').val(end.format('YYYY-MM-DDTHH:mm'));

    initChart();
    updateChart(start, end, 'day');

    // Handle manual date input changes
    $('.date-range input').on('change', function() {
        const start = moment($('#startDate').val());
        const end = moment($('#endDate').val());
        const timeframe = determineTimeframe(start, end);
        updateChart(start, end, timeframe);
    });

    // Toggle histogram visibility
    $('#toggle-btn').click(function(event) {
        event.stopPropagation();
        $('#histogram-container').slideToggle();
    });

    // Close when clicking outside
    $(document).click(function(event) {
        if (!$(event.target).closest('#histogram-container, #toggle-btn').length) {
            $('#histogram-container').slideUp();
        }
    });
});