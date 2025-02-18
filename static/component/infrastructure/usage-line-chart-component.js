class ClusterUsageChart {
    constructor(containerId, title, query, type) {
        this.container = $(`#${containerId}`);
        this.title = title;
        this.query = query;
        this.type = type;
        this.chart = null;
        this.init();
        this.fetchData();
    }

    init() {
        const chartDiv = this.container.find('.cluster-card > div:last-child');
        chartDiv.html('<canvas></canvas>');

        this.setupChart();
        this.setupEventHandlers();
        this.addStyles();
    }

    setupChart() {
        const ctx = this.container.find('canvas')[0].getContext('2d');
        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            boxWidth: 12,
                            padding: 15,
                            color: 'var(--text-color)',
                        },
                    },
                },
                scales: {
                    x: {
                        type: 'time',
                        time: {
                            unit: 'minute',
                            displayFormats: {
                                minute: 'HH:mm',
                            },
                        },
                        grid: {
                            color: 'rgba(128, 128, 128, 0.1)',
                            drawBorder: false,
                        },
                        ticks: {
                            color: 'var(--text-color)',
                        },
                    },
                    y: {
                        beginAtZero: true,
                        max: 1,
                        ticks: {
                            color: 'var(--text-color)',
                            callback: (value) => `${(value * 100).toFixed(0)}%`,
                        },
                        grid: {
                            color: 'rgba(128, 128, 128, 0.1)',
                            drawBorder: false,
                            drawOnChartArea: true,
                        },
                    },
                },
            },
            plugins: [
                {
                    id: 'thresholdLines',
                    beforeDraw: (chart) => {
                        const { ctx, chartArea, scales } = chart;
                        const { top, bottom, left, right } = chartArea;
                        const { y } = scales;

                        // Draw 90% threshold line
                        ctx.save();
                        ctx.beginPath();
                        ctx.setLineDash([5, 5]);
                        ctx.strokeStyle = '#EF4444';
                        ctx.lineWidth = 1;
                        const y90 = y.getPixelForValue(0.9);
                        ctx.moveTo(left, y90);
                        ctx.lineTo(right, y90);
                        ctx.stroke();

                        // Draw 60% threshold line
                        ctx.beginPath();
                        ctx.setLineDash([5, 5]);
                        ctx.strokeStyle = '#10B981';
                        ctx.lineWidth = 1;
                        const y60 = y.getPixelForValue(0.6);
                        ctx.moveTo(left, y60);
                        ctx.lineTo(right, y60);
                        ctx.stroke();
                        ctx.restore();
                    },
                },
            ],
        });
    }
    async fetchData() {
        const urlParams = new URLSearchParams(window.location.search);
        const startTime = urlParams.get('startEpoch') || 'now-1h';
        const endTime = urlParams.get('endEpoch') || 'now';
        const clusterFilter = urlParams.get('cluster') || 'all';

        const clusterMatch = clusterFilter === 'all' ? '.+' : clusterFilter;

        // Replace cluster filter in query
        const finalQuery = this.query.replace(/cluster=~".+"/, `cluster=~"${clusterMatch}"`);

        const requestData = {
            start: startTime,
            end: endTime,
            queries: [
                {
                    name: 'a',
                    query: finalQuery,
                    qlType: 'promql',
                    state: 'raw',
                },
            ],
            formulas: [
                {
                    formula: 'a',
                },
            ],
        };

        try {
            const response = await $.ajax({
                method: 'post',
                url: 'metrics-explorer/api/v1/timeseries',
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    Accept: '*/*',
                },
                crossDomain: true,
                dataType: 'json',
                data: JSON.stringify(requestData),
            });

            this.updateChart(response);
        } catch (error) {
            console.error('Error fetching data:', error);
            this.showError();
        }
    }

    updateChart(response) {
        if (!response || !response.series || response.series.length === 0) {
            this.showNoData();
            return;
        }

        const datasets = [];
        const timepoints = response.timepoints || [];
        const values = response.values || [];

        response.series.forEach((series, index) => {
            const match = series.match(/cluster:([^,}]+)/);
            const clusterName = match ? match[1] : 'unknown';

            const data = timepoints.map((time, i) => ({
                x: time * 1000, // Convert to milliseconds
                y: values[index][i],
            }));

            datasets.push({
                label: clusterName,
                data: data,
                borderColor: this.getClusterColor(index),
                tension: 0.4,
                fill: false,
            });
        });

        this.chart.data.datasets = datasets;
        this.chart.update();
    }

    getClusterColor(index) {
        const colors = ['#3B82F6', '#8B5CF6', '#10B981', '#F59E0B', '#EF4444'];
        return colors[index % colors.length];
    }

    showError() {
        if (this.chart) {
            this.chart.data.datasets = [];
            this.chart.update();
        }
    }

    showNoData() {
        if (this.chart) {
            this.chart.data.datasets = [];
            this.chart.update();
        }
    }

    setupEventHandlers() {
        const dropdown = this.container.find('.dropdown');
        const menuButton = dropdown.find('.menu-button');

        menuButton.on('click', (e) => {
            e.stopPropagation();
            const currentDropdown = $(e.currentTarget).closest('.dropdown');
            $('.dropdown').not(currentDropdown).removeClass('active');
            currentDropdown.toggleClass('active');
        });

        $(document).on('click', () => {
            $('.dropdown').removeClass('active');
        });
    }

    addStyles() {
        const styles = `
            <style>
                #${this.container.attr('id')} .cluster-card > div:last-child {
                    position: relative;
                    height: calc(100% - 50px);
                    padding: 10px;
                }
                #${this.container.attr('id')} canvas {
                    width: 100% !important;
                    height: 100% !important;
                }
            </style>
        `;
        this.container.append(styles);
    }
}
