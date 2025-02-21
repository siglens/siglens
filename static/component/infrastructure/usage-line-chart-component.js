class ClusterUsageChart {
    constructor(containerId, title, query, type) {
        this.container = $(`#${containerId}`);
        this.title = title;
        this.query = query;
        this.type = type;
        this.chart = null;
        this.loadingElement = null;
        this.init();
        this.fetchData();
        this.setupThemeListener();
    }

    init() {
        const chartDiv = this.container.find('.cluster-card > div:last-child');
        chartDiv.html('<canvas></canvas>');

        // Add loading spinner
        this.loadingElement = $('<div id="panel-loading"></div>');
        this.loadingElement.css({
            top: '50%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            display: 'none',
        });
        chartDiv.append(this.loadingElement);

        this.setupChart();
        this.setupEventHandlers();
        this.addStyles();
    }

    setupChart() {
        const ctx = this.container.find('canvas')[0].getContext('2d');
        let gridLineColor;
        let tickColor;
        if ($('html').attr('data-theme') == 'light') {
            gridLineColor = '#DCDBDF';
            tickColor = '#160F29';
        } else {
            gridLineColor = '#383148';
            tickColor = '#FFFFFF';
        }

        this.chart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: [],
                datasets: [],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: {
                            boxWidth: 12,
                            padding: 15,
                            color: tickColor,
                            usePointStyle: true,
                        },
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                let label = context.dataset.label || '';
                                if (label) {
                                    label += ': ';
                                }
                                if (context.parsed.y !== null) {
                                    label += `${(context.parsed.y * 100).toFixed(2)}%`;
                                }
                                return label;
                            },
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
                            tooltipFormat: 'MMM d, yyyy HH:mm:ss',
                        },
                        grid: {
                            color: gridLineColor,
                            drawBorder: false,
                        },
                        ticks: {
                            color: tickColor,
                            maxRotation: 0,
                            autoSkip: true,
                            maxTicksLimit: 10,
                        },
                    },
                    y: {
                        beginAtZero: true,
                        max: 1,
                        border: {
                            color: 'transparent',
                        },
                        ticks: {
                            color: tickColor,
                            callback: (value) => `${(value * 100).toFixed(0)}%`,
                        },
                        grid: {
                            color: gridLineColor,
                            drawBorder: false,
                            drawOnChartArea: true,
                        },
                    },
                },
            },
            plugins: [
                {
                    id: 'thresholdLines',
                    afterDraw: (chart) => {
                        const { ctx, chartArea, scales } = chart;
                        const { left, right } = chartArea;
                        const { y } = scales;

                        ctx.save();
                        ctx.beginPath();
                        ctx.setLineDash([5, 5]);
                        ctx.strokeStyle = '#EF4444';
                        ctx.lineWidth = 1.5;
                        const y90 = y.getPixelForValue(0.9);
                        ctx.moveTo(left, y90);
                        ctx.lineTo(right, y90);
                        ctx.stroke();

                        ctx.beginPath();
                        ctx.setLineDash([5, 5]);
                        ctx.strokeStyle = '#10B981';
                        ctx.lineWidth = 1.5;
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
        this.showLoading();

        const urlParams = new URLSearchParams(window.location.search);
        const startTime = urlParams.get('startEpoch') || 'now-1h';
        const endTime = urlParams.get('endEpoch') || 'now';
        const clusterFilter = urlParams.get('cluster') || 'all';

        const clusterMatch = clusterFilter === 'all' ? '.+' : clusterFilter;

        // Replace cluster filter in query
        const finalQuery = this.query.replace(/cluster=~".+"/g, `cluster=~"${clusterMatch}"`);
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
        } finally {
            this.hideLoading();
        }
    }

    updateChart(response) {
        if (!response || !response.series || response.series.length === 0 || !response.timestamps || !response.values) {
            this.showNoData();
            return;
        }

        const datasets = [];
        const timestamps = response.timestamps.map((t) => t * 1000);

        response.series.forEach((series, index) => {
            const match = series.match(/cluster:([^,}]+)/);
            const clusterName = match ? match[1] : 'unknown';

            const data = timestamps.map((time, i) => ({
                x: time,
                y: response.values[index][i],
            }));

            datasets.push({
                label: clusterName,
                data: data,
                borderColor: this.getClusterColor(index),
                backgroundColor: this.getClusterColor(index),
                tension: 0.4,
                fill: false,
                pointRadius: 0,
                borderWidth: 2,
            });
        });

        this.chart.data.datasets = datasets;
        this.chart.update('none');
    }

    setupThemeListener() {
        document.addEventListener('themeChanged', () => {
            let gridLineColor;
            let tickColor;
            if ($('html').attr('data-theme') == 'light') {
                gridLineColor = '#DCDBDF';
                tickColor = '#160F29';
            } else {
                gridLineColor = '#383148';
                tickColor = '#FFFFFF';
            }

            this.chart.options.scales.x.grid.color = gridLineColor;
            this.chart.options.scales.y.grid.color = gridLineColor;
            this.chart.options.scales.x.ticks.color = tickColor;
            this.chart.options.scales.y.ticks.color = tickColor;
            this.chart.options.plugins.legend.labels.color = tickColor;
            this.chart.update();
        });
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
                    min-height: 300px;
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

    showLoading() {
        if (this.loadingElement) {
            this.loadingElement.show();
        }
    }

    hideLoading() {
        if (this.loadingElement) {
            this.loadingElement.hide();
        }
    }
}
