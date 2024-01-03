'use strict';
let redMetrics ={
    "indexName":  "red-traces",
    "queryLanguage": "Splunk QL"
   
}
var RateCountChart;
var ErrCountChart;
var LatenciesChart;
$(document).ready(() => {
    setupEventHandlers();
    $(".theme-btn").on("click", themePickerHandler);
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $('.theme-btn').on('click', getOneServiceOverview);

   
    const serviceName = getParameterFromUrl('service');
    redMetrics['searchText']="service="  + serviceName + "";
    $('.service-name').text(serviceName);
    
    let stDate = "now-1h";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', isGraphsDatePickerHandler);
    let data = getTimeRange();
    
    redMetrics = {... redMetrics, ... data}
    getOneServiceOverview()
   
    $(".service-health-text").click(function () {
        window.location.href = "../service-health.html";
    })
});

function isGraphsDatePickerHandler(evt) {
    evt.preventDefault();
    getOneServiceOverview()
    $('#daterangepicker').hide();
}

function getTimeRange() {
    return {
        'startEpoch': filterStartDate || "now-1h",
        'endEpoch': filterEndDate || "now",
    };
}
function getParameterFromUrl(param) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(param);
}
let gridLineColor;
let tickColor;

function getOneServiceOverview(){
    let endDate = filterEndDate || "now";
    let stDate = filterStartDate || "now-1h";
    let data = getTimeRange();
    redMetrics = {... redMetrics, ... data}
    $.ajax({
        method: "POST",
        url: "api/search",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify(redMetrics),
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        if ($('body').attr('data-theme') == "light") {
            gridLineColor = "#DCDBDF";
            tickColor = "#160F29";
        }
        else {
            gridLineColor = "#383148";
            tickColor = "#FFFFFF"
        }
        if (RateCountChart !== undefined) {
            RateCountChart.destroy();
        }
        if (ErrCountChart !== undefined) {
            ErrCountChart.destroy();
        }
        if (LatenciesChart!==undefined){
            LatenciesChart.destroy();
        }
        rateChart(res.hits.records,gridLineColor,tickColor);
        errorChart(res.hits.records,gridLineColor,tickColor);
        latenciesChart(res.hits.records,gridLineColor,tickColor);
    })
}

function rateChart(rateData,gridLineColor,tickColor) {
    let graph_data = []
    for(let data of rateData){
        graph_data.push({
            x : new Date(data.timestamp).toISOString().split('T').join(" "),
            y: data.rate
        })
    }
    var RateCountChartCanvas = $("#ServiceHealthChart").get(0).getContext("2d");
    RateCountChart = new Chart(RateCountChartCanvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Rate',
                    data: graph_data,
                    borderColor: ['rgb(99,71,217)'],
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 5,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },

            ]
        },
        options: {
            responsive: true,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                }
            },
            plugins: {
                legend: {
                    display: false
                },
            }
        }
    });
    return RateCountChart;
}

function errorChart(errorData,gridLineColor,tickColor) {
    let graph_data_err = []
    for(let data of errorData){
            let formatted_date = new Date(data.timestamp).toISOString().split('T').join(" ")
            graph_data_err.push({
                x : formatted_date,
                y: data.error_rate
            }) 
    }
    var ErrorCountChartCanvas = $("#ServiceHealthChartErr").get(0).getContext("2d");
    ErrCountChart = new Chart(ErrorCountChartCanvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'Error Rate',
                    data: graph_data_err,
                    borderColor: ['rgb(99,71,217)'],
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 5,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },

            ]
        },
        options: {
            responsive: true,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                }
            },
            plugins: {
                legend: {
                    display: false
                },
            }
        }
    });
    return ErrCountChart;
}


function latenciesChart(latenciesData,gridLineColor,tickColor) {
    let graph_data_latencies = {
        p50: [],
        p90: [],
        p99: [],
    };

    for (let data of latenciesData) {
        const timestamp = new Date(data.timestamp).toISOString().split('T').join(" ");
        graph_data_latencies.p50.push({ x: timestamp, y: data.p50 });
        graph_data_latencies.p90.push({ x: timestamp, y: data.p90 });
        graph_data_latencies.p99.push({ x: timestamp, y: data.p99 });
    }
    var LatenciesChartCanvas = $("#ServiceHealthChart2").get(0).getContext("2d");
    LatenciesChart = new Chart(LatenciesChartCanvas, {
        type: 'line',
        data: {
            datasets: [
                {
                    label: 'P50 Latency',
                    data: graph_data_latencies.p50,
                    borderColor: '#FF6484',
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 5,
                    pointBorderColor: ['#FF6484'],
                    fill: false,
                },
                {
                    label: 'P90 Latency',
                    data: graph_data_latencies.p90,
                    borderColor: '#36A2EB',
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 5,
                    pointBorderColor: '#36A2EB',
                    fill: false,
                },
                {
                    label: 'P99 Latency',
                    data: graph_data_latencies.p99,
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 5,
                    pointBorderColor: "#4BC0C0",
                    borderColor: "#4BC0C0",
                    fill: false,
                },
            ]
        },
        options: {
            responsive: true,
            interaction: {
                intersect: false,
                mode: 'index',
            },
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                },
                x: {
                    ticks: {
                        color: tickColor,
                    },
                    grid: {
                        color: gridLineColor,
                    },
                }
            },
            plugins:{
                legend: {
                    position: 'bottom',
                    labels: {
                        boxHeight: 10,
                        padding: 20,
                    }
                },
            }
        }
    });

    return LatenciesChart;
}
