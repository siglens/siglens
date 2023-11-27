'use strict';
let redMetrics ={
    "indexName":  "red-traces",
    "queryLanguage": "Splunk QL"
   
}
var RateCountChart;
var ErrCountChart;
var LatenciesChart;
$(document).ready(() => {
    displayNavbar();
    setupEventHandlers();
    $(".theme-btn").on("click", themePickerHandler);
    $('.theme-btn').on('click',getAllService() );
    const serviceName = getParameterFromUrl('service');
    redMetrics['searchText']="service="  + serviceName + "";
    let stDate = "now-1h";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', isGraphsDatePickerHandler);
    let data = getTimeRange();
    redMetrics = {... redMetrics, ... data}
    getAllService()
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
});

function isGraphsDatePickerHandler(evt) {
    evt.preventDefault();
    getAllService()
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
function getAllService(){
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
        if (RateCountChart !== undefined) {
            RateCountChart.destroy();
        }
        if (ErrCountChart !== undefined) {
            ErrCountChart.destroy();
        }
        if (LatenciesChart!==undefined){
            LatenciesChart.destroy();
        }
        rateChart(res.hits.records)
        errorChart(res.hits.records)
        latenciesChart(res.hits.records)

    })
}

function rateChart(rateData) {
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
                    pointRadius: 10,
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
        }
    });
    return RateCountChart;
}

function errorChart(errorData) {
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
                    pointRadius: 10,
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
        }
    });
    return ErrCountChart;
}

function latenciesChart(latenciesData) {
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
                    borderColor: 'rgb(99, 71, 217)',
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 10,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },
                {
                    label: 'P90 Latency',
                    data: graph_data_latencies.p90,
                    borderColor: 'rgb(255, 0, 0)',
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 10,
                    pointBorderColor: ['rgb(99,71,217)'],
                    fill: false,
                },
                {
                    label: 'P99 Latency',
                    data: graph_data_latencies.p99,
                    yAxisID: 'y',
                    pointStyle: 'circle',
                    pointRadius: 10,
                    pointBorderColor: ['rgb(99,71,217)'],
                    borderColor: "green",
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
           
        }
    });

    return LatenciesChart;
}
