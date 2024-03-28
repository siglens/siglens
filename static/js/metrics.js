/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

'use strict';

var lineChart;

$(document).ready(() => {

    let stDate = "now-1h";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);

    setupMetricsEventHandlers();

    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    $('.theme-btn').on('click', themePickerHandler);

    $("#info-icon").tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'click'
    });

    $('#info-icon').on('click', function (e) {
        $('#info-icon').tooltip('show');
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest(".tooltip-inner").length === 0) {
            $('#info-icon').tooltip('hide');
        }
    });
});

// Show clear button when there's input
$("#metrics-input").on("input", function() {
    if ($(this).val().trim() !== "") {
        $("#clearMetricsInput").show();
    } else {
        $("#clearMetricsInput").hide();
    }
});

// Clear input when the clear button is clicked
$("#clearMetricsInput").click(function() {
    $("#metrics-input").val("").focus();
    $(this).hide();
});

function setupMetricsEventHandlers() {
    $('#filter-metrics-input').on('keyup', filterMetricsInputHandler);
    $('#run-metrics-query-btn').on('click', runMetricsFilterBtnHandler);
    $(document).on('keyup', runMetricsFilterBtnHandler);

    $('#date-picker-btn').on('show.bs.dropdown', showDatePickerHandler);
    $('#date-picker-btn').on('hide.bs.dropdown', hideDatePickerHandler);
    $('#reset-timepicker').on('click', resetDatePickerHandler);

    $('#date-start').on('change', getStartDateHandler);
    $('#date-end').on('change', getEndDateHandler);

    $('#time-start').on('change', getStartTimeHandler);
    $('#time-end').on('change', getEndTimeHandler);
    $('#customrange-btn').on('click', customRangeHandler);

    $('.range-item').on('click', rangeItemHandler)

    $('#corner-popup').on('click', '.corner-btn-close', function(){
        hideError();
        $("#metrics-input").val('');
        $("#metrics-graph-container").show();
        if (lineChart !== undefined) {
            lineChart.destroy();
            $('#metrics-legends').empty();
        }
        $('#metrics-legends').empty();
        $('.metrics-response').hide();
        lineChart.destroy();
        $('#metrics-legends').empty();
        $('.metrics-response').hide();
    });

}

function runMetricsFilterBtnHandler(evt) {
    if (evt.keyCode === 13 || evt.type == "click") {
        $('.popover').hide();
        evt.preventDefault();
        if ($('#run-metrics-query-btn').text() === "Run Query") {
            data = getMetricsSearchFilter(false, false);
            doMetricsSearch();
        } else {
            data = getMetricsSearchFilter(false, false);
            doCancel(data);
        }
        $('#daterangepicker').hide();
    }
}

function filterMetricsInputHandler(evt) {
    evt.preventDefault();

    if (evt.keyCode === 13 && $('#run-metrics-query-btn').text() === "Run Query") {
        data = getMetricsSearchFilter(false, false);
        doMetricsSearch();
    }
}

function getMetricsSearchFilter(skipPushState, scrollingTrigger) {
    let filterValue = $('#metrics-input').val().trim() || '*';
    let endDate = filterEndDate || "now";
    let stDate = filterStartDate || "now-15m";

    if (!isNaN(stDate)) {
        stDate = Number(stDate);
        endDate = Number(endDate);
        datePickerHandler(stDate, endDate, "custom");
        loadCustomDateTimeFromEpoch(stDate, endDate);
    } else if (stDate !== "now-15m") {
        datePickerHandler(stDate, endDate, stDate);
    } else {
        datePickerHandler(stDate, endDate, "");
    }

    addQSParm("query", filterValue);
    addQSParm("start", stDate);
    addQSParm("end", endDate);

    window.history.pushState({ path: myUrl }, '', myUrl);

    if (scrollingTrigger) {
        sFrom = scrollFrom;
    }

    return {
        'query': filterValue,
        'start': stDate.toString(),
        'end': endDate.toString(),
    };
}

function doMetricsSearch() {
    let startTime = (new Date()).getTime();
    let data = getMetricsSearchFilter();
    $('body').css('cursor', 'progress');
    $("#run-filter-btn").html("    ").attr("disabled", true);
    $("#run-filter-btn").removeClass("cancel-search");
    $("#query-builder-btn").html("    ").attr("disabled", true);
    $("#query-builder-btn").removeClass("cancel-search");
    $.ajax({
        method: 'post',
        url: 'promql/api/ui/query',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(data)
    })
        .then((res) => processSearchResult(res, startTime))
        .catch((res) => processSearchError(res));
}

function processSearchResult(res, startTime) {
    if (res.aggStats && Object.keys(res.aggStats).length === 0) {
        processNoResults();
    } else {
        hideError();
        renderResponseTime(startTime);
        var seriesArray = [];
        var label = [];
        $.each(res, function (key, value) {
            var series = value;
            $.each(series, function (key, value) {
                seriesArray.push({ seriesName: key, values: value });
                label = [];
                $.each(value, function (k, v) {
                    label.push(k);
                })
            })
        })
        var labels = label;

        $('body').css('cursor', 'default');
        $("#run-filter-btn").html("  ").attr("disabled", false);
        $("#run-filter-btn").removeClass("cancel-search");
        $("#query-builder-btn").html("  ").attr("disabled", false);
        $("#query-builder-btn").removeClass("cancel-search");
        if (lineChart !== undefined) {
            lineChart.destroy();
            $('#metrics-legends').empty();
        }
        updateContainers();
        displayTable(res);
        lineChart = displayGraph(seriesArray, labels, -1);
        return lineChart;
    }
}

function renderResponseTime(startTime) {
    $('.metrics-response').show();
    let responseTime = (new Date()).getTime() - startTime;
    $('.metrics-response').html(`Response: <span id="response-time">${responseTime} ms</span>`);
}

function displayGraph(seriesArray, labels, flag) {
    const colors = createColorsArray();
    let gridLineColor;
    let tickColor;
    if ($('body').attr('data-theme') == "light") {
        gridLineColor = "#DCDBDF";
        tickColor = "#160F29";
    }
    else {
        gridLineColor = "#383148";
        tickColor = "#FFFFFF"
    }

    let datasets = [];
    let data = {};

    datasets = seriesArray.map((o, i) => ({
        label: o.seriesName,
        data: Object.values(o.values),
        backgroundColor: colors[i],
        borderColor: colors[i],
        color: gridLineColor,
        borderWidth: 2,
        fill: false,
    }));

    data = {
        labels: labels,
        datasets: datasets
    }

    const config = {
        type: 'line',
        data,
        options: {
            maintainAspectRatio: false,
            responsive: true,
            aspectRatio: 2,
            title: {
                display: true,
            },
            plugins: {
                legend: {
                    display: false
                },
                tooltip: {
                    enabled: true,
                }
            },
            scales: {
                x: {
                    grid: {
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                    }
                },
                y: {
                    grid: {
                        color: gridLineColor,
                    },
                    ticks: {
                        color: tickColor,
                    }
                },
            }
        }
    }

    var lineCanvas = $('#metrics-graph').get(0).getContext('2d');

    // if chart is created for the first time after response
    if (flag === -1) {
        lineChart = new Chart(lineCanvas, config);
        displayLegends(seriesArray, labels, colors);
    }

    const bgColor = [];
    const bColor = data.datasets.map(color => {
        bgColor.push(color.backgroundColor);
        return color.borderColor;
    })

    if (flag == -2) {
        lineChart.config.data.datasets.map((dataset, index) => {
            dataset.backgroundColor = bgColor[index];
            dataset.borderColor = bColor[index];
        })
        lineChart.update();
    }

    if (flag >= 0) {
        const chartDataObject = lineChart.config.data.datasets.map(dataset => {
            dataset.borderColor = "rgba(0,0,0,0)";
            dataset.backgroundColor = "rgba(0,0,0,0)";
        })

        lineChart.config.data.datasets[flag].borderColor = bColor[flag];
        lineChart.config.data.datasets[flag].backgroundColor = bgColor[flag];
        lineChart.update();
    }
    return lineChart;
}

function createColorsArray() {
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let colorArray = [];
    for (let i = 1; i <= 20; i++) {
        colorArray.push(rootStyles.getPropertyValue(`--graph-line-color-${i}`));
    }
    return colorArray;
}

function getPrevSelectedElement(el = null) {
    let prevEl = el;
    return prevEl;
}

function displayLegends(seriesArray, labels, colors) {
    $.each(seriesArray, function (k, v) {
        $('#metrics-legends').append(`<div class="legend-element" id="legend-${k}"><span class="legend-colors" style="background-color:` + colors[k] + '"></span>' + v.seriesName + '</div>');
    });

    let prev = null;

    const legends = document.querySelectorAll('.legend-element');
    $.each(legends, function (i, legend) {
        legend.addEventListener('click', (e) => {
            let currSelectedEl = e.target;
            let currSelectedElId = parseInt((e.target.id).slice(7));
            if (currSelectedEl.classList.value == "legend-colors") {
                currSelectedEl = currSelectedEl.closest("div.legend-element")
                currSelectedElId = parseInt(currSelectedEl.id.slice(7))
            }
            if (prev == null) {
                currSelectedEl.classList.add("selected");
                prev = currSelectedElId;
                displayGraph(seriesArray, labels, currSelectedElId);
            } else if (prev == currSelectedElId) {
                currSelectedEl.classList.remove("selected");
                prev = null;
                displayGraph(seriesArray, labels, -2);
            } else {
                let prevEl = document.getElementById(`legend-${prev}`);
                prevEl.classList.remove("selected");
                currSelectedEl.classList.add("selected");
                prev = currSelectedElId;
                displayGraph(seriesArray, labels, currSelectedElId);
            }
        }
        )
    })
}

function showDatePickerHandler(evt) {
    evt.stopPropagation();
    $('#daterangepicker').toggle();
    $(evt.currentTarget).toggleClass('active');
}

function hideDatePickerHandler() {
    $('#daterangepicker').removeClass('active');
}

function resetDatePickerHandler(evt) {
    evt.stopPropagation();
    resetCustomDateRange();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });

}
function getStartDateHandler(evt) {
    let inputDate = new Date(this.value);
    filterStartDate = inputDate.getTime();
    $(this).addClass("active");
    Cookies.set('customStartDate', this.value);

}

function getEndDateHandler(evt) {
    let inputDate = new Date(this.value);
    filterEndDate = inputDate.getTime();
    $(this).addClass("active");
    Cookies.set('customEndDate', this.value);
}

function getStartTimeHandler() {
    let selectedTime = $(this).val();
    let temp = ((Number(selectedTime.split(':')[0]) * 60 + Number(selectedTime.split(':')[1])) * 60) * 1000;
    //check if filterStartDate is a number or now-*
    if (!isNaN(filterStartDate)) {
        filterStartDate = filterStartDate + temp;
    } else {
        let start = new Date();
        start.setUTCHours(0, 0, 0, 0);
        filterStartDate = start.getTime() + temp;
    }
    $(this).addClass("active");
    Cookies.set('customStartTime', selectedTime);
}

function getEndTimeHandler() {
    let selectedTime = $(this).val();
    let temp = ((Number(selectedTime.split(':')[0]) * 60 + Number(selectedTime.split(':')[1])) * 60) * 1000;
    if (!isNaN(filterEndDate)) {
        filterEndDate = filterEndDate + temp;
    } else {
        let start = new Date();
        start.setUTCHours(0, 0, 0, 0);
        filterEndDate = start.getTime() + temp;
    }
    $(this).addClass("active");
    Cookies.set('customEndTime', selectedTime);
}

function customRangeHandler(evtvt) {
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $('#date-picker-btn span').html("Custom");
}

function rangeItemHandler(evt) {
    resetCustomDateRange();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), "now", $(this).attr('id'))
}

function resetCustomDateRange() {
    // clear custom selections
    $('#date-start').val("");
    $('#date-end').val("");
    $('#time-start').val("00:00");
    $('#time-end').val("00:00");
    $('#date-start').removeClass('active');
    $('#date-end').removeClass('active');
    $('#time-start').removeClass('active');
    $('#time-end').removeClass('active');
    Cookies.remove('customStartDate');
    Cookies.remove('customEndDate');
    Cookies.remove('customStartTime');
    Cookies.remove('customEndTime');
}

function themePickerHandler(evt) {
    let newgridLineColor;
    let newTickColor;

    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
    } else {
        Cookies.set('theme', 'light');
        theme = 'light';
    }

    if (theme === 'light') {
        theme = 'dark';
        $(evt.currentTarget).removeClass('dark-theme');
        $(evt.currentTarget).addClass('light-theme');
        newgridLineColor = "#383148";
        newTickColor = "#FFFFFF";
    } else {
        theme = 'light';
        $(evt.currentTarget).removeClass('light-theme');
        $(evt.currentTarget).addClass('dark-theme');
        newgridLineColor = "#DCDBDF";
        newTickColor = "#160F29";
    }
    $('body').attr('data-theme', theme);

    if (lineChart !== undefined) {
        lineChart.config.options.scales.x.grid.color = newgridLineColor;
        lineChart.config.options.scales.y.grid.color = newgridLineColor;
        lineChart.config.options.scales.x.ticks.color = newTickColor;
        lineChart.config.options.scales.y.ticks.color = newTickColor;
        lineChart.update();
    }


    Cookies.set('theme', theme, { expires: 365 });
}

function processSearchError(res) {
    $('#metrics-graph-container').hide();
    $('#metrics-table-container').hide();
    showError(`${res.responseText}`)
}

function processNoResults() {
    $('#metrics-graph-container').hide();
    $('#metrics-table-container').hide();
    showError(`Your query returned no data, adjust your query.`);
}

$(".metrics-graph-btn, .metrics-table-btn").click(function() {
    $(".metrics-graph-btn, .metrics-table-btn").removeClass("active");
    $(this).addClass("active");
    updateContainers();
});

function updateContainers() {
    const isGraphActive = $(".metrics-graph-btn").hasClass("active");
    if ($("#corner-popup").is(":visible")) {
        $("#metrics-graph-container").hide();
        $("#metrics-table-container").hide();
    }else { 
    if (isGraphActive) {
        $("#metrics-graph-container").show();
        $("#metrics-table-container").hide();
    }else {
        $("#metrics-graph-container").hide();
        $("#metrics-table-container").show();
    }}
}

function displayTable(res) {
    let tableBody = $('#metrics-table-container table');
    tableBody.empty(); 
    tableBody.append('<tr><th>Metric</th><th>Value</th></tr>');
    $.each(res.aggStats, function(metric, timestampValues) {
        let metricName = metric;
        let timestamps = Object.keys(timestampValues);
        let lastValue = timestampValues[timestamps[timestamps.length - 1]];

        let newRow = $('<tr>');
        newRow.append($('<td>').text(metricName));
        newRow.append($('<td>').text(lastValue));
        tableBody.append(newRow);
    });
}

window.addEventListener('resize', function () {
    lineChart.resize();
});
