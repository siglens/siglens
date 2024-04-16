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

// @todo - get rid of these global variables

let timestampDateFmt = 'MMM Do, YYYY @ HH:mm:ss';
let defaultColumnCount = 2;
let dataTable = null;
let aggsDataTable = null;
let shouldCloseAllDetails = false;
let filterStartDate = "now-15m";
let filterEndDate = "now";
let displayStart = moment().subtract(15, 'minutes').valueOf();
let displayEnd = moment().valueOf();
let selectedSearchIndex = '*';
let canScrollMore = true;
let scrollFrom = 0;
let totalRrcCount = 0;
let pageScrollPos = 0;
let scrollPageNo = 1;
let availColNames = [];
let startQueryTime;
let renderTime = 0;
let wsState = 'query';
let newUri = null;
let socket = null;
let myUrl = window.location.protocol + "//" + window.location.host + window.location.pathname;
let data = null;
let theme = 'light';
let selectedFieldsList = [];
let updatedSelFieldList = false;
let aggsColumnDefs = [];
let segStatsRowData = [];
let GBCountChart, LogLinesCountChart, TotalVolumeChart;
let queryStr = '*';
let panelChart;
let metricsDatasets;
let liveTailState = false;
let tt;
let lockReconnect = false;
let totalMatchLogs = 0;
let firstBoxSet = new Set();
let secondBoxSet = new Set();
let thirdBoxSet = new Set();
let measureFunctions = [];
let measureInfo = [];
let isTimechart = false;
let isQueryBuilderSearch = false;
let sortByTimestampAtDefault = true;


let aggGridOptions = {
    columnDefs: aggsColumnDefs,
    rowData: [],
    animateRows: true,
    defaultColDef: {
        flex: 1,
        minWidth: 100,
        resizable: true,
        sortable: true,
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-up"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        cellRenderer: params => params.value ? params.value : 'null',
    },
    icons: {
        sortAscending: '<i class="fa fa-sort-alpha-up"/>',
        sortDescending: '<i class="fa fa-sort-alpha-down"/>',
    }
};

{{ .CommonExtraFunctions }}

function showError(errorMsg) {
    $("#logs-result-container").hide();
    $("#agg-result-container").hide();
    $("#data-row-container").hide();
    $('#empty-response').hide();
    let currentTab = $("#custom-chart-tab").tabs("option", "active");
    if (currentTab == 0) {
      $("#logs-view-controls").show();
    } else {
      $("#logs-view-controls").hide();
    }
    $("#custom-chart-tab").show();
    $('#corner-popup .corner-text').html(errorMsg);
    $('#corner-popup').show();
    $('body').css('cursor', 'default');
    $('#run-filter-btn').html(' ');
    $("#run-filter-btn").removeClass("cancel-search");
    $('#run-filter-btn').removeClass('active');
     $("#query-builder-btn").html(" ");
     $("#query-builder-btn").removeClass("cancel-search");
     $("#query-builder-btn").removeClass("active");
    $("#live-tail-btn").html("Live Tail");
    $("#live-tail-btn").removeClass("active");
    $('#run-metrics-query-btn').removeClass('active');

    wsState = 'query';
}

function showInfo(infoMsg) {
    $('#corner-popup .corner-text').html(infoMsg);
    $('#corner-popup').show();
    $('#corner-popup').css('position', 'absolute');
    $('#corner-popup').css('bottom', '3rem');
    $('body').css('cursor', 'default');
    $('#run-filter-btn').html(' ');
    $("#run-filter-btn").removeClass("cancel-search");
    $('#run-filter-btn').removeClass('active');
    $("#query-builder-btn").html(" ");
    $("#query-builder-btn").removeClass("cancel-search");
    $("#query-builder-btn").removeClass("active");
    $("#live-tail-btn").html("Live Tail");
    $("#live-tail-btn").removeClass("active");
    wsState = 'query';
}

function hideError() {
    $('#corner-popup').hide();
}

function decodeJwt(token) {
    let base64Payload = token.split(".")[1];
    let payload = decodeURIComponent(
        atob(base64Payload)
            .split("")
            .map(function (c) {
                return "%" + ("00" + c.charCodeAt(0).toString(16)).slice(-2);
            })
            .join("")
    );
    return JSON.parse(payload);
}

function resetDashboard() {
    resetAvailableFields();
    $("#LogResultsGrid").html('');
    $("#measureAggGrid").html('');
    gridDiv = null;
    eGridDiv = null;
}

function string2Hex(tmp) {
    let str = '';
    for (let i = 0; i < tmp.length; i++) {
        str += tmp[i].charCodeAt(0).toString(16);
    }
    return str;
}

function addQSParm(name, value) {
    let re = new RegExp("([?&]" + name + "=)[^&]+", "");

    function add(sep) {
        myUrl += sep + name + "=" + encodeURIComponent(value);
    }

    function change() {
        myUrl = myUrl.replace(re, "$1" + encodeURIComponent(value));
    }
    if (myUrl.indexOf("?") === -1) {
        add("?");
    } else {
        if (re.test(myUrl)) {
            change();
        } else {
            add("&");
        }
    }
}

function resetQueryResAttr(res, panelId){
    if (panelId == -1 && currentPanel) // if the query has been made from the editPanelScreen
        currentPanel.queryRes = res;
    else {
        for (let i = 0; i < localPanels.length; i++) {
            if (localPanels[i].panelId == panelId) {
                localPanels[i].queryRes = res;
                break;
            }
        }
    }
}

function renderPanelLogsQueryRes(data, panelId, logLinesViewType, res) {
    //if data source is metrics
      if(!res.qtype) {
        panelProcessEmptyQueryResults("Unsupported chart type. Please select a different chart type.",panelId);
        return;
    }
    if(res.hits){
        if (panelId == -1) { // for panel on the editPanelScreen page
            $(".panelDisplay #panelLogResultsGrid").show();
            $(".panelDisplay #empty-response").empty();
            $('.panelDisplay #corner-popup').hide();
            $(".panelDisplay #empty-response").hide();
            $('.panelDisplay .panEdit-panel').hide();
        } else { // for panels on the dashboard page
            $(`#panel${panelId} #panelLogResultsGrid`).show();
            $(`#panel${panelId} #empty-response`).empty();
            $(`#panel${panelId} #corner-popup`).hide();
            $(`#panel${panelId} #empty-response`).hide();
            $(`#panel${panelId} .panEdit-panel`).hide();
        }
        //for aggs-query and segstats-query
        if (res.measure && (res.qtype === "aggs-query" || res.qtype === "segstats-query")) {
            let columnOrder = []
            if (res.groupByCols) {
                columnOrder = _.uniq(_.concat(
                    res.groupByCols));
            }
            if (res.measureFunctions) {
                columnOrder = _.uniq(_.concat(
                    columnOrder, res.measureFunctions));
            }
            renderPanelAggsGrid(columnOrder, res.measure,panelId)
        }//for logs-query
        else if(res.hits && res.hits.records !== null && res.hits.records.length >= 1) {
            renderPanelLogsGrid(res.allColumns, res.hits.records, panelId, logLinesViewType);
        }
        allResultsDisplayed--;
        if(allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
    }
    canScrollMore = res.can_scroll_more;
    scrollFrom = res.total_rrc_count;
    if (res.hits.totalMatched.value === 0 || (!res.bucketCount &&  (res.qtype === "aggs-query" || res.qtype === "segstats-query"))) {
        panelProcessEmptyQueryResults("", panelId);
    }

    wsState = 'query'
    if (canScrollMore === false) {
        scrollFrom = 0;
    }
}

function runPanelLogsQuery(data, panelId,currentPanel,queryRes) {
    return new Promise(function(resolve, reject) {
        $('body').css('cursor', 'progress');
        if (queryRes) {
            renderChartByChartType(data,queryRes,panelId,currentPanel)
        }
        else {
            $.ajax({
                method: 'post',
                url: 'api/search/' + panelId,
                headers: {
                    'Content-Type': 'application/json; charset=utf-8',
                    'Accept': '*/*'
                },
                crossDomain: true,
                dataType: 'json',
                data: JSON.stringify(data)
            })
                .then((res) => {
                    resetQueryResAttr(res, panelId);
                    renderChartByChartType(data,res,panelId,currentPanel);
                    resolve();
                })
                .catch(function (xhr, err) {
                    if (xhr.status === 400) {
                        panelProcessSearchError(xhr, panelId);
                    }currentPanel
                    $('body').css('cursor', 'default');
                    $(`#panel${panelId} .panel-body #panel-loading`).hide();
                    reject();
                })
        }
    })
}

function panelProcessEmptyQueryResults(errorMsg, panelId) {
    let msg;
    if (errorMsg !== "") {
        msg = errorMsg;
    } else {
        msg = "Your query returned no data, adjust your query.";
    }
    if (panelId == -1) {
        $(`.panelDisplay .big-number-display-container`).hide();
        $(`.panelDisplay .big-number-display-container`).empty();
        $('.panelDisplay .panEdit-panel').hide();
        $('#corner-popup').hide();
        $('.panelDisplay #panelLogResultsGrid').hide();
        $('.panelDisplay #empty-response').show();
        let el = $('.panelDisplay #empty-response');
        $('.panelDisplay #empty-response').empty();

        el.append(`<span>${msg}</span>`);
    } else {
        $(`#panel${panelId} #panelLogResultsGrid`).hide();
        $(`#panel${panelId} #empty-response`).show();
        $(`#panel${panelId} #corner-popup`).hide();
        $(`#panel${panelId} .panEdit-panel`).hide();
        $(`#panel${panelId} .big-number-display-container`).hide();
        $(`#panel${panelId} #empty-response`).show();
        let el = $(`#panel${panelId} #empty-response`);
        $(`#panel${panelId} #empty-response`).empty();
        el.append(`<span>${msg}</span>`);
    }
    $('body').css('cursor', 'default');
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}

function panelProcessSearchError(res, panelId) {
    if (res.can_scroll_more === false) {
        showPanelInfo(`You've reached maximum scroll limit (10,000).`);
    }

    if (panelId == -1) {
        $(`.panelDisplay .big-number-display-container`).hide();
        $(`.panelDisplay .big-number-display-container`).empty();
        $('.panelDisplay .panEdit-panel').hide();
        $('.panelDisplay #corner-popup').show();
        $('.panelDisplay #panelLogResultsGrid').hide();
        $('.panelDisplay #empty-response').hide();
        let el = $('.panelDisplay #corner-popup');
        $('.panelDisplay #corner-popup').empty();

        el.append(`<span>${res.responseText}</span>`);
    } else {
        $(`#panel${panelId} #panelLogResultsGrid`).hide();
        $(`#panel${panelId} #empty-response`).show();
        $(`#panel${panelId} #corner-popup`).show();
        $(`#panel${panelId} .panEdit-panel`).hide();
        $(`#panel${panelId} .big-number-display-container`).hide();
        $(`#panel${panelId} #empty-response`).hide();
        let el = $(`#panel${panelId} #corner-popup`);
        $(`#panel${panelId} #corner-popup`).empty();
        el.append(`<span>${res.responseText}</span>`);
    }
    wsState = 'query';

}

function resetPanelContainer(firstQUpdate) {
    if (firstQUpdate) {
        $('#empty-response').hide();
        $('#panelLogResultsGrid').show();
        $(`.panelDisplay .big-number-display-container`).hide();

        hideError();
    }
}

function resetPanelGrid() {
    panelLogsRowData = [];
    panelGridDiv == null
}

function showPanelInfo(infoMsg) {
    $('#corner-popup .corner-text').html(infoMsg);
    $('#corner-popup').show();
    $('#corner-popup').css('position', 'absolute');
    $('#corner-popup').css('bottom', '3rem');
    $('body').css('cursor', 'default');

    wsState = 'query';
}

function getQueryParamsData(scrollingTrigger) {
    let sFrom = 0;
    let queryLanguage = $('.queryInput-container #query-language-btn span').html();

    if (scrollingTrigger) {
        sFrom = scrollFrom;
    }
    let data = {
        'state': wsState,
        'searchText': queryStr,
        'startEpoch': filterStartDate,
        'endEpoch': filterEndDate,
        'indexName': selectedSearchIndex,
        'from': sFrom,
        'queryLanguage' : queryLanguage,
    }
    return data;
}

function getCookie(cname) {
    let name = cname + "=";
    let decodedCookie = decodeURIComponent(document.cookie);
    let ca = decodedCookie.split(';');
    for (let i = 0; i < ca.length; i++) {
        let c = ca[i];
        while (c.charAt(0) == ' ') {
            c = c.substring(1);
        }
        if (c.indexOf(name) == 0) {
            return c.substring(name.length, c.length);
        }
    }
    return "";
}

function renderPanelAggsQueryRes(data, panelId, chartType, dataType, panelIndex, res) {
    resetQueryResAttr(res, panelId);
    //if data source is metrics
    if(!res.qtype && chartType != "number") {
        panelProcessEmptyQueryResults("Unsupported chart type. Please select a different chart type.",panelId)
    }
    if (res.qtype === "logs-query") {
        panelProcessEmptyQueryResults("", panelId);
    }
    
    if (res.qtype === "aggs-query" || res.qtype === "segstats-query") {
        if (panelId == -1) { // for panel on the editPanelScreen page
            $(".panelDisplay #panelLogResultsGrid").hide();
            $(".panelDisplay #empty-response").empty();
            $('.panelDisplay #corner-popup').hide();
            $(".panelDisplay #empty-response").hide();
            $('.panelDisplay .panEdit-panel').show();
            $(`.panelDisplay .big-number-display-container`).empty();
	        $(`.panelDisplay .big-number-display-container`).hide();
        } else { // for panels on the dashboard page
            $(`#panel${panelId} #panelLogResultsGrid`).hide();
            $(`#panel${panelId} #empty-response`).empty();
            $(`#panel${panelId} #corner-popup`).hide();
            $(`#panel${panelId} #empty-response`).hide();
            $(`#panel${panelId} .panEdit-panel`).show();
            $(`.panelDisplay .big-number-display-container`).empty();
	        $(`.panelDisplay .big-number-display-container`).hide();
        }

        let columnOrder = []
        if (res.groupByCols) {
            columnOrder = _.uniq(_.concat(
                res.groupByCols));
        }

        if (res.measureFunctions) {
            columnOrder = _.uniq(_.concat(
                columnOrder, res.measureFunctions));
        }

        if (res.errors) {
            panelProcessEmptyQueryResults(res.errors[0], panelId);
        } else {
            let resultVal;
            if (chartType === "number") {
                resultVal = Object.values(res.measure[0].MeasureVal)[0];
            }

            if ((chartType === "Pie Chart" || chartType === "Bar Chart") && (res.hits.totalMatched === 0 || res.hits.totalMatched.value === 0)) {
                panelProcessEmptyQueryResults("", panelId);
            } else if (chartType === "number" && (resultVal === undefined || resultVal === null)) {
                panelProcessEmptyQueryResults("", panelId);
            } else {
                // for number, bar and pie charts
                if(panelId ===-1)
                    renderPanelAggsGrid(columnOrder, res.measure,panelId);

                panelChart = renderBarChart(columnOrder, res.measure, panelId, chartType, dataType, panelIndex);
            }
        }
        allResultsDisplayed--;
        if(allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
    }
}

function runPanelAggsQuery(data, panelId, chartType, dataType, panelIndex, queryRes) {
    $('body').css('cursor', 'progress');
    if (queryRes) {
        renderPanelAggsQueryRes(data, panelId, chartType, dataType, panelIndex, queryRes)
    } else {
        $.ajax({
            method: 'post',
            url: 'api/search/' + panelId,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                'Accept': '*/*'
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(data)
        })
            .then(function (res) {
                renderPanelAggsQueryRes(data, panelId, chartType, dataType, panelIndex, res)
            })
            .catch(function (xhr, err) {
                if (xhr.status === 400) {
                    panelProcessSearchError(xhr, panelId);
                }
                $('body').css('cursor', 'default');
            })
    }
}

function runMetricsQuery(data, panelId, currentPanel, queryRes) {
    $('body').css('cursor', 'progress');
    if (queryRes) {
        renderChartByChartType(data,queryRes,panelId,currentPanel)
    } else {
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
            .then((res) => {
                renderChartByChartType(data,res,panelId,currentPanel)
            })
            .catch(function (xhr, err) {
                if (xhr.status === 400) {
                    panelProcessSearchError(xhr, panelId);
                }
                $('body').css('cursor', 'default')
            })
    }  
}

function processMetricsSearchResult(res, startTime, panelId, chartType, panelIndex,dataType) {
    resetQueryResAttr(res, panelId);
    let bigNumVal = null;
    if (panelId == -1) { // for panel on the editPanelScreen page
        $(".panelDisplay #panelLogResultsGrid").hide();
        $(".panelDisplay #empty-response").empty();
        $('.panelDisplay #corner-popup').hide();
        $(".panelDisplay #empty-response").hide();
        $('.panelDisplay .panEdit-panel').show();
    } else { // for panels on the dashboard page
        $(`#panel${panelId} #panelLogResultsGrid`).hide();
        $(`#panel${panelId} #empty-response`).empty();
        $(`#panel${panelId} #corner-popup`).hide();
        $(`#panel${panelId} #empty-response`).hide();
        $(`#panel${panelId} .panEdit-panel`).show();
    }

    if (res.aggStats && Object.keys(res.aggStats).length === 0) {
        panelProcessEmptyQueryResults("", panelId);
        $('body').css('cursor', 'default');
	    $(`#panel${panelId} .panel-body #panel-loading`).hide();
    } else {
        if (chartType === 'number'){
            $.each(res, function (key, value) {
                var series = value;
                $.each(series, function (key, value) {
                    var tsmap = value
                    $.each(tsmap, function (key, value) {
                        if (value > 0){
                            bigNumVal = value
                        }
                    })
                })         
            });   
            if(bigNumVal === undefined || bigNumVal === null){
                panelProcessEmptyQueryResults("", panelId);
            }else{
                displayBigNumber(bigNumVal.toString(), panelId, dataType, panelIndex);
                allResultsDisplayed--;
                if(allResultsDisplayed <= 0 || panelId === -1) {
                    $('body').css('cursor', 'default');
                }
                $(`#panel${panelId} .panel-body #panel-loading`).hide();   
            } 
        } else {
            hideError();
            const colors = createMetricsColorsArray();
            let seriesArray = [];
            let label = [];
            $.each(res, function (key, value) {
                var series = value;
                $.each(series, function (key, value) {
                    seriesArray.push({ seriesName: key, values: value });
                    label = [];
                    $.each(value, function (k, v) {
                        label.push(k);
                    })
                })
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
                metricsDatasets = seriesArray.map((o, i) => {
                    return {
                        name: o.seriesName,
                        data: Object.values(o.values),
                        type: chartType,
                        lineStyle: {
                            color: colors[i],
                            width: 2,
                            type: 'solid',
                            backgroundColor: colors[i],
                            borderColor: colors[i],
                        }, itemStyle: {
                            color: colors[i],
                            width: 2,
                            type: 'solid',
                            backgroundColor: colors[i],
                            borderColor: colors[i],
                        },
                    }
                }
                );
            })
            let labels = label;

            renderLineChart(seriesArray, metricsDatasets, labels, panelId, chartType, -1);
            allResultsDisplayed--;
            if(allResultsDisplayed <= 0 || panelId === -1) {
                $('body').css('cursor', 'default');
            }
        }
    }
}

function processMetricsSearchError() {
    showError(`Your query returned no data, adjust your query.`);
}

function createMetricsColorsArray() {
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let colorArray = [];
    for (let i = 1; i <= 20; i++) {
        colorArray.push(rootStyles.getPropertyValue(`--graph-line-color-${i}`));
    }
    return colorArray;
}

function loadCustomDateTimeFromEpoch(startEpoch, endEpoch) {
    let dateVal = new Date(startEpoch);
    $('#date-start').val(dateVal.toISOString().substring(0,10));
    $('#date-start').addClass('active');
    $('.panelEditor-container #date-start').val(dateVal.toISOString().substring(0,10));
    $('.panelEditor-container #date-start').addClass('active');
    let hours = addZero(dateVal.getUTCHours());
    let mins = addZero(dateVal.getUTCMinutes());
    $('#time-start').val(hours+':'+mins);
    $('#time-start').addClass('active');
    $('.panelEditor-container #time-start').val(hours+':'+mins);
    $('.panelEditor-container #time-start').addClass('active');

    dateVal = new Date(endEpoch);
    $('#date-end').val(dateVal.toISOString().substring(0,10));
    $('#date-end').addClass('active');
    $('.panelEditor-container #date-end').val(dateVal.toISOString().substring(0,10));
    $('.panelEditor-container #date-end').addClass('active');
    hours = addZero(dateVal.getUTCHours());
    mins = addZero(dateVal.getUTCMinutes());
    $('#time-end').val(hours+':'+mins);
    $('#time-end').addClass('active');
    $('.panelEditor-container #time-end').val(hours+':'+mins);
    $('.panelEditor-container #time-end').addClass('active');
}

function addZero(i) {
    if (i < 10) {i = "0" + i}
    return i;
}

function setTimePicker() {
    // set time picker of next page with updated time
    let stDate = Cookies.get('startEpoch') || "now-1h";
    let endDate = Cookies.get('endEpoch') || "now";
    if (stDate && endDate) {
        if(endDate === "now") {
            filterStartDate = stDate;
            filterEndDate =  endDate;
            $('.inner-range #' + filterStartDate).addClass('active');
            datePickerHandler(filterStartDate, filterEndDate, filterStartDate)
        } else {
            if (!isNaN(stDate)) {
                stDate = Number(stDate);
                endDate = Number(endDate);
                datePickerHandler(stDate, endDate, "custom");
                loadCustomDateTimeFromEpoch(stDate, endDate);
                filterStartDate = stDate;
                filterEndDate =  endDate;
            }
        }
    }
}

// my org page
function showToastMyOrgPage(msg) {
    let toast =
        `<div class="div-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}

function showSendTestDataUpdateToast(msg) {
    let toast =
        `<div class="test-data-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}

function showSendTestDataUpdateToast(msg) {
    let toast =
        `<div class="test-data-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}

function removeToast() {
    $('.div-toast').remove();
    $('.test-data-toast').remove();
    $('.ret-days-toast').remove();
    $('.usage-stats-toast').remove();
}

function showDeleteIndexToast(msg) {
    let toast =
        `<div class="usage-stats-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('.index-header').append(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}

function showRetDaysUpdateToast(msg) {
    let toast =
        `<div class="ret-days-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}





function myOrgSendTestData(token) {
    $('#test-data-btn').on('click', (e) => {
        if (selectedLogSource === 'Send Test Data') {
            var testDataBtn = document.getElementById("test-data-btn");
            // Disable testDataBtn
            testDataBtn.disabled = true;
            sendTestData(e, token);
        }
        else {
            showSendTestDataUpdateToast('Select Test Data');
        }
    })
}

function getIngestUrl() {
    return new Promise((resolve, reject) => {
        $.ajax({
            method: 'get',
            url: '/api/config',
            crossDomain: true,
            dataType: 'json',
            credentials: 'include'
        })
        .then((res) => {
            resolve(res.IngestUrl);
        })
        .catch((err) => {
            console.log(err);
            reject(err);
        });
    });
}

//renders the response from logs or metrics query to respective selected chart type
function renderChartByChartType(data,queryRes,panelId,currentPanel){
    if(!currentPanel.chartType){
        panelProcessEmptyQueryResults("Please select a suitable chart type.",panelId)
    }
    switch (currentPanel.chartType) {
        case "Data Table":
        case "loglines":
            $('.panelDisplay .panEdit-panel').hide();
            renderPanelLogsQueryRes(data, panelId,currentPanel.logLinesViewType,queryRes);
            break;
        case "Bar Chart":
        case "Pie Chart":
            renderPanelAggsQueryRes(data, panelId, currentPanel.chartType, currentPanel.dataType, currentPanel.panelIndex, queryRes)
            break;
        case "Line Chart":
            let startTime = (new Date()).getTime();
            processMetricsSearchResult(queryRes, startTime, panelId, currentPanel.chartType, currentPanel.panelIndex,"")
            break;
        case "number":
            
            if (currentPanel.unit === "" || currentPanel.dataType === "none" || currentPanel.dataType === ""){
                currentPanel.unit = "misc"
                currentPanel.dataType = "none"
            }
            if (currentPanel.queryType == 'metrics'){
                let startTime = (new Date()).getTime();
                processMetricsSearchResult(queryRes, startTime, panelId, currentPanel.chartType, currentPanel.panelIndex,currentPanel.dataType)
            }else{
                renderPanelAggsQueryRes(data, panelId, currentPanel.chartType, currentPanel.dataType, currentPanel.panelIndex, queryRes)
            }
            break;
    }
}
