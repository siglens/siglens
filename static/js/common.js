/*
 * Copyright (c) 2021-2024 SigScalr, Inc.
 *
 * This file is part of SigLens Observability Solution
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */

// @todo - get rid of these global variables
/*eslint-disable*/
let timestampDateFmt = 'MMM Do, YYYY @ HH:mm:ss';
let defaultColumnCount = 2;
// let dataTable = null;
// let aggsDataTable = null;
let shouldCloseAllDetails = false;
let filterStartDate = 'now-15m';
let filterEndDate = 'now';
let displayStart = moment().subtract(15, 'minutes').valueOf();
let displayEnd = moment().valueOf();
let selectedSearchIndex = '';
let canScrollMore = true;
let scrollFrom = 0;
let totalRrcCount = 0;
let pageScrollPos = 0;
let scrollPageNo = 1;
let currentPanel;
let availColNames = [];
let startQueryTime;
let renderTime = 0;
let wsState = 'query';
let newUri = null;
let socket = null;
let myUrl = window.location.protocol + '//' + window.location.host + window.location.pathname;
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
let defaultDashboardIds = ['10329b95-47a8-48df-8b1d-0a0a01ec6c42', 'a28f485c-4747-4024-bb6b-d230f101f852', 'bd74f11e-26c8-4827-bf65-c0b464e1f2a4', '53cb3dde-fd78-4253-808c-18e4077ef0f1'];
let initialSearchData = {};

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
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        cellRenderer: (params) => (params.value ? params.value : 'null'),
    },
    icons: {
        sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
        sortDescending: '<i class="fa fa-sort-alpha-down"/>',
    },
};
/*eslint-enable*/
{{ .CommonExtraFunctions }}

function showError(errorMsg) {
    $('#logs-result-container').hide();
    $('#agg-result-container').hide();
    $('#data-row-container').hide();
    $('#empty-response').hide();
    $('#initial-response').hide();
    let currentTab = $('#custom-chart-tab').tabs('option', 'active');
    if (currentTab == 0) {
        $('#save-query-div').children().show();
        $('#views-container').show();
    } else {
        $('#save-query-div').children().hide();
        $('#views-container').show();
    }
    $('#custom-chart-tab').show().css({ height: '100%' });
    $('#corner-popup .corner-text').html(errorMsg);
    $('#corner-popup').show();
    $('body').css('cursor', 'default');
    $('#run-filter-btn').html(' ');
    $('#run-filter-btn').removeClass('cancel-search');
    $('#run-filter-btn').removeClass('active');
    $('#query-builder-btn').html(' ');
    $('#query-builder-btn').removeClass('cancel-search');
    $('#query-builder-btn').removeClass('active');
    $('#live-tail-btn').html('Live Tail');
    $('#live-tail-btn').removeClass('active');
    $('#run-metrics-query-btn').removeClass('active');

    wsState = 'query';
}
//eslint-disable-next-line no-unused-vars
function showInfo(infoMsg) {
    $('#corner-popup .corner-text').html(infoMsg);
    $('#corner-popup').show();
    $('#corner-popup').css('position', 'absolute');
    $('#corner-popup').css('bottom', '3rem');
    $('body').css('cursor', 'default');
    $('#run-filter-btn').html(' ');
    $('#run-filter-btn').removeClass('cancel-search');
    $('#run-filter-btn').removeClass('active');
    $('#query-builder-btn').html(' ');
    $('#query-builder-btn').removeClass('cancel-search');
    $('#query-builder-btn').removeClass('active');
    $('#live-tail-btn').html('Live Tail');
    $('#live-tail-btn').removeClass('active');
    wsState = 'query';
}

function hideError() {
    $('#corner-popup').hide();
}

function hideCornerPopupError() {
    let message = $('.corner-text').text();
    $('#corner-popup').hide();
    $('#progress-div').html(``);
    $('#record-searched').html(``);
    processEmptyQueryResults(message);
}
//eslint-disable-next-line no-unused-vars
function decodeJwt(token) {
    let base64Payload = token.split('.')[1];
    let payload = decodeURIComponent(
        atob(base64Payload)
            .split('')
            .map(function (c) {
                return '%' + ('00' + c.charCodeAt(0).toString(16)).slice(-2);
            })
            .join('')
    );
    return JSON.parse(payload);
}
//eslint-disable-next-line no-unused-vars
function resetDashboard() {
    resetAvailableFields();
    $('#LogResultsGrid').html('');
    $('#measureAggGrid').html('');
    gridDiv = null;
    eGridDiv = null;
}
//eslint-disable-next-line no-unused-vars
function string2Hex(tmp) {
    let str = '';
    for (let i = 0; i < tmp.length; i++) {
        str += tmp[i].charCodeAt(0).toString(16);
    }
    return str;
}
//eslint-disable-next-line no-unused-vars
function addQSParm(name, value) {
    let re = new RegExp('([?&]' + name + '=)[^&]+', '');

    function add(sep) {
        myUrl += sep + name + '=' + encodeURIComponent(value);
    }

    function change() {
        myUrl = myUrl.replace(re, '$1' + encodeURIComponent(value));
    }
    if (myUrl.indexOf('?') === -1) {
        add('?');
    } else {
        if (re.test(myUrl)) {
            change();
        } else {
            add('&');
        }
    }
}

function resetQueryResAttr(res, panelId) {
    if (panelId == -1 && currentPanel)
        // if the query has been made from the editPanelScreen
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

function renderPanelLogsQueryRes(data, panelId, currentPanel, res) {
    //if data source is metrics
    if (!res.qtype) {
        panelProcessEmptyQueryResults('Unsupported chart type. Please select a different chart type.', panelId);
        return;
    }
    if (res.hits) {
        if (panelId == -1) {
            // for panel on the editPanelScreen page
            $('.panelDisplay #panelLogResultsGrid').show();
            $('.panelDisplay #empty-response').empty();
            $('.panelDisplay #corner-popup').hide();
            $('.panelDisplay #empty-response').hide();
            $('.panelDisplay .panEdit-panel').hide();
        } else {
            // for panels on the dashboard page
            $(`#panel${panelId} #panelLogResultsGrid`).show();
            $(`#panel${panelId} #empty-response`).empty();
            $(`#panel${panelId} #corner-popup`).hide();
            $(`#panel${panelId} #empty-response`).hide();
            $(`#panel${panelId} .panEdit-panel`).hide();
        }
        //for aggs-query and segstats-query
        if (res.measure && (res.qtype === 'aggs-query' || res.qtype === 'segstats-query')) {
            let columnOrder = [];
            if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
                columnOrder = res.columnsOrder;
            } else {
                if (res.groupByCols) {
                    columnOrder = _.uniq(_.concat(res.groupByCols));
                }
                if (res.measureFunctions) {
                    columnOrder = _.uniq(_.concat(columnOrder, res.measureFunctions));
                }
            }
            renderPanelAggsGrid(columnOrder, res, panelId);
        } //for logs-query
        else if (res.hits && res.hits.records !== null && res.hits.records.length >= 1) {
            let columnOrder = [];
            if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
                columnOrder = res.columnsOrder;
            } else {
                columnOrder = res.allColumns;
            }
            if (currentPanel.selectedFields) {
                selectedFieldsList = currentPanel.selectedFields;
            } else {
                selectedFieldsList = columnOrder;
            }
            renderAvailableFields(columnOrder);
            renderPanelLogsGrid(columnOrder, res.hits.records, panelId, currentPanel);
        }
        allResultsDisplayed--;
        if (allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
    }
    canScrollMore = res.can_scroll_more;
    scrollFrom = res.total_rrc_count;
    if (res.hits.totalMatched.value === 0 || (!res.bucketCount && (res.qtype === 'aggs-query' || res.qtype === 'segstats-query'))) {
        panelProcessEmptyQueryResults('', panelId);
    }

    wsState = 'query';
    if (canScrollMore === false) {
        scrollFrom = 0;
    }
}

function fetchLogsPanelData(data, panelId) {
    return $.ajax({
        method: 'post',
        url: 'api/search/' + panelId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(data),
    });
}

//eslint-disable-next-line no-unused-vars
function runPanelLogsQuery(data, panelId, currentPanel, queryRes) {
    return new Promise(function (resolve, reject) {
        $('body').css('cursor', 'progress');

        if (queryRes) {
            renderChartByChartType(data, queryRes, panelId, currentPanel);
            $('body').css('cursor', 'default');
        } else {
            fetchLogsPanelData(data, panelId)
                .then((res) => {
                    resetQueryResAttr(res, panelId);
                    renderChartByChartType(data, res, panelId, currentPanel);
                    resolve();
                })
                .catch(function (xhr, _err) {
                    if (xhr.status === 400) {
                        panelProcessSearchError(xhr, panelId);
                    }
                    $('body').css('cursor', 'default');
                    $(`#panel${panelId} .panel-body #panel-loading`).hide();
                    reject();
                });
        }
    });
}

function panelProcessEmptyQueryResults(errorMsg, panelId) {
    let msg;
    if (errorMsg !== '') {
        msg = errorMsg;
    } else {
        msg = 'Your query returned no data, adjust your query.';
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
//eslint-disable-next-line no-unused-vars
function resetPanelContainer(firstQUpdate) {
    if (firstQUpdate) {
        $('#empty-response').hide();
        $('#panelLogResultsGrid').show();
        $(`.panelDisplay .big-number-display-container`).hide();

        hideError();
    }
}
//eslint-disable-next-line no-unused-vars
function resetPanelGrid() {
    panelLogsRowData = [];
    panelGridDiv == null;
}

function showPanelInfo(infoMsg) {
    $('#corner-popup .corner-text').html(infoMsg);
    $('#corner-popup').show();
    $('#corner-popup').css('position', 'absolute');
    $('#corner-popup').css('bottom', '3rem');
    $('body').css('cursor', 'default');

    wsState = 'query';
}
//eslint-disable-next-line no-unused-vars
function getQueryParamsData(scrollingTrigger) {
    let sFrom = 0;
    let queryLanguage = $('#query-language-options .query-language-option.active').html();

    if (scrollingTrigger) {
        sFrom = scrollFrom;
    }

    isQueryBuilderSearch = $('#custom-code-tab').tabs('option', 'active') === 0;
    if (isQueryBuilderSearch) {
        queryStr = getQueryBuilderCode();
        queryMode = 'Builder';
    } else {
        queryStr = $('#filter-input').val();
        queryMode = 'Code';
    }

    let data = {
        state: wsState,
        searchText: queryStr,
        startEpoch: filterStartDate,
        endEpoch: filterEndDate,
        indexName: selectedSearchIndex,
        from: sFrom,
        queryLanguage: queryLanguage,
        queryMode: queryMode,
    };
    return data;
}
//eslint-disable-next-line no-unused-vars
function getCookie(cname) {
    let name = cname + '=';
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
    return '';
}

function renderPanelAggsQueryRes(data, panelId, chartType, dataType, panelIndex, res) {
    resetQueryResAttr(res, panelId);
    //if data source is metrics
    if (!res.qtype && chartType != 'number') {
        panelProcessEmptyQueryResults('Unsupported chart type. Please select a different chart type.', panelId);
    }
    if (res.qtype === 'logs-query') {
        panelProcessEmptyQueryResults('', panelId);
    }

    if (res.qtype === 'aggs-query' || res.qtype === 'segstats-query') {
        if (panelId == -1) {
            // for panel on the editPanelScreen page
            $('.panelDisplay #panelLogResultsGrid').hide();
            $('.panelDisplay #empty-response').empty();
            $('.panelDisplay #corner-popup').hide();
            $('.panelDisplay #empty-response').hide();
            $('.panelDisplay .panEdit-panel').show();
            $(`.panelDisplay .big-number-display-container`).empty();
            $(`.panelDisplay .big-number-display-container`).hide();
        } else {
            // for panels on the dashboard page
            $(`#panel${panelId} #panelLogResultsGrid`).hide();
            $(`#panel${panelId} #empty-response`).empty();
            $(`#panel${panelId} #corner-popup`).hide();
            $(`#panel${panelId} #empty-response`).hide();
            $(`#panel${panelId} .panEdit-panel`).show();
            $(`.panelDisplay .big-number-display-container`).empty();
            $(`.panelDisplay .big-number-display-container`).hide();
        }

        let columnOrder = [];
        if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
            columnOrder = res.columnsOrder;
        } else {
            if (res.groupByCols) {
                columnOrder = _.uniq(_.concat(res.groupByCols));
            }
            if (res.measureFunctions) {
                columnOrder = _.uniq(_.concat(columnOrder, res.measureFunctions));
            }
        }
        if (res.errors) {
            panelProcessEmptyQueryResults(res.errors[0], panelId);
        } else {
            let resultVal;
            if (chartType === 'number') {
                resultVal = Object.values(res.measure[0].MeasureVal)[0];
            }

            if ((chartType === 'Pie Chart' || chartType === 'Bar Chart') && (res.hits.totalMatched === 0 || res.hits.totalMatched.value === 0)) {
                panelProcessEmptyQueryResults('', panelId);
            } else if (chartType === 'number' && (resultVal === undefined || resultVal === null)) {
                panelProcessEmptyQueryResults('', panelId);
            } else {
                // for number, bar and pie charts
                if (panelId === -1) renderPanelAggsGrid(columnOrder, res, panelId);
                panelChart = renderBarChart(columnOrder, res, panelId, chartType, dataType, panelIndex);
            }
        }
        allResultsDisplayed--;
        if (allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
    }
}
//eslint-disable-next-line no-unused-vars
function runPanelAggsQuery(data, panelId, chartType, dataType, panelIndex, queryRes) {
    $('body').css('cursor', 'progress');
    if (queryRes) {
        renderPanelAggsQueryRes(data, panelId, chartType, dataType, panelIndex, queryRes);
    } else {
        $.ajax({
            method: 'post',
            url: 'api/search/' + panelId,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(data),
        })
            .then(function (res) {
                renderPanelAggsQueryRes(data, panelId, chartType, dataType, panelIndex, res);
            })
            .catch(function (xhr, _err) {
                if (xhr.status === 400) {
                    panelProcessSearchError(xhr, panelId);
                }
                $('body').css('cursor', 'default');
            });
    }
}

async function runMetricsQuery(data, panelId, currentPanel, _queryRes) {
    $('body').css('cursor', 'progress');
    if (panelId == -1) {
        // for panel on the editPanelScreen page
        $('.panelDisplay #panelLogResultsGrid').hide();
        $('.panelDisplay #empty-response').empty();
        $('.panelDisplay #corner-popup').hide();
        $('.panelDisplay #empty-response').hide();
        $('.panelDisplay .panEdit-panel').show();
    } else {
        // for panels on the dashboard page
        $(`#panel${panelId} #panelLogResultsGrid`).hide();
        $(`#panel${panelId} #empty-response`).empty();
        $(`#panel${panelId} #corner-popup`).hide();
        $(`#panel${panelId} #empty-response`).hide();
        $(`#panel${panelId} .panEdit-panel`).show();
    }
    let chartType = currentPanel.chartType;
    if (chartType === 'number') {
        let bigNumVal = null;
        let dataType = currentPanel.dataType;
        let rawTimeSeriesData;
        for (const queryData of data.queriesData) {
            rawTimeSeriesData = await fetchTimeSeriesData(queryData);
            const parsedQueryObject = parsePromQL(queryData.queries[0]);
            await addQueryElementForAlertAndPanel(queryData.queries[0].name, parsedQueryObject);
        }
        $.each(rawTimeSeriesData.values, function (_index, valueArray) {
            $.each(valueArray, function (_index, value) {
                if (value > bigNumVal) {
                    bigNumVal = value;
                }
            });
        });
        if (bigNumVal === undefined || bigNumVal === null) {
            panelProcessEmptyQueryResults('', panelId);
        } else {
            displayBigNumber(bigNumVal.toString(), panelId, dataType, panelIndex);
            allResultsDisplayed--;
            if (allResultsDisplayed <= 0 || panelId === -1) {
                $('body').css('cursor', 'default');
            }
            $(`#panel${panelId} .panel-body #panel-loading`).hide();
        }
    } else {
        chartDataCollection = {};
        if (panelId === -1) {
            formulas = {};
            // for panel on the editPanelScreen page
            for (const queryData of data.queriesData) {
                const parsedQueryObject = parsePromQL(queryData.queries[0]);
                await addQueryElementForAlertAndPanel(queryData.queries[0].name, parsedQueryObject);
            }
            for (const formulaData of data.formulasData) {
                let uniqueId = generateUniqueId();
                addMetricsFormulaElement(uniqueId, formulaData.formulas[0].formula);
            }
        } else {
            // for panels on the dashboard page
            for (const queryData of data.queriesData) {
                const rawTimeSeriesData = await fetchTimeSeriesData(queryData);
                const chartData = await convertDataForChart(rawTimeSeriesData);
                const queryString = queryData.queries[0].query;
                addVisualizationContainer(queryData.queries[0].name, chartData, queryString, panelId);
            }

            for (const formulaData of data.formulasData) {
                const rawTimeSeriesData = await fetchTimeSeriesData(formulaData);
                const chartData = await convertDataForChart(rawTimeSeriesData);
                let formulaString = formulaData.formulas[0].formula;
                // Replace a, b, etc., with actual query values
                formulaData.queries.forEach((query) => {
                    const regex = new RegExp(`\\b${query.name}\\b`, 'g');
                    formulaString = formulaString.replace(regex, query.query);
                });
                addVisualizationContainer(formulaData.formulas[0].formula, chartData, formulaString, panelId);
            }
        }
        $(`#panel${panelId} .panel-body #panel-loading`).hide();
        allResultsDisplayed--;
        if (allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
        $('body').css('cursor', 'default');
    }
}

function processMetricsSearchResult(res, startTime, panelId, chartType, panelIndex, dataType) {
    resetQueryResAttr(res, panelId);
    let bigNumVal = null;
    if (panelId == -1) {
        // for panel on the editPanelScreen page
        $('.panelDisplay #panelLogResultsGrid').hide();
        $('.panelDisplay #empty-response').empty();
        $('.panelDisplay #corner-popup').hide();
        $('.panelDisplay #empty-response').hide();
        $('.panelDisplay .panEdit-panel').show();
    } else {
        // for panels on the dashboard page
        $(`#panel${panelId} #panelLogResultsGrid`).hide();
        $(`#panel${panelId} #empty-response`).empty();
        $(`#panel${panelId} #corner-popup`).hide();
        $(`#panel${panelId} #empty-response`).hide();
        $(`#panel${panelId} .panEdit-panel`).show();
    }

    if (res.series && res.series.length === 0) {
        panelProcessEmptyQueryResults('', panelId);
        allResultsDisplayed--;
        $('body').css('cursor', 'default');
        $(`#panel${panelId} .panel-body #panel-loading`).hide();
    } else {
        if (chartType === 'number') {
            $.each(res.values, function (index, valueArray) {
                $.each(valueArray, function (index, value) {
                    if (value > bigNumVal) {
                        bigNumVal = value;
                    }
                });
            });
            if (bigNumVal === undefined || bigNumVal === null) {
                panelProcessEmptyQueryResults('', panelId);
            } else {
                displayBigNumber(bigNumVal.toString(), panelId, dataType, panelIndex);
                allResultsDisplayed--;
                if (allResultsDisplayed <= 0 || panelId === -1) {
                    $('body').css('cursor', 'default');
                }
                $(`#panel${panelId} .panel-body #panel-loading`).hide();
            }
        } else {
            hideError();
            let seriesArray = [];
            if (Object.prototype.hasOwnProperty.call(res, 'series') && Object.prototype.hasOwnProperty.call(res, 'timestamps') && Object.prototype.hasOwnProperty.call(res, 'values')) {
                for (let i = 0; i < res.series.length; i++) {
                    let series = {
                        seriesName: res.series[i],
                        values: {},
                    };

                    for (let j = 0; j < res.timestamps.length; j++) {
                        // Convert epoch seconds to milliseconds by multiplying by 1000
                        let timestampInMilliseconds = res.timestamps[j] * 1000;
                        let localDate = new Date(timestampInMilliseconds);
                        let formattedDate = localDate.toLocaleString();

                        series.values[formattedDate] = res.values[i][j];
                    }

                    seriesArray.push(series);
                }
            }
            renderLineChart(seriesArray, panelId);
            allResultsDisplayed--;
            if (allResultsDisplayed <= 0 || panelId === -1) {
                $('body').css('cursor', 'default');
            }
        }
    }
}
//eslint-disable-next-line no-unused-vars
function processMetricsSearchError() {
    showError(`Your query returned no data, adjust your query.`);
}
//eslint-disable-next-line no-unused-vars
function createMetricsColorsArray() {
    let root = document.querySelector(':root');
    let rootStyles = getComputedStyle(root);
    let colorArray = [];
    for (let i = 1; i <= 20; i++) {
        colorArray.push(rootStyles.getPropertyValue(`--graph-line-color-${i}`));
    }
    return colorArray;
}
//eslint-disable-next-line no-unused-vars
function loadCustomDateTimeFromEpoch(startEpoch, endEpoch) {
    let dateVal = new Date(startEpoch);
    $('#date-start').val(dateVal.toISOString().substring(0, 10));
    $('#date-start').addClass('active');
    $('.panelEditor-container #date-start').val(dateVal.toISOString().substring(0, 10));
    $('.panelEditor-container #date-start').addClass('active');
    let hours = addZero(dateVal.getUTCHours());
    let mins = addZero(dateVal.getUTCMinutes());
    $('#time-start').val(hours + ':' + mins);
    $('#time-start').addClass('active');
    $('.panelEditor-container #time-start').val(hours + ':' + mins);
    $('.panelEditor-container #time-start').addClass('active');

    dateVal = new Date(endEpoch);
    $('#date-end').val(dateVal.toISOString().substring(0, 10));
    $('#date-end').addClass('active');
    $('.panelEditor-container #date-end').val(dateVal.toISOString().substring(0, 10));
    $('.panelEditor-container #date-end').addClass('active');
    hours = addZero(dateVal.getUTCHours());
    mins = addZero(dateVal.getUTCMinutes());
    $('#time-end').val(hours + ':' + mins);
    $('#time-end').addClass('active');
    $('.panelEditor-container #time-end').val(hours + ':' + mins);
    $('.panelEditor-container #time-end').addClass('active');
}

function addZero(i) {
    if (i < 10) {
        i = '0' + i;
    }
    return i;
}

// my org page
//eslint-disable-next-line no-unused-vars
function showToastMyOrgPage(msg) {
    let toast = `<div class="div-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`;
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}
//eslint-disable-next-line no-unused-vars
function showSendTestDataUpdateToast(msg) {
    let toast = `<div class="test-data-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`;
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
//eslint-disable-next-line no-unused-vars
function showDeleteIndexToast(msg) {
    let toast = `<div class="usage-stats-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`;
    $('#logs-stats-header').append(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}
//eslint-disable-next-line no-unused-vars
function showRetDaysUpdateToast(msg) {
    let toast = `<div class="ret-days-toast">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`;
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast);
    setTimeout(removeToast, 3000);
}
//eslint-disable-next-line no-unused-vars
function showToast(msg, type = 'error') {
    let toastTypeClass = type === 'success' ? 'toast-success' : 'toast-error';
    let toast = `
        <div class="${toastTypeClass}" id="message-toast"> 
            <button type="button" aria-label="Close" class="toast-close">×</button>
            ${msg}
            <div class="toast-buttons">
                <button type="button" class="toast-ok btn">OK</button>
            </div>
        </div>`;

    $('body').prepend(toast);

    if (type === 'success') {
        setTimeout(removeToast, 3000);
    }
    $('.toast-close').on('click', removeToast);
    $('.toast-ok').on('click', removeToast);

    function removeToast() {
        $('#message-toast').remove();
    }
}

//eslint-disable-next-line no-unused-vars
function getIngestUrl() {
    return new Promise((resolve, reject) => {
        $.ajax({
            method: 'get',
            url: '/api/config',
            crossDomain: true,
            dataType: 'json',
            credentials: 'include',
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
function renderChartByChartType(data, queryRes, panelId, currentPanel) {
    if (!currentPanel.chartType) {
        panelProcessEmptyQueryResults('Please select a suitable chart type.', panelId);
    }
    switch (currentPanel.chartType) {
        case 'Data Table':
        case 'loglines':
            $('.panelDisplay .panEdit-panel').hide();
            renderPanelLogsQueryRes(data, panelId, currentPanel, queryRes);
            break;
        case 'Bar Chart':
        case 'Pie Chart':
            renderPanelAggsQueryRes(data, panelId, currentPanel.chartType, currentPanel.dataType, currentPanel.panelIndex, queryRes);
            break;
        case 'Line Chart': {
            let startTime = new Date().getTime();
            processMetricsSearchResult(queryRes, startTime, panelId, currentPanel.chartType, currentPanel.panelIndex, '');
            break;
        }
        case 'number':
            if (currentPanel.unit === '' || currentPanel.dataType === 'none' || currentPanel.dataType === '') {
                currentPanel.unit = 'misc';
                currentPanel.dataType = 'none';
            }
            if (currentPanel.queryType == 'metrics') {
                runMetricsQuery(data, panelId, currentPanel);
            } else {
                renderPanelAggsQueryRes(data, panelId, currentPanel.chartType, currentPanel.dataType, currentPanel.panelIndex, queryRes);
            }
            break;
    }
}
//eslint-disable-next-line no-unused-vars
function findColumnIndex(columnsMap, columnName) {
    // Iterate over the Map entries
    for (const [index, name] of columnsMap.entries()) {
        if (name === columnName) {
            return index; // Return the index if the column name matches
        }
    }
    return -1; // Return -1 if the column name is not found
}
//eslint-disable-next-line no-unused-vars
function setIndexDisplayValue(selectedSearchIndex) {
    if (selectedSearchIndex) {
        // Remove all existing selected indexes
        $('.index-container .selected-index').remove();
        const selectedIndexes = selectedSearchIndex.split(',');
        selectedIndexes.forEach(function (index) {
            addSelectedIndex(index);
            // Remove the selectedSearchIndex from indexValues
            const indexIndex = indexValues.indexOf(index);
            if (indexIndex !== -1) {
                indexValues.splice(indexIndex, 1);
            }
        });
    }
}
//eslint-disable-next-line no-unused-vars
function displayQueryLangToolTip(selectedQueryLangID) {
    $('#info-icon-sql, #info-icon-logQL, #info-icon-spl').hide();
    switch (selectedQueryLangID) {
        case '1':
        case 1:
            $('#info-icon-sql').show();
            $('#filter-input').attr('placeholder', "Enter your SQL query here, or click the 'i' icon for examples");
            break;
        case '2':
        case 2:
            $('#info-icon-logQL').show();
            $('#filter-input').attr('placeholder', "Enter your LogQL query here, or click the 'i' icon for examples");
            break;
        case '3':
        case 3:
            $('#info-icon-spl').show();
            $('#filter-input').attr('placeholder', "Enter your SPL query here, or click the 'i' icon for examples");
            break;
    }
}
function toggleClearButtonVisibility() {
    var filterInputValue = $('#filter-input').val().trim();
    if (filterInputValue === '') {
        $('#clearInput').hide();
    } else {
        $('#clearInput').show();
    }
}
//eslint-disable-next-line no-unused-vars
function initializeFilterInputEvents() {
    $('#filter-input').focus(function () {
        if ($(this).val() === '*') {
            $(this).val('');
        }
    });

    const LINE_HEIGHT = 21;
    const MAX_VISIBLE_LINES = 5;
    const PADDING = 8;

    function createTextAreaClone($textarea) {
        const $clone = $('<div id="textarea-clone"></div>')
            .css({
                position: 'absolute',
                top: -9999,
                left: -9999,
                width: $textarea.width(),
                height: 'auto',
                wordWrap: 'break-word',
                whiteSpace: 'pre-wrap',
                visibility: 'hidden',
            })
            .appendTo('body');

        const stylesToCopy = ['fontFamily', 'fontSize', 'fontWeight', 'letterSpacing', 'lineHeight', 'textTransform', 'wordSpacing', 'padding'];
        stylesToCopy.forEach((style) => {
            $clone.css(style, $textarea.css(style));
        });

        return $clone;
    }

    function updateTextarea() {
        const $textarea = $('#filter-input');
        const $clone = $('#textarea-clone');
        let $ellipsis = $('#textarea-ellipsis');

        if (!$clone.length) {
            createTextAreaClone($textarea);
        }

        if (!$ellipsis.length) {
            $ellipsis = $('<div id="textarea-ellipsis">...</div>');
            $textarea.parent().append($ellipsis);
        }

        $('#textarea-clone')
            .width($textarea.width())
            .text($textarea.val() + ' ');

        const contentHeight = $('#textarea-clone').height();
        const lines = Math.ceil((contentHeight - PADDING) / LINE_HEIGHT);
        const isFocused = $textarea.is(':focus');

        let newHeight;
        if (isFocused || lines <= MAX_VISIBLE_LINES) {
            newHeight = contentHeight + PADDING;
        } else {
            newHeight = MAX_VISIBLE_LINES * LINE_HEIGHT + PADDING;
        }

        $textarea.css('height', newHeight + 'px');

        // Show/hide ellipsis (...)
        if (lines > MAX_VISIBLE_LINES && !isFocused) {
            $ellipsis.show();
        } else {
            $ellipsis.hide();
        }
    }

    $('#filter-input').on('focus blur input', updateTextarea);
    $(window).on('resize', updateTextarea);

    // Initial setup
    updateTextarea();

    $('#filter-input').on('input', function () {
        toggleClearButtonVisibility();
    });

    $('#clearInput').click(function () {
        $('#filter-input').val('').focus();
        toggleClearButtonVisibility();
    });

    $('#filter-input').keydown(function (e) {
        toggleClearButtonVisibility();
    });
}

//eslint-disable-next-line no-unused-vars
function getMetricsQData() {
    let endDate = filterEndDate || 'now';
    let stDate = filterStartDate || 'now-15m';
    let queriesData = [];
    let formulasData = [];

    // Process each query
    for (const queryName of Object.keys(queries)) {
        let queryDetails = queries[queryName];
        let queryString;

        if (queryDetails.state === 'builder') {
            queryString = createQueryString(queryDetails);
        } else {
            queryString = queryDetails.rawQueryInput;
        }

        const query = {
            name: queryName,
            query: `(${queryString})`,
            qlType: 'promql',
            state: queryDetails.state,
        };

        queriesData.push({
            end: endDate,
            queries: [query],
            start: stDate,
            formulas: [{ formula: query.name }],
        });
    }

    // Process formulas
    if (Object.keys(formulas).length > 0) {
        for (const key of Object.keys(formulas)) {
            const formulaDetails = formulas[key];
            const queriesInFormula = formulaDetails.queryNames.map((name) => {
                const queryDetails = queries[name];
                let queryString;

                if (queryDetails.state === 'builder') {
                    queryString = createQueryString(queryDetails);
                } else {
                    queryString = queryDetails.rawQueryInput;
                }

                return {
                    name: name,
                    query: `(${queryString})`,
                    qlType: 'promql',
                };
            });
            let functionsArray = formulaDetailsMap[key].functions || [];
            // Update the formula by wrapping it with each function in the functionsArray
            let formula = formulaDetailsMap[key].formula;
            for (let func of functionsArray) {
                formula = `${func}(${formula})`;
            }
            formulasData.push({
                end: endDate,
                formulas: [{ formula: formula }],
                queries: queriesInFormula,
                start: stDate,
            });
        }
    }

    return { queriesData, formulasData };
}

//eslint-disable-next-line no-unused-vars
function updateQueryModeUI(queryMode) {
    $('.query-mode-option').removeClass('active');

    if (queryMode === 'Builder') {
        $('#query-mode-options #mode-option-1').addClass('active');
        $('#query-mode-btn span').html('Builder');
    } else {
        $('#query-mode-options #mode-option-2').addClass('active');
        $('#query-mode-btn span').html('Code');
    }
}
