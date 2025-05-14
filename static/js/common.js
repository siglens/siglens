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
let isTimechart = false;
let isQueryBuilderSearch = false;
let defaultDashboardIds = ['10329b95-47a8-48df-8b1d-0a0a01ec6c42', 'a28f485c-4747-4024-bb6b-d230f101f852', 'bd74f11e-26c8-4827-bf65-c0b464e1f2a4', '53cb3dde-fd78-4253-808c-18e4077ef0f1'];
let initialSearchData = {};
let isMetricsScreen = false;
const globalColorArray = ['#6347D9', '#01BFB3', '#E9DC6E', '#F2A52B', '#4BAE7F', '#9178C5', '#23A9E2', '#8C706B', '#22589D', '#B33B97', '#9FBF46', '#BF9A68', '#DC756F', '#E55D9A', '#597C53', '#63b598', '#ce7d78', '#ea9e70', '#a48a9e', '#c6e1e8', '#648177', '#0d5ac1', '#f205e6', '#14a9ad', '#4ca2f9', '#a4e43f', '#d298e2', '#6119d0', '#d2737d', '#c0a43c', '#f2510e', '#651be6', '#61da5e', '#cd2f00', '#9348af', '#01ac53', '#c5a4fb', '#996635', '#b11573', '#75d89e', '#2f3f94', '#2f7b99', '#da967d', '#34891f', '#b0d87b', '#ca4751', '#7e50a8', '#c4d647', '#11dec1', '#566ca0', '#ffdbe1', '#2f1179', '#916988', '#4b5bdc', '#0cd36d', '#cb5bea', '#df514a', '#539397', '#880977', '#f697c1', '#e1cf3b', '#5be4f0', '#d00043', '#a4d17a', '#be608b', '#96b00c', '#088baf', '#e145ba', '#ee91e3', '#05d371', '#802234', '#0971f0', '#8fb413', '#b2b4f0', '#c9a941', '#0023b8', '#986b53', '#f50422', '#983f7a', '#ea24a3', '#79352c', '#521250', '#c79ed2', '#d6dd92', '#e33e52', '#b2be57', '#fa06ec', '#1bb699', '#6b2e5f', '#21538e', '#89d534', '#d36647', '#996c48', '#9ab9b7', '#06e052', '#e3a481', '#fc458e', '#b2db15', '#aa226d', '#c9a945'];

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

function showError(mainText, subText) {
    $('#corner-popup .corner-text').html(mainText);
    $('#corner-popup .sub-message').html(subText);
    $('#corner-popup').show();

    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    $('#logs-result-container, #agg-result-container, #views-container, .fields-sidebar, #empty-response, #custom-chart-tab').hide();
    $('#save-query-div').children().hide();
    wsState = 'query';
}

function hideError() {
    $('#corner-popup').hide();
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

    // Clear Selected Fields if index changes
    if (initialSearchData && initialSearchData.indexName !== selectedSearchIndex) {
        selectedFieldsList = [];
    }

    $('#LogResultsGrid').html('');
    $('#measureAggGrid').html('');
    columnCount = 0;
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
            $('#avail-field-container ').css('display', 'none');
            renderPanelAggsGrid(columnOrder, res, panelId);
        }
        //for logs-query
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
            $('#avail-field-container ').css('display', 'inline-flex');
            renderAvailableFields(columnOrder);
            renderPanelLogsGrid(columnOrder, res.hits.records, panelId, currentPanel);
        }
        allResultsDisplayed--;
        if (allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
    }

    canScrollMore = res.can_scroll_more;
    scrollFrom = scrollFrom + res.hits.totalMatched.value;

    // Only show empty results error if this is the first request (not a scroll request)
    // or if there's no existing data in panelLogsRowData
    if (((res.hits.totalMatched.value === 0 && res.qtype === 'logs-query') || (!res.bucketCount && (res.qtype === 'aggs-query' || res.qtype === 'segstats-query'))) && (!data.from || data.from === 0 || panelLogsRowData.length === 0)) {
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

        if (currentPanel && currentPanel.isNewPanel === true && (!data.searchText || data.searchText.trim() === '')) {
            $('body').css('cursor', 'default');
            resolve();
            return;
        }

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

    if (res.qtype === 'logs-query') {
        panelProcessEmptyQueryResults('This chart type is not compatible with your query. Please select a different chart type.', panelId);
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

        let columnsOrder = [];
        if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
            columnsOrder = res.columnsOrder;
        } else {
            if (res.groupByCols) {
                columnsOrder = _.uniq(_.concat(res.groupByCols));
            }
            if (res.measureFunctions) {
                columnsOrder = _.uniq(_.concat(columnsOrder, res.measureFunctions));
            }
        }

        if (res.errors) {
            panelProcessEmptyQueryResults(res.errors[0], panelId);
        } else {
            let resultVal;
            if (chartType === 'number') {
                resultVal = Object.values(res?.measure?.[0]?.MeasureVal || {})[0] || null;
            }

            // Check if no measure data exists
            if (!res.measure || !Array.isArray(res.measure) || res.measure.length === 0) {
                panelProcessEmptyQueryResults('', panelId);
            } else if ((chartType === 'Pie Chart' || chartType === 'Bar Chart' || chartType === 'Line Chart') && res.qtype === 'segstats-query') {
                // Bar or Pie chart with segstats query is not compatible
                panelProcessEmptyQueryResults('This chart type is not compatible with your query. Please select a different chart type.', panelId);
            } else if (chartType === 'number' && (resultVal === undefined || resultVal === null)) {
                // Number chart with no valid value
                panelProcessEmptyQueryResults('', panelId);
            } else {
                if (panelId === -1) renderPanelAggsGrid(columnsOrder, res, panelId);
                panelChart = renderBarChart(columnsOrder, res, panelId, chartType, dataType, panelIndex);
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
    var chartType = currentPanel.chartType;

    if (chartType === 'Line Chart') {
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
            disableQueryRemoval();
        } else {
            // for panels on the dashboard page
            for (const queryData of data.queriesData) {
                try {
                    const rawTimeSeriesData = await fetchTimeSeriesData(queryData);
                    const chartData = await convertDataForChart(rawTimeSeriesData);
                    const queryString = queryData.queries[0].query;
                    addVisualizationContainer(queryData.queries[0].name, chartData, queryString, panelId);
                } catch (error) {
                    const errorMessage = (error.responseJSON && error.responseJSON.error) || (error.responseText && JSON.parse(error.responseText).error) || 'An unknown error occurred';
                    const errorCanvas = $(`#panel${panelId} .panel-body .panEdit-panel canvas`);
                    if (isDashboardScreen) {
                        if (errorCanvas.length > 0) {
                            errorCanvas.remove();
                        }
                        displayErrorMessage($(`#panel${panelId} .panel-body`), errorMessage);
                    } else {
                        console.error('Error fetching time series data:', error);
                    }
                }
            }

            for (const formulaData of data.formulasData) {
                try {
                    const rawTimeSeriesData = await fetchTimeSeriesData(formulaData);
                    const chartData = await convertDataForChart(rawTimeSeriesData);
                    let formulaString = formulaData.formulas[0].formula;

                    // Replace a, b, etc., with actual query values
                    formulaData.queries.forEach((query) => {
                        const regex = new RegExp(`\\b${query.name}\\b`, 'g');
                        formulaString = formulaString.replace(regex, query.query);
                    });

                    addVisualizationContainer(formulaData.formulas[0].formula, chartData, formulaString, panelId);
                } catch (error) {
                    const errorMessage = (error.responseJSON && error.responseJSON.error) || (error.responseText && JSON.parse(error.responseText).error) || 'An unknown error occurred';
                    const errorCanvas = $(`#panel${panelId} .panel-body .panEdit-panel canvas`);
                    if (isDashboardScreen) {
                        if (errorCanvas.length > 0) {
                            errorCanvas.remove();
                        }
                        displayErrorMessage($(`#panel${panelId} .panel-body`), errorMessage);
                    } else {
                        console.error('Error fetching time series data:', error);
                    }
                }
            }
        }
        if (currentPanel && currentPanel.style) {
            toggleLineOptions(currentPanel.style.display);
            chartType = currentPanel.style.display;
            toggleChartType(chartType);
            updateChartTheme(currentPanel.style.color);
            updateLineCharts(currentPanel.style.lineStyle, currentPanel.style.lineStroke);
        }
        $(`#panel${panelId} .panel-body #panel-loading`).hide();
        allResultsDisplayed--;
        if (allResultsDisplayed <= 0 || panelId === -1) {
            $('body').css('cursor', 'default');
        }
        $('body').css('cursor', 'default');
    }
}

//eslint-disable-next-line no-unused-vars
function loadCustomDateTimeFromEpoch(startEpoch, endEpoch) {
    function setDateTimeInputs(epochTime, dateId, timeId) {
        let dateVal = new Date(epochTime);
        let dateString = dateVal.toISOString().split('T')[0];
        let timeString = dateVal.toTimeString().substring(0, 5);

        $(`#${dateId}, .panelEditor-container #${dateId}`).val(dateString).addClass('active');
        $(`#${timeId}, .panelEditor-container #${timeId}`).val(timeString).addClass('active');

        return { date: dateString, time: timeString };
    }

    let startValues = setDateTimeInputs(startEpoch, 'date-start', 'time-start');
    let endValues = setDateTimeInputs(endEpoch, 'date-end', 'time-end');

    appliedStartDate = tempStartDate = startValues.date;
    appliedStartTime = tempStartTime = startValues.time;
    appliedEndDate = tempEndDate = endValues.date;
    appliedEndTime = tempEndTime = endValues.time;

    Cookies.set('customStartDate', appliedStartDate);
    Cookies.set('customStartTime', appliedStartTime);
    Cookies.set('customEndDate', appliedEndDate);
    Cookies.set('customEndTime', appliedEndTime);

    $('.range-item, .db-range-item').removeClass('active');
}

//eslint-disable-next-line no-unused-vars
function showToast(msg, type = 'error', autoCloseTime = null) {
    let toastTypeClass = type === 'success' ? 'toast-success' : 'toast-error';
    let toast = `
        <div class="${toastTypeClass}" id="message-toast">
            <button type="button" aria-label="Close" class="toast-close">×</button>
            ${msg}
            <div class="toast-buttons">
                <button type="button" class="toast-ok btn btn-secondary">OK</button>
            </div>
        </div>`;

    $('body').prepend(toast);

    if (autoCloseTime !== null) {
        setTimeout(removeToast, autoCloseTime);
    } else if (type === 'success') {
        setTimeout(removeToast, 3000);
    }
    $('.toast-close').on('click', removeToast);
    $('.toast-ok').on('click', removeToast);

    function removeToast() {
        $('#message-toast').remove();
    }
}

//eslint-disable-next-line no-unused-vars
function getOrgConfig() {
    return new Promise((resolve, reject) => {
        $.ajax({
            method: 'get',
            url: '/api/config',
            crossDomain: true,
            dataType: 'json',
            credentials: 'include',
        })
            .then((res) => {
                resolve(res);
            })
            .catch((err) => {
                console.log(err);
                reject(err);
            });
    });
}

//renders the response from Logs query to respective selected chart type
function renderChartByChartType(data, queryRes, panelId, currentPanel) {
    switch (currentPanel.chartType) {
        case 'Data Table':
        case 'loglines':
            $('.panelDisplay .panEdit-panel').hide();
            renderPanelLogsQueryRes(data, panelId, currentPanel, queryRes);
            break;
        case 'Bar Chart':
        case 'Pie Chart':
        case 'Line Chart':
            renderPanelAggsQueryRes(data, panelId, currentPanel.chartType, currentPanel.dataType, currentPanel.panelIndex, queryRes);
            break;
        case 'number':
            if (currentPanel.unit === '' || currentPanel.dataType === 'none' || currentPanel.dataType === '') {
                currentPanel.unit = 'misc';
                currentPanel.dataType = 'none';
            }
            renderPanelAggsQueryRes(data, panelId, currentPanel.chartType, currentPanel.dataType, currentPanel.panelIndex, queryRes);
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
            if (indexValues && indexValues.length > 0) {
                const indexIndex = indexValues.indexOf(index);
                if (indexIndex !== -1) {
                    indexValues.splice(indexIndex, 1);
                }
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
    // Function to check the visibility of the Format button
    function checkFormatButtonVisibility() {
        const selectedLanguage = $('#query-language-btn span').text().trim();
        const selectedTab = $('.tab-list .tab-li.ui-tabs-active').attr('id');
        const formatButton = $('#formatInput');
        if (selectedLanguage === 'Splunk QL' && selectedTab === 'tab-title2') {
            formatButton.show();
        } else {
            formatButton.hide();
        }
    }

    // Function to handle tab clicks
    function handleTabClick() {
        $('.tab-li').removeClass('active');
        $(this).addClass('active');
        checkFormatButtonVisibility();
    }

    // Initial visibility check
    checkFormatButtonVisibility();

    // Event listener for tab clicks
    $('.tab-list .tab-li').click(handleTabClick);

    // Event listeners for query language changes
    $('#query-language-options').on('click', '.query-language-option', function () {
        setTimeout(checkFormatButtonVisibility, 10);
    });

    // Handle input focus
    $('#filter-input').focus(function () {
        if ($(this).val() === '*') {
            $(this).val('');
        }
    });

    const LINE_HEIGHT = 20;
    const MAX_VISIBLE_LINES = 5;
    const PADDING = 4;

    // Create a clone of the textarea to measure its height
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

    // Update the textarea height and ellipsis
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

    // Event listeners for input and window resize
    $('#filter-input').on('focus blur input', updateTextarea);
    $(window).on('resize', updateTextarea);

    // Initial setup for textarea
    updateTextarea();

    // Toggle visibility of the clear button
    $('#filter-input').on('input', function () {
        toggleClearButtonVisibility();
    });

    $('#clearInput').click(function () {
        $('#filter-input').val('').focus();
        toggleClearButtonVisibility();
    });

    // Format button click event
    $('#formatInput').click(function () {
        let input = $('#filter-input');
        let value = input.val();

        // Format the input value by ensuring each '|' is preceded by a newline
        let formattedValue = '';
        for (let i = 0; i < value.length; i++) {
            if (value[i] === '|' && (i === 0 || value[i - 1] !== '\n')) {
                formattedValue += '\n|';
            } else {
                formattedValue += value[i];
            }
        }

        input.val(formattedValue);
        updateTextarea();
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
            query: `${queryString}`,
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
                    query: `${queryString}`,
                    qlType: 'promql',
                };
            });
            let functionsArray = formulaDetailsMap[key].functions || [];
            // Update the formula by wrapping it with each function in the functionsArray
            let formula = formulas[key].formula;

            for (let func of functionsArray) {
                // Create a regex to match the function being applied
                const funcRegex = new RegExp(`\\b${func}\\(`);

                // Check if the formula does not already contain the function
                if (!funcRegex.test(formula)) {
                    formula = `${func}(${formula})`;
                }
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

function calculateMutedFor(silenceEndTime) {
    if (!silenceEndTime) return '';
    const now = Math.floor(Date.now() / 1000);
    const remainingSeconds = silenceEndTime - now;
    if (remainingSeconds <= 0) return '';

    const days = Math.floor(remainingSeconds / 86400);
    const hours = Math.floor((remainingSeconds % 86400) / 3600);
    const minutes = Math.floor((remainingSeconds % 3600) / 60);
    const seconds = Math.floor(remainingSeconds % 60);

    let result = '';
    if (days > 0) result += `${days} day${days > 1 ? 's' : ''} `;
    if (hours > 0) result += `${hours} hr${hours > 1 ? 's' : ''} `;
    if (minutes > 0) result += `${minutes} min${minutes > 1 ? 's' : ''} `;
    if (minutes === 0 && seconds > 0) result += `${seconds} sec${seconds > 1 ? 's' : ''}`;

    return result.trim();
}

function createTooltip(selector, content) {
    //eslint-disable-next-line no-undef
    tippy(selector, {
        content: content,
        placement: 'top',
        arrow: true,
        animation: 'fade',
    });
}

function handleRelatedTraces(traceId, timestamp, newTab) {
    const url = `trace.html?trace_id=${traceId}&timestamp=${timestamp}`;
    if (newTab) {
        window.open(url, '_blank'); // Opens in a new tab
    } else {
        window.location.href = url;
    }
}

function handleRelatedLogs(id, traceStartTime, type = 'trace') {
    const traceStartEpoch = Math.floor(Number(traceStartTime) / 1000000);

    const fifteenMinutesMs = 15 * 60 * 1000;

    const startEpoch = traceStartEpoch - fifteenMinutesMs;
    const endEpoch = traceStartEpoch + fifteenMinutesMs;

    const searchQuery = type === 'span' ? `span_id="${id}"` : `trace_id="${id}"`;

    const searchParams = new URLSearchParams({
        searchText: searchQuery,
        startEpoch: startEpoch.toString(),
        endEpoch: endEpoch.toString(),
        indexName: 'trace-related-logs',
        queryLanguage: 'Splunk QL',
        filterTab: '1',
    });

    window.open(`index.html?${searchParams.toString()}`, '_blank');
}

function syntaxHighlight(json) {
    if (typeof json !== 'string') {
        json = JSON.stringify(json, null, 2);
    }
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(\.\d*)?([eE][+-]?\d+)?)/g, function (match) {
        let cls = 'json-value';
        if (/^"/.test(match) && /:$/.test(match)) {
            cls = 'json-key';
        }
        return `<span class="${cls}">${match}</span>`;
    });
}

function ExpandableJsonCellRenderer(type = 'events') {
    const state = {
        currentExpandedCell: null,
    };

    return class {
        init(params) {
            this.params = params;
            this.eGui = document.createElement('div');
            this.eGui.style.display = 'flex';
            this.isExpanded = false;
            this.rowElement = null;

            const displayValue = type === 'logs' && params.column.colId === 'timestamp' ? (typeof params.value === 'number' ? moment(params.value).format(timestampDateFmt) : params.value) : params.value;

            this.eGui.innerHTML = `
                <span class="expand-icon-box">
                    <button class="expand-icon-button">
                        <i class="fa-solid fa-up-right-and-down-left-from-center"></i>
                    </button>
                </span>
                <span>${displayValue}</span>
            `;

            this.expandBtn = this.eGui.querySelector('.expand-icon-box');
            this.expandIcon = this.eGui.querySelector('.expand-icon-button i');
            this.expandBtn.addEventListener('click', this.toggleJsonPanel.bind(this));

            document.addEventListener('jsonPanelClosed', () => {
                if (state.currentExpandedCell === this) {
                    this.isExpanded = false;
                    this.updateIcon();
                    state.currentExpandedCell = null;
                }
            });
        }

        updateIcon() {
            if (this.isExpanded) {
                this.expandIcon.classList.remove('fa-up-right-and-down-left-from-center');
                this.expandIcon.classList.add('fa-down-left-and-up-right-to-center');
            } else {
                this.expandIcon.classList.remove('fa-down-left-and-up-right-to-center');
                this.expandIcon.classList.add('fa-up-right-and-down-left-from-center');
            }
        }

        closeJsonPanel() {
            const jsonPopup = document.querySelector('.json-popup');
            jsonPopup.classList.remove('active');
            this.isExpanded = false;
            this.updateIcon();

            if (this.rowElement) {
                this.rowElement.classList.remove('highlighted-row');
            }

            state.currentExpandedCell = null;
            document.dispatchEvent(new CustomEvent('jsonPanelClosed'));
            this.params.api.sizeColumnsToFit();
        }

        initAceEditor(container, jsonData) {
            const editor = ace.edit(container);
            editor.session.setMode("ace/mode/json");
            editor.setOptions({
                readOnly: true,
                showPrintMargin: false,
                highlightActiveLine: false,
                highlightGutterLine: false,
                fontSize: "12px",
                showGutter: true,
                wrap: true
            });
            
            editor.setValue(JSON.stringify(jsonData, null, 2), -1);
            
            editor.selection.on('changeSelection', function(e) {
                editor.renderer.$cursorLayer.element.style.display = "none";
            });
            
            editor.resize();
            
            return editor;
        }

        toggleJsonPanel(event) {
            event.stopPropagation();

            if (!this.rowElement) {
                this.rowElement = this.findRowElement();
            }

            // If already expanded, close the panel
            if (this.isExpanded) {
                this.closeJsonPanel();
                return;
            }

            const jsonPopup = document.querySelector('.json-popup');
            const rowData = this.params.node.data;
            let trace_id = '';
            let time_stamp = '';

            if (this.rowElement) {
                this.rowElement.classList.add('highlighted-row');
            }

            if (state.currentExpandedCell && state.currentExpandedCell !== this) {
                state.currentExpandedCell.isExpanded = false;
                state.currentExpandedCell.updateIcon();

                if (state.currentExpandedCell.rowElement) {
                    state.currentExpandedCell.rowElement.classList.remove('highlighted-row');
                }
            }

            this.isExpanded = true;
            this.updateIcon();
            state.currentExpandedCell = this;

            window.copyJsonToClipboard = function () {
                const editor = ace.edit(document.querySelector('#json-tab .ace-editor-container'));
                const jsonContent = editor.getValue();
                navigator.clipboard
                    .writeText(jsonContent)
                    .then(() => {
                        const copyIcon = $('.copy-icon');
                        copyIcon.addClass('success');
                        setTimeout(function () {
                            copyIcon.removeClass('success');
                        }, 1000);
                    })
                    .catch((err) => console.error('Failed to copy: ', err));
            };

            window.switchTab = function (tab) {
                document.querySelectorAll('.tab-content').forEach((el) => el.classList.remove('active'));
                document.querySelectorAll('.tab-button').forEach((el) => el.classList.remove('active'));
                document.getElementById(tab + '-tab').classList.add('active');
                document.querySelector(`[onclick="switchTab('${tab}')"]`).classList.add('active');
                if (tab === 'table') populateTable();
            };

            function flattenJson(data, parentKey = '', result = {}) {
                Object.entries(data).forEach(([key, value]) => {
                    const newKey = parentKey ? `${parentKey}.${key}` : key;
                    if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                        flattenJson(value, newKey, result);
                    } else {
                        result[newKey] = value;
                    }
                });
                return result;
            }

            function populateTable() {
                const tableBody = document.getElementById('table-content');
                tableBody.innerHTML = '';
                const flattenedData = flattenJson(rowData);

                if (!flattenedData || Object.keys(flattenedData).length === 0) {
                    tableBody.innerHTML = '<tr><td colspan="2" style="text-align:center;">No data available</td></tr>';
                    return;
                }

                Object.entries(flattenedData).forEach(([key, value]) => {
                    const formattedValue = typeof value === 'object' ? JSON.stringify(value, null, 2) : value;
                    const row = document.createElement('tr');
                    const keyCell = document.createElement('td');
                    const valueCell = document.createElement('td');

                    keyCell.textContent = key;
                    valueCell.textContent = formattedValue;
                    [keyCell, valueCell].forEach((cell) => {
                        cell.style.border = '1px solid #ddd';
                        cell.style.padding = '6px';
                    });

                    row.appendChild(keyCell);
                    row.appendChild(valueCell);
                    tableBody.appendChild(row);
                });
            }

            const showRelatedTraceButton =
                type === 'logs' &&
                Object.keys(rowData).some((key) => {
                    if (key.toLowerCase() === 'timestamp') {
                        time_stamp = rowData[key];
                    }
                    if (key.toLowerCase() === 'trace_id') {
                        trace_id = rowData[key];
                        return trace_id !== null && trace_id !== '';
                    }
                    return false;
                });

            jsonPopup.innerHTML = `
                <div class="json-popup-header">
                    <div class="json-popup-header-buttons">
                        ${
                            showRelatedTraceButton
                                ? `
                            <div>
                                <button class="btn-related-trace btn btn-purple" onclick="handleRelatedTraces('${trace_id}', ${time_stamp}, true)">
                                    <i class="fa fa-file-text"></i>&nbsp; Related Trace
                                </button>
                            </div>
                        `
                                : ''
                        }
                        <div><button class="json-popup-close">×</button></div>
                    </div>
                </div>
                <div class="json-content-type-box">
                    <div class="tab-button-ctn">
                        <button class="tab-button active" onclick="switchTab('json')">JSON</button>
                        <button class="tab-button" onclick="switchTab('table')">Table</button>
                    </div>
                    <span class="copy-icon" onclick="copyJsonToClipboard()"></span>
                </div>
                <div class="json-popup-content">
                    <div id="json-tab" class="tab-content active">
                        <div class="ace-editor-container" style="width: 100%; height: 100%; line-height: 20px"></div>
                    </div>
                    <div id="table-tab" class="tab-content">
                        <table border="1" class="json-table">
                            <thead>
                                <tr>
                                    <th>Key</th>
                                    <th>Value</th>
                                </tr>
                            </thead>
                            <tbody id="table-content"></tbody>
                        </table>
                    </div>
                </div>
            `;

            jsonPopup.classList.add('active');
            $('.json-popup').show();

            const closeBtn = jsonPopup.querySelector('.json-popup-close');
            closeBtn.onclick = () => {
                this.closeJsonPanel();
            };

            const aceContainer = document.querySelector('#json-tab .ace-editor-container');
            const unflattenedData = JSON.unflatten(rowData);
            this.initAceEditor(aceContainer, unflattenedData);
            this.params.api.sizeColumnsToFit();
        }

        getGui() {
            return this.eGui;
        }

        findRowElement() {
            let element = this.eGui;
            while (element) {
                if (element.classList && element.classList.contains('ag-row')) {
                    return element;
                }
                element = element.parentElement;
            }
            return null;
        }

        refresh() {
            return false;
        }
    };
}

function formatByteSize(bytes) {
    if (bytes === 0) return '0 Bytes';

    const units = ['Bytes', 'KB', 'MB', 'GB', 'TB', 'PB'];
    const i = Math.floor(Math.log(bytes) / Math.log(1024));

    // Format with 2 decimal places for larger units, round to integers for bytes
    return i === 0 ? bytes + ' ' + units[i] : (bytes / Math.pow(1024, i)).toFixed(2) + ' ' + units[i];
}

function getGraphGridColors() {
    const rootStyles = getComputedStyle(document.documentElement);
    let isDarkTheme = document.documentElement.getAttribute('data-theme') === 'dark';
    const gridLineColor = isDarkTheme ? rootStyles.getPropertyValue('--black-3') : rootStyles.getPropertyValue('--white-1');
    const tickColor = isDarkTheme ? rootStyles.getPropertyValue('--white-0') : rootStyles.getPropertyValue('--white-6');

    return { gridLineColor, tickColor };
}