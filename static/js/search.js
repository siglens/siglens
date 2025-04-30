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
let lastQType = '';
let lastColumnsOrder = [];

function wsURL(path) {
    var protocol = location.protocol === 'https:' ? 'wss://' : 'ws://';
    var url = protocol + location.host;
    return url + path;
}
//eslint-disable-next-line no-unused-vars
function doCancel(data) {
    socket.send(JSON.stringify(data));
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    $('#progress-div').html(``);
    $('#record-searched').html(``);
}
//eslint-disable-next-line no-unused-vars
function doLiveTailCancel(_data) {
    $('body').css('cursor', 'default');
    $('#live-tail-btn').html('Live Tail');
    $('#live-tail-btn').removeClass('active');
    $('#progress-div').html(``);
}

function resetDataTable(firstQUpdate) {
    if (firstQUpdate) {
        $('#empty-response').hide();
        $('#custom-chart-tab').show().css({ height: '100%' });
        $('.tab-chart-list').show();
        let currentTab = $('#custom-chart-tab').tabs('option', 'active');
        if (currentTab == 0) {
            $('#save-query-div').children().show();
            $('#views-container, .fields-sidebar').show();
        } else {
            $('#save-query-div').children().hide();
            $('#views-container, .fields-sidebar').hide();
        }
        $('#agg-result-container').hide();
        hideError();
    }
}

let doSearchCounter = 0;
let columnCount = 0;
//eslint-disable-next-line no-unused-vars
function doSearch(data) {
    return new Promise((resolve, reject) => {
        startQueryTime = new Date().getTime();
        newUri = wsURL('/api/search/ws');
        socket = new WebSocket(newUri);
        let timeToFirstByte = 0;
        let firstQUpdate = true;
        let lastKnownHits = 0;
        let errorMessages = [];
        const timerName = `socket timing ${doSearchCounter}`;
        doSearchCounter++;
        console.time(timerName);

        socket.onopen = function (_e) {
            $('body').css('cursor', 'progress');
            $('#run-filter-btn').addClass('cancel-search').addClass('active');
            $('#query-builder-btn').addClass('cancel-search').addClass('active');

            try {
                socket.send(JSON.stringify(data));
            } catch (e) {
                reject(`Error sending message to server: ${e}`);
                console.timeEnd(timerName);
                return;
            }
        };

        socket.onmessage = function (event) {
            let jsonEvent = JSON.parse(event.data);
            let eventType = jsonEvent.state;
            let totalEventsSearched = jsonEvent.total_events_searched;
            let totalTime = new Date().getTime() - startQueryTime;
            switch (eventType) {
                case 'RUNNING':
                    break;
                case 'QUERY_UPDATE': {
                    console.time('QUERY_UPDATE');
                    if (timeToFirstByte === 0) {
                        timeToFirstByte = Number(totalTime).toLocaleString();
                    }
                    let totalHits;

                    if (jsonEvent && jsonEvent.hits && jsonEvent.hits.totalMatched) {
                        totalHits = jsonEvent.hits.totalMatched;
                        lastKnownHits = totalHits;
                    } else {
                        // we enter here only because backend sent null hits/totalmatched
                        totalHits = lastKnownHits;
                    }
                    resetDataTable(firstQUpdate);
                    processQueryUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, totalHits);
                    console.timeEnd('QUERY_UPDATE');
                    firstQUpdate = false;
                    break;
                }
                case 'COMPLETE': {
                    let eqRel = 'eq';
                    if (jsonEvent.totalMatched != null && jsonEvent.totalMatched.relation != null) {
                        eqRel = jsonEvent.totalMatched.relation;
                    }
                    console.time('COMPLETE');
                    canScrollMore = jsonEvent.can_scroll_more;
                    scrollFrom = jsonEvent.total_rrc_count;
                    processCompleteUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, eqRel);
                    console.timeEnd('COMPLETE');
                    socket.close(1000);
                    break;
                }
                case 'CANCELLED':
                    console.time('CANCELLED');
                    console.log(`[message] CANCELLED state received from server: ${jsonEvent}`);
                    processCancelUpdate(jsonEvent);
                    console.timeEnd('CANCELLED');
                    errorMessages.push(`CANCELLED: ${jsonEvent}`);
                    socket.close(1000);
                    break;
                case 'TIMEOUT':
                    console.time('TIMEOUT');
                    console.log(`[message] Timeout state received from server: ${jsonEvent}`);
                    processTimeoutUpdate(jsonEvent);
                    console.timeEnd('TIMEOUT');
                    errorMessages.push(`Timeout: ${jsonEvent}`);
                    socket.close(1000);
                    break;
                case 'ERROR':
                    console.time('ERROR');
                    console.log(`[message] Error state received from server: ${jsonEvent}`);
                    processErrorUpdate(jsonEvent);
                    console.timeEnd('ERROR');
                    errorMessages.push(`Error: ${jsonEvent}`);
                    break;
                default:
                    console.log(`[message] Unknown state received from server: ` + JSON.stringify(jsonEvent));
                    if (jsonEvent.message.includes('expected')) {
                        addSyntaxMessagePopup();
                    } else if (jsonEvent.message.includes('not present')) {
                        jsonEvent['no_data_err'] = 'No data found for the query';
                    }
                    processSearchErrorLog(jsonEvent);
                    errorMessages.push(`Unknown state: ${jsonEvent}`);
            }
        };

        socket.onclose = function (event) {
            if (event.wasClean) {
                console.log(`[close] Connection closed cleanly, code=${event.code} reason=${event.reason}`);
            } else {
                console.log(`Connection close not clean=${event} code=${event.code} reason=${event.reason} `);
                errorMessages.push(`Connection close not clean=${event} code=${event.code} reason=${event.reason}`);
            }

            if (errorMessages.length === 0) {
                resolve();
            } else {
                reject(errorMessages);
            }
            console.timeEnd(timerName);
            const finalResultResponseTime = (new Date().getTime() - startQueryTime).toLocaleString();
            $('#hits-summary .final-res-time span').html(`${finalResultResponseTime}`);
        };

        socket.addEventListener('error', (event) => {
            errorMessages.push(`WebSocket error: ${event}`);
        });
    });
}

function reconnect() {
    if (lockReconnect) {
        return;
    }
    lockReconnect = true;
    //keep reconnect，set delay to avoid much request, set tt, cancel first, then reset
    clearInterval(tt);
    tt = setInterval(function () {
        if (!liveTailState) {
            lockReconnect = false;
            return;
        }
        data = getLiveTailFilter(false, false, 30);
        createLiveTailSocket(data);
        lockReconnect = false;
    }, refreshInterval);
}

function createLiveTailSocket(data) {
    try {
        if (!liveTailState) return;
        startQueryTime = new Date().getTime();
        newUri = wsURL('/api/search/live_tail');
        socket = new WebSocket(newUri);
        doLiveTailSearch(data);
    } catch (e) {
        console.log('live tail connect websocket catch: ' + e);
        reconnect();
    }
}
function doLiveTailSearch(data) {
    let timeToFirstByte = 0;
    let firstQUpdate = true;
    let lastKnownHits = 0;
    socket.onopen = function (_e) {
        //  console.time("socket timing");
        $('body').css('cursor', 'progress');
        $('#live-tail-btn').html('Cancel Live Tail');
        $('#live-tail-btn').addClass('active');
        socket.send(JSON.stringify(data));
    };

    socket.onmessage = function (event) {
        let jsonEvent = JSON.parse(event.data);
        let eventType = jsonEvent.state;
        if (jsonEvent && jsonEvent.total_events_searched && jsonEvent.total_events_searched != 0) {
            total_liveTail_searched = jsonEvent.total_events_searched;
        }
        let totalEventsSearched = total_liveTail_searched;
        let totalTime = new Date().getTime() - startQueryTime;
        switch (eventType) {
            case 'RUNNING':
                console.time('RUNNING');
                console.timeEnd('RUNNING');
                break;
            case 'QUERY_UPDATE': {
                console.time('QUERY_UPDATE');
                if (timeToFirstByte === 0) {
                    timeToFirstByte = Number(totalTime).toLocaleString();
                }
                let totalHits;

                if (jsonEvent && jsonEvent.hits && jsonEvent.hits.totalMatched) {
                    totalHits = jsonEvent.hits.totalMatched;
                    lastKnownHits = totalHits;
                } else {
                    // we enter here only because backend sent null hits/totalmatched
                    totalHits = lastKnownHits;
                }
                resetDataTable(firstQUpdate);
                processLiveTailQueryUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, totalHits);
                //  console.timeEnd("QUERY_UPDATE");
                firstQUpdate = false;
                break;
            }
            case 'COMPLETE': {
                let eqRel = 'eq';
                if (jsonEvent.totalMatched != null && jsonEvent.totalMatched.relation != null) {
                    eqRel = jsonEvent.totalMatched.relation;
                }
                console.time('COMPLETE');
                console.log(new Date().getTime());
                canScrollMore = jsonEvent.can_scroll_more;
                scrollFrom = jsonEvent.total_rrc_count;
                processLiveTailCompleteUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, eqRel);
                console.timeEnd('COMPLETE');
                socket.close(1000);
                break;
            }
            case 'TIMEOUT':
                console.time('TIMEOUT');
                console.log(`[message] Timeout state received from server: ${jsonEvent}`);
                processTimeoutUpdate(jsonEvent);
                console.timeEnd('TIMEOUT');
                break;
            case 'CANCELLED':
                console.time('CANCELLED');
                console.log(`[message] CANCELLED state received from server: ${jsonEvent}`);
                processCancelUpdate(jsonEvent);
                console.timeEnd('CANCELLED');
                break;
            case 'ERROR':
                console.time('ERROR');
                console.log(`[message] Error state received from server: ${jsonEvent}`);
                processErrorUpdate(jsonEvent);
                console.timeEnd('ERROR');
                break;
            default:
                console.log(`[message] Unknown state received from server: ` + JSON.stringify(jsonEvent));
                if (jsonEvent.message.includes('expected')) {
                    jsonEvent.message = 'Your query contains syntax error';
                } else if (jsonEvent.message.includes('not present')) {
                    jsonEvent['no_data_err'] = 'No data found for the query';
                }
                processSearchErrorLog(jsonEvent);
        }
    };

    socket.onclose = function (event) {
        if (liveTailState) {
            reconnect();
            console.log('live tail reconnect websocket');
        } else {
            console.log('stop reconnect live tail');
            if (event.wasClean) {
                console.log(`[close] Connection closed cleanly, code=${event.code} reason=${event.reason}`);
            } else {
                console.log(`Connection close not clean=${event} code=${event.code} reason=${event.reason} `);
            }
            console.timeEnd('socket timing');
        }
    };

    socket.addEventListener('error', (event) => {
        console.log('WebSocket error: ', event);
    });
}
//eslint-disable-next-line no-unused-vars
function getInitialSearchFilter(skipPushState, scrollingTrigger) {
    let queryParams = new URLSearchParams(window.location.search);
    let stDate = queryParams.get('startEpoch') || Cookies.get('startEpoch') || 'now-15m';
    let endDate = queryParams.get('endEpoch') || Cookies.get('endEpoch') || 'now';
    let selIndexName = queryParams.get('indexName');
    let queryLanguage = queryParams.get('queryLanguage');
    let queryMode = Cookies.get('queryMode') || 'Builder';

    let fieldsHidden = queryParams.get('fieldsHidden') || true;
    applyFieldsSidebarState(fieldsHidden);

    queryLanguage = queryLanguage.replace('"', '');
    $('#query-language-btn span').html(queryLanguage);
    $('.query-language-option').removeClass('active');
    let selectedQueryLanguageId;
    if (queryLanguage == 'SQL') {
        $('#option-1').addClass('active');
        selectedQueryLanguageId = '1';
    } else if (queryLanguage == 'Log QL') {
        $('#option-2').addClass('active');
        selectedQueryLanguageId = '2';
    } else if (queryLanguage == 'Splunk QL') {
        $('#option-3').addClass('active');
        selectedQueryLanguageId = '3';
    }
    queryMode = queryMode.replace('"', '');
    updateQueryModeUI(queryMode);
    let filterTab = queryParams.get('filterTab');
    handleTabAndTooltip(selectedQueryLanguageId, parseInt(filterTab));
    let filterValue = queryParams.get('searchText');

    if (filterTab == '0' || filterTab == null) {
        if (filterValue === '* | head 10') {
            //Default search
            isQueryBuilderSearch = false;
        } else if (filterValue != '*') {
            codeToBuilderParsing(filterValue);
            $('#filter-input').val(filterValue).change();
            toggleClearButtonVisibility();
            isQueryBuilderSearch = true;
        } else {
            isQueryBuilderSearch = true;
        }
    } else {
        $('#custom-code-tab').tabs('option', 'active', 1);
        if (filterValue === '*') {
            $('#filter-input').val('').change();
        } else {
            $('#filter-input').val(filterValue).change();
            toggleClearButtonVisibility();
        }
        isQueryBuilderSearch = false;
    }
    let sFrom = 0;

    setIndexDisplayValue(selIndexName);

    selectedSearchIndex = selIndexName.split(',').join(',');
    Cookies.set('IndexList', selIndexName.split(',').join(','));

    if (!isNaN(stDate)) {
        stDate = Number(stDate);
        endDate = Number(endDate);
        datePickerHandler(stDate, endDate, 'custom');
        loadCustomDateTimeFromEpoch(stDate, endDate);
    } else if (stDate !== 'now-15m') {
        datePickerHandler(stDate, endDate, stDate);
    } else {
        datePickerHandler(stDate, endDate, '');
    }

    selectedSearchIndex = selIndexName;
    if (!skipPushState) {
        addQSParm('searchText', filterValue);
        addQSParm('startEpoch', stDate);
        addQSParm('endEpoch', endDate);
        addQSParm('indexName', selIndexName);
        addQSParm('queryLanguage', queryLanguage);
        addQSParm('filterTab', filterTab);
        addQSParm('fieldsHidden', fieldsHidden);
        window.history.pushState({ path: myUrl }, '', myUrl);
    }

    if (scrollingTrigger) {
        sFrom = scrollFrom;
    }

    return {
        state: 'query',
        searchText: filterValue,
        startEpoch: stDate,
        endEpoch: endDate,
        indexName: selIndexName,
        from: sFrom,
        queryLanguage: queryLanguage,
        includeNulls: false, // Exclude null values
        fieldsHidden: fieldsHidden,
    };
}

function getLiveTailFilter(skipPushState, scrollingTrigger, startTime) {
    let filterValue = $('#filter-input').val().trim() || '*';
    let endDate = 'now';
    let date = new Date();
    let stDate = new Date(date.getTime() - startTime * 1000).getTime();
    if (startTime == 1800) stDate = 'now-1h';
    let selIndexName = selectedSearchIndex;
    let sFrom = 0;
    let queryLanguage = $('#query-language-options .query-language-option.active').html();

    setIndexDisplayValue(selIndexName);

    selectedSearchIndex = selIndexName.split(',').join(',');
    Cookies.set('IndexList', selIndexName.split(',').join(','));

    addQSParm('searchText', filterValue);
    addQSParm('startEpoch', stDate);
    addQSParm('endEpoch', endDate);
    addQSParm('indexName', selIndexName);
    addQSParm('queryLanguage', queryLanguage);

    window.history.pushState({ path: myUrl }, '', myUrl);

    if (scrollingTrigger) {
        sFrom = scrollFrom;
    }

    return {
        state: wsState,
        searchText: filterValue,
        startEpoch: stDate,
        endEpoch: endDate,
        indexName: selIndexName,
        from: sFrom,
        queryLanguage: queryLanguage,
    };
}

function getQueryBuilderCode() {
    let filterValue = '';
    //concat the first input box
    let index = 0;
    if (firstBoxSet && firstBoxSet.size > 0) {
        firstBoxSet.forEach((value, _i) => {
            if (index != firstBoxSet.size - 1) filterValue += value + ' ';
            else filterValue += value;
            index++;
        });
    } else {
        filterValue = '*';
    }
    index = 0;
    let bothRight = 0;
    let showError = false;
    //concat the second input box
    if (secondBoxSet && secondBoxSet.size > 0) {
        bothRight++;
        filterValue += ' | stats';
        secondBoxSet.forEach((value, _i) => {
            if (index != secondBoxSet.size - 1) filterValue += ' ' + value + ',';
            else filterValue += ' ' + value;
            index++;
        });
    }
    index = 0;
    if (thirdBoxSet && thirdBoxSet.size > 0) {
        if (bothRight == 0) showError = true;
        //concat the third input box
        filterValue += ' BY';
        thirdBoxSet.forEach((value, _i) => {
            if (index != thirdBoxSet.size - 1) filterValue += ' ' + value + ',';
            else filterValue += ' ' + value;
            index++;
        });
    }
    if (filterValue == '') filterValue = '*';
    if (thirdBoxSet && thirdBoxSet.size > 0 && (secondBoxSet == null || secondBoxSet.size == 0)) $('#query-builder-btn').addClass('stop-search').prop('disabled', true);
    else $('#query-builder-btn').removeClass('stop-search').prop('disabled', false);
    return showError ? 'Searches with a Search Criteria must have an Aggregate Attribute' : filterValue;
}

//eslint-disable-next-line no-unused-vars
function getSearchFilter(skipPushState, scrollingTrigger, isInitialLoad = false) {
    let currentTab = $('#custom-code-tab').tabs('option', 'active');
    let endDate = filterEndDate || 'now';
    let stDate = filterStartDate || 'now-15m';
    let selIndexName = selectedSearchIndex;
    let sFrom = 0;
    let queryLanguage = $('#query-language-options .query-language-option.active').html();

    setIndexDisplayValue(selIndexName);

    selectedSearchIndex = selIndexName.split(',').join(',');
    Cookies.set('IndexList', selIndexName.split(',').join(','));

    if (!isNaN(stDate)) {
        datePickerHandler(Number(stDate), Number(endDate), 'custom');
    } else if (stDate !== 'now-15m') {
        datePickerHandler(stDate, endDate, stDate);
    } else {
        datePickerHandler(stDate, endDate, '');
    }
    let filterValue = '';
    if (currentTab == 0) {
        queryLanguage = 'Splunk QL';
        //concat the 3 input boxes
        filterValue = getQueryBuilderCode();
        isQueryBuilderSearch = true;

        //Default search
        if (isInitialLoad && (!filterValue || filterValue === '*')) {
            filterValue = '* | head 10';
        }
    } else {
        const inputValue = $('#filter-input').val().trim();

        // If it's initial load and the field is empty (Default search)
        if (isInitialLoad && !inputValue) {
            $('#filter-input').val('* | head 10');
            filterValue = '* | head 10';
        } else {
            filterValue = inputValue || '*';
        }
    }

    if (!skipPushState) {
        addQSParm('searchText', filterValue);
        addQSParm('startEpoch', stDate);
        addQSParm('endEpoch', endDate);
        addQSParm('indexName', selIndexName);
        addQSParm('queryLanguage', queryLanguage);
        addQSParm('filterTab', currentTab);
        addQSParm('fieldsHidden', false);
        window.history.pushState({ path: myUrl }, '', myUrl);
    }

    if (scrollingTrigger) {
        sFrom = totalLoadedRecords;
    }

    return {
        state: wsState,
        searchText: filterValue,
        startEpoch: stDate,
        endEpoch: endDate,
        indexName: selIndexName,
        from: sFrom,
        queryLanguage: queryLanguage,
    };
}
//eslint-disable-next-line no-unused-vars
function getSearchFilterForSave(qname, qdesc) {
    let currentTab = $('#custom-code-tab').tabs('option', 'active');
    let filterValue = '';
    if (currentTab == 0) {
        filterValue = getQueryBuilderCode();
    } else {
        filterValue = $('#filter-input').val().trim() || '*';
    }
    return {
        dataSource: 'logs',
        queryName: qname,
        queryDescription: qdesc || '',
        searchText: filterValue,
        indexName: selectedSearchIndex,
        filterTab: currentTab.toString(),
        endTime: data.endEpoch.toString(),
        startTime: data.startEpoch.toString(),
        queryLanguage: $('#query-language-options .query-language-option.active').html(),
    };
}
function processLiveTailQueryUpdate(res, eventType, totalEventsSearched, timeToFirstByte, totalHits) {
    if (res.hits && res.hits.records !== null && res.hits.records.length >= 1 && res.qtype === 'logs-query') {
        let columnOrder = [];
        if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
            columnOrder = _.uniq(
                _.concat(
                    // make timestamp the first column
                    'timestamp',
                    // make logs the second column
                    'logs',
                    res.columnsOrder
                )
            );
        } else {
            columnOrder = _.uniq(
                _.concat(
                    // make timestamp the first column
                    'timestamp',
                    // make logs the second column
                    'logs',
                    res.allColumns
                )
            );
        }
        allLiveTailColumns = res.allColumns;
        renderLogsGrid(columnOrder, res.hits.records);

        if (res && res.hits && res.hits.totalMatched) {
            totalHits = res.hits.totalMatched;
        }
    } else if (logsRowData.length > 0) {
        let columnOrder = [];
        if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
            columnOrder = _.uniq(
                _.concat(
                    // make timestamp the first column
                    'timestamp',
                    // make logs the second column
                    'logs',
                    res.columnsOrder
                )
            );
        } else {
            columnOrder = _.uniq(
                _.concat(
                    // make timestamp the first column
                    'timestamp',
                    // make logs the second column
                    'logs',
                    allLiveTailColumns
                )
            );
        }
        renderLogsGrid(columnOrder, logsRowData);
        totalHits = logsRowData.length;
    } else if (res.measure && (res.qtype === 'aggs-query' || res.qtype === 'segstats-query')) {
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

        aggsColumnDefs = [];
        segStatsRowData = [];
        renderMeasuresGrid(columnOrder, res);
    }
    let totalTime = Number(new Date().getTime() - startQueryTime).toLocaleString();
    let percentComplete = res.percent_complete;
    let totalPossibleEvents = res.total_possible_events;
    renderTotalHits(totalHits, totalTime, percentComplete, eventType, totalEventsSearched, timeToFirstByte, '', res.qtype, totalPossibleEvents);
    $('body').css('cursor', 'default');
}

function processQueryUpdate(res, eventType, totalEventsSearched, timeToFirstByte, totalHits) {
    lastQType = res.qtype;

    if (res.hits && res.hits.records !== null && res.hits.records.length >= 1 && res.qtype === 'logs-query') {
        if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
            lastColumnsOrder = _.uniq(['timestamp', 'logs', ...res.columnsOrder]);
        } else {
            lastColumnsOrder = _.uniq(['timestamp', 'logs', ...res.allColumns]);
        }

        columnCount = Math.max(columnCount, lastColumnsOrder.length) - 2;

        if (res.hits && res.hits.records) {
            accumulatedRecords = [...accumulatedRecords, ...res.hits.records];

            $('#logs-result-container').show();
            $('#agg-result-container').hide();
            $('#views-container, .fields-sidebar, .fields-resizer').show();

            //eslint-disable-next-line no-undef
            updatePaginationState(res);

            if (res.hits.totalMatched) {
                totalHits = res.hits.totalMatched;
            }
        }
    }

    let totalTime = Number(new Date().getTime() - startQueryTime).toLocaleString();
    let percentComplete = res.percent_complete;
    let totalPossibleEvents = res.total_possible_events;

    renderTotalHits(totalHits, totalTime, percentComplete, eventType, totalEventsSearched, timeToFirstByte, '', res.qtype, totalPossibleEvents, columnCount);
}

function processLiveTailCompleteUpdate(res, eventType, totalEventsSearched, timeToFirstByte, eqRel) {
    let columnOrder = [];
    let totalHits = res.totalMatched.value + logsRowData.length;
    if (res.totalMatched.value + logsRowData.length > 500) totalHits = 500;
    if (logsRowData.length == 0 && res.totalMatched.value === 0 && res.measure === undefined) {
        processEmptyQueryResults('Your query returned no data, adjust your query.');
    }
    if (res.measure) {
        if (res.groupByCols) {
            columnOrder = _.uniq(_.concat(res.groupByCols));
        }
        if (res.measureFunctions) {
            columnOrder = _.uniq(_.concat(columnOrder, res.measureFunctions));
        }
        resetDashboard();
        $('#logs-result-container').hide();
        $('#custom-chart-tab').show().css({ height: '100%' });
        $('#agg-result-container').show();
        aggsColumnDefs = [];
        segStatsRowData = [];
        renderMeasuresGrid(columnOrder, res);
        if ((res.qtype === 'aggs-query' || res.qtype === 'segstats-query') && res.bucketCount) {
            totalHits = res.bucketCount;
        }
    }

    let totalTime = Number(new Date().getTime() - startQueryTime).toLocaleString();
    let percentComplete = res.percent_complete;
    if (res.total_rrc_count > 0) {
        totalRrcCount += res.total_rrc_count;
    }
    let totalPossibleEvents = res.total_possible_events;
    renderTotalHits(totalHits, totalTime, percentComplete, eventType, totalEventsSearched, timeToFirstByte, eqRel, res.qtype, totalPossibleEvents);
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    wsState = 'query';
    if (canScrollMore === false) {
        scrollFrom = 0;
    }
}

function processCompleteUpdate(res, eventType, totalEventsSearched, timeToFirstByte, eqRel) {
    let totalHits = res.totalMatched ? res.totalMatched.value : 0;

    if (res.qtype === 'logs-query' && res.hits && res.hits.records) {
        accumulatedRecords = [...accumulatedRecords, ...res.hits.records];
    }

    //eslint-disable-next-line no-undef
    updatePaginationState(res);

    if ((totalHits === 0 || totalHits === undefined) && res.measure === undefined && accumulatedRecords.length === 0) {
        processEmptyQueryResults();
    } else {
        if (res.measureFunctions && res.measureFunctions.length > 0) {
            measureFunctions = res.measureFunctions;
        }

        if (res.qtype === 'aggs-query' || res.qtype === 'segstats-query') {
            measureInfo = res.measure;

            if (res.columnsOrder != undefined && res.columnsOrder.length > 0) {
                lastColumnsOrder = res.columnsOrder;
            } else {
                if (res.groupByCols) {
                    lastColumnsOrder = _.uniq(_.concat(res.groupByCols));
                }
                if (res.measureFunctions) {
                    lastColumnsOrder = _.uniq(_.concat(lastColumnsOrder, res.measureFunctions));
                }
            }

            resetDashboard();
            $('#logs-result-container').hide();
            $('#custom-chart-tab').show().css({ height: '100%' });
            $('#agg-result-container').show();
            aggsColumnDefs = [];
            segStatsRowData = [];

            renderMeasuresGrid(lastColumnsOrder, res);

            if (res.bucketCount) {
                totalHits = res.bucketCount;
                $('#views-container, .fields-sidebar, .fields-resizer').hide();
                columnCount = Math.max(columnCount, lastColumnsOrder.length);
            }
        } else if (res.qtype === 'logs-query' && accumulatedRecords.length > 0) {
            renderLogsGrid(lastColumnsOrder, accumulatedRecords);

            //eslint-disable-next-line no-undef
            initializeAvailableFieldsSidebar(lastColumnsOrder);
        }

        isTimechart = res.isTimechart;
        lastQType = res.qtype;
        timeChart(res.qtype);
    }

    let totalTime = Number(new Date().getTime() - startQueryTime).toLocaleString();
    let percentComplete = res.percent_complete;
    if (res.total_rrc_count > 0) {
        totalRrcCount += res.total_rrc_count;
    }
    let totalPossibleEvents = res.total_possible_events;

    renderTotalHits(totalHits, totalTime, percentComplete, eventType, totalEventsSearched, timeToFirstByte, eqRel, res.qtype, totalPossibleEvents, columnCount);

    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    wsState = 'query';
    if (canScrollMore === false) {
        scrollFrom = 0;
    }
    $('body').css('cursor', 'default');
}
function processTimeoutUpdate(res) {
    showError(`Query ${res.qid} timed out`, `Your query exceeded the <strong>${res.timeoutSeconds} second</strong> time limit.`);
}

function processCancelUpdate(res) {
    showError(`Query ${res.qid} has been cancelled`, 'The query was terminated before completion.');
    $('#show-record-intro-btn').hide();
}

function processErrorUpdate(res) {
    showError(`Message: ${res.message}`);
}

function processSearchErrorLog(res) {
    if (res.can_scroll_more === false) {
        showError('Scroll limit reached (10,000 logs)', `You've reached the maximum number of logs that can be displayed.`);
    } else if (res.message != '') {
        showErrorResponse(res);
        resetDashboard();
    }
}

function processEmptyQueryResults() {
    $('#views-container, .fields-sidebar, .pagination-container, #logs-result-container,#agg-result-container,#corner-popup').hide();
    $('#save-query-div').children().hide();
    $('#custom-chart-tab').hide();
    $('.json-popup').hide();

    $('#show-record-intro-btn').show();
    $('#empty-response').empty().show();

    addEmptyMessagePopup();
}

function showErrorResponse(res) {
    $('#views-container, .fields-sidebar, .pagination-container, #logs-result-container,#agg-result-container,#corner-popup').hide();
    $('#save-query-div').children().hide();
    $('#custom-chart-tab').hide();
    $('.json-popup').hide();

    $('#empty-response').empty().show();
    if (res && res.no_data_err && res.no_data_err.includes('No data found')) {
        addEmptyMessagePopup();
    } else {
        showError(`Message: ${res.message}`);
    }

    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');

    wsState = 'query';
}

function addEmptyMessagePopup() {
    let el = $('#empty-response');
    el.append(`
    <div class="no-results-container">
        <div class="icon-container">
        <svg width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="#6449D6" stroke-width="2">
            <circle cx="11" cy="11" r="8"></circle>
            <line x1="21" y1="21" x2="16.65" y2="16.65"></line>
        </svg>
        </div>
        <div class="main-message">No results found for your current query</div>
        <div class="sub-message">We couldn't find any logs matching your criteria. Try the following:</div>
        <div class="suggestions-container">
            <div class="suggestion-item"><span>⏰</span><span class="suggestion-text">Expand the time range</span></div>
            <div class="suggestion-item"><span>🔍</span><span class="suggestion-text">Remove some filters</span></div>
        </div>
    </div>
`);
}

function addSyntaxMessagePopup() {
    let el = $('#empty-response');
    el.append(`
        <div class="syntax-error-container">
        <div class="icon-container">
        <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="#ef4444" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
            <circle cx="12" cy="12" r="10"></circle>
            <line x1="12" y1="8" x2="12" y2="12"></line>
            <line x1="12" y1="16" x2="12.01" y2="16"></line>
        </svg>
        </div>
        <div class="main-message">Your query contains syntax error</div>
        <div class="sub-message">Please check your query for:</div>
        <div class="suggestions-container">
            <div class="suggestion-item"><span class="suggestion-text">Missing quotes or brackets</span></div>            
            <div class="suggestion-item"><span class="suggestion-text">Invalid operators</span></div>
            <div class="suggestion-item"><span class="suggestion-text">Incorrect function syntax</span></div>
        </div>
    </div>
`);
}

function renderTotalHits(totalHits, elapedTimeMS, percentComplete, eventType, totalEventsSearched, timeToFirstByte, eqRel, qtype, totalPossibleEvents, columnCount) {
    //update chart title
    console.log(`rendering total hits: ${totalHits}. elapedTimeMS: ${elapedTimeMS}`);
    let startDate = displayStart;
    let endDate = displayEnd;
    // Check if totalHits is undefined and set it to 0
    let totalHitsFormatted = Number(totalHits || 0).toLocaleString();

    if (eventType === 'QUERY_UPDATE') {
        if (totalHits > 0) {
            $('#hits-summary').html(`
            <div><span class="total-hits"><b>${totalHitsFormatted}</b> </span><span>of <b>${totalEventsSearched}</b> Records Matched</span> </div>
            <div>First Byte Response Time: <b>${timeToFirstByte} ms</b></div>
            <div>Elapsed Time: <b>${elapedTimeMS} ms</b></div>
            <div>${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
        `);
            $('#record-searched').html(`<div><span class="total-hits"><b>${totalHitsFormatted}</b> </span><span>of <b>${totalEventsSearched}</b> Records Matched (out of <b>${totalPossibleEvents}</b> Possible Records)</span> </div>`);
        } else {
            $('#hits-summary').html(`<div><span> <b>${totalEventsSearched} </b>Records Searched</span> </div>

            <div>First Byte Response Time:<b> ${timeToFirstByte} ms</b></div>
            <div>Elapsed Time: <b>${elapedTimeMS} ms</b></div>
            <div>${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
        `);
            $('#record-searched').html(`<div><span><b>${totalEventsSearched}</b></span> of <span><b>${totalPossibleEvents}</b> Records Searched</span> </div>`);
        }
        $('#progress-div').html(`
            <progress id="percent-complete" value=${percentComplete} max="100">${percentComplete}</progress>
            <div id="percent-value">${parseInt(percentComplete)}%</div>
        `);
    } else if (eventType === 'COMPLETE') {
        let operatorSign = '';
        if (eqRel === 'gte') {
            operatorSign = '>=';
        }
        if (qtype == 'aggs-query' || qtype === 'segstats-query') {
            let bucketGrammer = totalHits == 1 ? 'bucket was' : 'buckets were';
            $('#hits-summary').html(`
            <div>First Result Response Time: <b>${timeToFirstByte} ms</b></div>
            <div class="final-res-time">Final Result Response Time: <span></span><b> ms</b></div>
            <div><span class="total-hits"><b>${operatorSign} ${totalHitsFormatted}</b></span><span> ${bucketGrammer} created from <b>${totalEventsSearched}</b> records.</span></div>
            <div class="column-count">Column Count: <span></span><b> ${columnCount}</b></div>
            <div>${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
        `);
        } else if (totalHits > 0) {
            $('#hits-summary').html(`
            <div>First Result Response Time: <b>${timeToFirstByte} ms</b></div>
            <div class="final-res-time">Final Result Response Time: <span></span><b> ms</b></div>
            <div><span class="total-hits"><b>${operatorSign} ${totalHitsFormatted}</b></span><span> of <b>${totalEventsSearched}</b> Records Matched</span></div>
            <div class="column-count">Column Count: <span></span><b> ${columnCount}</b></div>
            <div>${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
        `);
        } else {
            $('#hits-summary').html(`
            <div>First Result Response Time: <b>${timeToFirstByte} ms</b></div>
            <div class="final-res-time">Final Result Response Time: <span></span><b> ms</b></div>
            <div>Records Searched: <span><b> ${totalEventsSearched} </b></span></div>
            <div class="column-count">Column Count: <span></span><b> ${columnCount}</b></div>
            <div>${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
        `);
        }
        $('#progress-div').html(``);
        $('#record-searched').html(``);
    }
}

// LiveTail Refresh Duration
let refreshInterval = 10000;

$('.refresh-range-item').on('click', refreshRangeItemHandler);

function refreshRangeItemHandler(evt) {
    $.each($('.refresh-range-item.active'), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    let interval = $(evt.currentTarget).attr('id');
    $('#refresh-picker-btn span').html(interval);

    refreshInterval = parseInterval(interval); // Parsing interval
    if (liveTailState) reconnect();
}

function parseInterval(interval) {
    const regex = /(\d+)([smhd])/;
    const match = interval.match(regex);
    const value = parseInt(match[1]);
    const unit = match[2];

    switch (unit) {
        case 's':
            return value * 1000;
        case 'm':
            return value * 60 * 1000;
        case 'h':
            return value * 60 * 60 * 1000;
        case 'd':
            return value * 24 * 60 * 60 * 1000;
        default:
            throw new Error('Invalid interval unit');
    }
}

function timeChart(qtype) {
    // Check if measureInfo is defined and contains at least one item
    qtype = qtype || lastQType;
    if (isTimechart || qtype === 'aggs-query') {
        $('#columnChart').show();
        $('#hideGraph').hide();
    } else {
        $('#columnChart').hide();
        $('#hideGraph').show();
        return;
    }

    if (!measureInfo || measureInfo.length === 0) {
        return;
    }

    // Ensure all items in measureInfo have GroupByValues property before proceeding
    const hasGroupByValues = measureInfo.every((item) => item.GroupByValues);

    if (!hasGroupByValues) {
        return;
    }

    // Check if there are multiple group-by columns
    var multipleGroupBy = measureInfo[0].GroupByValues.length > 1;

    // Determine the font size and rotation based on the number of data points
    var fontSize = measureInfo.length > 10 ? 10 : 12;
    var rotateLabels = measureInfo.length > 10 ? 45 : 0;

    var xData = measureInfo.map((item) => formatGroupByValues(item.GroupByValues, multipleGroupBy));

    var seriesData = measureFunctions.map(function (measureFunction) {
        return {
            name: measureFunction,
            type: 'bar',
            data: measureInfo.map(function (item) {
                return item.MeasureVal[measureFunction] || 0;
            }),
        };
    });

    // ECharts configuration
    var option = {
        tooltip: {
            trigger: 'item',
            formatter: function (params) {
                return params.seriesName + ': ' + params.value;
            },
        },
        legend: {
            textStyle: {
                color: '#6e7078',
                fontSize: 12,
            },
            data: measureFunctions,
            type: 'scroll', // Enable folding functionality
            orient: 'vertical',
            right: 10,
            top: 'middle',
            align: 'left',
            height: '70%',
            width: 150,
        },
        grid: {
            left: 10,
            right: 220,
            containLabel: true,
        },
        xAxis: {
            type: 'category',
            data: xData,
            scale: true,
            splitLine: { show: false },
            axisLabel: {
                fontSize: fontSize,
                rotate: rotateLabels,
            },
        },
        yAxis: {
            type: 'value',
            scale: true,
            splitLine: { show: false },
        },
        series: seriesData,
    };

    // Initialize ECharts
    let chart = echarts.init($('#columnChart')[0]);
    chart.clear(); // Clear previous data
    // Set the configuration to the chart
    chart.setOption(option);

    // Ensure the chart resizes properly
    chart.resize();

    // Made the chart responsive
    $(window).on('resize', function () {
        chart.resize();
    });
}

function formatGroupByValues(groupByValues, multipleGroupBy) {
    if (multipleGroupBy) {
        return groupByValues.map(convertIfTimestamp).join(', ');
    } else {
        return convertIfTimestamp(groupByValues[0]);
    }
}

function convertTimestamp(timestampString) {
    var timestamp = parseInt(timestampString);
    var date = new Date(timestamp);

    var year = date.getFullYear();
    var month = ('0' + (date.getMonth() + 1)).slice(-2);
    var day = ('0' + date.getDate()).slice(-2);

    var hours = ('0' + date.getHours()).slice(-2);
    var minutes = ('0' + date.getMinutes()).slice(-2);
    var seconds = ('0' + date.getSeconds()).slice(-2);

    var readableDate = year + '-' + month + '-' + day + ' ' + hours + ':' + minutes + ':' + seconds;
    return readableDate;
}

function convertIfTimestamp(value) {
    // Check if the value is a valid timestamp (e.g., length and date after 1970)
    const isTimestamp = !isNaN(value) && value.length === 13 && new Date(parseInt(value)).getTime() > 0;
    if (isTimestamp) {
        return convertTimestamp(value);
    }
    return value;
}

function codeToBuilderParsing(filterValue) {
    if (filterValue.indexOf('|') != -1) {
        firstBoxSet = new Set(filterValue.split(' | ')[0].match(/(?:[^\s"]+|"[^"]*")+/g));
        secondBoxSet = new Set(
            filterValue
                .split('stats ')[1]
                .split(' BY')[0]
                .split(/(?=[A-Z])/)
        );
        if (filterValue.includes(' BY ')) {
            thirdBoxSet = new Set(filterValue.split(' BY ')[1].split(','));
        }
    } else {
        firstBoxSet = new Set(filterValue.match(/(?:[^\s"]+|"[^"]*")+/g));
    }
    if (firstBoxSet && firstBoxSet.size > 0) {
        let tags = document.getElementById('tags');
        while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
        }
        firstBoxSet.forEach((value, _i) => {
            let tag = document.createElement('li');
            tag.innerText = value;
            // Add a delete button to the tag
            tag.innerHTML += '<button class="delete-button">×</button>';
            // Append the tag to the tags list
            tags.appendChild(tag);
        });
    }
    if (secondBoxSet && secondBoxSet.size > 0) {
        let tags = document.getElementById('tags-second');
        while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
        }
        secondBoxSet.forEach((value, _i) => {
            let tag = document.createElement('li');
            tag.innerText = value;
            // Add a delete button to the tag
            tag.innerHTML += '<button class="delete-button">×</button>';
            // Append the tag to the tags list
            tags.appendChild(tag);
        });
    }
    if (thirdBoxSet && thirdBoxSet.size > 0) {
        let tags = document.getElementById('tags-third');
        while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
        }
        thirdBoxSet.forEach((value, _i) => {
            let tag = document.createElement('li');
            tag.innerText = value;
            // Add a delete button to the tag
            tag.innerHTML += '<button class="delete-button">×</button>';
            // Append the tag to the tags list

            tags.appendChild(tag);
        });
    }
    if (thirdBoxSet.size > 0) $('#aggregations').hide();
    else $('#aggregations').show();
    if (secondBoxSet.size > 0) $('#aggregate-attribute-text').hide();
    else $('#aggregate-attribute-text').show();
    if (firstBoxSet.size > 0) $('#search-filter-text').hide();
    else $('#search-filter-text').show();
}

function renderLogsGrid(columnOrder, hits) {
    if (gridDiv == null) {
        gridDiv = document.querySelector('#LogResultsGrid');
        //eslint-disable-next-line no-undef
        new agGrid.Grid(gridDiv, gridOptions);
    }

    const logView = getLogView();

    let cols = columnOrder.map((colName) => {
        if (colName === 'timestamp') {
            return {
                field: colName,
                headerName: colName,
                cellRenderer: function (params) {
                    return moment(params.value).format(timestampDateFmt);
                },
            };
        } else {
            return {
                field: colName,
                headerName: colName,
                cellRenderer: myCellRenderer,
                cellRendererParams: { colName: colName },
            };
        }
    });
    if (hits.length !== 0) {
        logsRowData = hits;
        totalLoadedRecords = hits.length;
        updateGridView();
    }

    const logsColumnDefsMap = new Map(logsColumnDefs.map((logCol) => [logCol.field, logCol]));
    const combinedColumnDefs = cols.map((col) => logsColumnDefsMap.get(col.field) || col);

    logsColumnDefs.forEach((logCol) => {
        if (!combinedColumnDefs.some((col) => col.field === logCol.field)) {
            combinedColumnDefs.push(logCol);
        }
    });

    logsColumnDefs = combinedColumnDefs;
    gridOptions.api.setColumnDefs(logsColumnDefs);

    const allColumnIds = [];
    gridOptions.columnApi.getColumns().forEach((column) => {
        allColumnIds.push(column.getId());
    });
    gridOptions.columnApi.autoSizeColumns(allColumnIds, false);

    gridOptions.api.setRowData(logsRowData);

    //eslint-disable-next-line no-undef
    handleLogOptionChange(logView);
}

function getLogView() {
    //eslint-disable-next-line no-undef
    return Cookies.get('log-view') || VIEW_TYPES.TABLE;
}

function updateGridView() {
    if (!accumulatedRecords.length) return;

    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, totalLoadedRecords);

    const currentPageData = accumulatedRecords.slice(startIndex, endIndex);

    if (currentPageData.length > 0 && gridOptions?.api) {
        gridOptions.api.setRowData(currentPageData);

        const allColumnIds = [];
        gridOptions.columnApi.getColumns().forEach((column) => {
            allColumnIds.push(column.getId());
        });
        gridOptions.columnApi.autoSizeColumns(allColumnIds, false);
    }
}
