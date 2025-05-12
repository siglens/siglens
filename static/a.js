let lastQType = '';
let lastColumnsOrder = [];
let accumulatedRecords = []; // For histogram data
let total_liveTail_searched = 0;

function wsURL(path) {
    var protocol = location.protocol === 'https:' ? 'wss://' : 'ws://';
    var url = protocol + location.host;
    return url + path;
}

// Cancel a search query
function doCancel(data) {
    socket.send(JSON.stringify(data));
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    $('#progress-div').html(``);
    $('#record-searched').html(``);
}

// Cancel live tail mode
function doLiveTailCancel(_data) {
    liveTailState = false;
    $('body').css('cursor', 'default');
    $('#live-tail-btn').html('Live Tail');
    $('#live-tail-btn').removeClass('active');
    $('#progress-div').html(``);
    if (socket) {
        socket.close(1000);
    }
}

// Reset the data table UI
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

// Perform a search query via WebSocket
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
                    processErrorUpdate(jsonEvent.message);
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
            if (event.code === 1000 || event.code === 1001) {
                console.log(`[close] Connection closed normally with code ${event.code}`);
            } else if (event.wasClean) {
                console.log(`[close] Connection closed cleanly, code=${event.code}`);
            } else {
                let errorMessage;
                if (event.code === 1006) {
                    errorMessage = 'Connection failed - The server may be down or unreachable';
                } else {
                    errorMessage = `Connection closed abnormally (code: ${event.code})`;
                }
                console.log(`Abnormal WebSocket close: code=${event.code}`);
                errorMessages.push(errorMessage);

                processErrorUpdate(errorMessage);
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

// Reconnect for live tail
function reconnect() {
    if (lockReconnect) {
        return;
    }
    lockReconnect = true;
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

// Create WebSocket for live tail
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

// Perform live tail search
function doLiveTailSearch(data) {
    let timeToFirstByte = 0;
    let firstQUpdate = true;
    let lastKnownHits = 0;
    socket.onopen = function (_e) {
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
                    totalHits = lastKnownHits;
                }
                resetDataTable(firstQUpdate);
                processLiveTailQueryUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, totalHits);
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
                processErrorUpdate(jsonEvent.message);
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
        console.log(`[close] Live tail connection closed with code ${event.code}`);
        reconnect();
    };

    socket.onerror = function (error) {
        console.log(`[error] Live tail WebSocket error: ${error}`);
        reconnect();
    };
}

// Process QUERY_UPDATE state
function processQueryUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, totalHits) {
    if (jsonEvent.hits && jsonEvent.hits.records) {
        // Accumulate records for histogram
        accumulatedRecords = accumulatedRecords.concat(jsonEvent.hits.records);

        // Update UI
        $('#logs-result-container').show();
        renderLogsGrid(jsonEvent.hits.records, jsonEvent.allColumns || lastColumnsOrder, jsonEvent.qtype || lastQType);
        renderTotalHits(totalHits, eventType, totalEventsSearched, timeToFirstByte);

        // Render histogram if enabled
        if (runTimeChart && $('#histogram-container').is(':visible')) {
            renderHistogram();
        }
    }
}

// Process COMPLETE state
function processCompleteUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, eqRel) {
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');

    if (jsonEvent.hits && jsonEvent.hits.records) {
        // Accumulate records for histogram
        accumulatedRecords = accumulatedRecords.concat(jsonEvent.hits.records);

        // Update UI
        $('#logs-result-container').show();
        lastQType = jsonEvent.qtype || lastQType;
        lastColumnsOrder = jsonEvent.allColumns || lastColumnsOrder;
        renderLogsGrid(jsonEvent.hits.records, lastColumnsOrder, lastQType);
        renderTotalHits(jsonEvent.hits.totalMatched, eventType, totalEventsSearched, timeToFirstByte, eqRel);

        // Render histogram if enabled
        if (runTimeChart && $('#histogram-container').is(':visible')) {
            renderHistogram();
        }
    } else if (jsonEvent.hits.totalMatched.value === 0) {
        showError('No Results', 'Your query returned no data.');
    }

    wsState = 'query';
}

// Process CANCELLED state
function processCancelUpdate(jsonEvent) {
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    showToast('Search cancelled', 'info', 3000);
    wsState = 'query';
}

// Process TIMEOUT state
function processTimeoutUpdate(jsonEvent) {
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    showError('Query Timeout', 'The query took too long to execute.');
    wsState = 'query';
}

// Process ERROR state
function processErrorUpdate(message) {
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    showError('Search Error', message || 'An error occurred during the search.');
    wsState = 'query';
}

// Process search error log
function processSearchErrorLog(jsonEvent) {
    $('body').css('cursor', 'default');
    $('#run-filter-btn').removeClass('cancel-search').removeClass('active');
    $('#query-builder-btn').removeClass('cancel-search').removeClass('active');
    let errorMessage = jsonEvent.no_data_err || jsonEvent.message || 'An error occurred during the search.';
    showError('Search Error', errorMessage);
    wsState = 'query';
}

// Process live tail QUERY_UPDATE
function processLiveTailQueryUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, totalHits) {
    if (jsonEvent.hits && jsonEvent.hits.records) {
        // Accumulate records for histogram
        accumulatedRecords = accumulatedRecords.concat(jsonEvent.hits.records);

        // Update UI
        $('#logs-result-container').show();
        renderLogsGrid(jsonEvent.hits.records, jsonEvent.allColumns || lastColumnsOrder, jsonEvent.qtype || lastQType);
        renderTotalHits(totalHits, eventType, totalEventsSearched, timeToFirstByte);

        // Render histogram if enabled
        if (runTimeChart && $('#histogram-container').is(':visible')) {
            renderHistogram();
        }
    }
}

// Process live tail COMPLETE
function processLiveTailCompleteUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, eqRel) {
    if (jsonEvent.hits && jsonEvent.hits.records) {
        // Accumulate records for histogram
        accumulatedRecords = accumulatedRecords.concat(jsonEvent.hits.records);

        // Update UI
        $('#logs-result-container').show();
        lastQType = jsonEvent.qtype || lastQType;
        lastColumnsOrder = jsonEvent.allColumns || lastColumnsOrder;
        renderLogsGrid(jsonEvent.hits.records, lastColumnsOrder, lastQType);
        renderTotalHits(jsonEvent.hits.totalMatched, eventType, totalEventsSearched, timeToFirstByte, eqRel);

        // Render histogram if enabled
        if (runTimeChart && $('#histogram-container').is(':visible')) {
            renderHistogram();
        }
    }
    reconnect();
}

// Display syntax error popup
function addSyntaxMessagePopup() {
    showToast('Syntax Error', 'error', null);
}
