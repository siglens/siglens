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

let chart;
let currList = [];
let returnResTotal = [],
    scatterData = [];
let pageNumber = 1,
    traceSize = 0,
    params = {};
let limitation = -1;
let hasLoaded = false;
let allResultsFetched = false;
let totalTraces = 0;
let initialSearchPerformed = false;
let dropdownsInitialized = false;
$(document).ready(() => {
    allResultsFetched = false;
    $('.theme-btn').on('click', themePickerHandler);
    $('.theme-btn').on('click', showScatterPlot);

    initPage();

    // Add popstate event listener for browser navigation
    window.removeEventListener('popstate', handlePopState);
    window.addEventListener('popstate', handlePopState);

});
window.onload = function () {
    hasLoaded = true;

    const isRefresh = sessionStorage.getItem('pageRefreshed');

    if (!isRefresh) {
        sessionStorage.setItem('pageRefreshed', 'true');

        if (!initialSearchPerformed) {
            getInitialSearchState();

            const urlParams = new URLSearchParams(window.location.search);
            if (urlParams.toString()) {
                setTimeout(() => {
                    searchTraceHandler(new Event('init'));
                }, 300);
            }
        }
    } else {
        const urlParams = new URLSearchParams(window.location.search);
        if (urlParams.toString() && !initialSearchPerformed) {
            setTimeout(() => {
                searchTraceHandler(new Event('init'));
            }, 300);
        }
    }
};

function initPage() {
    initChart(true);
    // Initialize dropdowns with URL values first
    initializeDropdowns();
    handleSort();
    handleDownload();
    handleTimePicker();
    $('#search-trace-btn').on('click', searchTraceHandler);
}

function initializeDropdowns() {
    if (dropdownsInitialized) {
        return;
    }
    const state = getInitialSearchState();
    const serviceValue = state.service || 'All';
    const operationValue = state.operation || 'All';

    dropdownsInitialized = true;

    getValuesOfColumn('service', 'Service', serviceValue);

    setTimeout(() => {
        getValuesOfColumn('name', 'Operation', operationValue);
    }, 100);
}


function getValuesOfColumn(chooseColumn, spanName, defaultValue = 'All') {
    let searchText = 'SELECT DISTINCT ' + '`' + chooseColumn + '`' + ' FROM `traces`';

    // Add conditional logic for operations dropdown
    if (chooseColumn === 'name' && $('#service-span-name').text() && $('#service-span-name').text() !== 'All') {
        searchText += " WHERE service='" + $('#service-span-name').text() + "'";
    }

    const urlParams = new URLSearchParams(window.location.search);
    let startEpoch = urlParams.get('startEpoch') || Cookies.get('startEpoch') || 'now-1h';
    let endEpoch = urlParams.get('endEpoch') || Cookies.get('endEpoch') || 'now';

    let param = {
        state: 'query',
        searchText: searchText,
        startEpoch: startEpoch || 'now-1h',
        endEpoch: endEpoch || filterEndDate,
        indexName: 'traces',
        queryLanguage: 'SQL',
        from: 0,
    };

    return $.ajax({
        method: 'post',
        url: 'api/search',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(param),
    }).then((res) => {
        let valuesOfColumn = new Set();
        valuesOfColumn.add('All');
        if (res && res.hits && res.hits.records) {
            for (let i = 0; i < res.hits.records.length; i++) {
                let cur = res.hits.records[i][chooseColumn];
                if (typeof cur == 'string') valuesOfColumn.add(cur);
                else valuesOfColumn.add(cur.toString());
            }
        }
        currList = Array.from(valuesOfColumn);

        if ($(`#${chooseColumn}-dropdown`).length) {
            $(`#${chooseColumn}-dropdown`).singleBox({
                spanName: spanName,
                dataList: currList,
                defaultValue: defaultValue,
                dataUpdate: true,
                clickedHead: async function () {
                    await fetchData(chooseColumn);
                    return currList;
                },
            });
        }

        return currList;
    });
}

function fetchData(chooseColumn) {
    return new Promise((resolve, reject) => {
        let searchText = 'SELECT DISTINCT ' + '`' + chooseColumn + '`' + ' FROM `traces`';
        if (chooseColumn == 'name' && $('#service-span-name').text() && $('#service-span-name').text() != 'All') {
            searchText += " WHERE service='" + $('#service-span-name').text() + "'";
        } else if (chooseColumn == 'service' && $('#operation-span-name').text() && $('#operation-span-name').text() != 'All') {
            searchText += " WHERE name='" + $('#operation-span-name').text() + "'";
        }
        let param = {
            state: 'query',
            searchText: searchText,
            startEpoch: 'now-1h',
            endEpoch: filterEndDate,
            indexName: 'traces',
            queryLanguage: 'SQL',
            from: 0,
        };
        $.ajax({
            method: 'post',
            url: 'api/search',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(param),
        })
            .then((res) => {
                let valuesOfColumn = new Set();
                valuesOfColumn.add('All');
                if (res && res.hits && res.hits.records) {
                    for (let i = 0; i < res.hits.records.length; i++) {
                        let cur = res.hits.records[i][chooseColumn];
                        if (typeof cur == 'string') valuesOfColumn.add(cur);
                        else valuesOfColumn.add(cur.toString());
                    }
                }
                currList = Array.from(valuesOfColumn);
                resolve(currList);
            })
            .catch((error) => {
                reject(error);
            });
    });
}

function handleTimePicker() {
    const urlParams = new URLSearchParams(window.location.search);
    const startEpoch = urlParams.get('startEpoch') || Cookies.get('startEpoch') || 'now-1h';
    const endEpoch = urlParams.get('endEpoch') || Cookies.get('endEpoch') || 'now';

    Cookies.set('startEpoch', startEpoch);
    Cookies.set('endEpoch', endEpoch);

    // Convert startEpoch to display format
    let displayText = 'Last 1 Hr';
    if (startEpoch === 'now-1h') displayText = 'Last 1 Hr';
    else if (startEpoch === 'now-15m') displayText = 'Last 15 Min';
    else if (startEpoch === 'now-30m') displayText = 'Last 30 Min';
    else if (startEpoch === 'now-3h') displayText = 'Last 3 Hr';
    else if (startEpoch === 'now-6h') displayText = 'Last 6 Hr';
    else if (startEpoch === 'now-12h') displayText = 'Last 12 Hr';
    else if (startEpoch === 'now-24h') displayText = 'Last 24 Hr';
    else if (startEpoch === 'now-7d') displayText = 'Last 7 Days';
    else if (startEpoch === 'now-30d') displayText = 'Last 30 Days';
    else if (!isNaN(startEpoch)) displayText = 'Custom Range';

    $('#lookback').timeTicker({
        spanName: displayText,
        clicked: function(selectedTime) {
            // Map the display text to actual time values
            let startTime;
            let endTime = 'now';

            if (selectedTime === 'Last 15 Min') startTime = 'now-15m';
            else if (selectedTime === 'Last 30 Min') startTime = 'now-30m';
            else if (selectedTime === 'Last 1 Hr') startTime = 'now-1h';
            else if (selectedTime === 'Last 3 Hr') startTime = 'now-3h';
            else if (selectedTime === 'Last 6 Hr') startTime = 'now-6h';
            else if (selectedTime === 'Last 12 Hr') startTime = 'now-12h';
            else if (selectedTime === 'Last 24 Hr') startTime = 'now-24h';
            else if (selectedTime === 'Last 7 Days') startTime = 'now-7d';
            else if (selectedTime === 'Last 30 Days') startTime = 'now-30d';
            else startTime = 'now-1h'; // Default fallback

            // Update cookies with the new values
            Cookies.set('startEpoch', startTime);
            Cookies.set('endEpoch', endTime);

            // Update filter dates for searches
            filterStartDate = startTime;
            filterEndDate = endTime;

            // Trigger search with updated time range
            searchTraceHandler(new Event('timeChange'));
        }

    });
}

function handleSort() {
    let currList = ['Most Recent', 'Longest First', 'Shortest First', 'Most Spans', 'Least Spans'];
    $('#sort-dropdown').singleBox({
        spanName: 'Most Recent',
        defaultValue: 'Most Recent',
        dataList: currList,
        clicked: function (e) {
            if (e == 'Most Recent') {
                returnResTotal = returnResTotal.sort(compare('start_time', 'most'));
            } else if (e == 'Longest First') {
                returnResTotal = returnResTotal.sort(compareDuration('most'));
            } else if (e == 'Shortest First') {
                returnResTotal = returnResTotal.sort(compareDuration('least'));
            } else if (e == 'Most Spans') {
                returnResTotal = returnResTotal.sort(compare('span_count', 'most'));
            } else if (e == 'Least Spans') {
                returnResTotal = returnResTotal.sort(compare('span_count', 'least'));
            }
            reSort();
        },
    });
}
function compareDuration(method) {
    return function (object1, object2) {
        let value1 = object1['end_time'] - object1['start_time'];
        let value2 = object2['end_time'] - object2['start_time'];
        if (method == 'most') return value2 - value1;
        else return value1 - value2;
    };
}
function compare(property, method) {
    return function (object1, object2) {
        let value1 = object1[property];
        let value2 = object2[property];
        if (method == 'most') return value2 - value1;
        else return value1 - value2;
    };
}

function resetToDefaults() {
    // Reset UI elements to default values
    $('#service-span-name').text('All');
    $('#operation-span-name').text('All');
    $('#min-duration-input').val('');
    $('#max-duration-input').val('');
    $('#limit-result-input').val('');
    $('#tags-input').val('');

    Cookies.set('startEpoch', 'now-1h');
    Cookies.set('endEpoch', 'now');

    if ($('#lookback').data('timeTicker')) {
        $('#lookback').data('timeTicker').updateText('Last 1 Hr');
    }
    initialSearchPerformed = false;
    // Run default search
    initChart(false);
}

function handleDownload() {
    let currList = ['Download as CSV', 'Download as JSON', 'Download as XML', 'Download as SQL'];
    $('#download-dropdown').singleBox({
        fillIn: false,
        spanName: 'Download Result',
        dataList: currList,
        clicked: function (e) {
            if (e == 'Download as CSV') {
                $('#download-trace').download({
                    data: returnResTotal,
                    downloadMethod: '.csv',
                });
            } else if (e == 'Download as JSON') {
                $('#download-trace').download({
                    data: returnResTotal,
                    downloadMethod: '.json',
                });
            } else if (e == 'Download as XML') {
                $('#download-trace').download({
                    data: returnResTotal,
                    downloadMethod: '.xml',
                });
            } else if (e == 'Download as SQL') {
                $('#download-trace').download({
                    data: returnResTotal,
                    downloadMethod: '.sql',
                });
            }
        },
    });
}

let requestFlag = 0;

// Function to convert duration string to nanoseconds
function durationToNanoseconds(durationStr) {
    if (!durationStr) return null;

    const durationRegex = /([\d.]+)\s*(s|ms|µs|us|µ|u)?/i;
    const match = durationStr.match(durationRegex);

    if (!match) return null;

    const value = parseFloat(match[1]);
    let unit = match[2] ? match[2].toLowerCase() : 's'; // Default to seconds if no unit is specified

    switch (unit) {
        case 's':
            return value * 1e9; // seconds to nanoseconds
        case 'ms':
            return value * 1e6; // milliseconds to nanoseconds
        case 'µs':
        case 'us':
        case 'µ':
        case 'u':
            return value * 1e3; // microseconds to nanoseconds
        default:
            console.warn("No unit provided for duration. Assuming seconds.");
            return value * 1e9; // Assuming seconds if no unit is specified.
    }
}

function getInitialSearchState() {
    const urlParams = new URLSearchParams(window.location.search);
    const defaultParams = {
        service: 'All',
        operation: 'All',
        startEpoch: 'now-1h',
        endEpoch: 'now',
        minDuration: '',
        maxDuration: '',
        limit: '',
        tags: ''
    };

    // Get values from URL or use defaults
    const state = {
        service: urlParams.get('service') || defaultParams.service,
        operation: urlParams.get('operation') || defaultParams.operation,
        startEpoch: urlParams.get('startEpoch') || Cookies.get('startEpoch') || defaultParams.startEpoch,
        endEpoch: urlParams.get('endEpoch') || Cookies.get('endEpoch') || defaultParams.endEpoch,
        minDuration: urlParams.get('minDuration') || defaultParams.minDuration,
        maxDuration: urlParams.get('maxDuration') || defaultParams.maxDuration,
        limit: urlParams.get('limit') || defaultParams.limit,
        tags: urlParams.get('tags') || defaultParams.tags
    };

    // Update UI with URL parameters
    $('#service-span-name').text(state.service);
    $('#operation-span-name').text(state.operation);
    $('#min-duration-input').val(state.minDuration);
    $('#max-duration-input').val(state.maxDuration);
    $('#limit-result-input').val(state.limit);
    $('#tags-input').val(state.tags);

    // Set time range
    datePickerHandler(state.startEpoch, state.endEpoch, state.startEpoch);
    filterStartDate = state.startEpoch;
    filterEndDate = state.endEpoch;

    return state;
}

function updateUrlWithSearchState(state) {
    const url = new URL(window.location);
    const searchParams = url.searchParams;

    // Always update these core parameters
    searchParams.set('service', state.service);
    searchParams.set('operation', state.operation);
    searchParams.set('startEpoch', state.startEpoch);
    searchParams.set('endEpoch', state.endEpoch);

    // Only add non-default parameters if they have values
    if (state.minDuration && state.minDuration.trim() !== '') {
        searchParams.set('minDuration', state.minDuration);
    } else {
        searchParams.delete('minDuration');
    }

    if (state.maxDuration && state.maxDuration.trim() !== '') {
        searchParams.set('maxDuration', state.maxDuration);
    } else {
        searchParams.delete('maxDuration');
    }

    if (state.limit && state.limit.trim() !== '') {
        searchParams.set('limit', state.limit);
    } else {
        searchParams.delete('limit');
    }

    if (state.tags && state.tags.trim() !== '') {
        searchParams.set('tags', state.tags);
    } else {
        searchParams.delete('tags');
    }

    // Update URL without page reload
    window.history.pushState({searchState: state}, '', url);
}

function handlePopState(event) {
    const urlParams = new URLSearchParams(window.location.search);

    if (!urlParams.toString()) {
        resetToDefaults();
        return;
    }

    if (event.state && event.state.searchState) {
        const state = event.state.searchState;

        $('#service-span-name').text(state.service);
        $('#operation-span-name').text(state.operation);
        $('#min-duration-input').val(state.minDuration);
        $('#max-duration-input').val(state.maxDuration);
        $('#limit-result-input').val(state.limit);
        $('#tags-input').val(state.tags);

        datePickerHandler(state.startEpoch, state.endEpoch, state.startEpoch);
    } else {
        getInitialSearchState();
    }

    searchTraceHandler(new Event('popstate'));
}

function searchTraceHandler(e) {
    if (e.type !== 'popstate' && e.type !== 'init' && e.type !== 'timeChange') {  // Don't prevent default for popstate events
        e.stopPropagation();
        e.preventDefault();
    }

    returnResTotal = [];
    scatterData = [];
    pageNumber = 1;
    traceSize = 0;
    params = {};
    $('.warn-box').remove();
    $('#traces-number').text('');

    let serviceValue = $('#service-span-name').text();
    let operationValue = $('#operation-span-name').text();
    let tagValue = $('#tags-input').val();
    let maxDurationValueStr = $('#max-duration-input').val();
    let minDurationValueStr = $('#min-duration-input').val();
    let limitResValue = $('#limit-result-input').val();

    // Convert min and max duration to nanoseconds using the improved function
    let maxDurationValue = durationToNanoseconds(maxDurationValueStr);
    let minDurationValue = durationToNanoseconds(minDurationValueStr);

    if (maxDurationValue === null && maxDurationValueStr) {
        showToast("Invalid format for Max Duration. Examples: 1.2s, 100ms, 500µs", 'error');
        return;
    }

    if (minDurationValue === null && minDurationValueStr) {
        showToast("Invalid format for Min Duration. Examples: 1.2s, 100ms, 500µs", 'error');
        return;
    }

    if (limitResValue) limitation = parseInt(limitResValue);
    else limitation = -1;
    if (limitation > 0 && limitation < 50) {
        requestFlag = limitation;
        limitation = 0;
    }

    // Fix 5: Improved search text formatting
    let searchParts = [];

    searchParts.push(`service=${serviceValue === 'All' ? '*' : JSON.stringify(serviceValue)}`);

    searchParts.push(`name=${operationValue === 'All' ? '*' : JSON.stringify(operationValue)}`);

    if (maxDurationValue) {
        searchParts.push(`duration<=${maxDurationValue}`);
    }

    if (minDurationValue) {
        searchParts.push(`duration>=${minDurationValue}`);
    }

    if (tagValue && tagValue.trim() !== '') {
        searchParts.push(tagValue.trim());
    }

    let searchText = searchParts.length > 0 ? searchParts.join(' ') : '*';

    // Always use the current time range from cookies
    let stDate = Cookies.get('startEpoch') || 'now-1h';
    let endDate = Cookies.get('endEpoch') || 'now';

    // Update URL with current search state
    const currentState = {
        service: serviceValue,
        operation: operationValue,
        startEpoch: stDate,
        endEpoch: endDate,
        minDuration: minDurationValueStr,
        maxDuration: maxDurationValueStr,
        limit: limitResValue,
        tags: tagValue
    };

    updateUrlWithSearchState(currentState);

    pageNumber = 1;
    params = {
        searchText: searchText,
        startEpoch: stDate,
        endEpoch: endDate,
        queryLanguage: 'Splunk QL',
        page: pageNumber,
    };

    allResultsFetched = false;
    if (chart != null && chart !== '' && chart !== undefined) {
        echarts?.dispose(chart);
    }

    $('body').css('cursor', 'progress');
    $('#search-trace-btn').prop('disabled', true).addClass('disabled');

    searchTrace(params);
    handleSort();
    return false;
}

function initChart(skipSearch = false) {
    $('#graph-show').removeClass('empty-result-show');
    pageNumber = 1;
    traceSize = 0;
    returnResTotal = [];

    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.toString()) {
        return;
    }

    let stDate = 'now-1h';
    let endDate = 'now';
    params = {
        searchText: '*',
        startEpoch: stDate,
        endEpoch: endDate,
        queryLanguage: 'Splunk QL',
        page: pageNumber,
    };
    searchTrace(params);

    if (!skipSearch && !initialSearchPerformed) {
        searchTrace(params);
        initialSearchPerformed = true;
    }
}
async function getTotalTraces(params) {
    return $.ajax({
        method: 'post',
        url: 'api/traces/count',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(params),
    }).then((res) => {
        totalTraces = res;
        // Update the total traces number with the response
        $('#traces-number').text(res.toLocaleString('en-US') + ' Traces');
    });
}
function searchTrace(params) {
    $.ajax({
        method: 'post',
        url: 'api/traces/search',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
        data: JSON.stringify(params),
    })
        .then(async (res) => {
            if (res && res.traces && res.traces.length > 0) {
                if ((limitation < 50 && limitation > 0) || limitation == 0) {
                    let newArr = res.traces.sort(compare('start_time', 'most'));
                    if (limitation > 0) newArr.splice(limitation);
                    else newArr.splice(requestFlag);
                    limitation = 0;
                    requestFlag = 0;
                    returnResTotal = returnResTotal.concat(newArr);
                } else {
                    returnResTotal = returnResTotal.concat(res.traces);
                }
                //concat new traces results
                returnResTotal = returnResTotal.sort(compare('start_time', 'most'));
                //reset total size
                traceSize = returnResTotal.length;
                if ($('#traces-number').text().trim() === '') {
                    await getTotalTraces(params);
                }
                scatterData = [];
                for (let i = traceSize - 1; i >= 0; i--) {
                    let json = returnResTotal[i];
                    let milliseconds = Number(json.start_time / 1000000);
                    let dataInfo = new Date(milliseconds);
                    let dataStr = dataInfo.toLocaleString().toLowerCase();
                    let duration = Number((json.end_time - json.start_time) / 1000000);
                    scatterData.push([dataStr, duration, json.span_count, json.span_errors_count, json.service_name, json.operation_name, json.trace_id, json.start_time]);
                }
                showScatterPlot();
                reSort();

                // If the number of traces returned is 50, call getData again
                if (res.traces.length == 50 && params.page < 2) {
                    getData(params);
                }
                if (returnResTotal.length >= totalTraces && res.traces.length < 50) {
                    allResultsFetched = true;
                }
            } else {
                if (returnResTotal.length == 0) {
                    if (chart != null && chart != '' && chart != undefined) {
                        chart.dispose();
                    }
                    $('#traces-number').text('0 Traces');
                    let queryText = 'Your query returned no data, adjust your query.';
                    $('#graph-show').html(queryText);
                    $('#graph-show').addClass('empty-result-show');
                }
            }
            isLoading = false; // Set the flag to false after getting the response
        })
        .fail(() => {
            $('#traces-number').text('0 Traces');
            $('#graph-show').html('An error occurred while fetching data.');
            $('#graph-show').addClass('empty-result-show');
        })
        .always(() => {
            // Reset cursor
            $('body').css('cursor', 'default');
            $('#search-trace-btn').prop('disabled', false).removeClass('disabled');
            isLoading = false;
        });
}
const resizeObserver = new ResizeObserver((_entries) => {
    if (chart != null && chart != '' && chart != undefined) chart.resize();
});
resizeObserver.observe(document.getElementById('graph-show'));

function showScatterPlot() {
    $('#graph-show').removeClass('empty-result-show');
    let chartId = document.getElementById('graph-show');
    if (chart != null && chart != '' && chart != undefined) {
        echarts.dispose(chart);
    }
    chart = echarts.init(chartId);
    let theme = $('html').attr('data-theme') == 'light' ? 'light' : 'dark';
    let normalColor = theme == 'light' ? 'rgba(99, 71, 217, 0.6)' : 'rgba(99, 71, 217, 1)';
    let errorColor = theme == 'light' ? 'rgba(233, 49, 37, 0.6)' : 'rgba(233, 49, 37, 1)';
    let axisLineColor = theme == 'light' ? '#DCDBDF' : '#383148';
    let axisLabelColor = theme == 'light' ? '#160F29' : '#FFFFFF';

    scatterData.sort((a, b) => a[7] - b[7]);

    chart.setOption({
        xAxis: {
            type: 'category',
            name: 'Time',
            nameTextStyle: {
                color: axisLabelColor,
            },
            scale: true,
            axisLine: {
                lineStyle: {
                    color: axisLineColor,
                },
            },
            axisLabel: {
                color: axisLabelColor,
                formatter: function (value) {
                    return value.split(' ')[1];
                },
            },
            splitLine: { show: false },
        },
        yAxis: {
            type: 'value',
            name: 'Duration (ms)',
            nameTextStyle: {
                color: axisLabelColor,
            },
            scale: true,
            axisLine: {
                show: true,
                lineStyle: {
                    color: axisLineColor,
                },
            },
            axisLabel: {
                color: axisLabelColor,
            },
            splitLine: { show: false },
        },
        tooltip: {
            show: true,
            className: 'tooltip-design',
            formatter: function (param) {
                var service = param.value[4];
                var operation = param.value[5];
                var duration = param.value[1];
                var spans = param.value[2];
                var errors = param.value[3];
                var traceId = param.value[6] ? param.value[6] : '';
                var traceTimestamp = param.value[7];
                var date = new Date(traceTimestamp / 1000000).toLocaleString();

                return `<div class="custom-tooltip">
                    <div class="tooltip-content">
                        <div class="trace-name">${service}: ${operation}</div>
                        <div class="trace-id">Trace ID: ${traceId.substring(0, 7)}</div>
                        <div class="trace-time">Time: ${date}</div>
                        <hr>
                        <div>Duration: ${duration.toFixed(2)}ms</div>
                        <div>No. of Spans: ${spans}</div>
                        <div>No. of Error Spans: ${errors}</div>
                    </div>
                    <hr>
                    <div class="tooltip-context">
                        <div class="context-option" onclick="handleRelatedTraces('${traceId}', ${traceTimestamp}, false)">View Traces</div>
                        <div class="context-option" onclick="handleRelatedLogs('${traceId}', ${traceTimestamp}, 'trace')">Related Logs</div>
                    </div>
                </div>`;
            },
            enterable: true,
            position: function (point) {
                return [point[0], point[1]];
            },
        },
        series: [
            {
                type: 'effectScatter',
                showEffectOn: 'emphasis',
                rippleEffect: {
                    scale: 1.2,
                },
                data: scatterData,
                symbolSize: function (val) {
                    // Use logarithmic scaling for span count to prevent extremely large bubbles
                    let spanCount = val[2] || 0;
                    let baseSize = Math.max(5, Math.log10(spanCount + 1) * 10);

                    // For traces with errors
                    if (val[3] > 0) {
                        return baseSize + 2;
                    }

                    return baseSize;
                },
                itemStyle: {
                    color: function (params) {
                        // Color based on whether there are errors
                        return params.data[3] > 0 ? errorColor : normalColor;
                    },
                },
            },
        ],
    });

    // Open Gantt Chart when click on Scatter Chart
    chart.on('click', function (params) {
        const traceId = params.data[6];
        const traceTimestamp = params.data[7]; // nanoseconds
        window.location.href = `trace.html?trace_id=${traceId}&timestamp=${traceTimestamp}`;
    });
}

function reSort() {
    $('.warn-box').remove();
    for (let i = 0; i < returnResTotal.length; i++) {
        let json = returnResTotal[i];
        $('#warn-bottom').append(`<a href="../trace.html?trace_id=${json.trace_id}&timestamp=${json.start_time}" class="warn-box-anchor">
      <div class="warn-box warn-box-${i}"><div class="warn-head">
                              <div><span id="span-id-head-${i}"></span><span class="span-id-text" id="span-id-${i}"></span></div>
                              <span class = "duration-time" id  = "duration-time-${i}"></span>
                          </div>
                          <div class="warn-content">
                              <div class="spans-box">
                              <div class = "total-span" id = "total-span-${i}"></div>
                              <div class = "error-span" id = "error-span-${i}"></div>
                              </div>
                              <div> </div>
                              <div class="warn-content-right">
                                  <span class = "start-time" id = "start-time-${i}"></span>
                                  <span class = "how-long-time" id = "how-long-time-${i}"></span>
                              </div>
                          </div></div>
    </a>`);
        $(`.warn-box-${i}`).attr('id', json.trace_id);
        $(`#span-id-head-${i}`).text(json.service_name + ': ' + json.operation_name + '  ');
        $(`#span-id-${i}`).text(json.trace_id.substring(0, 7));
        $(`#total-span-${i}`).text(json.span_count + ' Spans');
        $(`#error-span-${i}`).text(json.span_errors_count + ' Errors');
        let duration = Number((json.end_time - json.start_time) / 1000000);
        $(`#duration-time-${i}`).text(Math.round(duration * 100) / 100 + 'ms');
        let milliseconds = Number(json.start_time / 1000000);
        let dataStr = new Date(milliseconds).toLocaleString();
        let dateText = '';
        let date = dataStr.split(',');
        let dateTime = date[0].split('/');
        //current date
        const currentDate = new Date();
        const currentYear = currentDate.getFullYear() + '';
        const currentMonth = currentDate.getMonth() + 1 + '';
        const currentDay = currentDate.getDate() + '';
        if (currentYear === dateTime[2] && currentMonth === dateTime[0] && currentDay === dateTime[1]) {
            dateText = 'Today | ';
        } else {
            dateText = date[0] + ' | ';
        }
        dateText = date[0] + ' | ';
        dateText += date[1].toLowerCase();
        $(`#start-time-${i}`).text(dateText);
        let timePass = calculateTimeToNow(json.start_time);
        let timePassText = '';
        if (timePass.days > 2) timePassText = 'a few days ago';
        else if (timePass.days > 1) timePassText = 'yesterday';
        else if (timePass.hours == 1) timePassText = timePass.hours + ' hour ago';
        else if (timePass.hours >= 1) timePassText = timePass.hours + ' hours ago';
        else if (timePass.minutes == 1) timePassText = timePass.minutes + ' minute ago';
        else if (timePass.minutes > 1) timePassText = timePass.minutes + ' minutes ago';
        else if (timePass.minutes < 1) timePassText = 'a few seconds ago';
        else timePassText = timePass + ' hours ago';
        $(`#how-long-time-${i}`).text(timePassText);
    }
}

function calculateTimeToNow(startTime) {
    const nanosecondsTimestamp = startTime;
    const millisecondsTimestamp = nanosecondsTimestamp / 1000000;
    const now = new Date();
    const timeDifference = now.getTime() - millisecondsTimestamp;

    const hours = Math.floor(timeDifference / 3600000);
    const minutes = Math.floor((timeDifference % 3600000) / 60000);
    const days = Math.floor((timeDifference % 3600000) / 86400000);

    return {
        hours: hours,
        minutes: minutes,
        days: days,
    };
}
let lastScrollPosition = 0;
let isLoading = false; // Flag to indicate whether an API call is in progress

$('#dashboard .scrollable-container').on('scroll', function () {
    const container = $(this);
    const scrollHeight = this.scrollHeight;
    const scrollPosition = container.height() + container.scrollTop();

    if (!isLoading && hasLoaded && !allResultsFetched && scrollPosition / scrollHeight >= 0.6) {
        isLoading = true;
        lastScrollPosition = container.scrollTop();

        getData();

        container.scrollTop(lastScrollPosition);
    }
});
function getData() {
    //users did not set limitation
    if (limitation == -1) {
        params.page = params.page + 1;
        searchTrace(params);
    } else if (limitation > 0) {
        if (limitation >= 50) {
            limitation = limitation - 50;
            params.page = params.page + 1;
            searchTrace(params);
        } else {
            params.page = params.page + 1;
            searchTrace(params);
        }
    }
}