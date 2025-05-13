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
let traces = [];
let scatterData = [];
let currentPage = 1;
let isLoading = false;
let allResultsFetched = false;
let totalTraces = 0;
let lastScrollTop = 0;
let limitSet = false;

let currentSearchParams = {
    searchText: '*',
    startEpoch: 'now-1h',
    endEpoch: 'now',
    queryLanguage: 'Splunk QL',
    page: 1,
};

$(document).ready(() => {
    $('.theme-btn').on('click', themePickerHandler);
    $('.theme-btn').on('click', redrawScatterPlot);

    $('#search-trace-btn').on('click', handleSearch);
    $('#dashboard .scrollable-container').on('scroll', handleScroll);
    window.addEventListener('popstate', handleUrlChange);

    initDropdowns();
    initSortDropdown();
    initDownloadDropdown();

    setupEventHandlers();

    const urlState = loadStateFromUrl();
    setTimeout(() => {
        if (urlState.hasParams) {
            handleSearch();
        } else {
            const defaultParams = {
                searchText: '*',
                startEpoch: filterStartDate.toString(),
                endEpoch: filterEndDate.toString(),
                queryLanguage: 'Splunk QL',
                page: 1,
            };

            currentSearchParams = defaultParams;

            searchTraces(defaultParams, true);
        }
    }, 300);
});

function loadStateFromUrl() {
    const urlParams = new URLSearchParams(window.location.search);
    const hasParams = urlParams.toString().length > 0;

    // Default values
    const state = {
        service: 'All',
        operation: 'All',
        startEpoch: 'now-1h',
        endEpoch: 'now',
        minDuration: '',
        maxDuration: '',
        limit: '',
        tags: '',
        hasParams,
    };

    // Override with URL values if present
    if (urlParams.has('service')) state.service = urlParams.get('service');
    if (urlParams.has('operation')) state.operation = urlParams.get('operation');
    if (urlParams.has('startEpoch')) state.startEpoch = urlParams.get('startEpoch');
    if (urlParams.has('endEpoch')) state.endEpoch = urlParams.get('endEpoch');
    if (urlParams.has('minDuration')) state.minDuration = urlParams.get('minDuration');
    if (urlParams.has('maxDuration')) state.maxDuration = urlParams.get('maxDuration');
    if (urlParams.has('limit')) state.limit = urlParams.get('limit');
    if (urlParams.has('tags')) state.tags = urlParams.get('tags');

    // Update UI with state values
    $('#service-span-name').text(state.service);
    $('#operation-span-name').text(state.operation);
    $('#min-duration-input').val(state.minDuration);
    $('#max-duration-input').val(state.maxDuration);
    $('#limit-result-input').val(state.limit);
    $('#tags-input').val(state.tags);
    const start = state.startEpoch;
    const end = state.endEpoch;
    $(`.ranges .inner-range .range-item`).removeClass('active');
    if (!isNaN(start)) {
        let stDate = Number(start);
        let endDate = Number(end);
        datePickerHandler(stDate, endDate, 'custom');
        loadCustomDateTimeFromEpoch(stDate, endDate);
    } else {
        $(`.ranges .inner-range #${start}`).addClass('active');
        datePickerHandler(start, end, start);
    }
    return state;
}

function initDropdowns() {
    $('#service-dropdown').singleBox({
        spanName: 'Service',
        dataList: ['All'],
        defaultValue: $('#service-span-name').text() || 'All',
        dataUpdate: true,
        clickedHead: async function () {
            return await fetchColumnValues('service');
        },
    });

    $('#name-dropdown').singleBox({
        spanName: 'Operation',
        dataList: ['All'],
        defaultValue: $('#operation-span-name').text() || 'All',
        dataUpdate: true,
        clickedHead: async function () {
            return await fetchColumnValues('name', $('#service-span-name').text());
        },
    });
}

async function fetchColumnValues(column, serviceFilter = null) {
    let searchText = `SELECT DISTINCT \`${column}\` FROM \`traces\``;

    if (column === 'name' && serviceFilter && serviceFilter !== 'All') {
        searchText += ` WHERE service='${serviceFilter}'`;
    }

    try {
        const response = await $.ajax({
            method: 'post',
            url: 'api/search',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify({
                state: 'query',
                searchText: searchText,
                startEpoch: filterStartDate || 'now-1h',
                endEpoch: filterEndDate || 'now',
                indexName: 'traces',
                queryLanguage: 'SQL',
                from: 0,
            }),
        });

        const values = new Set(['All']);

        if (response?.hits?.records) {
            response.hits.records.forEach((record) => {
                const value = record[column];
                values.add(typeof value === 'string' ? value : value.toString());
            });
        }

        return Array.from(values);
    } catch (error) {
        console.error(`Failed to fetch ${column} values:`, error);
        return ['All'];
    }
}

function initSortDropdown() {
    const sortOptions = ['Most Recent', 'Longest First', 'Shortest First', 'Most Spans', 'Least Spans'];

    $('#sort-dropdown').singleBox({
        spanName: 'Most Recent',
        defaultValue: 'Most Recent',
        dataList: sortOptions,
        clicked: function (selected) {
            sortTraces(selected);
            redrawResults();
        },
    });
}

function sortTraces(sortOption) {
    switch (sortOption) {
        case 'Most Recent':
            traces.sort((a, b) => b.start_time - a.start_time);
            break;
        case 'Longest First':
            traces.sort((a, b) => b.end_time - b.start_time - (a.end_time - a.start_time));
            break;
        case 'Shortest First':
            traces.sort((a, b) => a.end_time - a.start_time - (b.end_time - b.start_time));
            break;
        case 'Most Spans':
            traces.sort((a, b) => b.span_count - a.span_count);
            break;
        case 'Least Spans':
            traces.sort((a, b) => a.span_count - b.span_count);
            break;
    }
}

function initDownloadDropdown() {
    const downloadOptions = ['Download as CSV', 'Download as JSON', 'Download as XML', 'Download as SQL'];

    $('#download-dropdown').singleBox({
        fillIn: false,
        spanName: 'Download Result',
        dataList: downloadOptions,
        clicked: function (selected) {
            let format = selected.split(' ').pop().toLowerCase();
            $('#download-trace').download({
                data: traces,
                downloadMethod: '.' + format,
            });
        },
    });
}

// Handle search button click
function handleSearch() {
    // Reset state for new search
    traces = [];
    scatterData = [];
    $('.warn-box').remove();
    $('#traces-number').text('');

    // Get filter values
    const serviceValue = $('#service-span-name').text();
    const operationValue = $('#operation-span-name').text();
    const tagValue = $('#tags-input').val();
    const maxDurationStr = $('#max-duration-input').val();
    const minDurationStr = $('#min-duration-input').val();
    const limitValue = $('#limit-result-input').val();

    // Check if a limit is set
    limitSet = limitValue && parseInt(limitValue) > 0;

    if (limitSet) {
        allResultsFetched = true;
    } else {
        allResultsFetched = false;
    }

    // Convert duration strings to nanoseconds
    const maxDuration = parseDuration(maxDurationStr);
    const minDuration = parseDuration(minDurationStr);

    if ((maxDuration === null && maxDurationStr) || (minDuration === null && minDurationStr)) {
        showToast('Invalid duration format. Examples: 1.2s, 100ms, 500µs', 'error');
        return;
    }

    // Build search query
    const searchParts = [`service=${serviceValue === 'All' ? '*' : JSON.stringify(serviceValue)}`, `name=${operationValue === 'All' ? '*' : JSON.stringify(operationValue)}`];

    if (maxDuration) searchParts.push(`duration<=${maxDuration}`);
    if (minDuration) searchParts.push(`duration>=${minDuration}`);
    if (tagValue?.trim()) searchParts.push(tagValue.trim());

    let startTime = filterStartDate.toString();
    let endTime = filterEndDate.toString();

    updateUrl({
        service: serviceValue,
        operation: operationValue,
        startEpoch: startTime,
        endEpoch: endTime,
        minDuration: minDurationStr,
        maxDuration: maxDurationStr,
        limit: limitValue,
        tags: tagValue,
    });

    const searchParams = {
        searchText: searchParts.join(' '),
        startEpoch: startTime,
        endEpoch: endTime,
        queryLanguage: 'Splunk QL',
        page: 1,
    };

    currentSearchParams = { ...searchParams };

    if (!limitSet) {
        allResultsFetched = false;
    }
    if (chart) echarts?.dispose(chart);

    setLoading(true);

    searchTraces(searchParams, true);
}

// Update URL with search parameters
function updateUrl(params) {
    const url = new URL(window.location);
    const searchParams = url.searchParams;

    // Set core parameters
    searchParams.set('service', params.service);
    searchParams.set('operation', params.operation);
    searchParams.set('startEpoch', params.startEpoch);
    searchParams.set('endEpoch', params.endEpoch);

    ['minDuration', 'maxDuration', 'limit', 'tags'].forEach((param) => {
        if (params[param]?.trim()) {
            searchParams.set(param, params[param]);
        } else {
            searchParams.delete(param);
        }
    });

    window.history.pushState({ searchState: params }, '', url);
}

function handleUrlChange() {
    const urlParams = new URLSearchParams(window.location.search);

    if (!urlParams.toString()) {
        resetFilters();
        return;
    }

    loadStateFromUrl();
    handleSearch();
}

function resetFilters() {
    $('#service-span-name').text('All');
    $('#operation-span-name').text('All');
    $('#min-duration-input').val('');
    $('#max-duration-input').val('');
    $('#limit-result-input').val('');
    $('#tags-input').val('');
    datePickerHandler('now-1h', 'now', 'now-1h');
}

// Convert duration string to nanoseconds
function parseDuration(durationStr) {
    if (!durationStr) return null;

    const durationRegex = /([\d.]+)\s*(s|ms|µs|us|µ|u)?/i;
    const match = durationStr.match(durationRegex);

    if (!match) return null;

    const value = parseFloat(match[1]);
    const unit = match[2]?.toLowerCase() || 's';

    switch (unit) {
        case 's':
            return value * 1e9;
        case 'ms':
            return value * 1e6;
        case 'µs':
        case 'us':
        case 'µ':
        case 'u':
            return value * 1e3;
        default:
            return value * 1e9;
    }
}

// Handle infinite scroll
function handleScroll() {
    const container = $(this);
    const scrollTop = container.scrollTop();
    const scrollHeight = this.scrollHeight;
    const containerHeight = container.height();
    const isScrollingDown = scrollTop > lastScrollTop;
    lastScrollTop = scrollTop;
    // Don't load more if: Already loading, All results fetched, Scrolling up, Not near bottom, A limit is set
    if (!isLoading && !allResultsFetched && isScrollingDown && (scrollTop + containerHeight) / scrollHeight > 0.8 && !limitSet) {
        loadMoreResults();
    }
}

function loadMoreResults() {
    currentPage++;

    const nextPageParams = {
        ...currentSearchParams,
        page: currentPage,
    };

    setLoading(true);
    searchTraces(nextPageParams, false);
}

function searchTraces(params, isNewSearch) {
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
        .then(async (response) => {
            if (response?.traces?.length > 0) {
                // Process new traces
                const newTraces = processTraces(response.traces);

                // Add to existing results or replace
                if (isNewSearch) {
                    traces = newTraces;
                } else {
                    traces = traces.concat(newTraces);
                }

                // Get total if this is a new search
                if (isNewSearch) {
                    await getTotalTraceCount(params);
                }

                updateScatterData();

                redrawScatterPlot();
                redrawResults();

                if (traces.length >= totalTraces) {
                    allResultsFetched = true;
                }
            } else if (traces.length === 0) {
                $('#traces-number').text('0 Traces');
                $('#graph-show').html('Your query returned no data, adjust your query.').addClass('empty-result-show');

                if (chart) {
                    chart.dispose();
                }
            }
        })
        .fail(() => {
            $('#traces-number').text('0 Traces');
            $('#graph-show').html('An error occurred while fetching data.').addClass('empty-result-show');
        })
        .always(() => {
            setLoading(false);
        });
}

// Apply any limit filter
function processTraces(responseTraces) {
    const limit = $('#limit-result-input').val();

    if (limit && parseInt(limit) > 0) {
        const limitNum = parseInt(limit);

        allResultsFetched = true;

        if (limitNum < responseTraces.length) {
            return responseTraces.slice(0, limitNum);
        }
    }

    return responseTraces;
}

async function getTotalTraceCount(params) {
    try {
        const count = await $.ajax({
            method: 'post',
            url: 'api/traces/count',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(params),
        });

        totalTraces = count;
        $('#traces-number').text(count.toLocaleString('en-US') + ' Traces');
    } catch (error) {
        console.error('Failed to get trace count:', error);
    }
}

function updateScatterData() {
    scatterData = traces.map((trace) => {
        const milliseconds = Number(trace.start_time / 1000000);
        const date = new Date(milliseconds);
        const dateStr = date.toLocaleString().toLowerCase();
        const duration = Number((trace.end_time - trace.start_time) / 1000000);

        return [dateStr, duration, trace.span_count, trace.span_errors_count, trace.service_name, trace.operation_name, trace.trace_id, trace.start_time];
    });
}

function redrawScatterPlot() {
    $('#graph-show').removeClass('empty-result-show');
    const chartElement = document.getElementById('graph-show');

    if (chart) {
        echarts.dispose(chart);
    }

    chart = echarts.init(chartElement);

    const theme = $('html').attr('data-theme') === 'light' ? 'light' : 'dark';
    const normalColor = theme === 'light' ? 'rgba(99, 71, 217, 0.6)' : 'rgba(99, 71, 217, 1)';
    const errorColor = theme === 'light' ? 'rgba(233, 49, 37, 0.6)' : 'rgba(233, 49, 37, 1)';
    const axisLineColor = theme === 'light' ? '#DCDBDF' : '#383148';
    const axisLabelColor = theme === 'light' ? '#160F29' : '#FFFFFF';

    // Sort data by timestamp
    scatterData.sort((a, b) => a[7] - b[7]);

    chart.setOption({
        xAxis: {
            type: 'category',
            name: 'Time',
            nameTextStyle: { color: axisLabelColor },
            scale: true,
            axisLine: { lineStyle: { color: axisLineColor } },
            axisLabel: {
                color: axisLabelColor,
                formatter: (value) => value.split(' ')[1],
            },
            splitLine: { show: false },
        },
        yAxis: {
            type: 'value',
            name: 'Duration (ms)',
            nameTextStyle: { color: axisLabelColor },
            scale: true,
            axisLine: {
                show: true,
                lineStyle: { color: axisLineColor },
            },
            axisLabel: { color: axisLabelColor },
            splitLine: { show: false },
        },
        tooltip: {
            show: true,
            className: 'tooltip-design',
            formatter: function (param) {
                const service = param.value[4];
                const operation = param.value[5];
                const duration = param.value[1];
                const spans = param.value[2];
                const errors = param.value[3];
                const traceId = param.value[6] || '';
                const timestamp = param.value[7];
                const date = new Date(timestamp / 1000000).toLocaleString();

                return `
                    <div class="custom-tooltip">
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
                            <div class="context-option" onclick="handleRelatedTraces('${traceId}', ${timestamp}, false)">View Traces</div>
                            <div class="context-option" onclick="handleRelatedLogs('${traceId}', ${timestamp}, 'trace')">Related Logs</div>
                        </div>
                    </div>
                `;
            },
            enterable: true,
            position: (point) => [point[0], point[1]],
        },
        series: [
            {
                type: 'effectScatter',
                showEffectOn: 'emphasis',
                rippleEffect: { scale: 1.2 },
                data: scatterData,
                symbolSize: function (val) {
                    // Use logarithmic scaling for span count to prevent extremely large bubbles
                    const spanCount = val[2] || 0;
                    const baseSize = Math.max(5, Math.log10(spanCount + 1) * 10);

                    // Increase size for traces with errors
                    return val[3] > 0 ? baseSize + 2 : baseSize;
                },
                itemStyle: {
                    color: (params) => (params.data[3] > 0 ? errorColor : normalColor),
                },
            },
        ],
    });

    chart.on('click', (params) => {
        const traceId = params.data[6];
        const timestamp = params.data[7];
        window.location.href = `trace.html?trace_id=${traceId}&timestamp=${timestamp}`;
    });

    if (!window.resizeObserver) {
        window.resizeObserver = new ResizeObserver(() => {
            if (chart) chart.resize();
        });

        window.resizeObserver.observe(chartElement);
    }
}

function redrawResults() {
    $('.warn-box').remove();

    traces.forEach((trace, index) => {
        const traceHtml = createTraceHtml(trace, index);
        $('#warn-bottom').append(traceHtml);

        // Set trace data
        $(`.warn-box-${index}`).attr('id', trace.trace_id);
        $(`#span-id-head-${index}`).text(`${trace.service_name}: ${trace.operation_name}  `);
        $(`#span-id-${index}`).text(trace.trace_id.substring(0, 7));
        $(`#total-span-${index}`).text(`${trace.span_count} Spans`);
        $(`#error-span-${index}`).text(`${trace.span_errors_count} Errors`);

        // Set duration
        const durationMs = (trace.end_time - trace.start_time) / 1000000;
        $(`#duration-time-${index}`).text(`${Math.round(durationMs * 100) / 100}ms`);

        // Set timestamp
        const timestamp = trace.start_time / 1000000;
        const dateStr = new Date(timestamp).toLocaleString();
        $(`#start-time-${index}`).text(`${dateStr.split(',')[0]} | ${dateStr.split(',')[1].toLowerCase()}`);

        // Set relative time
        $(`#how-long-time-${index}`).text(getRelativeTimeText(trace.start_time));
    });
}

function createTraceHtml(trace, index) {
    return `
        <a href="../trace.html?trace_id=${trace.trace_id}&timestamp=${trace.start_time}" class="warn-box-anchor">
            <div class="warn-box warn-box-${index}">
                <div class="warn-head">
                    <div>
                        <span id="span-id-head-${index}"></span>
                        <span class="span-id-text" id="span-id-${index}"></span>
                    </div>
                    <span class="duration-time" id="duration-time-${index}"></span>
                </div>
                <div class="warn-content">
                    <div class="spans-box">
                        <div class="total-span" id="total-span-${index}"></div>
                        <div class="error-span" id="error-span-${index}"></div>
                    </div>
                    <div></div>
                    <div class="warn-content-right">
                        <span class="start-time" id="start-time-${index}"></span>
                        <span class="how-long-time" id="how-long-time-${index}"></span>
                    </div>
                </div>
            </div>
        </a>
    `;
}

// Get relative time text
function getRelativeTimeText(timestampNs) {
    const milliseconds = timestampNs / 1000000;
    const now = new Date();
    const diff = now.getTime() - milliseconds;

    const days = Math.floor(diff / 86400000);
    const hours = Math.floor(diff / 3600000);
    const minutes = Math.floor((diff % 3600000) / 60000);

    if (days > 2) return 'a few days ago';
    if (days > 1) return 'yesterday';
    if (hours === 1) return '1 hour ago';
    if (hours > 1) return `${hours} hours ago`;
    if (minutes === 1) return '1 minute ago';
    if (minutes > 1) return `${minutes} minutes ago`;
    return 'a few seconds ago';
}

function setLoading(loading) {
    isLoading = loading;
    $('body').css('cursor', loading ? 'progress' : 'default');
    $('#search-trace-btn').prop('disabled', loading).toggleClass('disabled', loading);
}
