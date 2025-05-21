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

const VIEW_TYPES = {
    SINGLE: 'single-line',
    MULTI: 'multi-line',
    TABLE: 'table',
};

//eslint-disable-next-line no-unused-vars
function setupEventHandlers() {
    $('#filter-input').off('keydown').on('keydown', filterInputHandler);

    $('#run-filter-btn').off('click').on('click', runFilterBtnHandler);
    $('#query-builder-btn').off('click').on('click', runFilterBtnHandler);
    $('#live-tail-btn').on('click', runLiveTailBtnHandler);

    $('#query-language-btn').on('show.bs.dropdown', qLangOnShowHandler);
    $('#query-language-btn').on('hide.bs.dropdown', qLangOnHideHandler);
    $('#query-language-options .query-language-option').on('click', setQueryLangHandler);
    $('#query-mode-options .query-mode-option').on('click', setQueryModeHandler);

    $('#log-opt-single-btn').on('click', function () {
        handleLogOptionChange(VIEW_TYPES.SINGLE);
    });

    $('#log-opt-multi-btn').on('click', function () {
        handleLogOptionChange(VIEW_TYPES.MULTI);
    });

    $('#log-opt-table-btn').on('click', function () {
        handleLogOptionChange(VIEW_TYPES.TABLE);
    });

    $('#date-picker-btn').on('show.bs.dropdown', showDatePickerHandler);
    $('#date-picker-btn').on('hide.bs.dropdown', hideDatePickerHandler);
    $('#reset-timepicker').on('click', resetDatePickerHandler);

    $('.panelEditor-container #date-start').on('change', getStartDateHandler);
    $('.panelEditor-container #date-end').on('change', getEndDateHandler);
    $('.panelEditor-container #time-start').on('change', getStartTimeHandler);
    $('.panelEditor-container #time-end').on('change', getEndTimeHandler);
    $('.panelEditor-container #customrange-btn').off('click').on('click', customRangeHandler);

    $('#date-start').on('change', getStartDateHandler);
    $('#date-end').on('change', getEndDateHandler);

    $('#time-start').on('change', getStartTimeHandler);
    $('#time-end').on('change', getEndTimeHandler);
    $('#customrange-btn').off('click').on('click', customRangeHandler);

    $('.range-item').on('click', rangeItemHandler);
    $('.db-range-item').off('click').on('click', rangeItemHandler);

    $('.ui-widget input').on('keyup', saveqInputHandler);

    $(window).bind('popstate', windowPopStateHandler);
}

function windowPopStateHandler(evt) {
    if (location.href.includes('index.html')) {
        let state = evt.originalEvent.state;
        if (state !== null) {
            data = getInitialSearchFilter(true, false);
            resetDashboard();
            wsState = 'query';
            doSearch(data);
        }
    }
}

let tempStartDate, tempStartTime, tempEndDate, tempEndTime;
let appliedStartDate, appliedStartTime, appliedEndDate, appliedEndTime;

function showDatePickerHandler(evt) {
    evt.stopPropagation();
    $('#daterangepicker').toggle();
    $(evt.currentTarget).toggleClass('active');
    initializeDatePicker();
}

function hideDatePickerHandler() {
    $('#date-picker-btn').removeClass('active');
    resetTempValues();
}

function resetDatePickerHandler(evt) {
    evt.stopPropagation();
    resetCustomDateRange();
    $.each($('.range-item.active'), function () {
        $(this).removeClass('active');
    });
}

function initializeDatePicker() {
    // Initialize with applied values, current values, or from cookies
    appliedStartDate = tempStartDate = $('#date-start').val() || Cookies.get('customStartDate') || '';
    appliedStartTime = tempStartTime = $('#time-start').val() || Cookies.get('customStartTime') || '';
    appliedEndDate = tempEndDate = $('#date-end').val() || Cookies.get('customEndDate') || '';
    appliedEndTime = tempEndTime = $('#time-end').val() || Cookies.get('customEndTime') || '';

    $('#date-start').val(appliedStartDate).toggleClass('active', !!appliedStartDate);
    $('#date-end').val(appliedEndDate).toggleClass('active', !!appliedEndDate);

    if (appliedStartDate) {
        $('#time-start').val(appliedStartTime).addClass('active');
    } else {
        $('#time-start').val('00:00').removeClass('active');
    }

    if (appliedEndDate) {
        $('#time-end').val(appliedEndTime).addClass('active');
    } else {
        $('#time-end').val('00:00').removeClass('active');
    }
}

function resetTempValues() {
    tempStartDate = appliedStartDate;
    tempStartTime = appliedStartTime;
    tempEndDate = appliedEndDate;
    tempEndTime = appliedEndTime;

    // Reset the input values to the applied values
    $('#date-start').val(appliedStartDate);
    $('#date-end').val(appliedEndDate);
    $('#time-start').val(appliedStartTime);
    $('#time-end').val(appliedEndTime);
}

function getStartDateHandler(_evt) {
    tempStartDate = this.value;
    $(this).addClass('active');
}

function getEndDateHandler(_evt) {
    tempEndDate = this.value;
    $(this).addClass('active');
}

function getStartTimeHandler() {
    tempStartTime = $(this).val();
    $(this).addClass('active');
}

function getEndTimeHandler() {
    tempEndTime = $(this).val();
    $(this).addClass('active');
}

function customRangeHandler(evt) {
    $('#date-range-error').remove();

    if (!tempStartDate || !tempEndDate) {
        evt.preventDefault();
        evt.stopPropagation();
        if (!tempStartDate) {
            $('#date-start').addClass('error');
            $('.panelEditor-container #date-start').addClass('error');
        }
        if (!tempEndDate) {
            $('#date-end').addClass('error');
            $('.panelEditor-container #date-end').addClass('error');
        }

        $('#daterange-to').after('<div id="date-range-error" class="date-range-error">Please select both start and end dates</div>');
        $('.panelEditor-container #daterange-to').after('<div id="date-range-error" class="date-range-error">Please select both start and end dates</div>');

        setTimeout(function () {
            $('#date-start, #date-end').removeClass('error');
            $('.panelEditor-container #date-start,.panelEditor-container #date-end').removeClass('error');
            $('#date-range-error').fadeOut(300, function () {
                $(this).remove();
            });
            $('.panelEditor-container #date-range-error').fadeOut(300, function () {
                $(this).remove();
            });
        }, 2000);
        $(this).trigger('dateRangeInvalid');
        return;
    } else {
        $.each($('.range-item.active, .db-range-item.active'), function () {
            $(this).removeClass('active');
        });
    }

    // Apply the temporary values
    appliedStartDate = tempStartDate;
    appliedStartTime = tempStartTime || '00:00';
    appliedEndDate = tempEndDate;
    appliedEndTime = tempEndTime || '00:00';
    $('#time-start', '#time-end').addClass('active');

    // Calculate start and end times
    let startDate = new Date(`${appliedStartDate}T${appliedStartTime}`);
    let endDate = new Date(`${appliedEndDate}T${appliedEndTime}`);

    filterStartDate = startDate.getTime();
    filterEndDate = endDate.getTime();

    if (filterEndDate <= filterStartDate) {
        evt.preventDefault();
        evt.stopPropagation();
        $('#date-start').addClass('error');
        $('#date-end').addClass('error');
        $('.panelEditor-container #date-start').addClass('error');
        $('.panelEditor-container #date-end').addClass('error');

        $('#daterange-to').after('<div id="date-range-error" class="date-range-error">End date must be after start date</div>');
        $('.panelEditor-container #daterange-to').after('<div id="date-range-error" class="date-range-error">End date must be after start date</div>');

        setTimeout(function () {
            $('#date-start, #date-end').removeClass('error');
            $('.panelEditor-container #date-start, .panelEditor-container #date-end').removeClass('error');
            $('#date-range-error').fadeOut(300, function () {
                $(this).remove();
            });
            $('.panelEditor-container #date-range-error').fadeOut(300, function () {
                $(this).remove();
            });
        }, 2000);
        $(this).trigger('dateRangeInvalid');
        return;
    }

    Cookies.set('customStartDate', appliedStartDate);
    Cookies.set('customStartTime', appliedStartTime);
    Cookies.set('customEndDate', appliedEndDate);
    Cookies.set('customEndTime', appliedEndTime);

    datePickerHandler(filterStartDate, filterEndDate, 'custom');
    $(this).trigger('dateRangeValid');
    // For dashboards
    const currentUrl = window.location.href;
    if (currentUrl.includes('dashboard.html')) {
        updateDashboardDateRange(filterStartDate, filterEndDate);
    }
}

function rangeItemHandler(evt) {
    resetCustomDateRange();
    $.each($('.range-item.active, .db-range-item.active'), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), 'now', $(this).attr('id'));

    const currentUrl = window.location.href;
    if (currentUrl.includes('dashboard.html')) {
        updateDashboardDateRange(filterStartDate, filterEndDate);
    }
}

function updateDashboardDateRange(startTimestamp, endTimestamp) {
    const startDateStr = startTimestamp.toString();
    const endDateStr = endTimestamp.toString();

    // if user is on edit panel screen
    if (currentPanel) {
        if (currentPanel.queryData) {
            if (currentPanel.queryType === 'metrics') {
                if (currentPanel.queryData) {
                    currentPanel.queryData.start = startDateStr;
                    currentPanel.queryData.end = endDateStr;

                    if (Array.isArray(currentPanel.queryData.queriesData)) {
                        currentPanel.queryData.queriesData.forEach((query) => {
                            query.start = startDateStr;
                            query.end = endDateStr;
                        });
                    }

                    if (Array.isArray(currentPanel.queryData.formulasData)) {
                        currentPanel.queryData.formulasData.forEach((formula) => {
                            formula.start = startDateStr;
                            formula.end = endDateStr;
                        });
                    }
                }
            } else {
                currentPanel.queryData.startEpoch = startTimestamp;
                currentPanel.queryData.endEpoch = endTimestamp;
            }
            runQueryBtnHandler();
        }
    } else if (!currentPanel) {
        // If user is on dashboard screen
        localPanels.forEach((panel) => {
            delete panel.queryRes;
            if (panel.queryData) {
                if (panel.queryType === 'metrics') {
                    if (panel.queryData) {
                        panel.queryData.start = startDateStr;
                        panel.queryData.end = endDateStr;

                        if (Array.isArray(panel.queryData.queriesData)) {
                            panel.queryData.queriesData.forEach((query) => {
                                query.start = startDateStr;
                                query.end = endDateStr;
                            });
                        }

                        if (Array.isArray(panel.queryData.formulasData)) {
                            panel.queryData.formulasData.forEach((formula) => {
                                formula.start = startDateStr;
                                formula.end = endDateStr;
                            });
                        }
                    }
                } else {
                    panel.queryData.startEpoch = startTimestamp;
                    panel.queryData.endEpoch = endTimestamp;
                }
            }
        });
        displayPanels();
    }
}

function resetCustomDateRange() {
    // clear custom selections
    $('#date-start').val('');
    $('#date-end').val('');
    $('#time-start').val('00:00');
    $('#time-end').val('00:00');
    $('#date-start').removeClass('active error');
    $('#date-end').removeClass('active error');
    $('#time-start').removeClass('active');
    $('#time-end').removeClass('active');
    Cookies.remove('customStartDate');
    Cookies.remove('customEndDate');
    Cookies.remove('customStartTime');
    Cookies.remove('customEndTime');
    appliedStartDate = tempStartDate = '';
    appliedEndDate = tempEndDate = '';
    appliedStartTime = tempStartTime = '';
    appliedEndTime = tempEndTime = '';
}

function setQueryLangHandler(_e) {
    let previousQueryLanguageId = $('#query-language-options .query-language-option.active').attr('id').split('-')[1];
    $('.query-language-option').removeClass('active');
    $('#setting-container').hide();
    let currentTab = $('#custom-code-tab').tabs('option', 'active');
    let selectedQueryLanguageId = $(this).attr('id').split('-')[1];
    if (!(previousQueryLanguageId === selectedQueryLanguageId)) {
        $('#filter-input').val('');
    }
    $('#query-language-btn span').html($(this).html());
    handleTabAndTooltip(selectedQueryLanguageId, currentTab);
    $(this).addClass('active');
}

function setQueryModeHandler(_e) {
    $('.query-mode-option').removeClass('active');
    $('#setting-container').hide();
    Cookies.set('queryMode', $(this).html());
    $('#query-mode-btn span').html($(this).html());
    $(this).addClass('active');
}

function handleTabAndTooltip(selectedQueryLanguageId, currentTab) {
    if (selectedQueryLanguageId !== '3' && currentTab === 0) {
        $('#custom-code-tab').tabs('option', 'active', 1);
        showDisabledTabTooltip();
        $('#ui-id-1').addClass('disabled-tab');
    } else if (selectedQueryLanguageId !== '3' && currentTab === 1) {
        showDisabledTabTooltip();
        $('#ui-id-1').addClass('disabled-tab');
    } else if (selectedQueryLanguageId === '3' && currentTab === 1) {
        $('#custom-code-tab').tabs('option', 'disabled', []);
        hideDisabledTabTooltip();
        $('#ui-id-1').removeClass('disabled-tab');
    }
    displayQueryLangToolTip(selectedQueryLanguageId);
}

function showDisabledTabTooltip() {
    hideDisabledTabTooltip();
    //eslint-disable-next-line no-undef
    tippy('#tab-title1', {
        content: 'Builder is only available for Splunk QL. Please change the settings to select Splunk QL in order to use the builder.',
        placement: 'top',
        arrow: true,
        trigger: 'mouseenter focus',
    });
}

function hideDisabledTabTooltip() {
    document.querySelectorAll('#tab-title1').forEach(function (el) {
        if (el._tippy) {
            el._tippy.destroy();
        }
    });
}

function qLangOnShowHandler() {
    $('#query-language-btn').addClass('active');
}

function qLangOnHideHandler() {
    $('#query-language-btn').removeClass('active');
}

function runLiveTailBtnHandler(evt) {
    $('.popover').hide();
    evt.preventDefault();
    liveTailState = true;
    if ($('#live-tail-btn').text() === 'Live Tail') {
        resetDashboard();
        $('#live-tail-btn').html('Cancel Live Tail');
        logsRowData = [];
        total_liveTail_searched = 0;
        wsState = 'query';
        data = getLiveTailFilter(false, false, 1800);
        createLiveTailSocket(data);
    } else {
        $('#live-tail-btn').html('Live Tail');
        liveTailState = false;
        wsState = 'cancel';
        data = getLiveTailFilter(false, false, 1800);
        doLiveTailCancel(data);
    }
    $('#daterangepicker').hide();
}

function runFilterBtnHandler(evt) {
    var currentPage = window.location.pathname;
    if (currentPage === '/alert.html') {
        let data = getQueryParamsData();
        fetchLogsPanelData(data, -1).then((res) => {
            alertChart(res);
        });
    } else if (currentPage === '/dashboard.html') {
        runQueryBtnHandler();
    } else {
        // index.html
        $('.popover').hide();
        evt.preventDefault();
        const runFilterBtn = $('#run-filter-btn');
        const queryBuilderBtn = $('#query-builder-btn');
        if (runFilterBtn.hasClass('cancel-search') || queryBuilderBtn.hasClass('cancel-search')) {
            wsState = 'cancel';
            data = getSearchFilter(false, false);
            initialSearchData = data;
            doCancel(data);
        } else {
            isSearchButtonTriggered = true;
            if (!isHistogramViewActive) {
                hasSearchSinceHistogramClosed = true; 
            }
            resetDashboard();
            logsRowData = [];
            accumulatedRecords = [];
            lastColumnsOrder = [];
            totalLoadedRecords = 0;
            wsState = 'query';
            data = getSearchFilter(false, false);
            initialSearchData = data;
            $('#pagination-container').hide();
            doSearch(data).finally(() => {
                //eslint-disable-next-line no-undef
                isSearchButtonTriggered = false; 
            });
        }
        $('#daterangepicker').hide();
    }
}

function filterInputHandler(evt) {
    if (!evt.shiftKey && evt.keyCode === 13) {
        const currentUrl = window.location.href;
        const url = new URL(currentUrl);
        const pathOnly = url.pathname;

        const isIndexPage = pathOnly === '/' || pathOnly === '' || pathOnly.endsWith('index.html');
        const isDashboardPage = pathOnly.includes('dashboard.html');

        if (isIndexPage) {
            evt.preventDefault();
            resetDashboard();
            logsRowData = [];
            accumulatedRecords = [];
            lastColumnsOrder = [];
            totalLoadedRecords = 0;
            data = getSearchFilter(false, false);
            initialSearchData = data;
            doSearch(data);
        } else if (isDashboardPage) {
            evt.preventDefault();
            runQueryBtnHandler();
        }
    }
}

//eslint-disable-next-line no-unused-vars
function themePickerHandler(evt) {
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
    } else {
        theme = 'light';
        $(evt.currentTarget).removeClass('light-theme');
        $(evt.currentTarget).addClass('dark-theme');
    }

    $('html').attr('data-theme', theme);

    Cookies.set('theme', theme, { expires: 365 });
}

function saveqInputHandler(evt) {
    evt.preventDefault();
    $(this).addClass('active');
}

function handleLogOptionChange(viewType) {
    const logsContainer = $('#logs-result-container');
    const viewButtons = $('#views-container .btn-group .btn');
    logsContainer.toggleClass('multi', viewType !== VIEW_TYPES.SINGLE);
    viewButtons.removeClass('active');

    switch (viewType) {
        case VIEW_TYPES.SINGLE:
            $('#log-opt-single-btn').addClass('active');
            break;
        case VIEW_TYPES.MULTI:
            $('#log-opt-multi-btn').addClass('active');
            break;
        case VIEW_TYPES.TABLE:
            $('#log-opt-table-btn').addClass('active');
            break;
    }

    const columnUpdates = {};

    if (viewType === VIEW_TYPES.TABLE) {
        columnUpdates.cellRenderer = (params) => {
            if (params.value === '' || params.value === null || params.value === undefined) {
                return '-';
            }

            return typeof params.value === 'number' ? formatNumber(params.value) : params.value;
        };
        logsColumnDefs.forEach((colDef) => {
            if (colDef.field !== 'timestamp') {
                colDef.cellRenderer = columnUpdates.cellRenderer;
            }

            if (colDef.field === 'logs') {
                colDef.cellStyle = null;
                colDef.autoHeight = null;
                colDef.suppressSizeToFit = false;
            }
        });

        gridOptions.api.setColumnDefs(logsColumnDefs);
        gridOptions.api.resetRowHeights();
    } else {
        // Configure logs column for single/multi view
        configureLogsColumn(viewType);
    }

    Cookies.set('log-view', viewType, { expires: 365 });

    refreshColumnVisibility();

    if (viewType === VIEW_TYPES.MULTI) {
        requestAnimationFrame(() => {
            gridOptions.api.refreshCells({ force: true });
            gridOptions.api.redrawRows();
        });
    }

    gridOptions.api.sizeColumnsToFit();
}

function configureLogsColumn(viewType) {
    const isSingleLine = viewType === VIEW_TYPES.SINGLE;

    const logsColDef = logsColumnDefs.find((colDef) => colDef.field === 'logs');

    if (!logsColDef) return;

    logsColDef.cellStyle = isSingleLine ? null : { 'white-space': 'normal' };
    logsColDef.autoHeight = isSingleLine ? null : true;
    logsColDef.suppressSizeToFit = isSingleLine;

    logsColDef.cellRenderer = function (params) {
        const data = params.data || {};
        const whiteSpaceStyle = isSingleLine ? 'nowrap' : 'pre-wrap';

        const selectedFieldsSet = selectedFieldsList.length > 0 ? new Set(selectedFieldsList) : null;

        if (Object.keys(data).length <= 1) {
            return `<div style="white-space: ${whiteSpaceStyle};">-</div>`;
        }

        const logParts = [];

        Object.entries(data)
            .filter(([key, value]) => {
                return key !== 'timestamp' && value !== '' && value !== null && value !== undefined && (key === 'logs' || !selectedFieldsSet || selectedFieldsSet.has(key));
            })
            .forEach(([key, value], index) => {
                const colSep = index > 0 ? '<span class="col-sep"> | </span>' : '';

                const formattedValue = typeof value === 'number' ? formatNumber(value) : isSingleLine ? (typeof value === 'object' && value !== null ? JSON.stringify(value) : value) : formatLogsValue(value);
                logParts.push(`${colSep}<span class="cname-hide-${string2Hex(key)}"><b>${key}</b> ${formattedValue}</span>`);
            });

        if (logParts.length === 0) {
            return `<div style="white-space: ${whiteSpaceStyle};">-</div>`;
        }

        return `<div style="white-space: ${whiteSpaceStyle};">${logParts.join('')}</div>`;
    };

    gridOptions.api.setColumnDefs(logsColumnDefs);

    if (isSingleLine) {
        gridOptions.api.resetRowHeights();
    }

    const columnsToHide = availColNames.filter((name) => name !== 'timestamp' && name !== 'logs');
    const columnsToShow = ['timestamp', 'logs'];

    if (columnsToHide.length > 0) {
        gridOptions.columnApi.setColumnsVisible(columnsToHide, false);
    }
    gridOptions.columnApi.setColumnsVisible(columnsToShow, true);

    gridOptions.columnApi.autoSizeColumn(gridOptions.columnApi.getColumn('logs'), false);
}

function formatLogsValue(value) {
    if (typeof value === 'string') {
        return value.replace(/\n/g, '<br>');
    } else if (typeof value === 'object' && value !== null) {
        return JSON.stringify(JSON.unflatten(value), null, 2).replace(/\n/g, '<br>');
    } else {
        return String(value);
    }
}

function refreshColumnVisibility() {
    if (!gridOptions || !gridOptions.columnApi) return;

    // Get current view mode
    const logView = Cookies.get('log-view') || VIEW_TYPES.SINGLE;
    const isTableView = logView === VIEW_TYPES.TABLE;

    const allColumns = gridOptions.columnApi.getColumns();
    if (!allColumns) return;

    const columnsToHide = [];
    const columnsToShow = [];

    allColumns.forEach((column) => {
        const colId = column.getColId();

        if (colId === 'timestamp') {
            // Timestamp is always visible
            columnsToShow.push(colId);
        } else if (colId === 'logs') {
            // Logs column visibility depends on view type
            if (isTableView) {
                columnsToHide.push(colId);
            } else {
                columnsToShow.push(colId);
            }
        } else if (isTableView) {
            // For table view, show selected fields
            const isVisible = selectedFieldsList.length === 0 || selectedFieldsList.includes(colId);
            if (isVisible) {
                columnsToShow.push(colId);
            } else {
                columnsToHide.push(colId);
            }
        } else {
            // For single/multi view, hide all other columns
            columnsToHide.push(colId);
        }
    });

    if (columnsToHide.length > 0) {
        gridOptions.columnApi.setColumnsVisible(columnsToHide, false);
    }

    if (columnsToShow.length > 0) {
        gridOptions.columnApi.setColumnsVisible(columnsToShow, true);
    }

    if (!isTableView) {
        updateLogsViewColumn(logView);
    }

    gridOptions.api.sizeColumnsToFit();
}

function updateLogsViewColumn(viewType) {
    if (!gridOptions || !gridOptions.api) return;

    const logsColDef = logsColumnDefs.find((col) => col.field === 'logs');
    if (!logsColDef) return;

    const isSingleLine = viewType === VIEW_TYPES.SINGLE;

    logsColDef.cellStyle = isSingleLine ? null : { 'white-space': 'normal' };
    logsColDef.autoHeight = isSingleLine ? null : true;
    logsColDef.suppressSizeToFit = isSingleLine;

    gridOptions.api.setColumnDefs(logsColumnDefs);

    gridOptions.api.refreshCells({
        force: true,
        columns: ['logs'],
    });
}
