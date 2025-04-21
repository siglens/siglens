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
    $('#filter-input').on('keydown', filterInputHandler);

    $('#run-filter-btn').off('click').on('click', runFilterBtnHandler);
    $('#query-builder-btn').off('click').on('click', runFilterBtnHandler);
    $('#live-tail-btn').on('click', runLiveTailBtnHandler);

    $('#corner-popup').on('click', '.corner-btn-close', hideCornerPopupError);

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
    $('.db-range-item').off('click').on('click', dashboardRangeItemHandler);

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

        if (window.fieldssidebarRenderer) {
            window.fieldssidebarRenderer.init();
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
        if (!tempStartDate) $('#date-start').addClass('error');
        if (!tempEndDate) $('#date-end').addClass('error');

        $('#daterange-to').after('<div id="date-range-error" class="date-range-error">Please select both start and end dates</div>');

        setTimeout(function () {
            $('#date-start, #date-end').removeClass('error');
            $('#date-range-error').fadeOut(300, function() { $(this).remove(); });
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

        setTimeout(function () {
            $('#date-start, #date-end').removeClass('error');
            $('.panelEditor-container #date-start, .panelEditor-container #date-end').removeClass('error');
            $('#date-range-error').fadeOut(300, function() { $(this).remove(); });
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
        if (currentPanel) {
            if (currentPanel.queryData) {
                if (currentPanel.chartType === 'Line Chart' || currentPanel.queryType === 'metrics') {
                    const startDateStr = filterStartDate.toString();
                    const endDateStr = filterEndDate.toString();

                    // Update start and end for queryData
                    if (currentPanel.queryData) {
                        currentPanel.queryData.start = startDateStr;
                        currentPanel.queryData.end = endDateStr;

                        // Update start and end for each item in queriesData
                        if (Array.isArray(currentPanel.queryData.queriesData)) {
                            currentPanel.queryData.queriesData.forEach((query) => {
                                query.start = startDateStr;
                                query.end = endDateStr;
                            });
                        }

                        // Update start and end for each item in formulasData
                        if (Array.isArray(currentPanel.queryData.formulasData)) {
                            currentPanel.queryData.formulasData.forEach((formula) => {
                                formula.start = startDateStr;
                                formula.end = endDateStr;
                            });
                        }
                    }
                } else {
                    currentPanel.queryData.startEpoch = filterStartDate;
                    currentPanel.queryData.endEpoch = filterEndDate;
                }
                runQueryBtnHandler();
            }
        } else if (!currentPanel) {
            // if user is on dashboard screen
            localPanels.forEach((panel) => {
                delete panel.queryRes;
                if (panel.queryData) {
                    if (panel.chartType === 'Line Chart' || panel.queryType === 'metrics') {
                        const startDateStr = filterStartDate.toString();
                        const endDateStr = filterEndDate.toString();

                        // Update start and end for queryData
                        if (panel.queryData) {
                            panel.queryData.start = startDateStr;
                            panel.queryData.end = endDateStr;

                            // Update start and end for each item in queriesData
                            if (Array.isArray(panel.queryData.queriesData)) {
                                panel.queryData.queriesData.forEach((query) => {
                                    query.start = startDateStr;
                                    query.end = endDateStr;
                                });
                            }

                            // Update start and end for each item in formulasData
                            if (Array.isArray(panel.queryData.formulasData)) {
                                panel.queryData.formulasData.forEach((formula) => {
                                    formula.start = startDateStr;
                                    formula.end = endDateStr;
                                });
                            }
                        }
                    } else {
                        panel.queryData.startEpoch = filterStartDate;
                        panel.queryData.endEpoch = filterEndDate;
                    }
                }
            });
            displayPanels();
        }
    }
}

function rangeItemHandler(evt) {
    resetCustomDateRange();
    $.each($('.range-item.active'), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), 'now', $(this).attr('id'));
}

async function dashboardRangeItemHandler(evt) {
    resetCustomDateRange();
    $.each($('.db-range-item.active'), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), 'now', $(this).attr('id'));

    // if user is on edit panel screen
    if (currentPanel) {
        if (currentPanel.queryData) {
            if (currentPanel.chartType === 'Line Chart' || currentPanel.queryType === 'metrics') {
                const startDateStr = filterStartDate.toString();
                const endDateStr = filterEndDate.toString();

                // Update start and end for queryData
                if (currentPanel.queryData) {
                    currentPanel.queryData.start = startDateStr;
                    currentPanel.queryData.end = endDateStr;

                    // Update start and end for each item in queriesData
                    if (Array.isArray(currentPanel.queryData.queriesData)) {
                        currentPanel.queryData.queriesData.forEach((query) => {
                            query.start = startDateStr;
                            query.end = endDateStr;
                        });
                    }

                    // Update start and end for each item in formulasData
                    if (Array.isArray(currentPanel.queryData.formulasData)) {
                        currentPanel.queryData.formulasData.forEach((formula) => {
                            formula.start = startDateStr;
                            formula.end = endDateStr;
                        });
                    }
                }
            } else {
                currentPanel.queryData.startEpoch = filterStartDate;
                currentPanel.queryData.endEpoch = filterEndDate;
            }
            runQueryBtnHandler();
        }
    } else if (!currentPanel) {
        // if user is on dashboard screen
        localPanels.forEach((panel) => {
            delete panel.queryRes;
            if (panel.queryData) {
                if (panel.chartType === 'Line Chart' || panel.queryType === 'metrics') {
                    const startDateStr = filterStartDate.toString();
                    const endDateStr = filterEndDate.toString();

                    // Update start and end for queryData
                    if (panel.queryData) {
                        panel.queryData.start = startDateStr;
                        panel.queryData.end = endDateStr;

                        // Update start and end for each item in queriesData
                        if (Array.isArray(panel.queryData.queriesData)) {
                            panel.queryData.queriesData.forEach((query) => {
                                query.start = startDateStr;
                                query.end = endDateStr;
                            });
                        }

                        // Update start and end for each item in formulasData
                        if (Array.isArray(panel.queryData.formulasData)) {
                            panel.queryData.formulasData.forEach((formula) => {
                                formula.start = startDateStr;
                                formula.end = endDateStr;
                            });
                        }
                    }
                } else {
                    panel.queryData.startEpoch = filterStartDate;
                    panel.queryData.endEpoch = filterEndDate;
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
            resetDashboard();
            logsRowData = [];
            accumulatedRecords = [];
            totalLoadedRecords = 0;
            wsState = 'query';
            data = getSearchFilter(false, false);
            initialSearchData = data;
            doSearch(data);
        }
        $('#daterangepicker').hide();
    }
}

function filterInputHandler(evt) {
    if (!evt.shiftKey && evt.keyCode === 13 && ($('#run-filter-btn').text() === ' ' || $('#query-builder-btn').text() === ' ')) {
        evt.preventDefault();
        resetDashboard();
        logsRowData = [];
        accumulatedRecords = [];
        totalLoadedRecords = 0;
        data = getSearchFilter(false, false);
        initialSearchData = data;
        doSearch(data);
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
    $('#logs-result-container').toggleClass('multi', viewType !== VIEW_TYPES.SINGLE);
    $('#views-container .btn-group .btn').removeClass('active');

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

    logsColumnDefs.forEach(function (colDef) {
        if (colDef.field !== 'timestamp' && colDef.field !== 'logs') {
            colDef.cellRenderer = null;
        }
    });

    if (viewType === VIEW_TYPES.TABLE) {
        logsColumnDefs.forEach(function (colDef) {
            if (colDef.field !== 'timestamp') {
                colDef.cellRenderer = function (params) {
                    return params.value === '' || params.value === null || params.value === undefined ? '-' : params.value;
                };
            }

            if (colDef.field === 'logs') {
                colDef.cellStyle = null;
                colDef.autoHeight = null;
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
        setTimeout(() => {
            gridOptions.api.refreshCells({ force: true });
            gridOptions.api.redrawRows();
        }, 50);
    }

    gridOptions.api.sizeColumnsToFit();
}

function configureLogsColumn(viewType) {
    const isSingleLine = viewType === VIEW_TYPES.SINGLE;

    logsColumnDefs.forEach(function (colDef) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = isSingleLine ? null : { 'white-space': 'normal' };
            colDef.autoHeight = isSingleLine ? null : true;
            colDef.suppressSizeToFit = isSingleLine;

            colDef.cellRenderer = function (params) {
                const data = params.data || {};
                let logString = '';
                let addSeparator = false;

                Object.entries(data)
                    .filter(([key]) => key !== 'timestamp')
                    .filter(([_, value]) => value !== '' && value !== null && value !== undefined) // Filter out the column which is empty
                    .forEach(([key, value]) => {
                        // Only show selected fields (or all if none selected)
                        if (key !== 'logs' && selectedFieldsList.length > 0 && !selectedFieldsList.includes(key)) {
                            return;
                        }

                        let colSep = addSeparator ? '<span class="col-sep"> | </span>' : '';
                        let formattedValue = isSingleLine ? (typeof value === 'object' && value !== null ? JSON.stringify(value) : value) : formatLogsValue(value);

                        logString += `${colSep}<span class="cname-hide-${string2Hex(key)}"><b>${key}</b> ${formattedValue}</span>`;
                        addSeparator = true;
                    });

                const whiteSpaceStyle = isSingleLine ? 'nowrap' : 'pre-wrap';

                if (!logString) {
                    return `<div style="white-space: ${whiteSpaceStyle};">-</div>`;
                }

                return `<div style="white-space: ${whiteSpaceStyle};">${logString}</div>`;
            };
        }
    });

    gridOptions.api.setColumnDefs(logsColumnDefs);

    if (isSingleLine) {
        gridOptions.api.resetRowHeights();
    }

    // Configure visibility
    availColNames.forEach((colName) => {
        gridOptions.columnApi.setColumnVisible(colName, false);
    });
    gridOptions.columnApi.setColumnVisible('timestamp', true);
    gridOptions.columnApi.setColumnVisible('logs', true);

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

    // Hide all non-essential columns first
    const allColumns = gridOptions.columnApi.getColumns();
    allColumns.forEach((column) => {
        const colId = column.getColId();
        if (colId !== 'timestamp' && colId !== 'logs') {
            gridOptions.columnApi.setColumnVisible(colId, false);
        }
    });

    gridOptions.columnApi.setColumnVisible('timestamp', true);
    gridOptions.columnApi.setColumnVisible('logs', !isTableView);

    if (isTableView) {
        // Show selected columns in table view
        availColNames.forEach((colName) => {
            if (colName !== 'timestamp' && colName !== 'logs') {
                const isVisible = selectedFieldsList.length === 0 || selectedFieldsList.includes(colName);
                gridOptions.columnApi.setColumnVisible(colName, isVisible);
            }
        });
    } else {
        // Update logs column for single/multi view
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
