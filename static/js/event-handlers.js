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

//eslint-disable-next-line no-unused-vars
function setupEventHandlers() {
    $('#filter-input').on('keyup', filterInputHandler);

    $('#run-filter-btn').off('click').on('click', runFilterBtnHandler);
    $('#query-builder-btn').off('click').on('click', runFilterBtnHandler);
    $('#live-tail-btn').on('click', runLiveTailBtnHandler);

    $('#available-fields').off('click').on('click', availableFieldsClickHandler);
    $('#views-container #available-fields .select-unselect-header').on('click', '.select-unselect-checkbox', toggleAllAvailableFieldsHandler);
    $('#views-container #available-fields .select-unselect-header').on('click', '.select-unselect-checkmark', toggleAllAvailableFieldsHandler);
    $('#available-fields .fields').off('click').on('click', '.available-fields-dropdown-item', availableFieldsSelectHandler);
    $('#hide-null-columns-checkbox').on('change', handleHideNullColumnsCheckbox);

    $('#corner-popup').on('click', '.corner-btn-close', hideCornerPopupError);

    $('#query-language-btn').on('show.bs.dropdown', qLangOnShowHandler);
    $('#query-language-btn').on('hide.bs.dropdown', qLangOnHideHandler);
    $('#query-language-options .query-language-option').on('click', setQueryLangHandler);
    $('#query-mode-options .query-mode-option').on('click', setQueryModeHandler);

    $('#logs-result-container').on('click', '.hide-column', hideColumnHandler);

    $('#log-opt-single-btn').on('click', function () {
        logOptionSingleHandler();
        refreshColumnVisibility();
    });

    $('#log-opt-multi-btn').on('click', function () {
        logOptionMultiHandler();
        refreshColumnVisibility();
    });

    $('#log-opt-table-btn').on('click', function () {
        logOptionTableHandler();
        refreshColumnVisibility();
    });

    $('#date-picker-btn').on('show.bs.dropdown', showDatePickerHandler);
    $('#date-picker-btn').on('hide.bs.dropdown', hideDatePickerHandler);
    $('#reset-timepicker').on('click', resetDatePickerHandler);

    $('.panelEditor-container #date-start').on('change', getStartDateHandler);
    $('.panelEditor-container #date-end').on('change', getEndDateHandler);
    $('.panelEditor-container #time-start').on('change', getStartTimeHandler);
    $('.panelEditor-container #time-end').on('change', getEndTimeHandler);
    $('.panelEditor-container #customrange-btn').on('click', customRangeHandler);

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
        }
        resetDashboard();
        wsState = 'query';
        doSearch(data);
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
    if (!tempStartDate || !tempEndDate) {
        evt.preventDefault();
        evt.stopPropagation();
        if (!tempStartDate) $('#date-start').addClass('error');
        if (!tempEndDate) $('#date-end').addClass('error');

        setTimeout(function () {
            $('#date-start, #date-end').removeClass('error');
        }, 2000);
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

    Cookies.set('customStartDate', appliedStartDate);
    Cookies.set('customStartTime', appliedStartTime);
    Cookies.set('customEndDate', appliedEndDate);
    Cookies.set('customEndTime', appliedEndTime);

    datePickerHandler(filterStartDate, filterEndDate, 'custom');
    // For dashboards
    const currentUrl = window.location.href;
    if (currentUrl.includes('dashboard.html')) {
        if (currentPanel) {
            if (currentPanel.queryData) {
                if (currentPanel.chartType === 'Line Chart' || currentPanel.queryType === 'metrics') {
                    currentPanel.queryData.start = filterStartDate.toString();
                    currentPanel.queryData.end = filterEndDate.toString();
                } else {
                    currentPanel.queryData.startEpoch = filterStartDate;
                    currentPanel.queryData.endEpoch = filterEndDate;
                }
            }
        } else if (!currentPanel) {
            // if user is on dashboard screen
            localPanels.forEach((panel) => {
                delete panel.queryRes;
                if (panel.queryData) {
                    if (panel.chartType === 'Line Chart' || panel.queryType === 'metrics') {
                        panel.queryData.start = filterStartDate.toString();
                        panel.queryData.end = filterEndDate.toString();
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

function hideColumnHandler(evt, isCloseIcon = false) {
    evt.preventDefault();
    evt.stopPropagation();

    availableFieldsSelectHandler(evt, isCloseIcon);
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
        availColNames = [];
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
            wsState = 'query';
            data = getSearchFilter(false, false);
            initialSearchData = data;
            availColNames = [];
            doSearch(data);
        }
        $('#daterangepicker').hide();
    }
}

function filterInputHandler(evt) {
    evt.preventDefault();
    if (evt.keyCode === 13 && ($('#run-filter-btn').text() === ' ' || $('#query-builder-btn').text() === ' ')) {
        resetDashboard();
        logsRowData = [];
        data = getSearchFilter(false, false);
        initialSearchData = data;
        availColNames = [];
        doSearch(data);
    }
}

// prevent the available fields popup from closing when you toggle an available field
function availableFieldsClickHandler(evt) {
    evt.stopPropagation();
}

function availableFieldsSelectHandler(evt, isCloseIcon = false) {
    let colName;

    if (isCloseIcon) {
        const outerDiv = evt.currentTarget.closest('.ag-header-cell');
        const colId = outerDiv.getAttribute('col-id');
        colName = colId;
    } else {
        colName = evt.currentTarget.dataset.index;
    }

    let encColName = string2Hex(colName);
    // don't toggle the timestamp column
    if (colName !== 'timestamp') {
        // toggle the column visibility
        $(`.toggle-${encColName}`).toggleClass('active');
        const isSelected = $(`.toggle-${encColName}`).hasClass('active');

        if (isSelected) {
            // Update the selectedFieldsList everytime a field is selected
            if (!selectedFieldsList.includes(colName)) {
                selectedFieldsList.push(colName);
            }
        } else {
            // Everytime the field is unselected, remove it from selectedFieldsList
            selectedFieldsList = selectedFieldsList.filter((field) => field !== colName);
        }

        // Check if the selected/unselected column is a null column
        //eslint-disable-next-line no-undef
        const nullColumns = Array.from(allColumns).filter((column) => columnsWithNullValues?.has(column) && !columnsWithNonNullValues?.has(column));
        const isNullColumn = nullColumns.includes(colName);

        if (isNullColumn) {
            const $checkbox = $('#hide-null-columns-checkbox');
            if (isSelected) {
                $checkbox.prop('checked', false);
            } else {
                $checkbox.prop('checked', true);
            }
        }
    }

    let visibleColumns = 0;
    let totalColumns = -1;

    availColNames.forEach((colName, _index) => {
        if (selectedFieldsList.includes(colName)) {
            visibleColumns++;
            totalColumns++;
        }
    });

    if (visibleColumns == 1) {
        shouldCloseAllDetails = true;
    } else {
        if (shouldCloseAllDetails) {
            shouldCloseAllDetails = false;
        }
    }
    let el = $('#available-fields .select-unselect-header');

    // uncheck the toggle-all fields if the selected columns count is different
    if (visibleColumns < totalColumns) {
        let cmClass = el.find('.select-unselect-checkmark');
        cmClass.remove();
    }
    // We do not count time and log column
    if (visibleColumns == totalColumns - 2) {
        if (theme === 'light') {
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        } else {
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }
    }

    if ($('#log-opt-single-btn').hasClass('active')) {
        hideOrShowFieldsInLineViews();
    } else if ($('#log-opt-multi-btn').hasClass('active')) {
        hideOrShowFieldsInLineViews();
    } else if ($('#log-opt-table-btn').hasClass('active')) {
        hideOrShowFieldsInLineViews();
        updateColumns();
    }

    if (window.location.pathname.includes('dashboard.html')) {
        hideOrShowFieldsInLineViews();
        updateColumns(); // Function for updating dashboard logs panel
        currentPanel.selectedFields = selectedFieldsList;
        panelGridOptions.api.sizeColumnsToFit();
    } else {
        gridOptions.api.sizeColumnsToFit();
    }

    updatedSelFieldList = true;
}

function toggleAllAvailableFieldsHandler(_evt) {
    processTableViewOption();
    let el = $('#available-fields .select-unselect-header');
    let isChecked = el.find('.select-unselect-checkmark');
    const nullColumnCheckbox = $('#hide-null-columns-checkbox');
    //eslint-disable-next-line no-undef
    const nullColumns = Array.from(allColumns).filter((column) => columnsWithNullValues.has(column) && !columnsWithNonNullValues.has(column));

    if (isChecked.length === 0) {
        if (theme === 'light') {
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        } else {
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }
        let tempFieldList = [];
        availColNames.forEach((colName, _index) => {
            $(`.toggle-${string2Hex(colName)}`).addClass('active');
            tempFieldList.push(colName);
            gridOptions.columnApi.setColumnVisible(colName, true);
        });
        selectedFieldsList = tempFieldList;

        // Uncheck the null column checkbox if there are any null columns
        if (nullColumns.length > 0) {
            nullColumnCheckbox.prop('checked', false);
        }
    } else {
        let cmClass = el.find('.select-unselect-checkmark');
        cmClass.remove();

        availColNames.forEach((colName, _index) => {
            $(`.toggle-${string2Hex(colName)}`).removeClass('active');
            gridOptions.columnApi.setColumnVisible(colName, false);
        });
        selectedFieldsList = [];
        nullColumnCheckbox.prop('checked', true);
    }
    updatedSelFieldList = true;
    // Always hide the logs column
    gridOptions.columnApi.setColumnVisible('logs', false);
}

function hideOrShowFieldsInLineViews() {
    let allSelected = true;
    availColNames.forEach((colName, _index) => {
        let encColName = string2Hex(colName);
        if ($(`.toggle-${encColName}`).hasClass('active')) {
            $(`.cname-hide-${encColName}`).show();
        } else {
            $(`.cname-hide-${encColName}`).hide();
            allSelected = false;
        }
    });
    let el = $('#available-fields .select-unselect-header');
    let isChecked = el.find('.select-unselect-checkmark');
    if (allSelected) {
        if (isChecked.length === 0) {
            if (theme === 'light') {
                el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
            } else {
                el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
            }
        }
    } else {
        isChecked.remove();
    }
}

function logOptionSingleHandler() {
    $('#logs-result-container').removeClass('multi');
    $('#views-container .btn-group .btn').removeClass('active');
    $('#log-opt-single-btn').addClass('active');

    logsColumnDefs.forEach(function (colDef, _index) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = null;
            colDef.autoHeight = null;
            colDef.suppressSizeToFit = true;
            colDef.cellRenderer = function (params) {
                const data = params.data || {};
                let logString = '';
                let addSeparator = false;

                Object.entries(data)
                    .filter(([key]) => key !== 'timestamp')
                    .forEach(([key, value]) => {
                        let colSep = addSeparator ? '<span class="col-sep"> | </span>' : '';

                        // Convert objects and arrays to JSON strings
                        let formattedValue = typeof value === 'object' && value !== null ? JSON.stringify(value) : value;

                        logString += `${colSep}<span class="cname-hide-${string2Hex(key)}">${key}=${formattedValue}</span>`;
                        addSeparator = true;
                    });

                return `<div style="white-space: nowrap;">${logString}</div>`;
            };
        }
    });

    gridOptions.api.setColumnDefs(logsColumnDefs);
    gridOptions.api.resetRowHeights();

    availColNames.forEach((colName, _index) => {
        gridOptions.columnApi.setColumnVisible(colName, false);
    });
    gridOptions.columnApi.setColumnVisible('logs', true);

    gridOptions.columnApi.autoSizeColumn(gridOptions.columnApi.getColumn('logs'), false);
    hideOrShowFieldsInLineViews();
    Cookies.set('log-view', 'single-line', { expires: 365 });
}

function logOptionMultiHandler() {
    $('#logs-result-container').addClass('multi');
    $('#views-container .btn-group .btn').removeClass('active');
    $('#log-opt-multi-btn').addClass('active');

    logsColumnDefs.forEach(function (colDef, _index) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = { 'white-space': 'normal' };
            colDef.autoHeight = true;
            colDef.suppressSizeToFit = false;
            colDef.cellRenderer = function (params) {
                const data = params.data || {};
                let logString = '';
                let addSeparator = false;
                Object.entries(data)
                    .filter(([key]) => key !== 'timestamp')
                    .forEach(([key, value]) => {
                        let colSep = addSeparator ? '<span class="col-sep"> | </span>' : '';
                        let formattedValue = formatLogsValue(value);
                        logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}${key}=${formattedValue}</span>`;
                        addSeparator = true;
                    });

                return `<div style="white-space: pre-wrap;">${logString}</div>`;
            };
        }
    });
    gridOptions.api.setColumnDefs(logsColumnDefs);

    availColNames.forEach((colName, _index) => {
        gridOptions.columnApi.setColumnVisible(colName, false);
    });
    gridOptions.columnApi.setColumnVisible('logs', true);

    gridOptions.columnApi.autoSizeColumn(gridOptions.columnApi.getColumn('logs'), false);
    gridOptions.api.setRowData(logsRowData);
    hideOrShowFieldsInLineViews();
    gridOptions.api.sizeColumnsToFit();
    Cookies.set('log-view', 'multi-line', { expires: 365 });
}

// Function to format logs value, replacing newlines with <br> tags
function formatLogsValue(value) {
    if (typeof value === 'string') {
        return value.replace(/\n/g, '<br>');
    } else {
        return JSON.stringify(JSON.unflatten(value), null, 2);
    }
}

function logOptionTableHandler() {
    processTableViewOption();
    updateColumns();
}

function processTableViewOption() {
    $('#logs-result-container').addClass('multi');
    $('#views-container .btn-group .btn').removeClass('active');
    $('#log-opt-table-btn').addClass('active');
    logsColumnDefs.forEach(function (colDef, _index) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = null;
            colDef.autoHeight = null;
        }
    });
    gridOptions.api.setColumnDefs(logsColumnDefs);
    gridOptions.api.resetRowHeights();
    gridOptions.api.sizeColumnsToFit();
    Cookies.set('log-view', 'table', { expires: 365 });
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
//eslint-disable-next-line no-unused-vars
function updateNullColumnsTracking(records) {
    if (!records || records.length === 0) return;

    records.forEach((record) => {
        Object.keys(record).forEach((column) => {
            allColumns.add(column);
            if (record[column] !== null && record[column] !== undefined && record[column] !== '') {
                //eslint-disable-next-line no-undef
                columnsWithNonNullValues.add(column);
            } else {
                //eslint-disable-next-line no-undef
                columnsWithNullValues.add(column);
            }
        });
    });
}
//eslint-disable-next-line no-unused-vars
function finalizeNullColumnsHiding() {
    //eslint-disable-next-line no-undef
    const nullColumns = Array.from(allColumns).filter((column) => columnsWithNullValues.has(column) && !columnsWithNonNullValues.has(column));
    const checkbox = $('#hide-null-columns-checkbox');
    const checkboxParent = $('#hide-null-column-box');

    if (nullColumns.length === 0) {
        // No null columns, hide checkbox
        checkboxParent.hide();
        updateColumnsVisibility(false, []); // Show all columns
        return;
    }

    checkboxParent.show();
    // Update column visibility if the checkbox is checked
    const hideNullColumns = checkbox.is(':checked');
    updateColumnsVisibility(hideNullColumns, nullColumns);
}

function handleHideNullColumnsCheckbox(event) {
    const hideNullColumns = event.target.checked;
    updateColumnsVisibility(hideNullColumns);
}

function updateColumnsVisibility(hideNullColumns, nullColumns = null) {
    const columnDefs = gridOptions.columnApi?.getColumns().map((col) => ({ field: col.getColId() }));
    let updatedSelectedFieldsList = [...selectedFieldsList];

    if (!nullColumns) {
        //eslint-disable-next-line no-undef
        nullColumns = Array.from(allColumns).filter((column) => columnsWithNullValues.has(column) && !columnsWithNonNullValues.has(column));
    }

    const currentView = Cookies.get('log-view');
    columnDefs?.forEach((colDef) => {
        const colField = colDef.field;
        if (colField !== 'timestamp' && colField !== 'logs') {
            const isSelected = selectedFieldsList.includes(colField);
            const isNullColumn = nullColumns.includes(colField);

            let shouldBeVisible = isSelected;

            if (hideNullColumns && isNullColumn && isSelected) {
                shouldBeVisible = false;
                updatedSelectedFieldsList = updatedSelectedFieldsList.filter((field) => field !== colField);
                if (currentView === 'table') {
                    gridOptions.columnApi.setColumnVisible(colField, shouldBeVisible);
                }
            }
        }
    });
    updateAvailableFieldsUI(updatedSelectedFieldsList);
    gridOptions.api?.sizeColumnsToFit();

    updateLogsColumnRenderer(currentView, updatedSelectedFieldsList, nullColumns);
}

function updateAvailableFieldsUI(updatedSelectedFieldsList) {
    let visibleColumns = 0;
    let totalColumns = availColNames.length;
    if (updatedSelectedFieldsList && updatedSelectedFieldsList.length > 0) {
        availColNames.forEach((colName) => {
            if (updatedSelectedFieldsList.includes(colName)) {
                visibleColumns++;
                $(`.toggle-${string2Hex(colName)}`).addClass('active');
            } else {
                $(`.toggle-${string2Hex(colName)}`).removeClass('active');
            }
        });

        let el = $('#available-fields .select-unselect-header');

        // Update the toggle-all checkbox
        if (visibleColumns === totalColumns - 2) {
            // Excluding timestamp and logs
            if (theme === 'light') {
                el.find('.select-unselect-checkmark').remove();
                el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
            } else {
                el.find('.select-unselect-checkmark').remove();
                el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
            }
        } else {
            el.find('.select-unselect-checkmark').remove();
        }
    }
}

function updateLogsColumnRenderer(currentView, selectedFields, nullColumns) {
    const logsColumnDef = gridOptions.columnApi?.getColumn('logs').getColDef();
    const hideNullColumns = $('#hide-null-columns-checkbox').is(':checked');

    if (logsColumnDef) {
        if (currentView === 'table') {
            logsColumnDef.cellRenderer = null;
        } else {
            logsColumnDef.cellRenderer = (params) => {
                const data = params.data || {};
                let logString = '';
                let addSeparator = false;

                Object.entries(data)
                    .filter(([key]) => key !== 'timestamp' && key !== 'logs')
                    .forEach(([key, value]) => {
                        let colSep = addSeparator ? '<span class="col-sep"> | </span>' : '';
                        let formattedValue;
                        if (currentView === 'single-line') {
                            formattedValue = typeof value === 'object' && value !== null ? JSON.stringify(value) : value;
                        } else if (currentView === 'multi-line') {
                            formattedValue = formatLogsValue(value);
                        }

                        const isVisible = selectedFields.includes(key) && (!nullColumns.includes(key) || !hideNullColumns);
                        const visibilityClass = isVisible ? '' : 'style="display:none;"';

                        logString += `<span class="test-hide-${string2Hex(key)}"${visibilityClass}>${colSep}<b>${key}</b></span>`;
                        logString +=  `<span class="cname-hide-${string2Hex(key)}"${visibilityClass}> ${formattedValue}</span>`;
                        addSeparator = true;
                    });

                return currentView === 'single-line' ? `<div style="white-space: nowrap;">${logString}</div>` : `<div style="white-space: pre-wrap;">${logString}</div>`;
            };
        }

        gridOptions.api.refreshCells({ force: true, columns: ['logs'] });
    }
}

function refreshColumnVisibility() {
    const hideNullColumns = $('#hide-null-columns-checkbox').is(':checked');
    updateColumnsVisibility(hideNullColumns);
}
