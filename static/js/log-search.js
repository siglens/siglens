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
let originalIndexValues = [];
//eslint-disable-next-line no-unused-vars
let indexValues = [];

$(document).ready(async () => {
    toggleClearButtonVisibility();

    // Call the function for each tooltip
    createTooltip('#add-index', 'Add New Index');
    createTooltip('#date-picker-btn', 'Pick the Time Window');
    createTooltip('#query-builder-btn', 'Run Query');
    createTooltip('#logs-settings', 'Settings');
    createTooltip('#saveq-btn', 'Save Query');
    createTooltip('#add-logs-to-db-btn', 'Add to Dashboards');
    createTooltip('#alert-from-logs-btn', 'Create Alert');
    createTooltip('.download-all-logs-btn', 'Download Logs');
    createTooltip('#show-record-intro-btn', 'View Query Results Info');
    createTooltip('#log-opt-single-btn', 'Single Line View');
    createTooltip('#log-opt-multi-btn', 'Multi Line View');
    createTooltip('#log-opt-table-btn', 'Tabular View');
    createTooltip('.avail-fields-btn', 'Select Field to Display');
    createTooltip('#run-filter-btn', 'Run query');
    createTooltip('#format-table', 'Format Table');

    setupTableFormatOptions();

    function updateTooltip(element) {
        if (element && element._tippy) {
            const newContent = element.classList.contains('cancel-search') ? 'Cancel Query' : 'Run Query';
            element._tippy.setContent(newContent);
        }
    }

    function handleClassChange(event) {
        updateTooltip(event.target);
    }

    $(document).on('classChange', '#run-filter-btn, #query-builder-btn', handleClassChange);

    const observerCallback = (mutations) => {
        mutations.forEach((mutation) => {
            if (mutation.type === 'attributes' && mutation.attributeName === 'class') {
                updateTooltip(mutation.target);
            }
        });
    };

    const observer = new MutationObserver(observerCallback);
    const config = { attributes: true, attributeFilter: ['class'] };

    ['run-filter-btn', 'query-builder-btn'].forEach((id) => {
        const element = document.getElementById(id);
        if (element) {
            observer.observe(element, config);
        }
    });

    setSaveQueriesDialog();
    let indexes = await getListIndices();
    if (indexes) {
        originalIndexValues = indexes.map((item) => item.index);
        indexValues = [...originalIndexValues];
    }
    initializeIndexAutocomplete();
    let queryMode = Cookies.get('queryMode');
    if (queryMode !== undefined) {
        const searchParams = new URLSearchParams(window.location.search);

        // Check if the URL has the 'filterTab' parameter
        const hasFilterTab = searchParams.has('filterTab');

        if (!hasFilterTab) {
            //If filter tab is not present then do trigger.
            if (queryMode === 'Builder') {
                $('.custom-code-tab a:first').trigger('click');
            } else {
                $('.custom-code-tab a[href="#tabs-2"]').trigger('click');
            }
        }
        // Add active class to dropdown options based on the queryMode selected.
        updateQueryModeUI(queryMode);
    }
    // If query string found , then do search
    if (window.location.search) {
        data = getInitialSearchFilter(false, false);
        initialSearchData = data;
    } else {
        setIndexDisplayValue(selectedSearchIndex);
        let stDate = Cookies.get('startEpoch') || 'now-15m';
        let endDate = Cookies.get('endEpoch') || 'now';
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
        $('#custom-chart-tab').hide();

        //No query string found, using default search filter
        data = getSearchFilter(false, false, true);
    }

    doSearch(data);
    //eslint-disable-next-line no-undef
    initSidebarToggle();
    initializePagination();
    const pageSizeSelect = document.getElementById('page-size-select');
    if (pageSizeSelect) {
        pageSizeSelect.value = pageSize.toString();
    }

    $('body').css('cursor', 'default');

    $('.theme-btn').on('click', themePickerHandler);
    // eslint-disable-next-line no-undef
    $('.theme-btn').on('click', updateTimeChartTheme);

    setupEventHandlers();

    resetDashboard();

    if (Cookies.get('startEpoch') && Cookies.get('endEpoch')) {
        let cookieVar = Cookies.get('endEpoch');
        if (cookieVar === 'now') {
            filterStartDate = Cookies.get('startEpoch');
            filterEndDate = Cookies.get('endEpoch');
            $('.inner-range #' + filterStartDate).addClass('active');
        } else {
            filterStartDate = Number(Cookies.get('startEpoch'));
            filterEndDate = Number(Cookies.get('endEpoch'));
        }
    }

    if (Cookies.get('customStartDate')) {
        let cookieVar = new Date(Cookies.get('customStartDate'));
        $('#date-start').val(cookieVar.toISOString().substring(0, 10));
        $('#date-start').addClass('active');
    }
    if (Cookies.get('customEndDate')) {
        let cookieVar = new Date(Cookies.get('customEndDate'));
        $('#date-end').val(cookieVar.toISOString().substring(0, 10));
        $('#date-end').addClass('active');
    }
    if (Cookies.get('customStartTime')) {
        $('#time-start').val(Cookies.get('customStartTime'));
        $('#time-start').addClass('active');
    }
    if (Cookies.get('customEndTime')) {
        $('#time-end').val(Cookies.get('customEndTime'));
        $('#time-end').addClass('active');
    }

    $('#info-icon-logQL').tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'click',
    });

    $('#info-icon-logQL').on('click', function (_e) {
        $('#info-icon-logQL').tooltip('show');
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest('.tooltip-inner').length === 0) {
            $('#info-icon-logQL').tooltip('hide');
        }
    });

    $('#info-icon-spl').tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'click',
    });

    $('#info-icon-spl').on('click', function (_e) {
        $('#info-icon-spl').tooltip('show');
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest('.tooltip-inner').length === 0) {
            $('#info-icon-spl').tooltip('hide');
        }
    });

    initializeFilterInputEvents();
});

const NUMBER_FORMAT_TYPES = {
    COMMA: 'comma',
    PLAIN: 'plain',
};

function setupTableFormatOptions() {
    if (Cookies.get('number-format') === undefined) {
        Cookies.set('number-format', NUMBER_FORMAT_TYPES.PLAIN, { expires: 365 });
    }

    let numberFormatState = Cookies.get('number-format');

    $(`input[name="numberFormat"][value="${numberFormatState}"]`).prop('checked', true);
    $('#format-table')
        .off('click')
        .on('click', function (e) {
            var dropdown = $('#format-dropdown');
            dropdown.toggle();
            e.stopPropagation();
        });

    $('input[name="numberFormat"]')
        .off('change')
        .on('change', function () {
            numberFormatState = $(this).val();
        });

    $('#apply-format')
        .off('click')
        .on('click', function () {
            Cookies.set('number-format', numberFormatState, { expires: 365 });
            $('#format-dropdown').hide();

            if (gridOptions && gridOptions.api) {
                gridOptions.api.refreshCells({ force: true });
            }

            if (aggGridOptions && aggGridOptions.api) {
                aggGridOptions.api.refreshCells({ force: true });
            }
        });

    $(document)
        .off('click.formatDropdown')
        .on('click.formatDropdown', function (e) {
            if (!$(e.target).closest('#format-dropdown, #format-table').length) {
                $('#format-dropdown').hide();
            }
        });
}

// eslint-disable-next-line no-unused-vars
function formatNumber(value) {
    if (typeof value !== 'number') {
        return value;
    }

    const useCommas = Cookies.get('number-format') !== 'plain';

    if (useCommas) {
        const parts = value.toString().split('.');
        const wholeFormatted = parseInt(parts[0], 10).toLocaleString('en-US');
        return parts.length > 1 ? `${wholeFormatted}.${parts[1]}` : wholeFormatted;
    } else {
        return value.toString();
    }
}
