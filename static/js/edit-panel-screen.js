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

let selectedChartTypeIndex = -1;
let selectedUnitTypeIndex = -1;
let selectedDataTypeIndex = -1;
let prevSelectedDataTypeIndex = -2;
let selectedLogLinesViewTypeIndex = -1;

let mapChartTypeToIndex = new Map([
    ['Line Chart', 0],
    ['Bar Chart', 1],
    ['Pie Chart', 2],
    ['Data Table', 3],
    ['number', 4],
    ['loglines', 5],
]);

let mapIndexToChartType = new Map([
    [0, 'Line Chart'],
    [1, 'Bar Chart'],
    [2, 'Pie Chart'],
    [3, 'Data Table'],
    [4, 'number'],
    [5, 'loglines'],
]);

let mapUnitTypeToIndex = new Map([
    ['', -1],
    ['misc', 0],
    ['data', 1],
    ['throughput', 2],
    ['time', 3],
    ['data Rate', 4],
]);

let mapMiscOptionsToIndex = new Map([
    ['', -1],
    ['none', 0],
    ['percent(0-100)', 1],
]);

let mapDataTypeToIndex = new Map([
    ['', -1],
    ['bytes', 0],
    ['kB', 1],
    ['MB', 2],
    ['GB', 3],
    ['TB', 4],
    ['PB', 5],
    ['EB', 6],
    ['ZB', 7],
    ['YB', 8],
]);

let mapIndexToUnitType = new Map([
    [-1, ''],
    [0, 'misc'],
    [1, 'data'],
    [2, 'throughput'],
    [3, 'time'],
    [4, 'data Rate'],
]);

let mapThroughputOptionsToIndex = new Map([
    ['', -1],
    ['counts/sec', 0],
    ['writes/sec', 1],
    ['reads/sec', 2],
    ['requests/sec', 3],
    ['ops/sec', 4],
]);

let mapTimeOptionsToIndex = new Map([
    ['', -1],
    ['hertz(1/s)', 0],
    ['nanoseconds(ns)', 1],
    ['microsecond(µs)', 2],
    ['milliseconds(ms)', 3],
    ['seconds(s)', 4],
    ['minutes(m)', 5],
    ['hours(h)', 6],
    ['days(d)', 7],
]);

let mapDataRateTypeToIndex = new Map([
    ['', -1],
    ['packets/sec', 0],
    ['bytes/sec', 1],
    ['bits/sec', 2],
    ['kilobytes/sec', 3],
    ['kilobits/sec', 4],
    ['megabytes/sec', 5],
    ['megabits/sec', 6],
    ['gigabytes/sec', 7],
    ['gigabits/sec', 8],
    ['terabytes/sec', 9],
    ['terabits/sec', 10],
    ['petabytes/sec', 11],
    ['petabits/sec', 12],
]);

let mapIndexToMiscOptions = new Map([
    [-1, ''],
    [0, 'none'],
    [1, 'percent(0-100)'],
]);

let mapIndexToDataType = new Map([
    [-1, ''],
    [0, 'bytes'],
    [1, 'kB'],
    [2, 'MB'],
    [3, 'GB'],
    [4, 'TB'],
    [5, 'PB'],
    [6, 'EB'],
    [7, 'ZB'],
    [8, 'YB'],
]);

let mapIndexToThroughputOptions = new Map([
    [-1, ''],
    [0, 'counts/sec'],
    [1, 'writes/sec'],
    [2, 'reads/sec'],
    [3, 'requests/sec'],
    [4, 'ops/sec'],
]);

let mapIndexToTimeOptions = new Map([
    [-1, ''],
    [0, 'hertz(1/s)'],
    [1, 'nanoseconds(ns)'],
    [2, 'microsecond(µs)'],
    [3, 'milliseconds(ms)'],
    [4, 'seconds(s)'],
    [5, 'minutes(m)'],
    [6, 'hours(h)'],
    [7, 'days(d)'],
]);

let mapIndexToDataRateType = new Map([
    [-1, ''],
    [0, 'packets/sec'],
    [1, 'bytes/sec'],
    [2, 'bits/sec'],
    [3, 'kilobytes/sec'],
    [4, 'kilobits/sec'],
    [5, 'megabytes/sec'],
    [6, 'megabits/sec'],
    [7, 'gigabytes/sec'],
    [8, 'gigabits/sec'],
    [9, 'terabytes/sec'],
    [10, 'terabits/sec'],
    [11, 'petabytes/sec'],
    [12, 'petabits/sec'],
]);

let mapIndexToLogLinesViewType = new Map([
    [0, 'Single line display view'],
    [1, 'Multi line display view'],
]);

$(document).ready(function () {
    $('#info-icon-logs').tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'click',
    });

    $('#info-icon-logs').on('click', function (_e) {
        $('#info-icon-logs').tooltip('show');
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest('.tooltip-inner').length === 0) {
            $('#info-icon-logs').tooltip('hide');
        }
    });

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

    $('#edit-button').click(function () {
        $('#panel-editor-left').show();
        $('#viewPanel-container').hide();
        $('#edit-button').addClass('active');
        $('#overview-button').removeClass('active');
        runQueryBtnHandler();
    });

    $('#overview-button').click(function () {
        $('#panel-editor-left').hide();
        $('#viewPanel-container').show();
        $('#edit-button').removeClass('active');
        $('#overview-button').addClass('active');
        displayPanelView(panelIndex);
    });
    let ele = $('#available-fields .select-unselect-header');

    if (theme === 'light') {
        ele.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
    } else {
        ele.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
    }
});

//eslint-disable-next-line no-unused-vars
async function editPanelInit(redirectedFromViewScreen, isNewPanel) {
    queries = {};
    formulas = {};
    if (redirectedFromViewScreen === -1) {
        $('#panel-editor-left').hide();
        $('#viewPanel-container').show();
        $('#edit-button').removeClass('active');
        $('#overview-button').addClass('active');
        displayPanelView(panelIndex);
    } else {
        $('#panel-editor-left').show();
        $('#edit-button').addClass('active');
        $('#overview-button').removeClass('active');
    }
    resetOptions();
    $('.panelDisplay #empty-response').empty();
    $('.panelDisplay #corner-popup').empty().hide();
    $('.panelDisplay #panelLogResultsGrid').empty().hide();
    $('.panelDisplay .panel-info-corner').hide();
    $('#metrics-queries,#metrics-formula').empty();
    $('#filter-input').val('');
    $('.tags-list').empty();
    [firstBoxSet, secondBoxSet, thirdBoxSet] = [new Set(), new Set(), new Set()];
    $('#aggregations, #aggregate-attribute-text, #search-filter-text').show();

    currentPanel = JSON.parse(JSON.stringify(localPanels[panelIndex]));

    $('.panEdit-navBar .panEdit-dbName').html(`${dbName}`);
    // reset inputs to show placeholders
    $('.panEdit-navBar .panelTitle').html(currentPanel.name);
    $('#panEdit-nameChangeInput').val(currentPanel.name);
    $('#panEdit-descrChangeInput').val(currentPanel.description);

    $('#panEdit-nameChangeInput').attr('placeholder', 'Name');
    $('#panEdit-descrChangeInput').attr('placeholder', 'Description (Optional)');

    // Display Visualization options based on query type/ panel type (Logs or Metrics)
    loadVisualizationOptions(currentPanel.queryType);

    if (currentPanel.description) {
        const panelInfoCorner = $('.panelEditor-container .panelDisplay .panel-info-corner');
        const panelDescIcon = $('.panelEditor-container .panelDisplay .panel-info-corner #panel-desc-info');
        panelInfoCorner.show();
        panelDescIcon.tooltip('dispose');
        panelDescIcon.attr('title', currentPanel.description);
        panelDescIcon.tooltip({
            delay: { show: 0, hide: 300 },
            trigger: 'hover',
        });
        panelInfoCorner.hover(
            function () {
                panelDescIcon.tooltip('show');
            },
            function () {
                panelDescIcon.tooltip('hide');
            }
        );
    }

    if (currentPanel.chartType != '') selectedChartTypeIndex = mapChartTypeToIndex.get(currentPanel.chartType);

    // Logs Panel
    if (currentPanel.queryType === 'logs') {
        $('.panEdit-navBar .panel-type').html('(Logs Panel)');

        // Search Text
        if (currentPanel.queryData && (currentPanel.queryData.searchText !== undefined || currentPanel.queryData?.queries?.[0]?.query !== undefined)) {
            if (currentPanel.queryType === 'logs') {
                let queryMode = currentPanel.queryData.queryMode;
                let queryText = currentPanel.queryData.searchText;
                if (queryMode === 'Code' || queryMode === undefined) {
                    // undefined case for previously created panels and open code for those panels
                    $('#custom-code-tab').tabs('option', 'active', 1);
                    $('#filter-input').val(queryText);
                } else if (queryMode === 'Builder') {
                    $('#custom-code-tab').tabs('option', 'active', 0);
                    codeToBuilderParsing(queryText);
                }
                setDashboardQueryModeHandler(queryMode);
            }
        }

        $('.index-container, .queryInput-container, #query-language-btn').css('display', 'inline-flex');
        $('#metrics-query-language').css('display', 'none');

        // Chart Type: Number
        if (selectedChartTypeIndex === 4) {
            $('.dropDown-unit').css('display', 'flex');
            $('#nestedDropDownContainer').css('display', 'flex');

            // Unit Type for Chart Type: Number
            selectedUnitTypeIndex = mapUnitTypeToIndex.get(currentPanel.unit);

            let currentPanelUnit = currentPanel.unit;
            if (currentPanelUnit === '') {
                selectedDataTypeIndex = -1;
            } else if (currentPanelUnit === 'misc') selectedDataTypeIndex = mapMiscOptionsToIndex.get(currentPanel.dataType);
            else if (currentPanelUnit === 'data') selectedDataTypeIndex = mapDataTypeToIndex.get(currentPanel.dataType);
            else if (currentPanelUnit === 'throughput') selectedDataTypeIndex = mapThroughputOptionsToIndex.get(currentPanel.dataType);
            else if (currentPanelUnit === 'time') selectedDataTypeIndex = mapTimeOptionsToIndex.get(currentPanel.dataType);
            else if (currentPanelUnit === 'data Rate') selectedDataTypeIndex = mapDataRateTypeToIndex.get(currentPanel.dataType);

            if (selectedDataTypeIndex == -1) {
                $('.dropDown-misc-options span').html('Misc');
                $('.dropDown-data-options span').html('Data');
                $('.dropDown-throughput-options span').html('Throughput');
                $('.dropDown-percent-options span').html('Percent');
                $('.dropDown-time-options span').html('Time');
                $('.dropDown-data-rate-options span').html('Data Rate');
                prevSelectedDataTypeIndex = -2;
            }

            if (selectedDataTypeIndex != -1 && selectedDataTypeIndex !== undefined) {
                if (currentPanelUnit === 'misc') refreshNestedMiscMenuOptions();
                else if (currentPanelUnit === 'data') refreshNestedDataMenuOptions();
                else if (currentPanelUnit === 'throughput') refreshNestedTptMenuOptions();
                else if (currentPanelUnit === 'percent') refreshNestedPercentMenuOptions();
                else if (currentPanelUnit === 'time') refreshNestedTimeMenuOptions();
                else if (currentPanelUnit === 'data Rate') refreshNestedDataRateMenuOptions();
            }
        } else {
            $('#nestedDropDownContainer').css('display', 'none');
            $('.dropDown-unit').css('display', 'none');
        }

        // Chart Type: LogLines
        if (selectedChartTypeIndex === 5) {
            $('.dropDown-logLinesView').css('display', 'flex');

            // View Type for Chart Type: LogLines (Single or Wrap)
            let currentPanelLogViewType = currentPanel.logLinesViewType;

            if (currentPanelLogViewType === undefined && selectedChartTypeIndex === 5) {
                selectedLogLinesViewTypeIndex = 0;
                currentPanel.logLinesViewType = 'Single line display view';
            } else if (currentPanelLogViewType === 'Single line display view') {
                selectedLogLinesViewTypeIndex = 0;
            } else if (currentPanelLogViewType === 'Multi line display view') {
                selectedLogLinesViewTypeIndex = 1;
            }
            if (currentPanelLogViewType && currentPanelLogViewType != 'Table view') refreshLogLinesViewMenuOptions();
        } else {
            $('.dropDown-logLinesView').css('display', 'none');
        }

        // Chart Type: Data Table
        if (selectedChartTypeIndex === 3) {
            currentPanel.logLinesViewType = 'Table view';
            $('#avail-field-container ').css('display', 'inline-flex');
        } else {
            $('#avail-field-container ').css('display', 'none');
        }

        // Chart Type: Bar or Line Chart
        if (selectedChartTypeIndex === 0 || selectedChartTypeIndex === 1) {
            console.log('selectedChartTypeIndex: ' + selectedChartTypeIndex);
            $('#logs-vis-format-options').show();

            // Initialize format settings if they don't exist
            if (!currentPanel.formatSettings) {
                currentPanel.formatSettings = {
                    xAxis: {
                        title: '',
                        labelRotation: 0,
                    },
                    yAxis: {
                        title: '',
                        interval: null,
                        minValue: null,
                        maxValue: null,
                        abbreviations: false,
                    },
                    chartOverlay: {
                        enabled: false,
                        title: '',
                        interval: null,
                        minValue: null,
                        maxValue: null,
                        abbreviations: false,
                        metrics: [],
                    },
                    legend: {
                        show: true,
                        position: 'top',
                    },
                };
            }

            initFormatAccordion(currentPanel);
        } else {
            $('#logs-vis-format-options').hide();
        }

        // Query Language
        if (currentPanel.queryData) {
            $('.panEdit-query-language-option').removeClass('active');
            if (currentPanel.queryData.queryLanguage === 'Log QL') {
                $('#query-language-options #option-2').addClass('active');
                $('#query-language-btn span').html('Log QL');
            } else if (currentPanel.queryData.queryLanguage === 'Splunk QL') {
                $('#query-language-options #option-3').addClass('active');
                $('#query-language-btn span').html('Splunk QL');
            }
        }
        // Tooltip based on Query Language
        displayQueryToolTip();

        // Handle index display
        indexValues = [...originalIndexValues];
        if (currentPanel.queryData && currentPanel.queryData.indexName) {
            selectedSearchIndex = currentPanel.queryData.indexName;
        }
        setIndexDisplayValue(selectedSearchIndex);
        $('#index-listing').autocomplete('option', 'source', indexValues);
    } else if (currentPanel.queryType === 'metrics') {
        $('.panEdit-navBar .panel-type').html('(Metrics Panel)');

        $('#metrics-query-language').css('display', 'inline-block');
        $('.index-container, .queryInput-container, #query-language-btn').css('display', 'none');
        $('#logs-vis-format-options').hide();

        if (!currentPanel.style) {
            currentPanel.style = {
                color: 'Palette',
                display: 'Line chart',
                lineStroke: 'Normal',
                lineStyle: 'Solid',
            };
        }

        // Show chart editing options for metrics graphs
        $('#visualization-options').addClass('d-flex');
        $('#visualization-options').show();

        applyLineChartStyle(currentPanel.style);
    }

    // Refreshing all the dropdown and menus
    if (selectedChartTypeIndex != -1 && selectedChartTypeIndex !== undefined) refreshChartMenuOptions();
    if (selectedUnitTypeIndex != -1 && selectedUnitTypeIndex !== undefined) refreshUnitMenuOptions();

    // Setting Event Handlers
    if ($('.dropDown-unit.active').length) handleUnitDropDownClick();
    if ($('.dropDown-logLinesView.active').length) handleLogLinesViewDropDownClick();
    $('.editPanelMenu-inner-options').slideUp();
    $('.inner-options').on('click', runQueryBtnHandler);

    $('.panelDisplay #empty-response').empty();
    $('.panelDisplay #empty-response').hide();
    $('.panelDisplay .panEdit-panel').show();

    // Pause the Refresh when on Edit Panel Screen
    pauseRefreshInterval();

    // For logs: Don't do initial search when a new panel is created
    if (currentPanel.queryType === 'logs' && isNewPanel && (!currentPanel.queryData.searchText || currentPanel.queryData.searchText.trim() === '')) {
        $('.panelEditor-container').css('display', 'flex');
        $('.panelDisplay .panEdit-panel').css('display', 'none');
        $('.panelDisplay #empty-response').html('<div>Create a query using the builder to access and view the logs.</div>').css({
            display: 'flex',
            'background-color': 'var(--datatable-bg-color)',
        });
    } else {
        $('.panelDisplay .panEdit-panel').show();
        await runQueryBtnHandler();
    }
}

function initFormatAccordion(panel) {
    $('.accordion-header, .accordion-content, .accordion-arrow').removeClass('expanded');

    $('.accordion-header')
        .off('click')
        .on('click', function () {
            const header = $(this);
            const content = $(this).next('.accordion-content');
            const arrow = $(this).find('.accordion-arrow');

            header.toggleClass('expanded');
            content.toggleClass('expanded');
            arrow.toggleClass('expanded');
        });

    chartSettings = JSON.parse(JSON.stringify(panel.formatSettings));
    updateFormatPanelFields(chartSettings);

    setupFormatPanel();
}

function applyLineChartStyle() {
    $('#display-input').val(currentPanel.style.display);
    $('#color-input').val(currentPanel.style.color);
    $('#line-style-input').val(currentPanel.style.lineStyle);
    $('#stroke-input').val(currentPanel.style.lineStroke);

    toggleLineOptions(currentPanel.style.display);
    chartType = currentPanel.style.display;
    toggleChartType(currentPanel.style.display);
    updateChartTheme(currentPanel.style.color);
    updateLineCharts(currentPanel.style.lineStyle, currentPanel.style.lineStroke);
}

function loadVisualizationOptions(panelType) {
    $('.chart-options').hide();
    $('.chart-options').removeClass('selected');

    if (panelType === 'logs') {
        $('.chart-options').show();
        $('[data-index="1"]').addClass('selected');
    } else if (panelType === 'metrics') {
        $('[data-index="0"]').show();
        $('[data-index="0"]').addClass('selected');
    }
}

$('.panEdit-discard').on('click', goToDashboard);

$('.panEdit-save').on('click', async function (_redirectedFromViewScreen) {
    if (currentPanel.queryType === 'metrics') {
        const data = getMetricsQData();
        currentPanel.queryData = data;
        currentPanel.style = {
            display: chartType,
            color: selectedTheme,
            lineStroke: selectedStroke,
            lineStyle: selectedLineStyle,
        };
    } else if (currentPanel.queryType === 'logs') {
        const data = getQueryParamsData();
        currentPanel.queryData = data;
    }

    localPanels[panelIndex] = JSON.parse(JSON.stringify(currentPanel));

    // Update originalQueries for the edited panel
    if (currentPanel.queryData && currentPanel.queryData.searchText) {
        //eslint-disable-next-line no-undef
        originalQueries[currentPanel.panelId] = currentPanel.queryData.searchText;
    }

    // Restore original queries for non-edited panels
    localPanels.forEach((panel) => {
        //eslint-disable-next-line no-undef
        if (panel.panelId !== currentPanel.panelId && originalQueries[panel.panelId]) {
            //eslint-disable-next-line no-undef
            panel.queryData.searchText = originalQueries[panel.panelId];
        }
    });

    $('.search-db-input').val('');
    updateTimeRangeForAllPanels(filterStartDate, filterEndDate);
    await updateDashboard();
    $('.panelEditor-container').hide();
    $('.popupOverlay').removeClass('active');
    $('#app-container').show();
    currentPanel = null;
    await displayPanels();
});

$('#panEdit-nameChangeInput').on('change keyup paste', updatePanelName);
$('#panEdit-descrChangeInput').on('change keyup paste', updatePanelDescr);

$('#panEdit-nameChangeInput').on('focus', function () {
    $('#panEdit-nameChangeInput').val(currentPanel.name);
});
$('#panEdit-descrChangeInput').on('focus', function () {
    $('#panEdit-descrChangeInput').val(currentPanel.description);
});

$('.dropDown-unit').on('click', handleUnitDropDownClick);
$('.dropDown-logLinesView').on('click', handleLogLinesViewDropDownClick);

// Nested dropdowns for number unit options
const nestedDropdowns = [
    { id: 'nestedMiscDropDown', optionsId: 'miscOptionsDropDown', optionsClass: 'misc-options' },
    { id: 'nestedDataDropDown', optionsId: 'dataOptionsDropDown', optionsClass: 'data-options' },
    { id: 'nestedThroughputDropDown', optionsId: 'throughputOptionsDropDown', optionsClass: 'throughput-options' },
    { id: 'nestedPercentDropDown', optionsId: 'percentOptionsDropDown', optionsClass: 'percent-options' },
    { id: 'nestedTimeDropDown', optionsId: 'timeOptionsDropDown', optionsClass: 'time-options' },
    { id: 'nestedDataRateDropDown', optionsId: 'dataRateOptionsDropDown', optionsClass: 'data-rate-options' },
];

nestedDropdowns.forEach((dropdown) => {
    $(`#${dropdown.id}`).on('click', function (e) {
        handleNestedDropDownClick(e, this, dropdown.id, dropdown.optionsId, dropdown.optionsClass);
    });
});

function handleUnitDropDownClick(_e) {
    $('.dropDown-unit').toggleClass('active');
    $('.editPanelMenu-inner-options').hide(); // Close inner dropdown when unit menu is clicked
    $('.editPanelMenu-unit').slideToggle();
    $('.dropDown-unit .caret').css('rotate', '180deg');
    $('.dropDown-unit.active .caret').css('rotate', '360deg');
}

function handleLogLinesViewDropDownClick(_e) {
    $('.dropDown-logLinesView').toggleClass('active');
    $('.editPanelMenu-logLinesView').slideToggle();
    $('.dropDown-logLinesView .caret').css('rotate', '180deg');
    $('.dropDown-logLinesView.active .caret').css('rotate', '360deg');
}

function handleNestedDropDownClick(e, element, dropdownId, optionsId, optionsClass) {
    // Unselect previous unit menu item
    let selectedUnitMenuItem = $('.editPanelMenu-unit .editPanelMenu-unit-options.selected');
    selectedUnitMenuItem.removeClass('selected');

    // Reset options if different unit type is selected
    const $element = $(element);
    if (parseInt(selectedUnitMenuItem.attr('data-index')) !== $element.data('index')) {
        resetNestedUnitMenuOptions(selectedUnitTypeIndex);
    }

    // Hide all other dropdown menus
    $('.editPanelMenu-inner-options').each(function () {
        if ($(this).attr('id') !== optionsId) {
            $(this).hide();
        }
    });

    // Toggle current dropdown
    $(`#${dropdownId}`).toggleClass('active');
    $(`#${optionsId}`).slideToggle();

    $(`#${dropdownId} .horizontalCaret`).css('rotate', '90deg');
    $(`#${dropdownId}.active .horizontalCaret`).css('rotate', '270deg');

    if (e) e.stopPropagation();

    // Update current unit selection
    selectedUnitTypeIndex = $element.data('index');
    currentPanel.unit = mapIndexToUnitType.get(selectedUnitTypeIndex);

    // Mark selected item in unit menu
    let unitTypeMenuItems = $('.editPanelMenu-unit .editPanelMenu-unit-options');
    if (unitTypeMenuItems[selectedUnitTypeIndex]) {
        unitTypeMenuItems[selectedUnitTypeIndex].classList.add('selected');
    }
    // Update unit display text
    let unit = mapIndexToUnitType.get(selectedUnitTypeIndex);
    unit = unit.charAt(0).toUpperCase() + unit.slice(1);
    $('.dropDown-unit span').html(unit);
}

// Handle selection of chart type
$('.editPanelMenu-chart #chart-type-options').on('click', function () {
    selectedChartTypeIndex = $(this).data('index');
    currentPanel.chartType = mapIndexToChartType.get(selectedChartTypeIndex);

    // Hide all option containers
    $('#nestedDropDownContainer, .dropDown-unit, .dropDown-logLinesView, #avail-field-container, #logs-vis-format-options').css('display', 'none');

    // Show relevant options based on selected chart type
    switch (selectedChartTypeIndex) {
        case 0:
        case 1:
            $('#logs-vis-format-options').show();
            break;
        case 3:
            currentPanel.logLinesViewType = 'Table view';
            $('#avail-field-container').css('display', 'inline-flex');
            break;
        case 4:
            $('.dropDown-unit, #nestedDropDownContainer').css('display', 'flex');
            break;
        case 5:
            currentPanel.logLinesViewType = 'Single line display view';
            $('.dropDown-logLinesView').css('display', 'flex');
            break;
    }

    refreshChartMenuOptions();
    runQueryBtnHandler();
});

$('.editPanelMenu-unit .editPanelMenu-unit-options').on('click', function () {
    selectedUnitTypeIndex = $(this).data('index');
    currentPanel.unit = mapIndexToUnitType.get(selectedUnitTypeIndex);
    refreshUnitMenuOptions();
});

$('.editPanelMenu-logLinesView .editPanelMenu-options').on('click', function () {
    selectedLogLinesViewTypeIndex = $(this).data('index');
    if (selectedLogLinesViewTypeIndex === 0) {
        currentPanel.logLinesViewType = 'Single line display view';
    } else if (selectedLogLinesViewTypeIndex === 1) {
        currentPanel.logLinesViewType = 'Multi line display view';
    }
    refreshLogLinesViewMenuOptions();
    runQueryBtnHandler();
});

$('.misc-options').on('click', function () {
    selectedDataTypeIndex = $(this).data('index');
    currentPanel.dataType = mapIndexToMiscOptions.get(selectedDataTypeIndex);
    $('#miscOptionsDropDown').slideToggle();
    $('#nestedMiscDropDown').toggleClass('active');
    $('#nestedMiscDropDown .horizontalCaret').css('rotate', '90deg');
    let dataTypeMenuItems = $('.misc-options');
    dataTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    $(this).addClass('selected');
    if (prevSelectedDataTypeIndex != selectedDataTypeIndex) {
        refreshNestedMiscMenuOptions();
    } else {
        $(this).removeClass('selected');
        $('.dropDown-misc-options span').html('Misc');
        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        selectedDataTypeIndex = -1;
    }
});

function refreshNestedMiscMenuOptions() {
    let dataType = mapIndexToMiscOptions.get(selectedDataTypeIndex);
    dataType = dataType.charAt(0).toUpperCase() + dataType.slice(1);
    $('.dropDown-misc-options span').html(dataType);
    prevSelectedDataTypeIndex = selectedDataTypeIndex;
}

$('.data-options').on('click', function () {
    selectedDataTypeIndex = $(this).data('index');
    currentPanel.dataType = mapIndexToDataType.get(selectedDataTypeIndex);

    $('#dataOptionsDropDown').slideToggle();
    $('#nestedDataDropDown').toggleClass('active');
    $('#nestedDataDropDown .horizontalCaret').css('rotate', '90deg');
    let dataTypeMenuItems = $('.data-options');
    dataTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    $(this).addClass('selected');
    if (prevSelectedDataTypeIndex != selectedDataTypeIndex) {
        refreshNestedDataMenuOptions();
    } else {
        $(this).removeClass('selected');
        $('.dropDown-data-options span').html('Data');
        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        selectedDataTypeIndex = -1;
    }
});

function refreshNestedDataMenuOptions() {
    let dataType = mapIndexToDataType.get(selectedDataTypeIndex);
    dataType = dataType.charAt(0).toUpperCase() + dataType.slice(1);
    $('.dropDown-data-options span').html(dataType);
    prevSelectedDataTypeIndex = selectedDataTypeIndex;
}

$('.throughput-options').on('click', function () {
    selectedDataTypeIndex = $(this).data('index');
    currentPanel.dataType = mapIndexToThroughputOptions.get(selectedDataTypeIndex);
    $('#throughputOptionsDropDown').slideToggle();
    $('#nestedThroughputDropDown').toggleClass('active');
    $('#nestedThroughputDropDown .horizontalCaret').css('rotate', '90deg');
    let dataTypeMenuItems = $('.throughput-options');
    dataTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    $(this).addClass('selected');
    if (prevSelectedDataTypeIndex != selectedDataTypeIndex) {
        refreshNestedTptMenuOptions();
    } else {
        $(this).removeClass('selected');
        $('.dropDown-throughput-options span').html('Throughput');
        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        selectedDataTypeIndex = -1;
    }
});

function refreshNestedTptMenuOptions() {
    let dataType = mapIndexToThroughputOptions.get(selectedDataTypeIndex);
    dataType = dataType.charAt(0).toUpperCase() + dataType.slice(1);
    $('.dropDown-throughput-options span').html(dataType);
    prevSelectedDataTypeIndex = selectedDataTypeIndex;
}

$('.percent-options').on('click', function () {
    selectedDataTypeIndex = $(this).data('index');
    // currentPanel.dataType = mapIndexToPercentOption.get(selectedDataTypeIndex);
    $('#percentOptionsDropDown').slideToggle();
    $('#nestedPercentDropDown').toggleClass('active');
    $('#nestedPercentDropDown .horizontalCaret').css('rotate', '90deg');
    let dataTypeMenuItems = $('.percent-options');
    dataTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    $(this).addClass('selected');
    if (prevSelectedDataTypeIndex != selectedDataTypeIndex) {
        refreshNestedPercentMenuOptions();
    } else {
        $(this).removeClass('selected');
        $('.dropDown-percent-options span').html('Percent');
        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        selectedDataTypeIndex = -1;
    }
});

function refreshNestedPercentMenuOptions() {
    // let dataType = mapIndexToPercentOption.get(selectedDataTypeIndex);
    // dataType = dataType.charAt(0).toUpperCase() + dataType.slice(1);
    // $('.dropDown-percent-options span').html(dataType);
    prevSelectedDataTypeIndex = selectedDataTypeIndex;
}

$('.time-options').on('click', function () {
    selectedDataTypeIndex = $(this).data('index');
    currentPanel.dataType = mapIndexToTimeOptions.get(selectedDataTypeIndex);
    $('#timeOptionsDropDown').slideToggle();
    $('#nestedTimeDropDown').toggleClass('active');
    $('#nestedTimeDropDown .horizontalCaret').css('rotate', '90deg');
    let dataTypeMenuItems = $('.time-options');
    dataTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    $(this).addClass('selected');
    if (prevSelectedDataTypeIndex != selectedDataTypeIndex) {
        refreshNestedTimeMenuOptions();
    } else {
        $(this).removeClass('selected');
        $('.dropDown-time-options span').html('Time');
        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        selectedDataTypeIndex = -1;
    }
});

function refreshNestedTimeMenuOptions() {
    let dataType = mapIndexToTimeOptions.get(selectedDataTypeIndex);
    dataType = dataType.charAt(0).toUpperCase() + dataType.slice(1);
    $('.dropDown-time-options span').html(dataType);
    prevSelectedDataTypeIndex = selectedDataTypeIndex;
}

$('.data-rate-options').on('click', function () {
    selectedDataTypeIndex = $(this).data('index');
    currentPanel.dataType = mapIndexToDataRateType.get(selectedDataTypeIndex);
    $('#dataRateOptionsDropDown').slideToggle();
    $('#nestedDataRateDropDown').toggleClass('active');
    $('#nestedDataRateDropDown .horizontalCaret').css('rotate', '90deg');
    let dataTypeMenuItems = $('.data-rate-options');
    dataTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    $(this).addClass('selected');
    if (prevSelectedDataTypeIndex != selectedDataTypeIndex) {
        refreshNestedDataRateMenuOptions();
    } else {
        $(this).removeClass('selected');
        $('.dropDown-data-rate-options span').html('Data Rate');
        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        selectedDataTypeIndex = -1;
    }
});

function refreshNestedDataRateMenuOptions() {
    let dataType = mapIndexToDataRateType.get(selectedDataTypeIndex);
    dataType = dataType.charAt(0).toUpperCase() + dataType.slice(1);
    $('.dropDown-data-rate-options span').html(dataType);
    prevSelectedDataTypeIndex = selectedDataTypeIndex;
}

// common function to reset all unit dropdowns
function resetNestedUnitMenuOptions(selectedUnitTypeIndex) {
    if (selectedUnitTypeIndex !== -1 && selectedUnitTypeIndex !== undefined) {
        $('.editPanelMenu-unit .editPanelMenu-unit-options').each(function (index, item) {
            item.classList.remove('active');
        });
        $('.horizontalCaret').css('rotate', '90deg');
        let prevDataTypeSelectedMenuID;
        if (selectedUnitTypeIndex === 0) {
            prevDataTypeSelectedMenuID = 'miscOptionsDropDown';
            $('.dropDown-misc-options span').html('Misc');
        } else if (selectedUnitTypeIndex === 1) {
            prevDataTypeSelectedMenuID = 'dataOptionsDropDown';
            $('.dropDown-data-options span').html('Data');
        } else if (selectedUnitTypeIndex === 2) {
            prevDataTypeSelectedMenuID = 'throughputOptionsDropDown';
            $('.dropDown-throughput-options span').html('Throughput');
        } else if (selectedUnitTypeIndex === 3) {
            prevDataTypeSelectedMenuID = 'timeOptionsDropDown';
            $('.dropDown-time-options span').html('Time');
        } else if (selectedUnitTypeIndex === 4) {
            prevDataTypeSelectedMenuID = 'dataRateOptionsDropDown';
            $('.dropDown-data-rate-options span').html('Data Rate');
        }

        let allInnerOptions = $(`#${prevDataTypeSelectedMenuID}`).find('.inner-options');
        allInnerOptions.each(function (index, item) {
            item.classList.remove('selected');
        });

        prevSelectedDataTypeIndex = -2;
        currentPanel.dataType = '';
        currentPanel.unit = '';
        selectedDataTypeIndex = -1;
    }
}

function updatePanelName() {
    let nameEl = $('#panEdit-nameChangeInput');
    currentPanel.name = nameEl.val();
    $('.panEdit-navBar .panelTitle').html(nameEl.val());
}

function updatePanelDescr() {
    let descrEl = $('#panEdit-descrChangeInput');
    currentPanel.description = descrEl.val();
}

function refreshChartMenuOptions() {
    let chartTypeMenuItems = $('.editPanelMenu-chart #chart-type-options');
    chartTypeMenuItems.each(function (_index, item) {
        item.classList.remove('selected');
    });
    chartTypeMenuItems[selectedChartTypeIndex].classList.add('selected');
}

function refreshUnitMenuOptions() {
    let unitTypeMenuItems = $('.editPanelMenu-unit .editPanelMenu-unit-options');
    unitTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });

    unitTypeMenuItems[selectedUnitTypeIndex].classList.add('selected');
    let unit = mapIndexToUnitType.get(selectedUnitTypeIndex);
    unit = unit.charAt(0).toUpperCase() + unit.slice(1);
    $('.dropDown-unit span').html(unit);
}

function refreshLogLinesViewMenuOptions() {
    let viewTypeMenuItems = $('.editPanelMenu-logLinesView .editPanelMenu-options');
    viewTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });
    viewTypeMenuItems[selectedLogLinesViewTypeIndex].classList.add('selected');
    let logLineView = mapIndexToLogLinesViewType.get(selectedLogLinesViewTypeIndex);
    logLineView = logLineView.charAt(0).toUpperCase() + logLineView.slice(1);
    $('.dropDown-logLinesView span').html(logLineView);
}

function goToDashboard() {
    // Don't add panel if cancel is clicked.
    let serverPanel = JSON.parse(JSON.stringify(localPanels[panelIndex]));
    if (!flagDBSaved) {
        if (serverPanel.panelIndex !== undefined) {
            if (serverPanel.queryRes === undefined) {
                localPanels = localPanels.filter((panel) => panel.panelIndex !== panelIndex);
            }
        }
    }
    resetNestedUnitMenuOptions(selectedUnitTypeIndex);
    currentPanel = null;
    resetEditPanelScreen();

    $('.panelEditor-container').hide();
    $('.popupOverlay').removeClass('active');
    $('#app-container').show();
    $('#viewPanel-container').hide();
    $('#overview-button').removeClass('active');
    setTimePickerValue();
    displayPanels();
    if (dbRefresh) {
        startRefreshInterval(dbRefresh);
    }
}

function resetEditPanelScreen() {
    resetEditPanel();
    panelGridDiv = null;
    $('.dropDown-unit span').html('Unit');
    $('.dropDown-logLinesView span').html('Single line display view');
    $('.index-container').css('display', 'none');
    $('#query-language-btn').css('display', 'none');
    $('#metrics-query-language').css('display', 'none');
    $('.query-language-option').removeClass('active');
    $('#query-language-btn span').html('Splunk QL');
    $('#query-language-options #option-3').addClass('active');
}

function resetEditPanel() {
    $('.panelDisplay .panEdit-panel').remove();
    const panEditEl = `<div id="panEdit-panel" class="panEdit-panel"></div>`;
    $('.panelDisplay').append(panEditEl);
}
function resetOptions() {
    selectedChartTypeIndex = -1;
    selectedLogLinesViewTypeIndex = -1;

    let chartTypeMenuItems = $('.editPanelMenu-chart #chart-type-options');
    chartTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });

    let unitTypeMenuItems = $('.editPanelMenu-unit .editPanelMenu-unit-options');
    unitTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });

    let viewTypeMenuItems = $('.editPanelMenu-logLinesView .editPanelMenu-options');
    viewTypeMenuItems.each(function (index, item) {
        item.classList.remove('selected');
    });

    $('.range-item').each(function () {
        if ($(this).hasClass('active')) {
            $(this).removeClass('active');
            return;
        }
    });
}

function displayQueryToolTip() {
    $('#info-icon-logQL, #info-icon-spl').hide();
    let queryLanguage = $('.panelEditor-container .queryInput-container #query-language-btn span').html();
    if (queryLanguage === 'Log QL') {
        $('#info-icon-logQL').show();
    } else if (queryLanguage === 'Splunk QL') {
        $('#info-icon-spl').show();
    }
}

async function runQueryBtnHandler() {
    // reset the current panel's queryRes attribute
    delete currentPanel.queryRes;
    resetEditPanel();
    panelGridDiv = null;
    panelLogsRowData = [];
    $('.panelDisplay .ag-root-wrapper').remove();
    $('.panelDisplay #empty-response').empty();
    $('.panelDisplay #empty-response').hide();
    $('.panelDisplay #corner-popup').hide();
    $('.panelDisplay #panelLogResultsGrid').hide();
    $('.panelDisplay .big-number-display-container').hide();
    $('#metrics-queries, #metrics-formula').empty();

    // runs the query according to the query type selected and irrespective of chart type
    if (currentPanel.queryType == 'metrics') {
        data = currentPanel.queryData;
        runMetricsQuery(data, -1, currentPanel);
    } else if (currentPanel.queryType == 'logs') {
        //eslint-disable-next-line no-undef
        resetPanelLogsColumnDefs();

        data = getQueryParamsData();
        currentPanel.queryData = data;

        $('.panelDisplay .panEdit-panel').hide();
        //eslint-disable-next-line no-undef
        initialSearchDashboardData = data;
        await runPanelLogsQuery(data, -1, currentPanel);
    }
}
$(document).on('click', function (event) {
    if (!$(event.target).closest('.dropDown-logLinesView').length) {
        $('.editPanelMenu-logLinesView').slideUp();
        $('.dropDown-logLinesView').removeClass('active');
    }

    if (!$(event.target).closest('.dropDown-unit').length) {
        $('.editPanelMenu-unit').slideUp();
        $('.dropDown-unit').removeClass('active');
    }

    if (!$(event.target).closest('.editPanelMenu-inner-options').length) {
        $('.editPanelMenu-inner-options').slideUp();
        $('.dropDown-unit').removeClass('active');
    }
});

function displayPanelView(panelIndex) {
    if (isDefaultDashboard) {
        $('.button-section, #edit-button').hide();
    }

    const panelLayout = `<div class="panel-body"><div class="panEdit-panel"></div></div>`;
    let localPanel = currentPanel ? currentPanel : JSON.parse(JSON.stringify(localPanels[panelIndex]));

    const panelId = localPanel.panelId;
    $(`#panel-container #panel${panelId}`).remove();
    $('#viewPanel-container').empty();

    const panel = $('<div>').append(panelLayout).addClass('panel').attr('id', `panel${panelId}`).attr('panel-index', localPanel.panelIndex);
    $('#viewPanel-container').append(`<div class="view-panel-name">${localPanel.name}</div>`);
    $('#viewPanel-container').append(panel);

    const panEl = $(`#panel${panelId} .panel-body`);
    let responseDiv;

    switch (localPanel.chartType) {
        case 'Data Table':
        case 'loglines':
            responseDiv = `<div id="panelLogResultsGrid" class="panelLogResultsGrid ag-theme-mycustomtheme"></div><div id="empty-response"></div>`;
            panEl.append(responseDiv);
            $('#panelLogResultsGrid').show();
            //eslint-disable-next-line no-undef
            initialSearchDashboardData = localPanel.queryData;
            runPanelLogsQuery(localPanel.queryData, panelId, localPanel);
            break;

        case 'Line Chart':
            responseDiv = `<div id="empty-response"></div><div id="corner-popup"></div>`;
            panEl.append(responseDiv);

            if (localPanel.queryType === 'metrics') {
                runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel);
            } else {
                runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
            }
            break;

        case 'Pie Chart':
        case 'Bar Chart':
        case 'number':
            if (localPanel.chartType) responseDiv = `<div id="empty-response"></div><div id="corner-popup"></div>`;
            panEl.append(responseDiv);
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
            break;
    }
}
