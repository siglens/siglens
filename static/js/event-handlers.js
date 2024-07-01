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

'use strict';

function setupEventHandlers() {
    $('#filter-input').on('keyup', filterInputHandler);

    $('#run-filter-btn').on('click', runFilterBtnHandler);
    $("#query-builder-btn").on("click", runFilterBtnHandler);
    $("#live-tail-btn").on("click", runLiveTailBtnHandler);

    $('#available-fields').on('click', availableFieldsClickHandler);
    $('#available-fields .select-unselect-header').on('click','.select-unselect-checkbox', toggleAllAvailableFieldsHandler);
    $('#available-fields .select-unselect-header').on('click','.select-unselect-checkmark', toggleAllAvailableFieldsHandler);
    $('#available-fields .fields').on('click', '.available-fields-dropdown-item', availableFieldsSelectHandler);

    $('#corner-popup').on('click', '.corner-btn-close', hideError);

    $('#search-query-btn').on('click', searchSavedQueryHandler);

    $('#query-language-btn').on('show.bs.dropdown', qLangOnShowHandler);
    $('#query-language-btn').on('hide.bs.dropdown', qLangOnHideHandler);
    $('#query-language-options .query-language-option').on('click', setQueryLangHandler);
    $('#query-mode-options .query-mode-option').on('click', setQueryModeHandler);

    $('#index-btn').on('show.bs.dropdown', indexOnShowHandler);
    $('#index-btn').on('hide.bs.dropdown', indexOnHideHandler);
    $('#available-indexes').on('click', '.index-dropdown-item', indexOnSelectHandler);

    $('#logs-result-container').on('click', '.hide-column', hideColumnHandler);

    $('#log-opt-single-btn').on('click', logOptionSingleHandler);
    $('#log-opt-multi-btn').on('click', logOptionMultiHandler);
    $('#log-opt-table-btn').on('click', logOptionTableHandler);

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
    $('#customrange-btn').on('click', customRangeHandler);

    $('.range-item').on('click', rangeItemHandler)
    $('.db-range-item').on('click', dashboardRangeItemHandler)

    $('.ui-widget input').on('keyup', saveqInputHandler);

    $(window).bind('popstate', windowPopStateHandler);
}


function windowPopStateHandler(evt) {
    if(location.href.includes("index.html")){
        let state = evt.originalEvent.state;
        if (state !== null) {
            data = getInitialSearchFilter(true, false);
        }
        resetDashboard();
        wsState = 'query';
        doSearch(data);
    }
}

function showDatePickerHandler(evt){
    evt.stopPropagation();
    $('#daterangepicker').toggle();
    $(evt.currentTarget).toggleClass('active');
}

function hideDatePickerHandler(){
    $('#date-picker-btn').removeClass('active');
}

function resetDatePickerHandler(evt){
    evt.stopPropagation();
    resetCustomDateRange();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });

}
function getStartDateHandler(evt){
    let inputDate = new Date(this.value);
    filterStartDate = inputDate.getTime();
    $(this).addClass("active");
    Cookies.set('customStartDate', this.value);
}

function getEndDateHandler(evt){
    let inputDate = new Date(this.value);
    filterEndDate = inputDate.getTime();
    $(this).addClass("active");
    Cookies.set('customEndDate', this.value);
}

function getStartTimeHandler(){
    let selectedTime = $(this).val();
    let temp = ((Number(selectedTime.split(':')[0]) * 60 + Number(selectedTime.split(':')[1])) * 60) * 1000;
    //check if filterStartDate is a number or now-*
    if (!isNaN(filterStartDate)) {
        filterStartDate = filterStartDate + temp;
    } else {
        let start = new Date();
        start.setUTCHours(0,0,0,0);
        filterStartDate = start.getTime() + temp;
    }
    $(this).addClass("active");
    Cookies.set('customStartTime', selectedTime);
}

function getEndTimeHandler(){
    let selectedTime = $(this).val();
    let temp = ((Number(selectedTime.split(':')[0]) * 60 + Number(selectedTime.split(':')[1])) * 60) * 1000;
    if (!isNaN(filterEndDate)) {
        filterEndDate =  filterEndDate + temp;
    } else {
        let start = new Date();
        start.setUTCHours(0,0,0,0);
        filterEndDate = start.getTime() + temp;
    }
    $(this).addClass("active");
    Cookies.set('customEndTime', selectedTime);
}

function customRangeHandler(evt){
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $.each($(".db-range-item.active"), function () {
        $(this).removeClass('active');
    });
    datePickerHandler(filterStartDate, filterEndDate, "custom")

    if(currentPanel) {
        if(currentPanel.queryData) {
            if(currentPanel.chartType === "Line Chart" || currentPanel.queryType === "metrics") {
                currentPanel.queryData.start = filterStartDate.toString();
                currentPanel.queryData.end = filterEndDate.toString();
            } else {
                currentPanel.queryData.startEpoch = filterStartDate
                currentPanel.queryData.endEpoch = filterEndDate
            }
        }
    }else if(!currentPanel) {     
            // if user is on dashboard screen
            localPanels.forEach(panel => {
                delete panel.queryRes
                if(panel.queryData) {
                    if(panel.chartType === "Line Chart" || panel.queryType === "metrics") {
                        panel.queryData.start = filterStartDate.toString();
                        panel.queryData.end = filterEndDate.toString();
                    } else {
                        panel.queryData.startEpoch = filterStartDate
                        panel.queryData.endEpoch = filterEndDate
                    }
                }
            })
        displayPanels();
    }
}

function rangeItemHandler(evt){
    resetCustomDateRange();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), "now", $(this).attr('id'))
}
    
async function dashboardRangeItemHandler(evt){
    resetCustomDateRange();
    $.each($(".db-range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), "now", $(this).attr('id'))
   
    // if user is on edit panel screen
    if(currentPanel) {
        if(currentPanel.queryData) {
            if(currentPanel.chartType === "Line Chart" || currentPanel.queryType === "metrics") {
                currentPanel.queryData.start = filterStartDate.toString();
                currentPanel.queryData.end = filterEndDate.toString();
            } else {
                currentPanel.queryData.startEpoch = filterStartDate
                currentPanel.queryData.endEpoch = filterEndDate
            }
            runQueryBtnHandler();
        }
    }else if(!currentPanel) {       
            // if user is on dashboard screen
            localPanels.forEach(panel => {
                delete panel.queryRes
                if(panel.queryData) {
                    if(panel.chartType === "Line Chart" || panel.queryType === "metrics") {
                        panel.queryData.start = filterStartDate.toString();
                        panel.queryData.end = filterEndDate.toString();
                    } else {
                        panel.queryData.startEpoch = filterStartDate
                        panel.queryData.endEpoch = filterEndDate
                    }
                }
            })
            
        displayPanels();
        // Don't update if its a default dashboard
        if (!isDefaultDashboard) {
            await updateDashboard();
        }
    }
}
function resetCustomDateRange(){
        // clear custom selections
        $('#date-start').val("");
        $('#date-end').val("");
        $('#time-start').val("00:00");
        $('#time-end').val("00:00");
        $('#date-start').removeClass('active');
        $('#date-end').removeClass('active');
        $('#time-start').removeClass('active');
        $('#time-end').removeClass('active');
        Cookies.remove('customStartDate');
        Cookies.remove('customEndDate');
        Cookies.remove('customStartTime');
        Cookies.remove('customEndTime');
}
function hideColumnHandler(evt, isCloseIcon = false) {
    evt.preventDefault();
    evt.stopPropagation();

    availableFieldsSelectHandler(evt, isCloseIcon);
}

function setQueryLangHandler(e) {
    let previousQueryLanguageId = $('#query-language-options .query-language-option.active').attr('id').split('-')[1];
    $('.query-language-option').removeClass('active');
    $("#setting-container").hide();
    let currentTab = $("#custom-code-tab").tabs("option", "active");
    let selectedQueryLanguageId = $(this).attr("id").split("-")[1];
    if (!(previousQueryLanguageId === selectedQueryLanguageId)) {
        $('#filter-input').val("");
    }
    $('#query-language-btn span').html($(this).html());
    handleTabAndTooltip(selectedQueryLanguageId, currentTab);
    $(this).addClass('active');
}

function setQueryModeHandler(e) {
    $('.query-mode-option').removeClass('active');
    $("#setting-container").hide();
    Cookies.set('queryMode',$(this).html());
    $('#query-mode-btn span').html($(this).html());
    $(this).addClass('active');
}

function handleTabAndTooltip(selectedQueryLanguageId, currentTab) {
    if (selectedQueryLanguageId !== "3" && currentTab === 0) {
        $("#custom-code-tab").tabs("option", "active", 1);
        showDisabledTabTooltip();
        $('#ui-id-1').addClass('disabled-tab');
    } else if (selectedQueryLanguageId !== "3" && currentTab === 1) {
        showDisabledTabTooltip();
        $('#ui-id-1').addClass('disabled-tab');
    } else if (selectedQueryLanguageId === "3" && currentTab === 1) {
        $("#custom-code-tab").tabs("option", "disabled", []);
        hideDisabledTabTooltip();
        $('#ui-id-1').removeClass('disabled-tab');
    }
    displayQueryLangToolTip(selectedQueryLanguageId);
}

function showDisabledTabTooltip() {
    hideDisabledTabTooltip();
    tippy('#tab-title1', {
        content: 'Builder is only available for Splunk QL. Please change the settings to select Splunk QL in order to use the builder.',
        placement: 'top',
        arrow: true,
        trigger: 'mouseenter focus',
    });
}

function hideDisabledTabTooltip() {
    document.querySelectorAll('#tab-title1').forEach(function(el) {
        if (el._tippy) {
            el._tippy.destroy();
        }
    });
}

function qLangOnShowHandler() {
    $("#query-language-btn").addClass("active");
    
}

function qLangOnHideHandler() {
    $('#query-language-btn').removeClass('active');
}

function indexOnShowHandler(){
    $('#index-btn').addClass('active');
    if (Cookies.get('IndexList')) {
        let allAvailableIndices = [];
        let selectedIndexList = (Cookies.get('IndexList')).split(',');
        // Get all available indices

        $.each($(".index-dropdown-item"), function () {
            allAvailableIndices.push($(this).data("index"));
        });
        let allSelectedIndices = _.intersection(selectedIndexList, allAvailableIndices);
        $.each($(".index-dropdown-item"), function () {
            let indexName = $(this).data("index");
            if(allSelectedIndices.includes(indexName)){
                $(this).addClass('active');
            }
        });
        selectedSearchIndex = allSelectedIndices.join(",");
        Cookies.set('IndexList', allSelectedIndices.join(","));
    }
}

function indexOnHideHandler(){
    $('#index-btn').removeClass('active');
}

function indexOnSelectHandler(evt) {
    evt.stopPropagation();

    var target = $(evt.currentTarget);
    var isChecked = target.hasClass('active');

    if ($(".index-dropdown-item.active").length === 1 && isChecked) {
        // If only one index is selected and it's being clicked again, prevent deselection
        return;
    }

    target.toggleClass('active');

    let checkedIndices = [];
    $(".index-dropdown-item.active").each(function () {
        checkedIndices.push($(this).data("index"));
    });
    selectedSearchIndex = checkedIndices.join(",");
    setIndexDisplayValue(selectedSearchIndex);
    Cookies.set('IndexList', selectedSearchIndex)
}

function runLiveTailBtnHandler(evt) {
  $(".popover").hide();
  evt.preventDefault();
  liveTailState = true;
  if ($("#live-tail-btn").text() === "Live Tail") {
    resetDashboard();
    $("#live-tail-btn").html("Cancel Live Tail");
    logsRowData = [];
    total_liveTail_searched = 0;
    wsState = "query";
    data = getLiveTailFilter(false, false, 1800);
    availColNames = [];
    createLiveTailSocket(data);
  } else {
    $("#live-tail-btn").html("Live Tail");
    liveTailState = false;
    wsState = "cancel";
    data = getLiveTailFilter(false, false, 1800);
    doLiveTailCancel(data);
  }
  $("#daterangepicker").hide();
}

function runFilterBtnHandler(evt) {
    var currentPage = window.location.pathname;
    if (currentPage === "/index.html"){
        $('.popover').hide();
        evt.preventDefault();
        if (
          $("#run-filter-btn").text() === " " ||
          $("#query-builder-btn").text() === " "
        ) {
    
          resetDashboard();
          logsRowData = [];
          wsState = "query";
          data = getSearchFilter(false, false);
          initialSearchData = data;
          availColNames = [];
          doSearch(data);
        } else {
          wsState = "cancel";
          data = getSearchFilter(false, false);
          initialSearchData = data;
          doCancel(data);
        }
        $('#daterangepicker').hide();
    }else if (currentPage === '/alert.html'){
        let data = getQueryParamsData();
        data.searchText = getQueryBuilderCode();
        isQueryBuilderSearch = $("#custom-code-tab").tabs("option", "active") === 0;
        if(isQueryBuilderSearch) {
            data.searchText = getQueryBuilderCode();
        }else{
            data.searchText = $('#filter-input').val();
        }
        fetchLogsPanelData(data,-1).then((res)=>{
            alertChart(res);
        });
    }
}

function filterInputHandler(evt) {
    evt.preventDefault();
    if (
      evt.keyCode === 13 &&
      ($("#run-filter-btn").text() === " " ||
        $("#query-builder-btn").text() === " ")
    ) {
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
    let colName

    if(isCloseIcon){
        const outerDiv = evt.currentTarget.closest('.ag-header-cell');
        const colId = outerDiv.getAttribute('col-id');
        colName = colId
    }
    else{
        colName = evt.currentTarget.dataset.index
    }

    let encColName = string2Hex(colName);
    // don't toggle the timestamp column
    if (colName !== "timestamp") {
        // toggle the column visibility
        $(`.toggle-${encColName}`).toggleClass('active');
        if ($(`.toggle-${encColName}`).hasClass('active')) {
            // Update the selectedFieldsList everytime a field is selected
            selectedFieldsList.push(colName);
        } else{
            // Everytime the field is unselected, remove it from selectedFieldsList
            for( let i = 0; i < selectedFieldsList.length; i++){

                if ( selectedFieldsList[i] === colName) {
                    selectedFieldsList.splice(i, 1);
                    i--;
                }
            }
        }
    }

    let visibleColumns = 0;
    let totalColumns = -1;

    availColNames.forEach((colName, index) => {
        if(selectedFieldsList.includes(colName)) {

            visibleColumns++;
            totalColumns++;
        }else{

        }
    })

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
        let cmClass =  el.find('.select-unselect-checkmark');
        cmClass.remove();
    }
    // We do not count time and log column
    if (visibleColumns == totalColumns-2) {

        if (theme === "light"){
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        }else{
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }

    }

    if ( $('#log-opt-single-btn').hasClass('active')){
        hideOrShowFieldsInLineViews();
    } else if ($('#log-opt-multi-btn').hasClass('active')){
        hideOrShowFieldsInLineViews();
    } else if ($('#log-opt-table-btn').hasClass('active')){
        hideOrShowFieldsInLineViews();
        updateColumns();
    }
    gridOptions.api.sizeColumnsToFit();
    updatedSelFieldList = true
}

function toggleAllAvailableFieldsHandler(evt) {
    processTableViewOption();
    let el = $('#available-fields .select-unselect-header');
    let isChecked = el.find('.select-unselect-checkmark');
    if (isChecked.length === 0) {
        if (theme === "light"){
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        }else{
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }
        let tempFieldList = [];
        availColNames.forEach((colName, index) => {
            $(`.toggle-${string2Hex(colName)}`).addClass('active');
                tempFieldList.push(colName);
            gridOptions.columnApi.setColumnVisible(colName, true);
        });
        selectedFieldsList = tempFieldList;
    } else{
        let cmClass =  el.find('.select-unselect-checkmark');
        cmClass.remove();

        availColNames.forEach((colName, index) => {
           
            $(`.toggle-${string2Hex(colName)}`).removeClass('active');
            gridOptions.columnApi.setColumnVisible(colName, false);
        });
        selectedFieldsList = []
    }
    updatedSelFieldList = true;
    // Always hide the logs column
    gridOptions.columnApi.setColumnVisible("logs", false);
}


function hideOrShowFieldsInLineViews(){
    let allSelected = true;
    availColNames.forEach((colName, index) => {
        let encColName = string2Hex(colName);
        if($(`.toggle-${encColName}`).hasClass('active')){
            $(`.cname-hide-${encColName}`).show();
        } else {
            $(`.cname-hide-${encColName}`).hide();
            allSelected = false;
        }
    })
    let el = $('#available-fields .select-unselect-header');
    let isChecked = el.find('.select-unselect-checkmark');
    if (allSelected) {
        if (isChecked.length === 0){
            if (theme === "light") {
                el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
            } else {
                el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
            }
        }
    }
    else {
        isChecked.remove();
    }
}

function logOptionSingleHandler() {
    $('#logs-result-container').removeClass('multi');
    $('#views-container .btn-group .btn').removeClass('active');
    $('#log-opt-single-btn').addClass('active');
    logsColumnDefs.forEach(function (colDef, index) {
        if (colDef.field === "logs"){
            colDef.cellStyle = null;
            colDef.autoHeight = null;
            colDef.cellRenderer = function(params) {
                const data = params.data || {};
                let logString = '';
                let addSeparator = false;
                Object.entries(data)
                    .filter(([key]) => key !== 'timestamp')
                    .forEach(([key, value]) => {
                        let colSep = addSeparator ? '<span class="col-sep"> | </span>' : '';
                        logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}${key}=${value}</span>`;
                        addSeparator = true;
                    });
            
                return `<div style="white-space: nowrap;">${logString}</div>`;
            }; 
        }
    });
    gridOptions.api.setColumnDefs(logsColumnDefs);
    gridOptions.api.resetRowHeights()

    availColNames.forEach((colName, index) => {
        gridOptions.columnApi.setColumnVisible(colName, false);
    });
    gridOptions.columnApi.setColumnVisible("logs", true);
    
    gridOptions.columnApi.autoSizeColumn(gridOptions.columnApi.getColumn("logs"), false);
    hideOrShowFieldsInLineViews();
    Cookies.set('log-view', 'single-line',  {expires: 365});
}

function logOptionMultiHandler() {
    $('#logs-result-container').addClass('multi');
    $('#views-container .btn-group .btn').removeClass('active');
    $('#log-opt-multi-btn').addClass('active');

    logsColumnDefs.forEach(function (colDef, index) {
        if (colDef.field === "logs"){
            
            colDef.cellStyle = {'white-space': 'normal'};
            colDef.autoHeight = true;
            colDef.cellRenderer = function(params) {
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
    
    availColNames.forEach((colName, index) => {
        gridOptions.columnApi.setColumnVisible(colName, false);
    });
    gridOptions.columnApi.setColumnVisible("logs", true);
    
    gridOptions.columnApi.autoSizeColumn(gridOptions.columnApi.getColumn("logs"), false);
    gridOptions.api.setRowData(logsRowData);
    hideOrShowFieldsInLineViews();
    gridOptions.api.sizeColumnsToFit();
    Cookies.set('log-view', 'multi-line',  {expires: 365});
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
    logsColumnDefs.forEach(function (colDef, index) {
        if (colDef.field === "logs"){
            colDef.cellStyle = null;
            colDef.autoHeight = null;
        }
    });
    gridOptions.api.setColumnDefs(logsColumnDefs);
    gridOptions.api.resetRowHeights()
    gridOptions.api.sizeColumnsToFit();
    Cookies.set('log-view', 'table',  {expires: 365});
}

function themePickerHandler(evt) {

    if (Cookies.get('theme')){
        theme = Cookies.get('theme');
    } else {
        Cookies.set('theme', 'light');
        theme = 'light';
    }

    if(theme === 'light'){
        theme = 'dark';
        $(evt.currentTarget).removeClass('dark-theme');
        $(evt.currentTarget).addClass('light-theme');
    } else {
        theme = 'light';
        $(evt.currentTarget).removeClass('light-theme');
        $(evt.currentTarget).addClass('dark-theme');
    }

    $('html').attr('data-theme', theme);


    Cookies.set('theme', theme,  {expires: 365});
}

function saveqInputHandler(evt) {
    evt.preventDefault();
    $(this).addClass("active");
}

function searchSavedQueryHandler(evt){
    evt.preventDefault();
    $('#empty-qsearch-response').hide()
    let searchText = $('#sq-filter-input').val();
    if (searchText === ""){
        
        return;
        
    }else{
        getSearchedQuery()
    }
}