/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
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
    } else if($(`#viewPanel-container`).css('display').toLowerCase() !== 'none') {
            // if user is on view panel screen
            // get panel-index by attribute
            let panelIndex = ($(`#viewPanel-container .panel`).attr('panel-index'));
            // if panel has some stored query data, reset it
            if(localPanels[panelIndex].queryData) {
                delete localPanels[panelIndex].queryRes
                if(localPanels[panelIndex].chartType === "Line Chart" || localPanels[panelIndex].queryType === "metrics") {
                    localPanels[panelIndex].queryData.start = filterStartDate.toString();
                    localPanels[panelIndex].queryData.end = filterEndDate.toString();
                } else {
                        localPanels[panelIndex].queryData.startEpoch = filterStartDate
                        localPanels[panelIndex].queryData.endEpoch = filterEndDate
                        }
                }
            displayPanelView(panelIndex);
    } else if(!currentPanel) {     
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
    
function dashboardRangeItemHandler(evt){
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
        }
    } else if($(`#viewPanel-container`).css('display').toLowerCase() !== 'none') {
            // if user is on view panel screen
            // get panel-index by attribute
            let panelIndex = ($(`#viewPanel-container .panel`).attr('panel-index'));
            // if panel has some stored query data, reset it
            if(localPanels[panelIndex].queryData) {
                delete localPanels[panelIndex].queryRes
                if(localPanels[panelIndex].chartType === "Line Chart" || localPanels[panelIndex].queryType === "metrics") {
                    localPanels[panelIndex].queryData.start = filterStartDate.toString();
                    localPanels[panelIndex].queryData.end = filterEndDate.toString();
                } else {
                        localPanels[panelIndex].queryData.startEpoch = filterStartDate
                        localPanels[panelIndex].queryData.endEpoch = filterEndDate
                        }
                }
            displayPanelView(panelIndex)
    } else if(!currentPanel) {       
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
function hideColumnHandler(evt) {
    evt.preventDefault();
    evt.stopPropagation();

    availableFieldsSelectHandler(evt);
}

function setQueryLangHandler(e) {
    $('.query-language-option').removeClass('active');
    let currentTab = $("#custom-code-tab").tabs("option", "active");
    let selectedQueryLanguageId = $(this).attr("id").split("-")[1];
    if (selectedQueryLanguageId !== "3" && currentTab === 0) {
        $("#custom-code-tab").tabs("option", "active", 1);
    } else if (selectedQueryLanguageId !== "3" && currentTab === 1) {
        $("#custom-code-tab").tabs("option", "disabled", [0]);
    } else if (selectedQueryLanguageId === "3" && currentTab === 1) {
        $("#custom-code-tab").tabs("option", "disabled", []);
    }
    $('#query-language-btn span').html($(this).html());
    displayQueryLangToolTip(selectedQueryLanguageId);
    $(this).addClass('active');
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

    // If the user chooses any index from dropdown, un-highlight the "*" from the dropdown
    if ($(this).data("index") !== "*"){
        $(`.index-dropdown-item[data-index="*"]`).removeClass('active');
    }

    $(evt.currentTarget).toggleClass('active');
    let checkedIndices = [];

    $.each($(".index-dropdown-item.active"), function () {
        checkedIndices.push($(this).data("index"));
    });
    selectedSearchIndex = checkedIndices.join(",");
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
      availColNames = [];
      doSearch(data);
    } else {
      wsState = "cancel";
      data = getSearchFilter(false, false);
      doCancel(data);
    }
    $('#daterangepicker').hide();
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
      availColNames = [];
      doSearch(data);
    }
}


// prevent the available fields popup from closing when you toggle an available field
function availableFieldsClickHandler(evt) {
    evt.stopPropagation();
}

function availableFieldsSelectHandler(evt) {

    let colName = evt.currentTarget.dataset.index;
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
            if(index>1){
                tempFieldList.push(colName);
            }
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

    $('body').attr('data-theme', theme);


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