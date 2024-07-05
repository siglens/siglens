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

let originalIndexValues = [];
let indexValues = [];

$(document).ready(async () => {

    function createTooltip(selector, content) {
        tippy(selector, {
          content: content,      // Tooltip content
          placement: 'top',      // Tooltip placement (top, bottom, left, right)
          arrow: true,           // Show an arrow
          animation: 'fade'      // Tooltip animation
        });
      }
      
      // Call the function for each tooltip
      createTooltip('#add-index', 'Add New Index');
      createTooltip('#date-picker-btn', 'Pick the Time Window');
      createTooltip('#query-builder-btn', 'Run Query');
      createTooltip('#logs-settings', 'Settings');
      createTooltip('#saveq-btn', 'Save query');
      createTooltip('#add-logs-to-db-btn', 'Add to dashboards');
      createTooltip('#alert-from-logs-btn', 'Create alert');
      createTooltip('.download-all-logs-btn', 'Download logs');
      createTooltip('#show-record-intro-btn', 'Query Response Information');
      createTooltip('#log-opt-single-btn', 'Single Line Display View');
      createTooltip('#log-opt-multi-btn', 'Wrap Line Display View');
      createTooltip('#log-opt-table-btn', 'Tabular View');
      createTooltip('.avail-fields-btn', 'Select field names to display');
      createTooltip('#run-filter-btn', 'Run query');
    setSaveQueriesDialog();
    let indexes = await getListIndices();
    if (indexes){
        originalIndexValues = indexes.map(item => item.index);
        indexValues = [...originalIndexValues];
    }
    initializeIndexAutocomplete();
    let queryMode = Cookies.get('queryMode');
    if (queryMode !== undefined){
        const searchParams = new URLSearchParams(window.location.search);

        // Check if the URL has the 'filterTab' parameter
        const hasFilterTab = searchParams.has('filterTab');

        if (!hasFilterTab){
           //If filter tab is not present then do trigger.
            if(queryMode === "Builder"){
                $('.custom-code-tab a:first').trigger('click');
            }else{
                $('.custom-code-tab a[href="#tabs-2"]').trigger('click');
            }
        }
        // Add active class to dropdown options based on the queryMode selected.
        if(queryMode === "Builder"){
            $(".query-mode-option").removeClass("active");
            $("#query-mode-options #mode-option-1").addClass("active");
            $('#query-mode-btn span').html("Builder");
        }else{
            $(".query-mode-option").removeClass("active");
            $("#query-mode-options #mode-option-2").addClass("active");
            $('#query-mode-btn span').html("Code");
        }
    }
    // If query string found , then do search
    if (window.location.search) {
        data = getInitialSearchFilter(false, false);
        initialSearchData = data;
        doSearch(data);
    } else {
        setIndexDisplayValue(selectedSearchIndex);
        let stDate = Cookies.get('startEpoch') || "now-15m";
        let endDate = Cookies.get('endEpoch') || "now";
        if (!isNaN(stDate)) {
            stDate = Number(stDate);
            endDate = Number(endDate);
            datePickerHandler(stDate, endDate, "custom");
            loadCustomDateTimeFromEpoch(stDate,endDate);
        } else if (stDate !== "now-15m") {
            datePickerHandler(stDate, endDate, stDate);
        } else {
            datePickerHandler(stDate, endDate, "");
        }
        $('#run-filter-btn').html(' ');
        $("#query-builder-btn").html(" ");
        $("#custom-chart-tab").hide();
        $('#initial-response').show();
    }

    $('body').css('cursor', 'default');


    $('.theme-btn').on('click', themePickerHandler);
    let ele = $('#available-fields .select-unselect-header');

    if (theme === "light"){
        ele.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
    }else{
        ele.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
    }

    setupEventHandlers();

    resetDashboard();

    if (Cookies.get('startEpoch') && Cookies.get('endEpoch')){
        let cookieVar = Cookies.get('endEpoch');
        if(cookieVar === "now"){
            filterStartDate = Cookies.get('startEpoch');
            filterEndDate =  Cookies.get('endEpoch');
            $('.inner-range #' + filterStartDate).addClass('active');
        } else {
            filterStartDate = Number(Cookies.get('startEpoch'));
            filterEndDate =  Number(Cookies.get('endEpoch'));
        }
    }

    if (Cookies.get('customStartDate')){
        let cookieVar = new Date(Cookies.get('customStartDate'));
        $('#date-start').val(cookieVar.toISOString().substring(0,10));
        $('#date-start').addClass('active');
    }
    if (Cookies.get('customEndDate')){
        let cookieVar = new Date(Cookies.get('customEndDate'));
        $('#date-end').val(cookieVar.toISOString().substring(0,10));
        $('#date-end').addClass('active');
    }
    if (Cookies.get('customStartTime')){
        $('#time-start').val(Cookies.get('customStartTime'));
        $('#time-start').addClass('active');
    }
    if (Cookies.get('customEndTime')){
        $('#time-end').val(Cookies.get('customEndTime'));
        $('#time-end').addClass('active');
    }

	$("#info-icon-sql").tooltip({
		delay: { show: 0, hide: 300 },
		trigger: 'click'
	});

	$('#info-icon-sql').on('click', function (e) {
		$('#info-icon-sql').tooltip('show');
	});

	$(document).mouseup(function (e) {
		if ($(e.target).closest(".tooltip-inner").length === 0) {
			$('#info-icon-sql').tooltip('hide');
		}
	});

	$("#info-icon-logQL").tooltip({
		delay: { show: 0, hide: 300 },
		trigger: 'click'
	});

	$('#info-icon-logQL').on('click', function (e) {
		$('#info-icon-logQL').tooltip('show');
	});

	$(document).mouseup(function (e) {
		if ($(e.target).closest(".tooltip-inner").length === 0) {
			$('#info-icon-logQL').tooltip('hide');
		}
	});

    $("#info-icon-spl").tooltip({
		delay: { show: 0, hide: 300 },
		trigger: 'click'
	});

	$('#info-icon-spl').on('click', function (e) {
		$('#info-icon-spl').tooltip('show');
	});

	$(document).mouseup(function (e) {
		if ($(e.target).closest(".tooltip-inner").length === 0) {
			$('#info-icon-spl').tooltip('hide');
		}
	});

    $("#logs-settings").click(function(){
        event.stopPropagation();
        $("#setting-container").fadeToggle("fast");
    });
    
    $(document).click(function(event) {
        if (!$(event.target).closest('#setting-container').length) {
            $("#setting-container").hide();
        }
    });

    initializeFilterInputEvents();
});
