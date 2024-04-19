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

$(document).ready(() => {
    setSaveQueriesDialog();
    getListIndices()
    .then(function() {
        if (window.location.search) {
            data = getInitialSearchFilter(false, false);
        } else {
            console.log(`No query string found, using default search filter.`);
            data = getSearchFilter(false, false);
        }
        return data;
    })
    .then(function(data) {
        doSearch(data);
    })
    .finally(function() {
        $('body').css('cursor', 'default');
        getDisplayTextForIndex()
    });

    const currentUrl = window.location.href;
    if (currentUrl.includes("live-tail.html")) {
        $(".nav-live").addClass("active");
        $(".nav-search").removeClass("active");
    }else{
        $(".nav-search").addClass("active");
    }

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


    $("#filter-input").focus(function() {
        if ($(this).val() === "*") {
          $(this).val("");
        }
    });
});
function displayQueryLangToolTip(selectedQueryLangID) {
    $('#info-icon-sql, #info-icon-logQL, #info-icon-spl').hide();
    $("#clearInput").hide();
    switch (selectedQueryLangID) {
        case "1":
            $('#info-icon-sql').show();
            $("#filter-input").attr("placeholder", "Enter your SQL query here, or click the 'i' icon for examples");
            break;
        case "2":
            $('#info-icon-logQL').show();
            $("#filter-input").attr("placeholder", "Enter your LogQL query here, or click the 'i' icon for examples");
            break;
        case "3":
            $('#info-icon-spl').show();
            $("#filter-input").attr("placeholder", "Enter your SPL query here, or click the 'i' icon for examples");
            break;
    }
}

$("#filter-input").on("input", function() {
    if ($(this).val().trim() !== "") {
      $("#clearInput").show();
    } else {
      $("#clearInput").hide();
    }
});

$("#clearInput").click(function() {
    $("#filter-input").val("").focus();
    $(this).hide();
});

/*
Function to clear the query input field, search filter tags, and related elements
*/
function clearQueryInput() {
    // Clear the query input field
    $("#query-input").val("*").focus();

    // Hide the clear button for the query input field if it's empty
    if ($("#query-input").val().trim() !== "") {
        $("#clear-query-btn").show();
    } else {
        $("#clear-query-btn").hide();
    }

    // Clear all search filter tags and related elements
    $("#tags, #tags-second, #tags-third").empty();
    firstBoxSet.clear();
    secondBoxSet.clear();
    thirdBoxSet.clear();

    // Show the default text for search filters, aggregation attribute, and aggregations
    $("#search-filter-text, #aggregate-attribute-text, #aggregations").show();
}

// Event handler for the clear button associated with the query input field
$("#clear-query-btn").click(function() {
    clearQueryInput();
});
