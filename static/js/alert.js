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

let alertData = {};
let alertEditFlag = 0;
let alertID;
let alertRule_name = "alertRule_name";
let query_string = "query_string";
let condition = "condition";
let notification_channel_type = "notification_channel_type";
let messageTemplateInfo =
  '<i class="fa fa-info-circle position-absolute info-icon sendMsg" rel="tooltip" id="info-icon-msg" style="display: block;" title = "You can use following template variables:' +
'\n' + inDoubleBrackets("alert_rule_name") +
'\n' + inDoubleBrackets("query_string") +
'\n' + inDoubleBrackets('condition') +
'\n' + inDoubleBrackets('queryLanguage') + '"></i>';
let messageInputBox = document.getElementById("message-info");
if(messageInputBox)
    messageInputBox.innerHTML += messageTemplateInfo;

// If there's double brackets next to each other, the templating system will
// try to replace what's inside the brackets with a value. We don't want that
// in this case.
function inDoubleBrackets(str) {
    return "{" + "{" + str + "}" + "}";
}

let mapConditionTypeToIndex =new Map([
    ["Is above",0],
    ["Is below",1],
    ["Equal to",2],
    ["Not equal to",3]
]);

let mapIndexToConditionType =new Map([
    [0,"Is above"],
    [1,"Is below"],
    [2,"Equal to"],
    [3,"Not equal to"]
]);

let mapIndexToAlertState=new Map([
    [0,"Normal"],
    [1,"Pending"],
    [2,"Firing"],
]);

const alertForm =$('#alert-form');

$(document).ready(function () {

    $('.theme-btn').on('click', themePickerHandler);
    $("#logs-language-btn").show();
    let startTime = "now-30m";
    let endTime = "now";
    datePickerHandler(startTime, endTime, startTime);
    setupEventHandlers();

    $('.alert-condition-options li').on('click', setAlertConditionHandler);
    $('#contact-points-dropdown').on('click', contactPointsDropdownHandler);
    $('#logs-language-options li').on('click', setLogsLangHandler);
    $('#data-source-options li').on('click', function(){
        let alertType;
        if ($(this).html() === 'Logs'){
            alertType = 1;
        }else {
            alertType = 2;
        }
        setDataSourceHandler(alertType)
    });
    $('#cancel-alert-btn').on('click',function(){
        window.location.href='../all-alerts.html';
        resetAddAlertForm();
    });
    
    alertForm.on('submit',(e)=>submitAddAlertForm(e));
  
    const tooltipIds = ["info-icon-spl", "info-icon-msg", "info-evaluate-every", "info-evaluate-for"];

    tooltipIds.forEach(id => {
        $(`#${id}`).tooltip({
            delay: { show: 0, hide: 300 },
            trigger: "click"
        }).on("click", function () {
            $(`#${id}`).tooltip("show");
        });
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest(".tooltip-inner").length === 0) {
            tooltipIds.forEach(id => $(`#${id}`).tooltip("hide"));
        }
    });
    getAlertId();
    if(window.location.href.includes("alert-details.html")){
        alertDetailsFunctions();
    }
});

function getAlertId() {
    const urlParams = new URLSearchParams(window.location.search);

    if (urlParams.has('id')) {
        const id = urlParams.get('id');
        editAlert(id);
        alertID = id;
    } else if (urlParams.has('queryLanguage')) {
        const queryLanguage = urlParams.get('queryLanguage');
        const searchText = urlParams.get('searchText');
        const startEpoch = urlParams.get('startEpoch');
        const endEpoch = urlParams.get('endEpoch');
    
        createAlertFromLogs(queryLanguage, searchText, startEpoch, endEpoch);
    }
}

function editAlert(alertId){
    $.ajax({
        method: "get",
        url: "api/alerts/" + alertId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        if(window.location.href.includes("alert-details.html")){
            displayAlertProperties(res.alert)
        }else{
            alertEditFlag = 1;
            displayAlert(res.alert);
        }
    })
}

function setAlertConditionHandler(e) {
    $('.alert-condition-option').removeClass('active');
    $('#alert-condition span').html($(this).html());
    $(this).addClass('active');
    let optionId = $(this).attr('id');  
}

function contactPointsDropdownHandler() {
    //get all contact points 
    $.ajax({
        method: "get",
        url: "api/alerts/allContacts",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        if (res.contacts) {
        let dropdown = $('.contact-points-options');
        
        res.contacts.forEach((cp) => {
            if (!$(`.contact-points-option:contains(${cp.contact_name})`).length) {
                dropdown.append(`<li class="contact-points-option" id="${cp.contact_id}">${cp.contact_name}</li>`);
            }
        });
    }
})}


$('.contact-points-options').on('click', 'li', function () {
    $('.contact-points-option').removeClass('active');
    $('#contact-points-dropdown span').html($(this).html());
    $('#contact-points-dropdown span').attr('id', $(this).attr('id'));
    $(this).addClass('active');

    if ($(this).html() === 'Add New') {
        $('.popupOverlay, .popupContent').addClass('active');
        $('#contact-form-container').css("display", "block");
    }
});

$(document).keyup(function(e) {
    if (e.key === "Escape" || e.key === "Esc") {
        $('.popupOverlay, .popupContent').removeClass('active');
    }
});



//create new alert rule
function submitAddAlertForm(e){
    e.preventDefault();
    setAlertRule();
    alertEditFlag ? updateAlertRule(alertData) : createNewAlertRule(alertData);
}

function setAlertRule(){
    let dataSource = $('#alert-data-source span').text();
    if (dataSource === "Logs") {
        alertData.alert_type = 1 ;
        alertData.queryParams = {
            data_source: dataSource,
            queryLanguage: $('#logs-language-btn span').text(),
            queryText: $('#query').val(),
            startTime: filterStartDate,
            endTime: filterEndDate
        };
    } else if (dataSource === "Metrics") {
        alertData.alert_type = 2 ;
        alertData.metricsQueryParams = JSON.stringify(metricsQueryParams);
    }
    alertData.alert_name = $('#alert-rule-name').val();
    alertData.condition= mapConditionTypeToIndex.get($('#alert-condition span').text()) ;
    alertData.eval_interval= parseInt($('#evaluate-every').val()) ;
    alertData.eval_for= parseInt($('#evaluate-for').val()) ;
    alertData.contact_name= $('#contact-points-dropdown span').text() ;
    alertData.contact_id= $('#contact-points-dropdown span').attr('id') ;
    alertData.message= $('.message').val() ;
    alertData.value = parseFloat($('#threshold-value').val());
    alertData.message = $(".message").val();
    alertData.labels =[]
    
    $('.label-container').each(function() {
      let labelName = $(this).find('#label-key').val();
      let labelVal = $(this).find('#label-value').val();
        if (labelName && labelVal) {
            let labelEntry  = {
                label_name: labelName,
                label_value: labelVal
              };
              alertData.labels.push(labelEntry);
        }
    })

    console.log(alertData);
}

function createNewAlertRule(alertData){
    if (!alertData.alert_type) {
        alertData.alert_type = 1;
    }
    $.ajax({
        method: "post",
        url: "api/alerts/create",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify(alertData),
        dataType: 'json',
        crossDomain: true,
    }).then((res)=>{
        resetAddAlertForm();
        window.location.href='../all-alerts.html';
    }).catch((err)=>{
        showToast(err.responseJSON.error)
    });
}

// update alert rule
function updateAlertRule(alertData){
        $.ajax({
        method: "post",
        url: "api/alerts/update",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify(alertData),
        dataType: 'json',
        crossDomain: true,
    }).then((res)=>{
        resetAddAlertForm();
        window.location.href='../all-alerts.html';
    }).catch((err)=>{
        showToast(err.responseJSON.error)
    });
}

//reset alert form
function resetAddAlertForm(){
    alertForm[0].reset();
}

async function displayAlert(res){
    console.log(res);
    $('#alert-rule-name').val(res.alert_name);
    setDataSourceHandler(res.alert_type) 
    if( res.alert_type === 1 ){
        $('#alert-data-source span').html(res.queryParams.data_source);
        const queryLanguage = res.queryParams.queryLanguage;
        $('#logs-language-btn span').text(queryLanguage);
        $('.logs-language-option').removeClass('active');
        $(`.logs-language-option:contains(${queryLanguage})`).addClass('active');
        displayQueryToolTip(queryLanguage);
        $('#query').val(res.queryParams.queryText);
        $(`.ranges .inner-range #${res.queryParams.startTime}`).addClass('active');
        datePickerHandler(res.queryParams.startTime, res.queryParams.endTime, res.queryParams.startTime)
    } else if (res.alert_type === 2){
        let metricsQueryParams = JSON.parse(res.metricsQueryParams);
        console.log(metricsQueryParams);
        $(`.ranges .inner-range #${metricsQueryParams.start}`).addClass('active');
        datePickerHandler(metricsQueryParams.start, metricsQueryParams.end, metricsQueryParams.start);
        console.log("Length of metrics query",metricsQueryParams.queries.length);
        for (const index in metricsQueryParams.queries) {
            const query = metricsQueryParams.queries[index];
            const parsedQueryObject = parsePromQL(query.query);
            await addQueryElementOnAlertEdit(query.name, parsedQueryObject);
        }
        if(metricsQueryParams.queries.length>1){
            await addAlertsFormulaElement(metricsQueryParams.formulas[0].formula);
        }
    }
    let conditionType = mapIndexToConditionType.get(res.condition)
    $('.alert-condition-option').removeClass('active');
    $(`.alert-condition-options #option-${res.condition}`).addClass('active');
    $('#alert-condition span').text(conditionType);
    $('#threshold-value').val(res.value);
    $('#evaluate-every').val(res.eval_interval);
    $('#evaluate-for').val(res.eval_for);
    $('.message').val(res.message);
    if(alertEditFlag){
        alertData.alert_id = res.alert_id;
    }
    $('#contact-points-dropdown span').html(res.contact_name);
    $('#contact-points-dropdown span').attr('id', res.contact_id);
    
    let isFirst = true;
    (res.labels).forEach(function(label){
        let labelContainer;
        if (isFirst) {
            labelContainer = $('.label-container');
            isFirst = false;
        } else {
            labelContainer = $('.label-container').first().clone();
            labelContainer.append('<button class="btn-simple delete-icon" type="button" id="delete-alert-label"></button>');
        }
        labelContainer.find("#label-key").val(label.label_name);
        labelContainer.find("#label-value").val(label.label_value);
        labelContainer.appendTo('.label-main-container');
    })
}

function showToast(msg) {
    let toast =
        `<div class="div-toast" id="save-db-modal"> 
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast)
    setTimeout(removeToast, 2000);
}

function removeToast() {
    $('.div-toast').remove();
}

function setLogsLangHandler(e) {
    $('.logs-language-option').removeClass('active');
    $('#logs-language-btn span').html($(this).html());
    $(this).addClass('active');
    displayQueryToolTip($(this).html());
}

function setDataSourceHandler(alertType) {
    $('.data-source-option').removeClass('active');
    const isLogs = alertType === 1;
    const sourceText = isLogs ? "Logs" : "Metrics";
    const $span = $('#alert-data-source span');

    $span.html(sourceText);
    $(`.data-source-option:contains("${sourceText}")`).addClass('active');
    
    $('.query-container, .logs-lang-container').toggle(isLogs);
    $('#metrics-explorer, #metrics-graphs').toggle(!isLogs);
    
    if (isLogs) {
        $('#query').attr('required', 'required');
    } else {
        $('#query').removeAttr('required');
    }
}


function displayQueryToolTip(selectedQueryLang) {
    $('#info-icon-pipeQL, #info-icon-spl').hide();
    if (selectedQueryLang === "Pipe QL") {
        $('#info-icon-pipeQL').show();
    } else if (selectedQueryLang === "Splunk QL") {
        $('#info-icon-spl').show();
    }
}

// Display Alert Details
function displayAlertProperties(res) {
    const queryParams = res.queryParams;
    $('.alert-name').text(res.alert_name);
    $('.alert-status').text(mapIndexToAlertState.get(res.state));
    $('.alert-query').val(queryParams.queryText);
    $('.alert-type').text(queryParams.data_source);
    $('.alert-query-language').text(queryParams.queryLanguage);
    $('.alert-condition').text(mapIndexToConditionType.get(res.condition));
    $('.alert-value').text(res.value);
    $('.alert-every').text(res.eval_interval);
    $('.alert-for').text(res.eval_for);
    $('.alert-contact-point').text(res.contact_name);
    const labelContainer = $('.alert-labels-container');
    const labels = res.labels;
    labels.forEach(label => {
        const labelElement = $('<div>').addClass('label-element').text(`${label.label_name}=${label.label_value}`);
        labelContainer.append(labelElement);
    })
}

// Add Label
$(".add-label-container").on("click", function () {
    var labelContainer = $(".label-container").first().clone();
    labelContainer.find("#label-key").val("");
    labelContainer.find("#label-value").val("");
    labelContainer.append('<button class="btn-simple delete-icon" type="button" id="delete-alert-label"></button>');
    labelContainer.appendTo(".label-main-container");
});

// Delete Label
$(".label-main-container").on("click", ".delete-icon", function () {
    $(this).closest(".label-container").remove();
});

//On Alert Details Page 
function alertDetailsFunctions(){
    function editAlert(event){        
        var queryString = "?id=" + alertID;
        window.location.href = "../alert.html" + queryString;
        event.stopPropagation();
    }

    function deleteAlert() {
        $.ajax({
            method: 'delete',
            url: 'api/alerts/delete',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            data: JSON.stringify({
                alert_id: alertID
            }),
            crossDomain: true,
        }).then(function (res) {
            showToast(res.message)
            window.location.href='../all-alerts.html';
        });
    }

    function showPrompt(event) {
        event.stopPropagation();
        $('.popupOverlay, .popupContent').addClass('active');

        $('#cancel-btn, .popupOverlay, #delete-btn').click(function () {
            $('.popupOverlay, .popupContent').removeClass('active');
        });
        $('#delete-btn').click(deleteAlert)
    }

    $('#edit-alert-btn').on('click',editAlert)
    $('#delete-alert').on('click',showPrompt)
    $('#cancel-alert-details').on('click',function(){
        window.location.href='../all-alerts.html';
    })
}

//Create alert from logs
function createAlertFromLogs(queryLanguage, query, startEpoch, endEpoch){
    $('#alert-rule-name').focus();
    $('#query').val(query);
    $(`.ranges .inner-range #${startEpoch}`).addClass('active');
    datePickerHandler(startEpoch, endEpoch , startEpoch)
}


async function addQueryElementOnAlertEdit(queryName, queryDetails) {
    // Clone the first query element if it exists, otherwise create a new one
    var queryElement;

        queryElement = $(`
    <div class="metrics-query">
        <div class="query-box">
            <div class="query-name active">${queryName}</div>
            <div class="query-builder">
                <input type="text" class="metrics" placeholder="Select a metric" >
                <div>from</div>
                <div class="tag-container">
                    <input type="text" class="everywhere" placeholder="(everywhere)">
                </div>
                <input class="agg-function" value="avg by">
                <div class="value-container">
                    <input class="everything" placeholder="(everything)">
                </div>
                <div class="functions-container">
                    <div class="all-selected-functions">
                    </div>
                    <div class="position-container">
                        <div class="show-functions">
                        </div>
                        <div class="options-container">
                            <input type="text" id="functions-search-box" class="search-box" placeholder="Search...">
                        </div>
                    </div>
                </div>
            </div>
            <div class="raw-query" style="display: none;">
                <input type="text" class="raw-query-input"><button class="btn run-filter-btn" id="run-filter-btn" title="Run your search"> </button>
            </div>
        </div>
        <div>
            <div class="raw-query-btn">&lt;/&gt;</div>
            <div class="remove-query">×</div>
        </div>
    </div>`);

    $('#metrics-queries').append(queryElement);
    await getMetricNames();
    // Initialize autocomplete with the details of the previous query if it exists
    await populateQueryElement(queryElement, queryDetails);
    await initializeAutocomplete(queryElement, queryDetails);

    // Show or hide the query close icon based on the number of queries
    updateCloseIconVisibility();

    queryIndex++;

    // Remove query element
    queryElement.find('.remove-query').on('click', function() {
        var queryName = queryElement.find('.query-name').text();
        // Check if the query name exists in any of the formula input fields
        var queryNameExistsInFormula = $('.formula').toArray().some(function(formulaInput) {
            return $(formulaInput).val().includes(queryName);
        });

        // If query name exists in any formula, prevent removal of the query element
        if (queryNameExistsInFormula) {
            $(this).addClass('disabled').css('cursor', 'not-allowed').attr('title', 'Query used in other formulas.');
        } else {
            delete queries[queryName];
            queryElement.remove();
            removeVisualizationContainer(queryName);
            // Show or hide the close icon based on the number of queries
            updateCloseIconVisibility();

            // For Alerts Screen
            if(isAlertScreen){
                if ($('#metrics-formula .formula-box').length > 0 && $('#metrics-formula .formula-box .formula').val().trim() === "") {
                    $('#metrics-queries .metrics-query:first').find('.query-name').addClass('active');
                    let queryName = $('#metrics-queries .metrics-query:first').find('.query-name').html();
                    let queryDetails = queries[queryName];
                    getQueryDetails(queryName, queryDetails)
                }
            }
        }
    });

    // Alias button
    queryElement.find('.as-btn').on('click', function() {
        $(this).hide(); // Hide the "as..." button
        $(this).siblings('.alias-filling-box').show(); // Show alias input box
    });

    // Alias close button
    queryElement.find('.alias-filling-box div').last().on('click', function() {
        $(this).parent().hide();
        $(this).parent().siblings('.as-btn').show();
    });

    // Hide or Show query element and graph on click on query name
    queryElement.find('.query-name').on('click', function() {
        var queryNameElement = $(this);
        var queryName = queryNameElement.text();
        var numberOfGraphVisible = $('#metrics-graphs').children('.metrics-graph').filter(':visible').length;
        var metricsGraph = $('#metrics-graphs').find('.metrics-graph[data-query="' + queryName + '"]');

        if (numberOfGraphVisible > 1 || !metricsGraph.is(':visible')) {
            metricsGraph.toggle();
            queryNameElement.toggleClass('active');
        }
        numberOfGraphVisible = $('#metrics-graphs').children('.metrics-graph').filter(':visible').length;
        if (numberOfGraphVisible === 1) {
            $('.metrics-graph').addClass('full-width');
        } else {
            $('.metrics-graph').removeClass('full-width');
        }
    });

    queryElement.find('.show-functions').on('click', function() {
        event.stopPropagation();
        var inputField = queryElement.find('#functions-search-box');
        var optionsContainer = queryElement.find('.options-container');
        var isContainerVisible = optionsContainer.is(':visible');
    
        if (!isContainerVisible) {
            optionsContainer.show();
            inputField.val('')
            inputField.focus();
            inputField.autocomplete('search', '');
        } else {
            optionsContainer.hide();
        }
    });
    
    $('body').on('click', function(event) {
        var optionsContainer = queryElement.find('.options-container');
        var showFunctionsButton = queryElement.find('.show-functions');
    
        // Check if the clicked element is not part of the options container or the show-functions button
        if (!$(event.target).closest(optionsContainer).length && !$(event.target).is(showFunctionsButton)) {
            optionsContainer.hide(); // Hide the options container if clicked outside of it
        }
    });

    queryElement.find('.raw-query-btn').on('click', function() {
        queryElement.find('.query-builder').toggle();
        queryElement.find('.raw-query').toggle();
        var queryName = queryElement.find('.query-name').text();
        var queryDetails = queries[queryName];

        if (queryDetails.state === 'builder') {
            // Switch to raw mode
            queryDetails.state = 'raw';
            const queryString = createQueryString(queryDetails);
                if (!queryDetails.rawQueryExecuted){
                    queryDetails.rawQueryInput = queryString;
                    queryElement.find('.raw-query-input').val(queryString);
                }
        } else {
            // Switch to builder mode
            queryDetails.state = 'builder';
            getQueryDetails(queryName, queryDetails);
        }
    });

    queryElement.find('.raw-query').on('click', '#run-filter-btn', async function() {
        var queryName = queryElement.find('.query-name').text();
        var queryDetails = queries[queryName];
        var rawQuery = queryElement.find('.raw-query-input').val();
        queryDetails.rawQueryInput = rawQuery;
        queryDetails.rawQueryExecuted = true; // Set the flag to indicate that raw query has been executed
        // Perform the search with the raw query
        await getQueryDetails(queryName, queryDetails);
    });
    
}


async function populateQueryElement(queryElement, queryDetails) {
    console.log("queryDetails",queryDetails);
    // Set the metric
    queryElement.find('.metrics').val(queryDetails.metrics);

    // Add 'everywhere' tags
    queryDetails.everywhere.forEach(tag => {
        addTagToContainer(queryElement, tag);
    });

    // Add 'everything' values
    queryDetails.everything.forEach(value => {
        addValueToContainer(queryElement, value);
    });

    // Set the aggregation function
    if(queryDetails.aggFunction){
        queryElement.find('.agg-function').val(queryDetails.aggFunction);
    }

    // Add functions
    queryDetails.functions.forEach(fn => {
        addFunctionToContainer(queryElement, fn);
    });
}

function addTagToContainer(queryElement, value) {
    var tagContainer = queryElement.find('.everywhere');
    var tag = $('<span class="tag">' + value + '<span class="close">×</span></span>');
    tagContainer.before(tag);

    if (queryElement.find('.tag-container').find('.tag').length === 0) {
        tagContainer.attr('placeholder', '(everywhere)');
        tagContainer.css('width', '100%');
    } else {
        tagContainer.removeAttr('placeholder');
        tagContainer.css('width', '5px');
    }
}

function addValueToContainer(queryElement, value) {
    var valueContainer = queryElement.find('.everything');
    var value = $('<span class="value">' + value + '<span class="close">×</span></span>');
    valueContainer.before(value);

    if (queryElement.find('.value-container').find('.value').length === 0) {
        valueContainer.attr('placeholder', '(everything)');
        valueContainer.css('width', '100%');
    } else {
        valueContainer.removeAttr('placeholder');
        valueContainer.css('width', '5px');
    }
}

function addFunctionToContainer(queryElement, fnName) {
    var newDiv = $('<div class="selected-function">' + fnName + '<span class="close">×</span></div>');
    queryElement.find('.all-selected-functions').append(newDiv);
}