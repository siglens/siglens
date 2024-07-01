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
let alertID;
let alertEditFlag = 0;
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
if (messageInputBox)
    messageInputBox.innerHTML += messageTemplateInfo;

function inDoubleBrackets(str) {
    return "{" + "{" + str + "}" + "}";
}

let mapConditionTypeToIndex = new Map([
    ["Is above", 0],
    ["Is below", 1],
    ["Equal to", 2],
    ["Not equal to", 3]
]);

let mapIndexToConditionType = new Map([
    [0, "Is above"],
    [1, "Is below"],
    [2, "Equal to"],
    [3, "Not equal to"]
]);

let mapIndexToAlertState = new Map([
    [0, "Inactive"],
    [1, "Normal"],
    [2, "Pending"],
    [3, "Firing"],
]);

const alertForm = $('#alert-form');

const propertiesGridOptions = {
    columnDefs: [
        { 
            headerName: "Config Variable Name", 
            field: "name", 
            sortable: true, 
            filter: true, 
            cellStyle: { 'white-space': 'normal', 'word-wrap': 'break-word' }, 
            width: 150 
        },
        { 
            headerName: "Config Variable Value", 
            field: "value", 
            sortable: true, 
            filter: true, 
            cellStyle: { 'white-space': 'normal', 'word-wrap': 'break-word' }, 
            autoHeight: true 
        }
    ],
    defaultColDef: {
        resizable: true,
        flex: 1,
        minWidth: 100
    },
    rowData: [],
    domLayout: 'autoHeight'
};

const historyGridOptions = {
    columnDefs: [
        { headerName: "Timestamp", field: "timestamp", sortable: true, filter: true },
        { headerName: "Action", field: "action", sortable: true, filter: true },
        { headerName: "State", field: "state", sortable: true, filter: true }
    ],
    defaultColDef: {
        resizable: true,
        flex: 1,
        minWidth: 150
    },
    rowData: [],
    domLayout: 'autoHeight'
};

let originalIndexValues, indexValues = [];

$(document).ready(async function () {

    $('.theme-btn').on('click', themePickerHandler);
    $("#logs-language-btn").show();
    let startTime = "now-30m";
    let endTime = "now";
    datePickerHandler(startTime, endTime, startTime);
    setupEventHandlers();
    const urlParams = new URLSearchParams(window.location.search);
    $("#alert-rule-name").val(urlParams.get('alertRule_name'));
    $('.alert-condition-options li').on('click', setAlertConditionHandler);
    $('#contact-points-dropdown').on('click', contactPointsDropdownHandler);
    $('#logs-language-options li').on('click', setLogsLangHandler);
    $('#data-source-options li').on('click', function(){
        let alertType;
        if ($(this).html() === 'Logs'){
            alertType = 1;
        }else {
            alertType = 2;
            $("#save-alert-btn").on("click", function (event) {
                if($("#select-metric-input").val===""){
                    $("#save-alert-btn").prop("disabled", true);
                }
                else{
                    $("#save-alert-btn").prop("disabled", false);
                }
            
            });
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
        if ($(`#${id}`).length) {
            $(`#${id}`).tooltip({
                delay: { show: 0, hide: 300 },
                trigger: "click"
            }).on("click", function () {
                $(`#${id}`).tooltip("show");
            });
        }
    });

     // Initialize ag-Grid only if the elements exist
     if ($('#properties-grid').length) {
        new agGrid.Grid(document.querySelector('#properties-grid'), propertiesGridOptions);
    }
    if ($('#history-grid').length) {
        new agGrid.Grid(document.querySelector('#history-grid'), historyGridOptions);
    }

    $(document).mouseup(function (e) {
        if ($(e.target).closest(".tooltip-inner").length === 0) {
            tooltipIds.forEach(id => $(`#${id}`).tooltip("hide"));
        }
    });
    await getAlertId();
    if(window.location.href.includes("alert-details.html")){
        alertDetailsFunctions();
        fetchAlertProperties();
        displayHistoryData();
    }
   

// Enable the save button when a contact point is selected
$(".contact-points-options li").on("click", function () {
    $("#contact-points-dropdown span").text($(this).text());
    $("#save-alert-btn").prop("disabled", false);
    $("#contact-point-error").css("display","none"); // Hide error message when a contact point is selected
});

$("#save-alert-btn").on("click", function (event) {
    if ($("#contact-points-dropdown span").text() === "Choose" || $("#contact-points-dropdown span").text() === "Add New") {
        event.preventDefault();
        $("#contact-point-error").css("display","inline-block");
    } else {
        $("#contact-point-error").css("display","none"); // Hide error message if a valid contact point is selected
    }
});

// Hide the error message if a contact point is selected again
$("#contact-points-dropdown").on("click", function() {
    if ($("#contact-point-error").css("display") === "inline-block") {
        $("#contact-point-error").css("display", "none");
    }
});

    

    
});

async function getAlertId() {
    const urlParams = new URLSearchParams(window.location.search);
    // Index
    if(!(window.location.href.includes("alert-details.html"))){
        let indexes = await getListIndices();
        if (indexes){
            originalIndexValues = indexes.map(item => item.index);
            indexValues = [...originalIndexValues];
        }
        initializeIndexAutocomplete();
        setIndexDisplayValue(selectedSearchIndex);
    }
    if (urlParams.has('id')) {
        const id = urlParams.get('id');
        alertID = id;
       const editFlag = await editAlert(id);
       alertEditFlag = editFlag;
    } else if (urlParams.has('queryLanguage')) {
        const queryLanguage = urlParams.get('queryLanguage');
        const searchText = urlParams.get('searchText');
        const startEpoch = urlParams.get('startEpoch');
        const endEpoch = urlParams.get('endEpoch');

        createAlertFromLogs(queryLanguage, searchText, startEpoch, endEpoch);
    }

    if(!alertEditFlag && !(window.location.href.includes("alert-details.html"))){
        addQueryElement();
    }
}

async function editAlert(alertId){
    const res = await $.ajax({
        method: "get",
        url: "api/alerts/" + alertId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    })
    if (window.location.href.includes("alert-details.html")) {
        displayAlertProperties(res.alert)
        return false
    } else {
        alertEditFlag = true;
        displayAlert(res.alert);
        return true
    }

    
}

function setAlertConditionHandler(e) {
    $('.alert-condition-option').removeClass('active');
    $('#alert-condition span').html($(this).html());
    $(this).addClass('active');
    let optionId = $(this).attr('id');
}

function contactPointsDropdownHandler() {
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
        if (res.contacts && Array.isArray(res.contacts)) {
            let dropdown = $('.contact-points-options');
            
            res.contacts.forEach((cp) => {
                if (cp && cp.contact_name && !$(`.contact-points-option:contains(${cp.contact_name})`).length) {
                    dropdown.append(`<li class="contact-points-option" id="${cp.contact_id}">${cp.contact_name}</li>`);
                }
            });
        }
    }).catch(function (error) {
        console.error('Error fetching contacts:', error);
    });    
}

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

const propertiesBtn = document.getElementById('properties-btn');
const historyBtn = document.getElementById('history-btn');

if (propertiesBtn) {
    propertiesBtn.addEventListener('click', function() {
        document.getElementById('properties-grid').style.display = 'block';
        document.getElementById('history-grid').style.display = 'none';
        document.getElementById('history-search-container').style.display = 'none';
        propertiesBtn.classList.add('active');
        historyBtn.classList.remove('active');
        fetchAlertProperties();
    });
}

if (historyBtn) {
    historyBtn.addEventListener('click', function() {
        document.getElementById('properties-grid').style.display = 'none';
        document.getElementById('history-grid').style.display = 'block';
        document.getElementById('history-search-container').style.display = 'block';
        historyBtn.classList.add('active');
        propertiesBtn.classList.remove('active');
        displayHistoryData();
    });
}

function submitAddAlertForm(e) {
    e.preventDefault();
    setAlertRule();
    alertEditFlag ? updateAlertRule(alertData) : createNewAlertRule(alertData);
}

function setAlertRule() {
    let dataSource = $('#alert-data-source span').text();
    if (dataSource === "Logs") {
        let searchText, queryMode;
        if(isQueryBuilderSearch){
            searchText = getQueryBuilderCode();
            queryMode = "Builder"
        }else {
            searchText = $('#filter-input').val();
            queryMode = "Code"
        }
        alertData.alert_type = 1 ;
        alertData.queryParams = {
            data_source: dataSource,
            queryLanguage: $('#logs-language-btn span').text(),
            queryText: searchText,
            startTime: filterStartDate,
            endTime: filterEndDate,
            index: selectedSearchIndex,
            queryMode : queryMode
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
    alertData.labels = []

    $('.label-container').each(function() {
        let labelName = $(this).find('#label-key').val();
        let labelVal = $(this).find('#label-value').val();
        if (labelName && labelVal) {
            let labelEntry = {
                label_name: labelName,
                label_value: labelVal
            };
            alertData.labels.push(labelEntry);
        }
    })

}

function createNewAlertRule(alertData) {
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
    }).then((res) => {
        resetAddAlertForm();
        window.location.href='../all-alerts.html';
    }).catch((err)=>{
        $("#metric-error").css("display","inline-block");
        setTimeout(function() {
            $("#metric-error").css("display","none");
        }, 3000); 
        showToast(err.responseJSON.error, "error")
    });
}

// update alert rule
function updateAlertRule(alertData){
    if (!alertData.alert_type) {
        alertData.alert_type = 1;
    }
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
    }).then((res) => {
        resetAddAlertForm();
        window.location.href='../all-alerts.html';
    }).catch((err)=>{
        showToast(err.responseJSON.error, "error");
    });
}

function resetAddAlertForm() {
    alertForm[0].reset();
}

async function displayAlert(res){

    $('#alert-rule-name').val(res.alert_name);
    setDataSourceHandler(res.alert_type) 
    if( res.alert_type === 1 ){
        const { data_source, queryLanguage, startTime, endTime, queryText, queryMode, index } = res.queryParams;

        $('#alert-data-source span').html(data_source);
        $('#logs-language-btn span').text(queryLanguage);
        $('.logs-language-option').removeClass('active');
        $(`.logs-language-option:contains(${queryLanguage})`).addClass('active');
        displayQueryToolTip(queryLanguage);
        
        $(`.ranges .inner-range #${startTime}`).addClass('active');
        datePickerHandler(startTime, endTime, startTime);
        setIndexDisplayValue(index);
        
        if (queryMode === 'Builder') {
            codeToBuilderParsing(queryText);
        } else if (queryMode === 'Code') {
            $("#custom-code-tab").tabs("option", "active", 1);
            $('#filter-input').val(queryText);
        }
        let data = {
            'state': wsState,
            'searchText': queryText,
            'startEpoch': startTime,
            'endEpoch': endTime,
            'indexName': index,
            'queryLanguage' : queryLanguage,
        }
        fetchLogsPanelData(data,-1).then((res)=>{
            alertChart(res);
        });
    } else if (res.alert_type === 2){
        let metricsQueryParams = JSON.parse(res.metricsQueryParams);
        const { start, end, queries, formulas } = metricsQueryParams;
        
        $(`.ranges .inner-range #${start}`).addClass('active');
        datePickerHandler(start, end, start);
        
        if (functionsArray) {
            const allFunctions = await getFunctions();
            functionsArray = allFunctions.map(item => item.fn);
        }
        
        for (const query of queries) {
            const parsedQueryObject = parsePromQL(query.query);
            await addQueryElementOnAlertEdit(query.name, parsedQueryObject);
        }
        
        if (queries.length > 1) {
            await addAlertsFormulaElement(formulas[0].formula);
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

    if (alertEditFlag) {
        alertData.alert_id = res.alert_id;
    }

    $('#contact-points-dropdown span').html(res.contact_name).attr('id', res.contact_id);

    (res.labels).forEach(function(label){
        var labelContainer = $(`
        <div class="label-container d-flex align-items-center">
            <input type="text" id="label-key" class="form-control" placeholder="Label name" tabindex="7" value="">
            <span class="label-equal"> = </span>
            <input type="text" id="label-value" class="form-control" placeholder="Value" value="" tabindex="8">
            <button class="btn-simple delete-icon" type="button" id="delete-alert-label"></button>
        </div>
    `)
        labelContainer.find("#label-key").val(label.label_name);
        labelContainer.find("#label-value").val(label.label_value);
        labelContainer.appendTo('.label-main-container');
    })
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
    
    $('.query-container, .logs-lang-container, .index-box, #logs-explorer').toggle(isLogs);
    $('#metrics-explorer, #metrics-graphs').toggle(!isLogs);
    
    if (isLogs) {
        $('#query').attr('required', 'required');
    } else {
        $('#query').removeAttr('required');
    }
}

$('#search-history-btn').on('click', function() {
    performSearch();
});

$('#history-filter-input').on('keypress', function(e) {
    if (e.which === 13) { 
        performSearch();
    }
});

$('#history-filter-input').on('input', function() {
    if ($(this).val().trim() === "") {
        displayHistoryData();
    }
});

function performSearch() {
    const searchTerm = $('#history-filter-input').val().trim().toLowerCase();
    if (searchTerm) {
        filterHistoryData(searchTerm);
    } else {
        displayHistoryData();
    }
}

function fetchAlertProperties() {
    if (alertID) {
        $.ajax({
            method: "get",
            url: "api/alerts/" + alertID,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                'Accept': '*/*'
            },
            dataType: 'json',
            crossDomain: true,
        }).then(function (res) {
            const alert = res.alert;
            let propertiesData = [];

            if (alert.alert_type === 1) {
                propertiesData.push(
                    { name: "Query", value: alert.queryParams.queryText },
                    { name: "Type", value: alert.queryParams.data_source },
                    { name: "Query Language", value: alert.queryParams.queryLanguage }
                );
            } else if (alert.alert_type === 2) {
                const metricsQueryParams = JSON.parse(alert.metricsQueryParams || '{}');
                let formulaString = metricsQueryParams.formulas && metricsQueryParams.formulas.length > 0
                    ? metricsQueryParams.formulas[0].formula
                    : 'No formula';

                // Replace a, b, etc., with actual query values
                metricsQueryParams.queries.forEach(query => {
                    const regex = new RegExp(`\\b${query.name}\\b`, 'g');
                    formulaString = formulaString.replace(regex, query.query);
                });

                propertiesData.push(
                    { name: "Query", value: formulaString },
                    { name: "Type", value: "Metrics" },
                    { name: "Query Language", value: "PromQL" }
                );
            }

            propertiesData.push(
                { name: "Status", value: mapIndexToAlertState.get(alert.state) },
                { name: "Condition", value: mapIndexToConditionType.get(alert.condition) },
                { name: "Evaluate", value: `every ${alert.eval_interval} minutes for ${alert.eval_for} minutes` },
                { name: "Contact Point", value: alert.contact_name }
            );

            if (alert.labels && alert.labels.length > 0) {
                const labelsValue = alert.labels.map(label => `${label.label_name}:${label.label_value}`).join(", ");
                propertiesData.push({ name: "Label", value: labelsValue });
            }

            if (propertiesGridOptions.api) {
                propertiesGridOptions.api.setRowData(propertiesData);
            } else {
                console.error("propertiesGridOptions.api is not defined");
            }
        }).catch(function (err) {
            console.error('Error fetching alert properties:', err);
        });
    }
}

function displayHistoryData() {
    if (alertID) {
        $.ajax({
            method: "get",
            url: `api/alerts/${alertID}/history`,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                'Accept': '*/*'
            },
            dataType: 'json',
            crossDomain: true,
        }).then(function (res) {
            const historyData = res.alertHistory.map(item => ({
                timestamp: new Date(item.event_triggered_at).toLocaleString(),
                action: item.event_description,
                state: mapIndexToAlertState.get(item.alert_state)
            }));

            if (historyGridOptions.api) {
                historyGridOptions.api.setRowData(historyData);
            } else {
                console.error("historyGridOptions.api is not defined");
            }
        }).catch(function (err) {
            console.error('Error fetching alert history:', err);
        });
    }
}


function filterHistoryData(searchTerm) {
    if (alertID) {
        $.ajax({
            method: "get",
            url: `api/alerts/${alertID}/history`,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                'Accept': '*/*'
            },
            dataType: 'json',
            crossDomain: true,
        }).then(function (res) {
            const filteredData = res.alertHistory.filter(item => {
                const description = item.event_description.toLowerCase();
                const state = mapIndexToAlertState.get(item.alert_state).toLowerCase();
                return description.includes(searchTerm) || state.includes(searchTerm);
            }).map(item => ({
                timestamp: new Date(item.event_triggered_at).toLocaleString(),
                action: item.event_description,
                state: mapIndexToAlertState.get(item.alert_state)
            }));

            if (historyGridOptions.api) {
                historyGridOptions.api.setRowData(filteredData);
            } else {
                console.error("historyGridOptions.api is not defined");
            }
        }).catch(function (err) {
            console.error('Error fetching alert history:', err);
        });
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

function displayAlertProperties(res) {
    const queryParams = res.queryParams;
    const metricsQueryParams = JSON.parse(res.metricsQueryParams || '{}');
    
    $('.alert-name').text(res.alert_name);
    $('.alert-status').text(mapIndexToAlertState.get(res.state));
    
    if (res.alert_type === 1) {
        $('.alert-query').val(queryParams.queryText);
        $('.alert-type').text(queryParams.data_source);
        $('.alert-query-language').text(queryParams.queryLanguage);
    } else if (res.alert_type === 2) {
        $('.alert-type').text('Metrics');
        $('.alert-query-language').text('PromQL');

        // Extract and display the formula string
        const formulaString = metricsQueryParams.formulas && metricsQueryParams.formulas.length > 0
            ? metricsQueryParams.formulas[0].formula
            : 'No formula';
        
        $('.alert-query').val(formulaString);
    }
    
    $('.alert-condition').text(mapIndexToConditionType.get(res.condition));
    $('.alert-value').text(res.value);
    $('.alert-every').text(res.eval_interval);
    $('.alert-for').text(res.eval_for);
    $('.alert-contact-point').text(res.contact_name);
    const labelContainer = $('.alert-labels-container');
    labelContainer.empty(); // Clear previous labels
    const labels = res.labels;
    labels.forEach(label => {
        const labelElement = $('<div>').addClass('label-element').text(`${label.label_name}=${label.label_value}`);
        labelContainer.append(labelElement);
    });
}


// Add Label
$(".add-label-container").on("click", function () {
    var newLabelContainer = `
        <div class="label-container d-flex align-items-center">
            <input type="text" id="label-key" class="form-control" placeholder="Label name" tabindex="7" value="">
            <span class="label-equal"> = </span>
            <input type="text" id="label-value" class="form-control" placeholder="Value" value="" tabindex="8">
            <button class="btn-simple delete-icon" type="button" id="delete-alert-label"></button>
        </div>
    `;
    $(".label-main-container").append(newLabelContainer);
});

$(".label-main-container").on("click", ".delete-icon", function() {
    $(this).closest(".label-container").remove();
});

function alertDetailsFunctions() {
    function editAlert(event) {
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
        }).then(function(res) {
            showToast(res.message)
            window.location.href='../all-alerts.html';
        }).catch((err)=>{
            showToast(err.responseJSON.error, "error");
        });
    }

    function showPrompt(event) {
        event.stopPropagation();
        $('.popupOverlay, .popupContent').addClass('active');

        $('#cancel-btn, .popupOverlay, #delete-btn').click(function() {
            $('.popupOverlay, .popupContent').removeClass('active');
        });
        $('#delete-btn').click(deleteAlert)
    }

    $('#edit-alert-btn').on('click', editAlert)
    $('#delete-alert').on('click', showPrompt)
    $('#cancel-alert-details').on('click', function() {
        window.location.href = '../all-alerts.html';
    })
}

function createAlertFromLogs(queryLanguage, query, startEpoch, endEpoch) {
    $('#alert-rule-name').focus();
    $('#query').val(query);
    $(`.ranges .inner-range #${startEpoch}`).addClass('active');
    datePickerHandler(startEpoch, endEpoch, startEpoch)
}

function alertChart(res) {
    const logsExplorer = document.getElementById('logs-explorer');
    logsExplorer.style.display = 'flex';
    logsExplorer.innerHTML = ''; // Clear previous content

    if (res.qtype === "logs-query") {
        const errorMsg = $('<div>')
            .text("Error : query does not contain any aggregation. Expected Stats Query");
        $('#logs-explorer').empty().append(errorMsg);
        return;
    }

    if (res.qtype === "aggs-query" || res.qtype === "segstats-query") {
        let columnOrder = []
        if (res.columnsOrder !=undefined && res.columnsOrder.length > 0) {
            columnOrder = res.columnsOrder
        }else{
            if (res.groupByCols) {
                columnOrder = _.uniq(_.concat(
                    res.groupByCols));
            }
            if (res.measureFunctions) {
                columnOrder = _.uniq(_.concat(
                    columnOrder, res.measureFunctions));
            }
        }
        
        if (res.errors) {
            const errorMsg = document.createElement('div');
            errorMsg.textContent = res.errors[0];
            logsExplorer.appendChild(errorMsg);
            return;
        }

        let xAxisData = [];
        let yAxisData = [];
        let hits = res.measure;
        let columns = columnOrder;

        // loop through the hits and create the data for the bar chart
        for (let i = 0; i < hits.length; i++) {
            let hit = hits[i];
    
            let xAxisValue = hit.GroupByValues[0];
            let yAxisValue;
            let measureVal = hit.MeasureVal;
            yAxisValue = measureVal[columns[1]]
            xAxisData.push(xAxisValue);
            yAxisData.push(yAxisValue);
        }

        let barOptions = loadBarOptions(xAxisData, yAxisData);
        let chartDom = document.createElement('div');
        chartDom.style.width = '100%';
        chartDom.style.height = '100%';
        logsExplorer.appendChild(chartDom);
        let myChart = echarts.init(chartDom);
        myChart.setOption(barOptions);

        window.addEventListener('resize', () => {
            myChart.resize();
        });

    }
}
