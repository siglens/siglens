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

let alertData = {queryParams :{}};
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
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
        
    }
    $('.theme-btn').on('click', themePickerHandler);
    $("#logs-language-btn").show();
    let startTime = "now-30m";
    let endTime = "now";
    datePickerHandler(startTime, endTime, startTime);
    setupEventHandlers();

    $('.alert-condition-options li').on('click', setAlertConditionHandler);
    $('#contact-points-dropdown').on('click', contactPointsDropdownHandler);
    $('#logs-language-options li').on('click', setLogsLangHandler);
    $('#data-source-options li').on('click', setDataSourceHandler);
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
    alertData.alert_name = $('#alert-rule-name').val(),
    alertData.queryParams.data_source = dataSource;
    alertData.queryParams.queryLanguage = $('#logs-language-btn span').text();
    alertData.queryParams.queryText= $('#query').val(),
    alertData.queryParams.startTime= filterStartDate,
    alertData.queryParams.endTime= filterEndDate,
    alertData.condition= mapConditionTypeToIndex.get($('#alert-condition span').text()),
    alertData.eval_interval= parseInt($('#evaluate-every').val()),
    alertData.eval_for= parseInt($('#evaluate-for').val()),
    alertData.contact_name= $('#contact-points-dropdown span').text(),
    alertData.contact_id= $('#contact-points-dropdown span').attr('id'),
    alertData.message= $('.message').val()
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
}

function createNewAlertRule(alertData){
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

function displayAlert(res){
    $('#alert-rule-name').val(res.alert_name);
    $('#alert-data-source span').html(res.queryParams.data_source);
    const queryLanguage = res.queryParams.queryLanguage;
    $('#logs-language-btn span').text(queryLanguage);
    $('.logs-language-option').removeClass('active');
    $(`.logs-language-option:contains(${queryLanguage})`).addClass('active');
    displayQueryToolTip(queryLanguage);
    $('#query').val(res.queryParams.queryText);
    $(`.ranges .inner-range #${res.queryParams.startTime}`).addClass('active');
    datePickerHandler(res.queryParams.startTime, res.queryParams.endTime, res.queryParams.startTime)
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

function setDataSourceHandler(e) {
    $('.data-source-option').removeClass('active');
    $('#alert-data-source span').html($(this).html());
    $(this).addClass('active');
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