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

$(document).ready(function () {
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    $('.theme-btn').on('click', themePickerHandler);
    getRetentionDataFromConfig();
    getPersistentQueriesSetting();
    {{ .SettingsExtraOnReadySetup }}
});

function getRetentionDataFromConfig() {
    $.ajax({
        method: 'get',
        url: 'api/config',
        crossDomain: true,
        dataType: 'json',
        credentials: 'include'
    })
    {{ if .SettingsRetentionDataThenBlock }}
        {{ .SettingsRetentionDataThenBlock }}
    {{ else }}
        .then((res) => {
            let ret_days = res.RetentionHours / 24;
            $('#retention-days-value').html(`${ret_days} days`);
        })
    {{ end }}
    .catch((err) => {
        console.log(err)
    });
}

function getPersistentQueriesSetting(){
    console.log("getPersistentQueriesSetting");
    $.ajax({
        method: "GET",
        url: "/api/pqs/get",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
        crossDomain: true,
        success: function (res) {
            console.log("Update successful:", res);
            setPersistentQueries(res.pqsEnabled);
        },
        error: function (xhr, status, error) {
            console.error("Update failed:", xhr, status, error);
        },
    });
}
function updatePersistentQueriesSetting(pqsEnabled) {
    console.log("function updatePersistentQueriesSetting");
    $.ajax({
        method: "POST",
        url: "/api/pqs/update",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
        crossDomain: true,
        data: JSON.stringify({ pqsEnabled: pqsEnabled }),
        success: function (res) {
            console.log("Update successful:", res);
        },
        error: function (xhr, status, error) {
            console.error("Update failed:", xhr, status, error);
        },
    });
}

$(document).on('click', '.contact-option', updatePQS);

function updatePQS() {
    var selectedOption = $(this).text();
    $('.contact-option').removeClass('active');

    if (selectedOption.toLowerCase() === 'disabled') {
        $('.popupOverlay, .popupContent').addClass('active');
        $('#cancel-disable-pqs').on('click', function() {
            $('.popupOverlay, .popupContent').removeClass('active');
            $(`.contact-option:contains("Enabled")`).addClass('active');
        });
        
        $('#disable-pqs').on('click', function() {
            $('#contact-types span').text(selectedOption); 
            $('.popupOverlay, .popupContent').removeClass('active');
            $(`.contact-option:contains("Disabled")`).addClass('active');
            updatePersistentQueriesSetting(false);
        });
    }
    if(selectedOption.toLowerCase() === 'enabled') {
        updatePersistentQueriesSetting(true);
        $('#contact-types span').text(selectedOption); 
        $(`.contact-option:contains("Enabled")`).addClass('active');
    }
}

function setPersistentQueries(pqsEnabled) {
    $('.contact-option').removeClass('active');
    $('#contact-types span').text(pqsEnabled ? "Enabled" : "Disabled");
    $('.contact-option:contains("' + (pqsEnabled ? "Enabled" : "Disabled") + '")').addClass('active');
}

{{ .SettingsExtraFunctions }}
