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

let selectedLogSource = "";

$(document).ready(function () {
    $('#data-ingestion,#test-data-btn').hide();
    $('.theme-btn').on('click', themePickerHandler);
    let org_name;

    $.ajax({
        method: 'get',
        url: 'api/config',
        crossDomain: true,
        dataType: 'json',
        credentials: 'include'
    })
        .then((res) => {
            let token;
            {{ if .TestDataSendData }}
                {{ .TestDataSendData }}
            {{ else }}
                myOrgSendTestData(token);
            {{ end }}
        })
        .catch((err) => {
            console.log(err)
        });
        var currentUrl = window.location.href;
        var url = new URL(currentUrl);
        var baseUrl = url.protocol + '//' + url.hostname;

        $('#source-options').on('click', '.source-option', function() {
            selectedLogSource = $(this).text().trim();
            $('.source-option').removeClass("active");
            $(this).addClass("active");
            $('#source-selection span').html(selectedLogSource);
        
            var showDataIngestion = ['Vector', 'Logstash', 'Fluentd', 'Filebeat', 'Promtail','Elastic Bulk','Splunk HEC'].includes(selectedLogSource);
            $('#data-ingestion').toggle(showDataIngestion);
            $('#test-data-btn').toggle(!showDataIngestion);
        
            var curlCommand = 'curl -X POST "' + baseUrl + ':8081/elastic/_bulk" \\\n' +
                            '-H \'Content-Type: application/json\' \\\n' +
                            '-d \'{ "index" : { "_index" : "test" } }\n' +
                            '{ "name" : "john", "age":"23" }\'';
            $('#verify-command').text(curlCommand);
        
            switch (selectedLogSource) {
                case 'Vector':
                case 'Logstash':
                case 'Fluentd':
                case 'Filebeat':
                case 'Promtail':
                    $('#platform-input').val(selectedLogSource);
                    $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/' + selectedLogSource.toLowerCase());
                    break;
                case 'Elastic Bulk':
                    $('#platform-input').val(selectedLogSource);
                    $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd');
                    break;
                case 'Splunk HEC':
                    $('#platform-input').val(selectedLogSource);
                    $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd');
                    curlCommand = 'curl -X POST "' + baseUrl + ':8081/splunk/services/collector/event" \\\n' +
                            '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
                            '-d \'{ "index" : { "_index" : "test" } }\n' +
                            '{ "name" : "john", "age":"23" }\'';
                    $('#verify-command').text(curlCommand);
                    break;
                case 'Send Test Data':
                    $('#test-data-btn').show();
                    break;
                default:
                    break;
            }
        });
        

    //Copy Handler
    $('.copyable').each(function() {
        var copyIcon = $('<span class="copy-icon"></span>');
        $(this).after(copyIcon);
    });

    $('.copy-icon').on('click', function(event) {
        var copyIcon = $(this);
        var inputOrTextarea = copyIcon.prev('.copyable');
        var inputValue = inputOrTextarea.val();
    
        var tempInput = document.createElement("textarea");
        tempInput.value = inputValue;
        document.body.appendChild(tempInput);
        tempInput.select();
        document.execCommand("copy");
        document.body.removeChild(tempInput);
    
        copyIcon.addClass('success');
        setTimeout(function() {
            copyIcon.removeClass('success'); 
        }, 1000);
    });
    
    {{ .Button1Function }}
})

function dropdown() {
    $('.dropdown-option').toggleClass('active');
}

function sendTestData(e, token) {
    
    if (token) {
        sendTestDataWithBearerToken(token).then((res) => {
            showSendTestDataUpdateToast('Sent Test Data Successfully');
            let testDataBtn = document.getElementById("test-data-btn");
            testDataBtn.disabled = false;
        })
            .catch((err) => {
                console.log(err)
                showSendTestDataUpdateToast('Error Sending Test Data');
                let testDataBtn = document.getElementById("test-data-btn");
                testDataBtn.disabled = false;
            });
    } else {
        sendTestDataWithoutBearerToken().then((res) => {
            showSendTestDataUpdateToast('Sent Test Data Successfully');
            let testDataBtn = document.getElementById("test-data-btn");
            testDataBtn.disabled = false;
        })
            .catch((err) => {
                console.log(err)
                showSendTestDataUpdateToast('Error Sending Test Data');
                let testDataBtn = document.getElementById("test-data-btn");
                testDataBtn.disabled = false;
            });
    }

        

    function sendTestDataWithBearerToken( token) {
        return new Promise((resolve, reject) => {
            $.ajax({
                method: 'post',
                url: '/api/sampledataset_bulk',
                headers: {
                    'Authorization': `Bearer ${token}`,
                },
                crossDomain: true,
                dataType: 'json',
                credentials: 'include'
            }).then((res) => {
                resolve(res);
            })
                .catch((err) => {
                    console.log(err);
                    reject(err);
                });
        });
    }

    function sendTestDataWithoutBearerToken() {
        return new Promise((resolve, reject) => {
            $.ajax({
                method: 'post',
                url:  '/api/sampledataset_bulk',
                crossDomain: true,
                dataType: 'json',
                credentials: 'include'
            }).then((res) => {
                resolve(res);
            })
                .catch((err) => {
                    console.log(err);
                    reject(err);
                });
        });
    }
}
