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

let selectedLogSource = "";

$(document).ready(function () {
    $('#data-ingestion,#test-data-btn').hide();
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
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

        $('#source-options').on('click', '.source-option', function() {
            selectedLogSource = $(this).text().trim();
            $('.source-option').removeClass("active");
            $(this).addClass("active");
            $('#source-selection span').html(selectedLogSource);
        
            var showDataIngestion = ['Vector', 'Logstash', 'Fluentd', 'Filebeat', 'Promtail'].includes(selectedLogSource);
            $('#data-ingestion').toggle(showDataIngestion);
            $('#test-data-btn').toggle(!showDataIngestion);
        
            var curlCommand = 'curl -X POST "localhost:8081/elastic/_bulk" \\\n' +
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
                    $('#source-token-input').val('3yoZAtXwKrjWaSfWxSDmPVGv');
                    $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/' + selectedLogSource.toLowerCase());
                    break;
                case 'Person Profile':
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
        navigator.clipboard.writeText(inputValue)
            .then(function() {
                copyIcon.addClass('success');
                setTimeout(function() {
                    copyIcon.removeClass('success'); 
                }, 1000);
            })
            .catch(function(err) {
                console.error('Failed to copy text: ', err);
            });
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
