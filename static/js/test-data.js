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
let iToken = "";
$(document).ready(async function () {
    $('#data-ingestion,#test-data-btn').hide();
    $('.theme-btn').on('click', themePickerHandler);
    $(".custom-chart-tab").tabs();
    $(".custom-chart-tab").show();

    let baseUrl = "";
    try {
        const config = await $.ajax({
            method: 'GET',
            url: 'api/config',
            crossDomain: true,
            dataType: 'json',
            xhrFields: { withCredentials: true }
        });
        if (config.IngestUrl) {
            baseUrl = config.IngestUrl.replace(/^http:/, 'https:');
        }
        {{ if .TestDataSendData }}
            {{ .TestDataSendData }}
        {{ else }}
            myOrgSendTestData(iToken);
        {{ end }}
    } catch (err) {
        console.log(err);
    }

    function setCodeBlockContainerBackground() {
        const preElement = $('.code-block-container pre.language-yaml');
        if (preElement.length) {
            const preBackgroundColor = preElement.css('background-color');
            $('.code-block-container').css('background-color', preBackgroundColor);
        }
    }

    $('#custom-chart-tab').on('click', '.tab-li', function () {
        selectedLogSource = $(this).text().trim();
        $('.tab-li').removeClass("active");
        $(this).addClass("active");

        var ingestCmd = ""
        {{ if .IngestDataCmd }}
            {{ .IngestDataCmd }}
        {{ end }}

        // Hide test data button by default
        $('#test-data-btn').hide();
        $('#data-ingestion').show();


        switch (selectedLogSource) {
            case 'OpenTelemetry':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry/');
                // Example OTLP command for logs
                var curlCommand = 'curl -X POST "' + baseUrl + '/v1/logs" \\\n' +
                    '-H \'Content-Type: application/json\' \\\n' +
                    '-H \'Accept: application/json\' \\\n' +
                    ingestCmd +
                    '-d \'{\n' +
                    '  "resourceLogs": [{\n' +
                    '    "resource": {\n' +
                    '      "attributes": [{\n' +
                    '        "key": "service.name",\n' +
                    '        "value": { "stringValue": "test-service" }\n' +
                    '      }]\n' +
                    '    },\n' +
                    '    "scopeLogs": [{\n' +
                    '      "logRecords": [{\n' +
                    '        "timeUnixNano": "1234567890000000000",\n' +
                    '        "severityText": "INFO",\n' +
                    '        "body": { "stringValue": "This is a test log" }\n' +
                    '      }]\n' +
                    '    }]\n' +
                    '  }]\n' +
                    '}\'';
                $('#verify-command').text(curlCommand);
                Prism.highlightElement($('#verify-command')[0]);
                updateCodeDisplay();
                setCodeBlockContainerBackground();
                break;
            case 'Vector':
            case 'Logstash':
            case 'Fluentd':
            case 'Filebeat':
            case 'Promtail':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/' + selectedLogSource.toLowerCase());
                // Keep existing elastic bulk command
                var curlCommand = 'curl -X POST "' + baseUrl + '/elastic/_bulk" \\\n' +
                    '-H \'Content-Type: application/json\' \\\n' +
                    ingestCmd +
                    '-d \'{ "index" : { "_index" : "test" } }\n' +
                    '{ "name" : "john", "age":"23" }\'';
                $('#verify-command').text(curlCommand);
                Prism.highlightElement($('#verify-command')[0]);
                updateCodeDisplay();
                setCodeBlockContainerBackground();
                break;
            case 'Elastic Bulk':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd');
                // Keep existing elastic bulk command
                var curlCommand = 'curl -X POST "' + baseUrl + '/elastic/_bulk" \\\n' +
                    '-H \'Content-Type: application/json\' \\\n' +
                    ingestCmd +
                    '-d \'{ "index" : { "_index" : "test" } }\n' +
                    '{ "name" : "john", "age":"23" }\'';
                $('#verify-command').text(curlCommand);
                Prism.highlightElement($('#verify-command')[0]);
                updateCodeDisplay();
                setCodeBlockContainerBackground();
                break;
            case 'Splunk HEC':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd');
                curlCommand = 'curl -X POST "' + baseUrl + '/services/collector/event" \\\n' +
                    '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
                    ingestCmd +
                    '-d \'{ "index": "test", "name": "john", "age": "23"}\'';
                $('#verify-command').text(curlCommand);
                Prism.highlightElement($('#verify-command')[0]);
                updateCodeDisplay();
                setCodeBlockContainerBackground();
                break;
            case 'Send Test Data':
                $('#data-ingestion').hide();
                $('#test-data-btn').show();
                break;
            default:
                break;
        }
    });

    // Update the initial tab selection to be OpenTelemetry
    $('.custom-chart-tab ul li:first a').click();

    //Copy Handler
    $('.copyable').each(function () {
        var copyIcon = $('<span class="copy-icon"></span>');
        $(this).after(copyIcon);
    });

    // Replace the copy button handler in your JavaScript
    $('.copy-btn').on('click', function (_event) {
        var copyBtn = $(this);
        var textarea = copyBtn.siblings('#verify-command');
        var textToCopy = textarea.val();

        var tempInput = document.createElement("textarea");
        tempInput.value = textToCopy;
        document.body.appendChild(tempInput);
        tempInput.select();
        document.execCommand("copy");
        document.body.removeChild(tempInput);

        copyBtn.addClass('success');

        var originalSvg = copyBtn.html();

        copyBtn.html('<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>');

        setTimeout(function () {
            copyBtn.removeClass('success');
            copyBtn.html(originalSvg);
        }, 1000);
    });

    // Function to check if the code overflows and show/hide the expand button
    function checkCodeOverflow() {
        $('.code-wrapper').each(function() {
            var codeWrapper = $(this);
            var preElement = codeWrapper.find('pre');
            var expandBtn = codeWrapper.parent().find('.expand-btn');

            // Reset to collapsed state if not already expanded
            if (!codeWrapper.hasClass('expanded')) {
                expandBtn.find('span').text('Expand code');
                expandBtn.find('.expand-icon').css('transform', 'rotate(0deg)');
            }

            // Force a reflow to ensure accurate measurements
            preElement.width();

            var isVerticalOverflow = preElement[0].scrollHeight > codeWrapper.height();
            var isHorizontalOverflow = preElement[0].scrollWidth > preElement.width();


            if (isVerticalOverflow || isHorizontalOverflow) {
                expandBtn.css('display', 'flex');
            } else {
                expandBtn.css('display', 'none');
            }
        });
    }

    $('.expand-btn').on('click', function() {
        var expandBtn = $(this);
        var codeWrapper = expandBtn.parent().find('.code-wrapper');
        var isExpanded = codeWrapper.hasClass('expanded');

        if (isExpanded) {
            // Collapse the code block
            codeWrapper.removeClass('expanded');
            expandBtn.find('span').text('Expand code');
            expandBtn.find('.expand-icon').css('transform', 'rotate(0deg)');
        } else {
            // Expand the code block
            codeWrapper.addClass('expanded');
            expandBtn.find('span').text('Collapse code');
            expandBtn.find('.expand-icon').css('transform', 'rotate(180deg)');
        }
    });

    // Initial check for code overflow
    setTimeout(checkCodeOverflow, 100);

    // Add window resize handler
    $(window).on('resize', function() {
        checkCodeOverflow();
    });

    // Make sure to call this after any content changes
    function updateCodeDisplay() {
        if (window.Prism) {
            Prism.highlightAll();
        }
        setTimeout(checkCodeOverflow, 100);
    }

    {{ .Button1Function }}
})


function sendTestData() {
    sendTestDataWithoutBearerToken().then((_res) => {
        showToast('Sent Test Data Successfully', 'success');
        let testDataBtn = document.getElementById("test-data-btn");
        testDataBtn.disabled = false;
    })
        .catch((err) => {
            console.log(err)
            showToast('Error Sending Test Data', 'error');
            let testDataBtn = document.getElementById("test-data-btn");
            testDataBtn.disabled = false;
        });
}

function sendTestDataWithoutBearerToken() {
    return new Promise((resolve, reject) => {
        $.ajax({
            method: 'post',
            url: '/api/sampledataset_bulk',
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


function myOrgSendTestData(_token) {
    $('#test-data-btn').on('click', (_e) => {
        if (selectedLogSource === 'Send Test Data') {
            var testDataBtn = document.getElementById("test-data-btn");
            testDataBtn.disabled = true;
            sendTestData();
        }
    })
}
