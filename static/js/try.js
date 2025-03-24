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

$(document).ready(async function () {
    $('#data-ingestion, #test-data-btn').hide();
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
            baseUrl = config.IngestUrl;
        }
        {{ if .TestDataSendData }}
            {{ .TestDataSendData }}
        {{ else }}
            myOrgSendTestData(iToken);
        {{ end }}
    } catch (err) {
        console.log(err);
    }

    $('#custom-chart-tab').on('click', '.tab-li', function () {
        selectedLogSource = $(this).text().trim();
        $('.tab-li').removeClass("active");
        $(this).addClass("active");

        var ingestCmd = "";
        {{ if .IngestDataCmd }}
            {{ .IngestDataCmd }}
        {{ end }}

        $('#test-data-btn').hide();
        $('#data-ingestion').show();

        switch (selectedLogSource) {
            case 'OpenTelemetry':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/opentelemetry');
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
                break;
            case 'Vector':
            case 'Logstash':
            case 'Fluentd':
            case 'Filebeat':
            case 'Promtail':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/' + selectedLogSource.toLowerCase());
                var curlCommand = 'curl -X POST "' + baseUrl + '/elastic/_bulk" \\\n' +
                    '-H \'Content-Type: application/json\' \\\n' +
                    ingestCmd +
                    '-d \'{ "index" : { "_index" : "test" } }\n' +
                    '{ "name" : "john", "age":"23" }\'';
                $('#verify-command').text(curlCommand);
                Prism.highlightElement($('#verify-command')[0]);
                break;
            case 'Elastic Bulk':
                $('#platform-input').val(selectedLogSource);
                $('#setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd');
                var curlCommand = 'curl -X POST "' + baseUrl + '/elastic/_bulk" \\\n' +
                    '-H \'Content-Type: application/json\' \\\n' +
                    ingestCmd +
                    '-d \'{ "index" : { "_index" : "test" } }\n' +
                    '{ "name" : "john", "age":"23" }\'';
                $('#verify-command').text(curlCommand);
                Prism.highlightElement($('#verify-command')[0]);
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

    // Copy Handler for the new copy button
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
        setTimeout(function () {
            copyBtn.removeClass('success');
        }, 1000);
    });

    {{ .Button1Function }}
});