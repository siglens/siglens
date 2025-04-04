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
let selectedMetricsSource = "";
let selectedTracesSource = "";
let iToken = "";

$(document).ready(async function () {
    // Initial setup for logs ingestion
    $('#data-ingestion').hide();
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

    // Handle LOGS card clicks to navigate to details view
    $('.ingestion-card.logs-card').on('click', function () {
        selectedLogSource = $(this).data('source');
        $('#logs-cards-view').hide();
        $('#logs-ingestion-details').show();

        // Scroll to the top of the logs details section
        $('html, body').animate({
            scrollTop: $("#logs-ingestion-details").offset().top
        }, 500);

        // Show/hide appropriate sections based on the selected logs card
        if (selectedLogSource === 'Send Test Data') {
            $('#data-ingestion').hide();
            $('#sample-data').show();
        } else {
            $('#data-ingestion').show();
            $('#sample-data').hide();
        }

        var ingestCmd = "";
        {{ if .IngestDataCmd }}
            {{ .IngestDataCmd }}
        {{ end }}

        var curlCommand = 'curl -X POST "' + baseUrl + '/elastic/_bulk" \\\n' +
            '-H \'Content-Type: application/json\' \\\n' +
            ingestCmd +
            '-d \'{ "index" : { "_index" : "test" } }\n' +
            '{ "name" : "john", "age":"23" }\'';
        $('#verify-command').text(curlCommand);

        // Update setup instructions based on selected logs source
        switch (selectedLogSource) {
            case 'OpenTelemetry':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry');
                break;
            case 'Vector':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/vector');
                break;
            case 'Logstash':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/logstash');
                break;
            case 'Fluentd':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/fluentd');
                break;
            case 'Filebeat':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/filebeat');
                break;
            case 'Promtail':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/promtail');
                break;
            case 'Elastic Bulk':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd');
                break;
            case 'Splunk HEC':
                $('#platform-input').val(selectedLogSource);
                $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd');
                curlCommand = 'curl -X POST "' + baseUrl + '/services/collector/event" \\\n' +
                    '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
                    ingestCmd +
                    '-d \'{ "index": "test", "name": "john", "age": "23"}\'';
                $('#verify-command').text(curlCommand);
                break;
            case 'Send Test Data':
                // No setup instructions for Test Data
                break;
            default:
                break;
        }
    });

    // Handle back button to return to logs cards view
    $('#back-to-logs-cards').on('click', function () {
        $('#logs-ingestion-details').hide();
        $('#logs-cards-view').show();

        $('html, body').animate({
            scrollTop: $("#logs-cards-view").offset().top
        }, 500);
    });

    // Handle METRICS card clicks to navigate to details view
    $('.ingestion-card.metrics-card').on('click', function () {
        selectedMetricsSource = $(this).data('source');
        $('#metrics-cards-view').hide();
        $('#metrics-ingestion-details').show();

        $('html, body').animate({
            scrollTop: $("#metrics-ingestion-details").offset().top
        }, 500);

        switch (selectedMetricsSource) {
            case 'VectorMetrics':
                $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/vector-metrics');
                break;
            case 'Opentelemetry':
                $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/open-telemetry');
                break;
            default:
                break;
        }
    });

    $('#back-to-metrics-cards').on('click', function () {
        $('#metrics-ingestion-details').hide();
        $('#metrics-cards-view').show();

        $('html, body').animate({
            scrollTop: $("#metrics-cards-view").offset().top
        }, 500);
    });

    // Handle TRACES card clicks to navigate to details view
    $('.ingestion-card.traces-card').on('click', function () {
        selectedTracesSource = $(this).data('source');
        $('#traces-cards-view').hide();
        $('#traces-ingestion-details').show();

        $('html, body').animate({
            scrollTop: $("#traces-ingestion-details").offset().top
        }, 500);

        // Update setup instructions based on selected traces source
        switch (selectedTracesSource) {
            case 'Go App':
                $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/go-app');
                break;
            case 'Java App':
                $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/java-app');
                break;
            case 'Python App':
                $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/python-app');
                break;
            case '.Net App':
                $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/dotnet-app');
                break;
            case 'Javascript App':
                $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/js-app');
                break;
            default:
                break;
        }
    });

    $('#back-to-traces-cards').on('click', function () {
        $('#traces-ingestion-details').hide();
        $('#traces-cards-view').show();

        $('html, body').animate({
            scrollTop: $("#traces-cards-view").offset().top
        }, 500);
    });

    // Copy Handler (used by both logs and metrics)
    $('.copyable').each(function () {
        var copyIcon = $('<span class="copy-icon"></span>');
        $(this).after(copyIcon);
    });

    $('.copy-icon').on('click', function (_event) {
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
        setTimeout(function () {
            copyIcon.removeClass('success');
        }, 1000);
    });

    {{ .Button1Function }}
});

function sendTestData() {
    sendTestDataWithoutBearerToken().then((_res) => {
        showToast('Sent Test Data Successfully', 'success');
        let testDataBtn = document.getElementById("test-data-btn");
        testDataBtn.disabled = false;
    })
        .catch((err) => {
            console.log(err);
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
    });
}