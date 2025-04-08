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
//  */
let selectedLogSource = "";
let selectedMetricsSource = "";
let selectedTracesSource = "";
let iToken = "";

$(document).ready(async function () {
    const urlParams = new URLSearchParams(window.location.search);
    const method = urlParams.get('method');

    // Initial setup for logs ingestion
    $('#data-ingestion').hide();
    $('#sample-data').show();  // Show sample-data div by default
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
            myOrgSendTestData();
        {{ end }}
    } catch (err) {
        console.log(err);
    }

    if (method) {
        handleMethodUrlParam(method, baseUrl);
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

    $('#test-data-btn').on('click', (_e) => {
        if (selectedLogSource === 'Send Test Data') {
            var testDataBtn = document.getElementById("test-data-btn");
            testDataBtn.disabled = true;
            sendTestData();
        }
    });

    {{ .Button1Function }}
});

// Function to handle method parameter in URL
function handleMethodUrlParam(method, baseUrl) {
    method = method.toLowerCase();

    // For log ingestion methods
    if (['opentelemetry', 'vector', 'logstash', 'fluentd', 'filebeat', 'promtail', 'elasticbulk', 'splunkhec', 'testdata'].includes(method)) {
        // Map URL parameter to the actual data-source value used in the UI
        let mappedSource;
        switch (method) {
            case 'opentelemetry':
                mappedSource = 'OpenTelemetry';
                break;
            case 'vector':
                mappedSource = 'Vector';
                break;
            case 'logstash':
                mappedSource = 'Logstash';
                break;
            case 'fluentd':
                mappedSource = 'Fluentd';
                break;
            case 'filebeat':
                mappedSource = 'Filebeat';
                break;
            case 'promtail':
                mappedSource = 'Promtail';
                break;
            case 'elasticbulk':
                mappedSource = 'Elastic Bulk';
                break;
            case 'splunkhec':
                mappedSource = 'Splunk HEC';
                break;
            case 'testdata':
                mappedSource = 'Send Test Data';
                break;
        }

        // Find the matching card and simulate a click on it
        const matchingCard = $(`.ingestion-card.logs-card[data-source="${mappedSource}"]`);
        if (matchingCard.length) {
            selectedLogSource = mappedSource;
            $('#logs-cards-view').hide();
            $('#logs-ingestion-details').show();

            // Show/hide appropriate sections
            if (selectedLogSource === 'Send Test Data') {
                $('#data-ingestion').hide();
                $('#sample-data').show();
            } else {
                $('#data-ingestion').show();
                $('#sample-data').hide();
            }

            // Set up the appropriate URL and commands based on the selected log source
            setupLogSourceDetails(selectedLogSource, baseUrl);

            // Scroll to details section
            $('html, body').animate({
                scrollTop: $("#logs-ingestion-details").offset().top
            }, 500);
        }
    }

    // For metrics ingestion methods
    if (['vectormetrics', 'opentelemetrymetrics'].includes(method)) {
        // Map URL parameter to the actual data-source value used in the UI
        let mappedSource;
        switch (method) {
            case 'vectormetrics':
                mappedSource = 'VectorMetrics';
                break;
            case 'opentelemetrymetrics':
                mappedSource = 'Opentelemetry';
                break;
        }

        // Find the matching card and simulate its behavior
        const matchingCard = $(`.ingestion-card.metrics-card[data-source="${mappedSource}"]`);
        if (matchingCard.length) {
            selectedMetricsSource = mappedSource;
            $('#metrics-cards-view').hide();
            $('#metrics-ingestion-details').show();

            // Set up the appropriate URL based on the selected metrics source
            setupMetricsSourceDetails(selectedMetricsSource);

            // Scroll to details section
            $('html, body').animate({
                scrollTop: $("#metrics-ingestion-details").offset().top
            }, 500);
        }
    }

    // For traces ingestion methods
    if (['goapp', 'javaapp', 'pythonapp', 'dotnetapp', 'javascriptapp'].includes(method)) {
        // Map URL parameter to the actual data-source value used in the UI
        let mappedSource;
        switch (method) {
            case 'goapp':
                mappedSource = 'Go App';
                break;
            case 'javaapp':
                mappedSource = 'Java App';
                break;
            case 'pythonapp':
                mappedSource = 'Python App';
                break;
            case 'dotnetapp':
                mappedSource = '.Net App';
                break;
            case 'javascriptapp':
                mappedSource = 'Javascript App';
                break;
        }

        // Find the matching card and simulate its behavior
        const matchingCard = $(`.ingestion-card.traces-card[data-source="${mappedSource}"]`);
        if (matchingCard.length) {
            selectedTracesSource = mappedSource;
            $('#traces-cards-view').hide();
            $('#traces-ingestion-details').show();

            // Set up the appropriate URL based on the selected traces source
            setupTracesSourceDetails(selectedTracesSource);

            // Scroll to details section
            $('html, body').animate({
                scrollTop: $("#traces-ingestion-details").offset().top
            }, 500);
        }
    }
}

// Helper function to set up log source details
function setupLogSourceDetails(source, baseUrl) {
    $('#platform-input').val(source);

    var ingestCmd = "";
    {{ if .IngestDataCmd }}
        {{ .IngestDataCmd }}
    {{ end }}

    var curlCommand = 'curl -X POST "' + baseUrl + '/elastic/_bulk" \\\n' +
        '-H \'Content-Type: application/json\' \\\n' +
        ingestCmd +
        '-d \'{ "index" : { "_index" : "test" } }\n' +
        '{ "name" : "john", "age":"23" }\'';

    // Update setup instructions link and curl command based on selected logs source
    switch (source) {
        case 'OpenTelemetry':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry');
            break;
        case 'Vector':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/vector');
            break;
        case 'Logstash':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/logstash');
            break;
        case 'Fluentd':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/fluentd');
            break;
        case 'Filebeat':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/filebeat');
            break;
        case 'Promtail':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/promtail');
            break;
        case 'Elastic Bulk':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd');
            break;
        case 'Splunk HEC':
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd');
            curlCommand = 'curl -X POST "' + baseUrl + '/services/collector/event" \\\n' +
                '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
                ingestCmd +
                '-d \'{ "index": "test", "name": "john", "age": "23"}\'';
            break;
    }

    $('#verify-command').text(curlCommand);
}

// Helper function to set up metrics source details
function setupMetricsSourceDetails(source) {
    switch (source) {
        case 'VectorMetrics':
            $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/vector-metrics');
            break;
        case 'Opentelemetry':
            $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/open-telemetry');
            break;
    }
}

// Helper function to set up traces source details
function setupTracesSourceDetails(source) {
    switch (source) {
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
    }
}

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

function myOrgSendTestData() {
    // Remove any existing click handlers to prevent duplicates
    $('#test-data-btn').off('click').on('click', () => {
        const testDataBtn = document.getElementById("test-data-btn");
        testDataBtn.disabled = true;
        sendTestData();
    });
}

// class IngestionView {
//     constructor() {
//         // Parse URL parameters
//         const urlParams = new URLSearchParams(window.location.search);
//         this.method = urlParams.get('method') || 'default'; // Default to 'default' if no method is provided
//         this.selectedLogSource = '';
//         this.selectedMetricsSource = '';
//         this.selectedTracesSource = '';
//         this.baseUrl = '';
//         this.iToken = '';

//         // Initialize the view
//         this.init();
//     }

//     init() {
//         // Hide/show initial sections
//         $('#data-ingestion').hide();
//         $('#sample-data').show();
//         $('.theme-btn').on('click', themePickerHandler);
//         $(".custom-chart-tab").tabs();
//         $(".custom-chart-tab").show();

//         // Load configuration and set base URL
//         this.loadConfig();

//         // Setup event handlers based on the method
//         switch (this.method.toLowerCase()) {
//             case 'opentelemetry':
//                 this.initOpenTelemetryView();
//                 break;
//             case 'vector':
//                 this.initVectorView();
//                 break;
//             case 'logstash':
//                 this.initLogstashView();
//                 break;
//             case 'fluentd':
//                 this.initFluentdView();
//                 break;
//             case 'filebeat':
//                 this.initFilebeatView();
//                 break;
//             case 'promtail':
//                 this.initPromtailView();
//                 break;
//             case 'elasticbulk':
//                 this.initElasticBulkView();
//                 break;
//             case 'splunkhec':
//                 this.initSplunkHecView();
//                 break;
//             case 'testdata':
//                 this.initTestDataView();
//                 break;
//             default:
//                 this.initDefaultView();
//                 break;
//         }

//         // Setup common event handlers
//         this.setupEventHandlers();
//     }

//     async loadConfig() {
//         try {
//             const config = await $.ajax({
//                 method: 'GET',
//                 url: 'api/config',
//                 crossDomain: true,
//                 dataType: 'json',
//                 xhrFields: { withCredentials: true }
//             });
//             if (config.IngestUrl) {
//                 this.baseUrl = config.IngestUrl;
//             }
//         } catch (err) {
//             console.log('Error loading config:', err);
//         }
//     }

//     setupEventHandlers() {
//         // Handle LOGS card clicks
//         $('.ingestion-card.logs-card').on('click', (e) => {
//             this.selectedLogSource = $(e.currentTarget).data('source');
//             $('#logs-cards-view').hide();
//             $('#logs-ingestion-details').show();
//             this.updateLogsDetailsView();
//         });

//         // Handle back button
//         $('#back-to-logs-cards').on('click', () => {
//             $('#logs-ingestion-details').hide();
//             $('#logs-cards-view').show();
//             this.scrollToTop('#logs-cards-view');
//         });

//         // Handle METRICS card clicks
//         $('.ingestion-card.metrics-card').on('click', (e) => {
//             this.selectedMetricsSource = $(e.currentTarget).data('source');
//             $('#metrics-cards-view').hide();
//             $('#metrics-ingestion-details').show();
//             this.updateMetricsDetailsView();
//         });

//         $('#back-to-metrics-cards').on('click', () => {
//             $('#metrics-ingestion-details').hide();
//             $('#metrics-cards-view').show();
//             this.scrollToTop('#metrics-cards-view');
//         });

//         // Handle TRACES card clicks
//         $('.ingestion-card.traces-card').on('click', (e) => {
//             this.selectedTracesSource = $(e.currentTarget).data('source');
//             $('#traces-cards-view').hide();
//             $('#traces-ingestion-details').show();
//             this.updateTracesDetailsView();
//         });

//         $('#back-to-traces-cards').on('click', () => {
//             $('#traces-ingestion-details').hide();
//             $('#traces-cards-view').show();
//             this.scrollToTop('#traces-cards-view');
//         });

//         // Copy handler
//         this.setupCopyHandler();

//         // Test data button
//         $('#test-data-btn').on('click', () => {
//             if (this.selectedLogSource === 'Send Test Data') {
//                 const testDataBtn = document.getElementById("test-data-btn");
//                 testDataBtn.disabled = true;
//                 this.sendTestData();
//             }
//         });
//     }

//     updateLogsDetailsView() {
//         this.scrollToTop('#logs-ingestion-details');
//         if (this.selectedLogSource === 'Send Test Data') {
//             $('#data-ingestion').hide();
//             $('#sample-data').show();
//         } else {
//             $('#data-ingestion').show();
//             $('#sample-data').hide();
//         }
//         this.updateCurlCommand();
//         this.updateSetupInstructions();
//     }

//     updateMetricsDetailsView() {
//         this.scrollToTop('#metrics-ingestion-details');
//         switch (this.selectedMetricsSource) {
//             case 'VectorMetrics':
//                 $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/vector-metrics');
//                 break;
//             case 'Opentelemetry':
//                 $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/open-telemetry');
//                 break;
//         }
//     }

//     updateTracesDetailsView() {
//         this.scrollToTop('#traces-ingestion-details');
//         switch (this.selectedTracesSource) {
//             case 'Go App':
//                 $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/go-app');
//                 break;
//             case 'Java App':
//                 $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/java-app');
//                 break;
//             case 'Python App':
//                 $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/python-app');
//                 break;
//             case '.Net App':
//                 $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/dotnet-app');
//                 break;
//             case 'Javascript App':
//                 $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/js-app');
//                 break;
//             default:
//                 break;
//             // Add other cases as needed
//         }
//     }

//     updateCurlCommand() {
//         let curlCommand = '';
//         if (this.selectedLogSource === 'Splunk HEC') {
//             curlCommand = `curl -X POST "${this.baseUrl}/services/collector/event" \\\n` +
//                 '-H "Authorization: A94A8FE5CCB19BA61C4C08" \\\n' +
//                 '-d \'{"index": "test", "name": "john", "age": "23"}\'';
//         } else {
//             curlCommand = `curl -X POST "${this.baseUrl}/elastic/_bulk" \\\n` +
//                 '-H \'Content-Type: application/json\' \\\n' +
//                 '-d \'{"index": {"_index": "test"}}\n' +
//                 '{"name": "john", "age": "23"}\'';
//         }
//         $('#verify-command').text(curlCommand);
//     }

//     updateSetupInstructions() {
//         const links = {
//             'OpenTelemetry': 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry',
//             'Vector': 'https://www.siglens.com/siglens-docs/log-ingestion/vector',
//             'Logstash': 'https://www.siglens.com/siglens-docs/log-ingestion/logstash',
//             'Fluentd': 'https://www.siglens.com/siglens-docs/log-ingestion/fluentd',
//             'Filebeat': 'https://www.siglens.com/siglens-docs/log-ingestion/filebeat',
//             'Promtail': 'https://www.siglens.com/siglens-docs/log-ingestion/promtail',
//             'Elastic Bulk': 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd',
//             'Splunk HEC': 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd'
//         };
//         $('#platform-input').val(this.selectedLogSource);
//         if (links[this.selectedLogSource]) {
//             $('#logs-setup-instructions-link').attr('href', links[this.selectedLogSource]);
//         }
//     }

//     initOpenTelemetryView() {
//         this.selectedLogSource = 'OpenTelemetry';
//         $('#logs-cards-view').hide();
//         $('#logs-ingestion-details').show();
//         this.updateLogsDetailsView();
//     }

//     // Add similar methods for other ingestion types
//     initVectorView() { /* Similar to initOpenTelemetryView */ }
//     initLogstashView() { /* Similar to initOpenTelemetryView */ }
//     initFluentdView() { /* Similar to initOpenTelemetryView */ }
//     initFilebeatView() { /* Similar to initOpenTelemetryView */ }
//     initPromtailView() { /* Similar to initOpenTelemetryView */ }
//     initElasticBulkView() { /* Similar to initOpenTelemetryView */ }
//     initSplunkHecView() { /* Similar to initOpenTelemetryView */ }
//     initTestDataView() { /* Similar to initOpenTelemetryView */ }
//     initDefaultView() {
//         $('#logs-cards-view').show();
//         $('#logs-ingestion-details').hide();
//     }

//     scrollToTop(selector) {
//         $('html, body').animate({
//             scrollTop: $(selector).offset().top
//         }, 500);
//     }

//     setupCopyHandler() {
//         $('.copyable').each(function () {
//             var copyIcon = $('<span class="copy-icon"></span>');
//             $(this).after(copyIcon);
//         });

//         $('.copy-icon').on('click', function () {
//             var copyIcon = $(this);
//             var inputValue = copyIcon.prev('.copyable').val();
//             navigator.clipboard.writeText(inputValue).then(() => {
//                 copyIcon.addClass('success');
//                 setTimeout(() => copyIcon.removeClass('success'), 1000);
//             });
//         });
//     }

//     sendTestData() {
//         sendTestDataWithoutBearerToken().then(() => {
//             showToast('Sent Test Data Successfully', 'success');
//             document.getElementById("test-data-btn").disabled = false;
//         }).catch((err) => {
//             console.log(err);
//             showToast('Error Sending Test Data', 'error');
//             document.getElementById("test-data-btn").disabled = false;
//         });
//     }
// }

// // Instantiate the class when the document is ready
// $(document).ready(() => {
//     new IngestionView();
// });