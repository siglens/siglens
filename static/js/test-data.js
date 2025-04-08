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
    let pageName = window.location.pathname.split('/').pop() || 'index.html';
    const pageConfig = navigationStructure[pageName];

    if (pageConfig && pageConfig.breadcrumbs) {
        initializeBreadcrumbs(pageConfig.breadcrumbs);
    }

    // Get method parameters from URL if they exist
    const urlParams = new URLSearchParams(window.location.search);
    const logMethodParam = urlParams.get('method');
    const metricsMethodParam = urlParams.get('method');
    const tracesMethodParam = urlParams.get('method');

    // Determine which page we're on based on the URL
    const currentPath = window.location.pathname;
    const isMetricsPage = currentPath.includes('metrics-ingestion.html');
    const isTracesPage = currentPath.includes('traces-ingestion.html');
    const isLogsPage = !isMetricsPage && !isTracesPage; // Default to logs page

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

    // Handle LOGS card clicks to navigate to details view
    $('.ingestion-card.logs-card').on('click', function () {
        selectedLogSource = $(this).data('source');
        navigateToLogDetails(selectedLogSource, baseUrl);
    });

    // Handle METRICS card clicks to navigate to details view
    $('.ingestion-card.metrics-card').on('click', function () {
        selectedMetricsSource = $(this).data('source');
        navigateToMetricsDetails(selectedMetricsSource);
    });

    // Handle TRACES card clicks to navigate to details view
    $('.ingestion-card.traces-card').on('click', function () {
        selectedTracesSource = $(this).data('source');
        navigateToTracesDetails(selectedTracesSource);
    });

    // Process URL parameters based on the current page
    if (isLogsPage && logMethodParam) {
        const formattedMethod = formatLogMethodName(logMethodParam);
        const matchingCard = $(`.ingestion-card.logs-card[data-source="${formattedMethod}"]`);
        if (matchingCard.length) {
            selectedLogSource = formattedMethod;
            navigateToLogDetails(selectedLogSource, baseUrl);
        }
    } else if (isMetricsPage && metricsMethodParam) {
        const formattedMethod = formatMetricsMethodName(metricsMethodParam);
        const matchingCard = $(`.ingestion-card.metrics-card[data-source="${formattedMethod}"]`);
        if (matchingCard.length) {
            selectedMetricsSource = formattedMethod;
            navigateToMetricsDetails(selectedMetricsSource);
        }
    } else if (isTracesPage && tracesMethodParam) {
        const formattedMethod = formatTracesMethodName(tracesMethodParam);
        const matchingCard = $(`.ingestion-card.traces-card[data-source="${formattedMethod}"]`);
        if (matchingCard.length) {
            selectedTracesSource = formattedMethod;
            navigateToTracesDetails(selectedTracesSource);
        }
    }

    if (logMethodParam && pageName === 'test-data.html') {
        updateBreadcrumbsForIngestion('Log Ingestion Methods', formattedMethod);
    }
    if (logMethodParam && pageName === 'metrics-ingestion.html') {
        updateBreadcrumbsForIngestion('Metrics Ingestion Methods', formattedMethod);
    }
    if (logMethodParam && pageName === 'traces-ingestion.html') {
        updateBreadcrumbsForIngestion('Traces Ingestion Methods', formattedMethod);
    }

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

// Helper functions to format method names from URL parameters
function formatLogMethodName(methodParam) {
    // Handle special cases
    if (methodParam.toLowerCase() === 'opentelemetry') {
        return 'OpenTelemetry';
    } else if (methodParam.toLowerCase() === 'elasticbulk') {
        return 'Elastic Bulk';
    } else if (methodParam.toLowerCase() === 'splunkhec') {
        return 'Splunk HEC';
    } else if (methodParam.toLowerCase() === 'sendtestdata') {
        return 'Send Test Data';
    } else {
        // Capitalize first letter for simple cases
        return methodParam.charAt(0).toUpperCase() + methodParam.slice(1);
    }
}

function formatMetricsMethodName(methodParam) {
    // Handle special cases for metrics
    if (methodParam.toLowerCase() === 'vectormetrics') {
        return 'VectorMetrics';
    } else if (methodParam.toLowerCase() === 'opentelemetry') {
        return 'Opentelemetry';
    } else {
        // Capitalize first letter for simple cases
        return methodParam.charAt(0).toUpperCase() + methodParam.slice(1);
    }
}

function formatTracesMethodName(methodParam) {
    // Handle special cases for traces
    if (methodParam.toLowerCase() === 'goapp') {
        return 'Go App';
    } else if (methodParam.toLowerCase() === 'javaapp') {
        return 'Java App';
    } else if (methodParam.toLowerCase() === 'pythonapp') {
        return 'Python App';
    } else if (methodParam.toLowerCase() === 'dotnetapp') {
        return '.Net App';
    } else if (methodParam.toLowerCase() === 'javascriptapp') {
        return 'Javascript App';
    } else {
        // Capitalize first letter for simple cases
        return methodParam.charAt(0).toUpperCase() + methodParam.slice(1);
    }
}

// Function to handle navigation to log details
function navigateToLogDetails(source, baseUrl) {
    $('#logs-cards-view').hide();
    $('#logs-ingestion-details').show();

    // Scroll to the top of the logs details section
    $('html, body').animate({
        scrollTop: $("#logs-ingestion-details").offset().top
    }, 500);

    updateBreadcrumbsForIngestion('Log Ingestion Methods', source);

    // Show/hide appropriate sections based on the selected logs card
    if (source === 'Send Test Data') {
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
    switch (source) {
        case 'OpenTelemetry':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry');
            // Update URL with the method parameter without refreshing the page
            updateUrlParameter('method', 'opentelemetry');
            break;
        case 'Vector':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/vector');
            updateUrlParameter('method', 'vector');
            break;
        case 'Logstash':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/logstash');
            updateUrlParameter('method', 'logstash');
            break;
        case 'Fluentd':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/fluentd');
            updateUrlParameter('method', 'fluentd');
            break;
        case 'Filebeat':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/filebeat');
            updateUrlParameter('method', 'filebeat');
            break;
        case 'Promtail':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/log-ingestion/promtail');
            updateUrlParameter('method', 'promtail');
            break;
        case 'Elastic Bulk':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd');
            updateUrlParameter('method', 'elasticbulk');
            break;
        case 'Splunk HEC':
            $('#platform-input').val(source);
            $('#logs-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd');
            updateUrlParameter('method', 'splunkhec');
            curlCommand = 'curl -X POST "' + baseUrl + '/services/collector/event" \\\n' +
                '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
                ingestCmd +
                '-d \'{ "index": "test", "name": "john", "age": "23"}\'';
            $('#verify-command').text(curlCommand);
            break;
        case 'Send Test Data':
            // No setup instructions for Test Data
            updateUrlParameter('method', 'sendtestdata');
            break;
        default:
            break;
    }
}

// Function to handle navigation to metrics details
function navigateToMetricsDetails(source) {
    $('#metrics-cards-view').hide();
    $('#metrics-ingestion-details').show();

    $('html, body').animate({
        scrollTop: $("#metrics-ingestion-details").offset().top
    }, 500);

    updateBreadcrumbsForIngestion('Metrics Ingestion Methods', selectedMetricsSource);

    // Update setup instructions based on selected metrics source
    switch (source) {
        case 'VectorMetrics':
            $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/vector-metrics');
            updateUrlParameter('method', 'vectorMetrics');
            break;
        case 'Opentelemetry':
            $('#metrics-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/metric-ingestion/open-telemetry');
            updateUrlParameter('method', 'opentelemetry');
            break;
        default:
            break;
    }
}

// Function to handle navigation to traces details
function navigateToTracesDetails(source) {
    $('#traces-cards-view').hide();
    $('#traces-ingestion-details').show();

    $('html, body').animate({
        scrollTop: $("#traces-ingestion-details").offset().top
    }, 500);

    updateBreadcrumbsForIngestion('Traces Ingestion Methods', selectedTracesSource);

    // Update setup instructions based on selected traces source
    switch (source) {
        case 'Go App':
            $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/go-app');
            updateUrlParameter('method', 'goApp');
            break;
        case 'Java App':
            $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/java-app');
            updateUrlParameter('method', 'javaApp');
            break;
        case 'Python App':
            $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/python-app');
            updateUrlParameter('method', 'pythonApp');
            break;
        case '.Net App':
            $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/dotnet-app');
            updateUrlParameter('method', 'dotnetApp');
            break;
        case 'Javascript App':
            $('#traces-setup-instructions-link').attr('href', 'https://www.siglens.com/siglens-docs/instrument-traces/js-app');
            updateUrlParameter('method', 'javascriptApp');
            break;
        default:
            break;
    }
}

// Function to update URL parameters without page refresh
function updateUrlParameter(key, value) {
    const url = new URL(window.location.href);
    url.searchParams.set(key, value);
    window.history.pushState({}, '', url);
}

function updateBreadcrumbsForIngestion(baseTitle, selectedSource) {
    const breadcrumbConfig = [
        { name: baseTitle, url: window.location.pathname },
        { name: selectedSource }
    ];
    initializeBreadcrumbs(breadcrumbConfig);
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