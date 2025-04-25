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
let selectedLogSource = '';
let selectedMetricsSource = '';
let selectedTracesSource = '';
let iToken = '';

$(document).ready(async function () {
    let pageName = window.location.pathname.split('/').pop() || 'index.html';
    const pageConfig = navigationStructure[pageName];

    if (pageConfig && pageConfig.breadcrumbs) {
        initializeBreadcrumbs(pageConfig.breadcrumbs);
    }

    const urlParams = new URLSearchParams(window.location.search);
    const methodParam = urlParams.get('method');

    const currentPath = window.location.pathname;
    const isMetricsPage = currentPath.includes('metrics-ingestion.html');
    const isTracesPage = currentPath.includes('traces-ingestion.html');
    const isLogsPage = !isMetricsPage && !isTracesPage;
    
    $('.theme-btn').on('click', themePickerHandler);

    let ingestURL = '';
    try {
        const config = await $.ajax({
            method: 'GET',
            url: 'api/config',
            crossDomain: true,
            dataType: 'json',
            xhrFields: { withCredentials: true },
        });
        if (config.IngestUrl) {
            ingestURL = config.IngestUrl;
        }
        {{ if .TestDataSendData }}
            {{ .TestDataSendData }}
        {{ else }}
            $('#test-data-btn')
            .off('click')
            .on('click', () => {
                const testDataBtn = document.getElementById('test-data-btn');
                testDataBtn.disabled = true;
                sendTestData();
            });
        {{ end }}
    } catch (err) {
        console.log(err);
    }

    $('.ingestion-card.logs-card').on('click', function () {
        selectedLogSource = $(this).data('source');
        navigateToLogDetails(selectedLogSource, ingestURL);
    });

    $('.ingestion-card.metrics-card').on('click', function () {
        selectedMetricsSource = $(this).data('source');
        navigateToMetricsDetails(selectedMetricsSource);
    });

    $('.ingestion-card.traces-card').on('click', function () {
        selectedTracesSource = $(this).data('source');
        navigateToTracesDetails(selectedTracesSource);
    });

    if (methodParam) {
        let formattedMethod;

        if (isLogsPage) {
            formattedMethod = formatMethodName(methodParam, 'log');
            const matchingCard = $(`.ingestion-card.logs-card[data-source="${formattedMethod}"]`);
            if (matchingCard.length) {
                selectedLogSource = formattedMethod;
                navigateToLogDetails(selectedLogSource, ingestURL);
            }
        }

        if (pageName === 'log-ingestion.html') {
            updateBreadcrumbsForIngestion('Log Ingestion Methods', formattedMethod);
        } else if (pageName === 'metrics-ingestion.html') {
            updateBreadcrumbsForIngestion('Metrics Ingestion Methods', formattedMethod);
        } else if (pageName === 'traces-ingestion.html') {
            updateBreadcrumbsForIngestion('Traces Ingestion Methods', formattedMethod);
        }
    }

    setupCopyFunctionality();

    {{ .Button1Function }}
});

function formatMethodName(methodParam, type) {
    methodParam = methodParam.toLowerCase();
    
    if (methodParam === 'opentelemetry') return 'OpenTelemetry Collector';
    
    if (type === 'log') {
        if (methodParam === 'elasticbulk') return 'Elastic Bulk';
        if (methodParam === 'splunkhec') return 'Splunk HEC';
        if (methodParam === 'sendtestdata') return 'Send Test Data';
        if (methodParam === 'verifyconnection') return 'Verify Connection';
    }
    
    return methodParam.charAt(0).toUpperCase() + methodParam.slice(1);
}

function navigateToLogDetails(source, ingestURL) {
    if (source === 'Promtail' || source === 'OpenTelemetry Collector' || source === 'Filebeat' || source === 'Fluentd' || source === 'Fluent-bit' || source === 'Logstash' || source === 'Vector') {
        switch (source) {
            case 'Promtail':
                mdFileName = 'promtail';
                break;
            case 'OpenTelemetry Collector':
                mdFileName = 'opentelemetry';
                break;
            case 'Filebeat':
                mdFileName = 'filebeat';
                break;
            case 'Fluentd':
                mdFileName = 'fluentd';
                break;
            case 'Fluent-bit':
                mdFileName = 'fluent-bit';
                break;
            case 'Logstash':
                mdFileName = 'logstash';
                break;
            case 'Vector':
                mdFileName = 'vector';
                break;
        }
        
        window.open(`instructions.html?type=logs&method=${mdFileName}`, '_self');
        return;
    }

    $('#logs-cards-view').hide();
    $('#logs-ingestion-details').show();

    $('html, body').animate(
        {
            scrollTop: $('#logs-ingestion-details').offset().top,
        },
        500
    );

    updateBreadcrumbsForIngestion('Log Ingestion Methods', source);
    $('#data-ingestion, #sample-data, #verify-connection').hide();

    if (source === 'Send Test Data') {
        $('#sample-data').show();
    } else if (source === 'Verify Connection'){
        $('#verify-connection').show();
    }else {
        $('#data-ingestion').show();
    }

    var ingestCmd = "";
    var esBulkCommand = 'curl -X POST "' + ingestURL + '/elastic/_bulk" \\\n' +
        '-H \'Content-Type: application/json\' \\\n' +
        ingestCmd +
        '-d \'{ "index" : { "_index" : "test" } }\n' +
        '{ "name" : "john", "age":"23" }\'';
    
    var hecCommand = 'curl -X POST "' + ingestURL + '/services/collector/event" \\\n' +
        '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
        ingestCmd +
        '-d \'{ "index": "test", "name": "john", "age": "23"}\'';
    
    $('#verify-command-esbulk').text(esBulkCommand);
    $('#verify-command-hec').text(hecCommand);
    $('#platform-input').val(source);

    const docsBaseUrl = 'https://www.siglens.com/siglens-docs/';
    let docPath = '';
    let urlParam = '';

    switch (source) {
        case 'Elastic Bulk':
            docPath = 'migration/elasticsearch/fluentd';
            urlParam = 'elasticbulk';
            break;
        case 'Splunk HEC':
            docPath = 'migration/splunk/fluentd';
            urlParam = 'splunkhec';
            break;
        case 'Send Test Data':
            urlParam = 'sendtestdata';
            break;
        case 'Verify Connection':
            urlParam = 'verifyconnection';
            break;
    }

    if (docPath) {
        $('#logs-setup-instructions-link').attr('href', docsBaseUrl + docPath);
    }

    updateUrlParameter('method', urlParam);
}

function navigateToMetricsDetails(source) {

    if (source === 'Vector Metrics' || source === 'OpenTelemetry Collector') {
        switch (source) {
            case 'Vector Metrics':
                mdFileName = 'vector-metrics';
                break;
            case 'OpenTelemetry Collector':
                mdFileName = 'open-telemetry';
                break;
        }
        
        window.open(`instructions.html?type=metrics&method=${mdFileName}`, '_self');
        return;
    }

    $('#metrics-cards-view').hide();
    $('#metrics-ingestion-details').show();
    
    $('html, body').animate({
        scrollTop: $("#metrics-ingestion-details").offset().top
    }, 500);
    
    updateBreadcrumbsForIngestion('Metrics Ingestion Methods', source);
    
    updateUrlParameter('method', urlParam);
}

function navigateToTracesDetails(source) {
    if (source === 'Go App' || source === 'Java App' || source === 'Python App' || source === '.Net App' || source === 'Javascript App' || source === 'Vector' || source === 'OpenTelemetry Collector') {
        switch (source) {
            case 'Go App':
                mdFileName = 'go-app';
                break;
            case 'Java App':
                mdFileName = 'java-app';
                break;
            case 'Python App':
                mdFileName = 'python-app';
                break;
            case '.Net App':
                mdFileName = 'dotnet-app';
                break;
            case 'Javascript App':
                mdFileName = 'js-app';
                break;
            case 'OpenTelemetry Collector':
                mdFileName = 'opentelemetry';
                break;
        }
        
        window.open(`instructions.html?type=traces&method=${mdFileName}`, '_self');
        return;
    }

    $('#traces-cards-view').hide();
    $('#traces-ingestion-details').show();
    
    $('html, body').animate({
        scrollTop: $("#traces-ingestion-details").offset().top
    }, 500);
    
    updateBreadcrumbsForIngestion('Traces Ingestion Methods', source);
    
    updateUrlParameter('method', urlParam);
}

function updateUrlParameter(key, value) {
    const url = new URL(window.location.href);
    url.searchParams.set(key, value);
    window.history.pushState({}, '', url);
}

function updateBreadcrumbsForIngestion(baseTitle, selectedSource) {
    const breadcrumbConfig = [
        { name: 'Ingestion', url: './ingestion.html' },
        { name: baseTitle, url: window.location.pathname },
        { name: selectedSource }
    ];
    initializeBreadcrumbs(breadcrumbConfig);
}

function sendTestData() {
    sendTestDataWithoutBearerToken()
        .then((_res) => {
            showToast('Sent Test Data Successfully', 'success');
            let testDataBtn = document.getElementById('test-data-btn');
            testDataBtn.disabled = false;
        })
        .catch((err) => {
            console.log(err);
            showToast('Error Sending Test Data', 'error');
            let testDataBtn = document.getElementById('test-data-btn');
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
            credentials: 'include',
        })
            .then((res) => {
                resolve(res);
            })
            .catch((err) => {
                console.log(err);
                reject(err);
            });
    });
}


function setupCopyFunctionality() {
    $('.copyable').each(function () {
        var copyIcon = $('<span class="copy-icon"></span>');
        $(this).after(copyIcon);
    });

    $('.copy-icon').on('click', function (_event) {
        var copyIcon = $(this);
        var inputOrTextarea = copyIcon.prev('.copyable');
        var inputValue = inputOrTextarea.val();

        var tempInput = document.createElement('textarea');
        tempInput.value = inputValue;
        document.body.appendChild(tempInput);
        tempInput.select();
        document.execCommand('copy');
        document.body.removeChild(tempInput);

        copyIcon.addClass('success');
        setTimeout(function () {
            copyIcon.removeClass('success');
        }, 1000);
    });
}