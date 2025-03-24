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
    $('.theme-btn').on('click', themePickerHandler);

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

    const ingestionMethods = {
        'OpenTelemetry': {
            title: 'Open Telemetry',
            subtitle: 'Ingesting logs into SigLens using OpenTelemetry',
            setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/open-telemetry/',
            steps: [
                {
                    heading: 'Pull OTEL Collector Docker Image',
                    description: 'Pull the latest Docker image for OpenTelemetry Collector Contrib:',
                    code: 'docker pull otel/opentelemetry-collector-contrib:latest'
                },
                {
                    heading: 'Configure OTEL Collector',
                    description: 'Download the 2kevents.json file if you are looking for a sample log file:',
                    code: 'curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz'
                },
                {
                    heading: 'Create a Config File',
                    description: 'Create a configuration file for the OTEL Collector to send logs to SigLens.',
                    code: '' // Add config file example if needed
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/v1/logs" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '-H \'Accept: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
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
                '}\''
        },
        'Vector': {
            title: 'Vector',
            subtitle: 'Ingesting logs into SigLens using Vector',
            setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/vector',
            steps: [
                {
                    heading: 'Install Vector',
                    description: 'Install Vector on your system.',
                    code: ''
                },
                {
                    heading: 'Configure Vector',
                    description: `Configure Vector to send logs to <code>${baseUrl}/elastic/_bulk</code>.`,
                    code: ''
                },
                {
                    heading: 'Start Vector',
                    description: 'Start the Vector service to begin log collection.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/elastic/_bulk" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index" : { "_index" : "test" } }\n' +
                '{ "name" : "john", "age":"23" }\''
        },
        'Logstash': {
            title: 'Logstash',
            subtitle: 'Ingesting logs into SigLens using Logstash',
            setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/logstash',
            steps: [
                {
                    heading: 'Install Logstash',
                    description: 'Install Logstash on your system.',
                    code: ''
                },
                {
                    heading: 'Configure Logstash',
                    description: `Set up a pipeline to forward logs to <code>${baseUrl}/elastic/_bulk</code>.`,
                    code: ''
                },
                {
                    heading: 'Run Logstash',
                    description: 'Run Logstash with your configuration.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/elastic/_bulk" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index" : { "_index" : "test" } }\n' +
                '{ "name" : "john", "age":"23" }\''
        },
        'Fluentd': {
            title: 'Fluentd',
            subtitle: 'Ingesting logs into SigLens using Fluentd',
            setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/fluentd',
            steps: [
                {
                    heading: 'Install Fluentd',
                    description: 'Install Fluentd on your system.',
                    code: ''
                },
                {
                    heading: 'Configure Fluentd',
                    description: `Configure Fluentd to output logs to <code>${baseUrl}/elastic/_bulk</code>.`,
                    code: ''
                },
                {
                    heading: 'Start Fluentd',
                    description: 'Start Fluentd to process logs.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/elastic/_bulk" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index" : { "_index" : "test" } }\n' +
                '{ "name" : "john", "age":"23" }\''
        },
        'Filebeat': {
            title: 'Filebeat',
            subtitle: 'Ingesting logs into SigLens using Filebeat',
            setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/filebeat',
            steps: [
                {
                    heading: 'Install Filebeat',
                    description: 'Install Filebeat on your system.',
                    code: ''
                },
                {
                    heading: 'Configure Filebeat',
                    description: `Configure Filebeat to send logs to <code>${baseUrl}/elastic/_bulk</code>.`,
                    code: ''
                },
                {
                    heading: 'Run Filebeat',
                    description: 'Run Filebeat to start shipping logs.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/elastic/_bulk" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index" : { "_index" : "test" } }\n' +
                '{ "name" : "john", "age":"23" }\''
        },
        'Promtail': {
            title: 'Promtail',
            subtitle: 'Ingesting logs into SigLens using Promtail',
            setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/promtail',
            steps: [
                {
                    heading: 'Install Promtail',
                    description: 'Install Promtail on your system.',
                    code: ''
                },
                {
                    heading: 'Configure Promtail',
                    description: `Configure Promtail to forward logs to <code>${baseUrl}/elastic/_bulk</code>.`,
                    code: ''
                },
                {
                    heading: 'Start Promtail',
                    description: 'Start Promtail to collect logs.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/elastic/_bulk" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index" : { "_index" : "test" } }\n' +
                '{ "name" : "john", "age":"23" }\''
        },
        'Splunk HEC': {
            title: 'Splunk HEC',
            subtitle: 'Ingesting logs into SigLens using Splunk HEC',
            setupLink: 'https://www.siglens.com/siglens-docs/migration/splunk/fluentd',
            steps: [
                {
                    heading: 'Configure Splunk HEC',
                    description: 'Configure your Splunk HEC endpoint.',
                    code: ''
                },
                {
                    heading: 'Set Output',
                    description: `Set the output to <code>${baseUrl}/services/collector/event</code>.`,
                    code: ''
                },
                {
                    heading: 'Authorize',
                    description: 'Ensure proper authorization is included.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/services/collector/event" \\\n` +
                '-H "Authorization: A94A8FE5CCB19BA61C4C08"  \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index": "test", "name": "john", "age": "23"}\''
        },
        'Elastic Bulk': {
            title: 'Elastic Bulk',
            subtitle: 'Ingesting logs into SigLens using Elastic Bulk',
            setupLink: 'https://www.siglens.com/siglens-docs/migration/elasticsearch/fluentd',
            steps: [
                {
                    heading: 'Prepare Data',
                    description: 'Prepare your data in Elastic Bulk format.',
                    code: ''
                },
                {
                    heading: 'Send Data',
                    description: `Send it to <code>${baseUrl}/elastic/_bulk</code>.`,
                    code: ''
                },
                {
                    heading: 'Verify Ingestion',
                    description: 'Verify ingestion in SigLens.',
                    code: ''
                }
            ],
            curlCommand: `curl -X POST "${baseUrl}/elastic/_bulk" \\\n` +
                '-H \'Content-Type: application/json\' \\\n' +
                '{{ if .IngestDataCmd }}{{ .IngestDataCmd }}{{ end }}' +
                '-d \'{ "index" : { "_index" : "test" } }\n' +
                '{ "name" : "john", "age":"23" }\''
        }
    };

    $('.ingestion-card').on('click', function(e) {
        if ($(e.target).is('button')) return;

        selectedLogSource = $(this).data('source');
        if (selectedLogSource === 'Send Test Data') {
            $('#test-data-btn').trigger('click');
            return;
        }

        $('.ingestion-cards-container').hide();
        $('#configuration-section').show();
        updateConfiguration(selectedLogSource);
    });

    $('.configure-btn').on('click', function(e) {
        e.stopPropagation();
        selectedLogSource = $(this).closest('.ingestion-card').data('source');
        $('.ingestion-cards-container').hide();
        $('#configuration-section').show();
        updateConfiguration(selectedLogSource);
    });

    $('.back-button').on('click', function() {
        $('#configuration-section').hide();
        $('.ingestion-cards-container').show();
    });

    function updateConfiguration(source) {
        $('#platform-input').val(source);
        const method = ingestionMethods[source];
        if (!method) return;

        let stepsHtml = '';
        method.steps.forEach((step, index) => {
            stepsHtml += `
                <div class="ingestion-step">
                    <h5 class="step-heading">${index + 1}. ${step.heading}</h5>
                    <p>${step.description}</p>
                    ${step.code ? `
                        <div class="code-container">
                            <div class="code-wrapper">
                                <pre class="language-yaml"><code>${step.code}</code></pre>
                            </div>
                            <button class="expand-btn" title="Expand/Collapse">
                                <svg class="expand-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <path d="M15 3h6v6"></path>
                                    <path d="M9 21H3v-6"></path>
                                    <path d="M21 3l-7 7"></path>
                                    <path d="M3 21l7-7"></path>
                                </svg>
                                <span class="sr-only">Expand</span>
                            </button>
                            <button class="copy-btn" title="Copy">
                                <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                                    <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                                    <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                                </svg>
                            </button>
                        </div>
                    ` : ''}
                </div>
            `;
        });

        $('#data-ingestion').html(`
            <h2 class="main-heading">${method.title}</h2>
            <p class="subtitle">${method.subtitle}</p>
            ${stepsHtml}
            <div class="ingestion-step">
                <h5 class="step-heading">Verify Data Collection</h5>
                <p>Test your ${source} setup by running this sample curl command in your terminal:</p>
                <div class="code-container">
                    <div class="code-wrapper">
                        <pre class="language-yaml"><code>${method.curlCommand}</code></pre>
                    </div>
                    <button class="expand-btn" title="Expand/Collapse">
                        <svg class="expand-icon" xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <path d="M15 3h6v6"></path>
                            <path d="M9 21H3v-6"></path>
                            <path d="M21 3l-7 7"></path>
                            <path d="M3 21l7-7"></path>
                        </svg>
                        <span class="sr-only">Expand</span>
                    </button>
                    <button class="copy-btn" title="Copy">
                        <svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">
                            <rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect>
                            <path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path>
                        </svg>
                    </button>
                </div>
            </div>
        `);

        // Highlight code after DOM is updated
        Prism.highlightAll();

        // Set code container background
        setCodeBlockContainerBackground();

        // Initialize event handlers for copy and expand buttons
        initializeCodeBlockButtons();

        // Check for code overflow to show/hide expand buttons
        checkCodeOverflow();
    }

    function setCodeBlockContainerBackground() {
        $('.code-container').each(function() {
            const preElement = $(this).find('pre');
            if (preElement.length) {
                // Get computed background color from the pre element
                const backgroundColor = window.getComputedStyle(preElement[0]).backgroundColor;
                $(this).css('background-color', backgroundColor);
            }
        });
    }

    function initializeCodeBlockButtons() {
        // Reinitialize copy button click handlers
        $('.copy-btn').off('click').on('click', function() {
            const codeText = $(this).siblings('.code-wrapper').find('code').text();

            // Create a temporary element for copying
            const tempElement = document.createElement('textarea');
            tempElement.value = codeText;
            document.body.appendChild(tempElement);
            tempElement.select();
            document.execCommand('copy');
            document.body.removeChild(tempElement);

            // Show success state
            const copyBtn = $(this);
            const originalSvg = copyBtn.html();
            copyBtn.addClass('success');
            copyBtn.html('<svg xmlns="http://www.w3.org/2000/svg" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="white" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg>');

            setTimeout(function() {
                copyBtn.removeClass('success');
                copyBtn.html(originalSvg);
            }, 1000);
        });

        // Reinitialize expand button click handlers
        $('.expand-btn').off('click').on('click', function() {
            const codeWrapper = $(this).siblings('.code-wrapper');
            const isExpanded = codeWrapper.hasClass('expanded');

            if (isExpanded) {
                codeWrapper.removeClass('expanded');
                $(this).find('.expand-icon').css('transform', 'rotate(0deg)');
            } else {
                codeWrapper.addClass('expanded');
                $(this).find('.expand-icon').css('transform', 'rotate(180deg)');
            }
        });
    }

    function checkCodeOverflow() {
        $('.code-wrapper').each(function() {
            const codeWrapper = $(this);
            const preElement = codeWrapper.find('pre');
            const expandBtn = codeWrapper.siblings('.expand-btn');

            // Check if content overflows
            const isVerticalOverflow = preElement[0].scrollHeight > codeWrapper.height();
            const isHorizontalOverflow = preElement[0].scrollWidth > preElement.width();

            // Show/hide expand button based on overflow
            if (isVerticalOverflow || isHorizontalOverflow) {
                expandBtn.show();
            } else {
                expandBtn.hide();
            }
        });
    }

    setTimeout(checkCodeOverflow, 100);
    $(window).on('resize', function() {
        checkCodeOverflow();
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