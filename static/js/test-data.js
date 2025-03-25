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

function sendTestData() {
    // Disable the button to prevent multiple clicks
    let testDataBtn = document.getElementById("test-data-btn");
    if (testDataBtn) {
        testDataBtn.disabled = true;
    }

    sendTestDataWithoutBearerToken().then((_res) => {
        showToast('Sent Test Data Successfully', 'success');
        if (testDataBtn) {
            testDataBtn.disabled = false;
        }
    })
    .catch((err) => {
        console.log(err);
        showToast('Error Sending Test Data', 'error');
        if (testDataBtn) {
            testDataBtn.disabled = false;
        }
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
    const testDataBtn = $('#test-data-btn');
    if (testDataBtn.length === 0) {
        console.error("Test data button not found in the DOM");
        return;
    }

    testDataBtn.off('click').on('click', function() {
        sendTestData();
    });
}


$(document).ready(async function () {
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
            // Initialize the test data button immediately
            myOrgSendTestData(iToken);
        {{ end }}
    } catch (err) {
        console.log("Error loading config:", err);
        // Still try to initialize the button even if config fails
        myOrgSendTestData(iToken);
    }

    function setCodeBlockContainerBackground() {
        const preElement = $('.code-container pre.language-yaml');
        if (preElement.length) {
            const preBackgroundColor = preElement.css('background-color');
            $('.code-container').css('background-color', preBackgroundColor);
        }
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
                    description: 'Download the sample log file:',
                    code: 'curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz'
                },
                {
                    heading: 'Create OpenTelemetry Configuration',
                    description: 'Create an <code>otelconfig.yaml</code> file with the following configuration:',
                    code: `receivers:
        filelog:
            include: [ /var/log/*.log ]  # replace with your log file path

        processors:
        batch:

        exporters:
        elasticsearch:
            endpoints: ["http://host.docker.internal:8081/elastic"]
            logs_index: "logs-%{+yyyy.MM.dd}"

        service:
        pipelines:
            logs:
            receivers: [filelog]
            processors: [batch]
            exporters: [elasticsearch]`
                },
                {
                    heading: 'Run OTEL Collector',
                    description: 'Run the OpenTelemetry Collector with your configuration:',
                    code: 'docker run -v <path_to_your_otel_config_directory>:/etc/otel -v <path_to_your_log_directory>:/var/log -p 4317:4317 -p 8888:8888 otel/opentelemetry-collector-contrib:latest --config /etc/otel/<your_config_file>'
                },
                {
                    heading: 'Example Command',
                    description: 'A sample command to run the OTEL Collector:',
                    code: 'docker run -v $HOME/otel:/etc/otel -v /var/log:/var/log -p 4317:4317 -p 8888:8888 otel/opentelemetry-collector-contrib:latest --config /etc/otel/otelconfig.yaml'
                }
            ],
            notes: [
                'Port 4317 is the default port for the OTLP gRPC receiver.',
                'Port 8888 is used for metrics exposition.',
                'Replace ports if your setup uses different configurations.'
            ]
        },
        'Vector': {
    title: 'Vector',
    subtitle: 'Ingesting logs into SigLens using Vector',
    setupLink: 'https://www.siglens.com/siglens-docs/log-ingestion/vector',
    steps: [
        {
            heading: 'Install Vector',
            description: 'Install Vector on your system. Options include Linux, macOS, Windows, and other platforms.',
            code: `# For Linux (Debian/Ubuntu)
curl -O https://packages.timber.io/vector/0.X.X/vector_0.X.X-1_amd64.deb
sudo dpkg -i vector_0.X.X-1_amd64.deb

# For Linux (CentOS/RHEL/Amazon Linux)
curl -O https://packages.timber.io/vector/0.X.X/vector-0.X.X-1.x86_64.rpm
sudo rpm -i vector-0.X.X-1.x86_64.rpm`
        },
        {
            heading: 'Download Sample Events',
            description: 'Download the sample events file:',
            code: `curl -s -L https://github.com/siglens/pub-datasets/releases/download/v1.0.0/2kevents.json.tar.gz -o 2kevents.json.tar.gz && tar -xvf 2kevents.json.tar.gz`
        },
        {
            heading: 'Configure Vector',
            description: 'Create a <code>vector.yaml</code> configuration file:',
            code: `data_dir: /var/lib/vector

        sources:
        read_from_file:
            type: file
            include:
            - 2kevents.json # Path to the log file

        sinks:
        siglens:
            type: elasticsearch
            inputs:
            - read_from_file
            endpoints:
            - http://localhost:8081/elastic/
            mode: bulk
            healthcheck:
            enabled: false`
                },
                {
                    heading: 'Run Vector',
                    description: 'Start Vector with your configuration:',
                    code: `vector --config vector.yaml`
                }
            ],
            notes: [
                'You might need to add transforms to your Vector configuration based on your data structure.',
                'Refer to the official Vector documentation for advanced configuration details.',
                'Ensure you replace paths and endpoints with your specific environment settings.'
            ]
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


    const savedSource = localStorage.getItem('selectedLogSource');
    if (savedSource) {
        selectedLogSource = savedSource;

        if (selectedLogSource === 'Send Test Data') {
            setTimeout(() => {
                const testDataBtn = $('#test-data-btn');
                if (testDataBtn.length) {
                    testDataBtn.trigger('click');
                }
            }, 500);
        } else {
            $('.ingestion-cards-container').hide();
            $('#configuration-section').show();
            updateConfiguration(selectedLogSource);
        }
    }

    $('.ingestion-card').on('click', function(e) {
        if ($(e.target).is('button')) return;

        selectedLogSource = $(this).data('source');
        if (selectedLogSource === 'Send Test Data') {
            localStorage.setItem('selectedLogSource', selectedLogSource);
            const testDataBtn = $('#test-data-btn');
            if (testDataBtn.length) {
                testDataBtn.trigger('click');
            } else {
                console.error("Test data button not found");
                sendTestData();
            }
            return;
        }
        localStorage.setItem('selectedLogSource', selectedLogSource);

        $('.ingestion-cards-container').hide();
        $('#configuration-section').show();
        updateConfiguration(selectedLogSource);
    });

    $('.configure-btn').on('click', function(e) {
        e.stopPropagation();
        selectedLogSource = $(this).closest('.ingestion-card').data('source');

        if (selectedLogSource === 'Send Test Data') {
            localStorage.setItem('selectedLogSource', selectedLogSource);
            const testDataBtn = $('#test-data-btn');
            if (testDataBtn.length) {
                testDataBtn.trigger('click');
            } else {
                sendTestData();
            }
            return;
        }

        localStorage.setItem('selectedLogSource', selectedLogSource);

        $('.ingestion-cards-container').hide();
        $('#configuration-section').show();
        updateConfiguration(selectedLogSource);
    });

    $('.back-button').on('click', function() {
        localStorage.removeItem('selectedLogSource');

        $('#configuration-section').hide();
        $('.ingestion-cards-container').show();
    });

    function updateConfiguration(source) {
        $('#platform-input').val(source);
        const method = ingestionMethods[source];
        if (!method) return;

        let stepsHtml = '';
        method.steps.forEach((step, index) => {
            const isYaml = step.heading.toLowerCase().includes('configuration');
            const languageClass = isYaml ? 'language-yaml' : 'language-bash';

            stepsHtml += `
                <div class="ingestion-step">
                    <h5 class="step-heading">${index + 1}. ${step.heading}</h5>
                    <p>${step.description}</p>
                    ${step.code ? `
                        <div class="code-container">
                            <div class="code-wrapper">
                                <div class="code-actions">
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
                                <pre class="line-numbers language-${isYaml ? 'yaml' : 'bash'}"><code class="language-${isYaml ? 'yaml' : 'bash'}">${step.code.replace(/</g, '&lt;').replace(/>/g, '&gt;')}</code></pre>
                            </div>
                        </div>
                    ` : ''}
                </div>
            `;
        });

        // Add notes section if available
        let notesHtml = '';
        if (method.notes && method.notes.length > 0) {
            notesHtml = `
                <div class="ingestion-notes">
                    <h5 class="notes-heading">Important Notes</h5>
                    <ul>
                        ${method.notes.map(note => `<li>${note}</li>`).join('')}
                    </ul>
                </div>
            `;
        }

        $('#data-ingestion').html(`
            <h2 class="main-heading">${method.title}</h2>
            <p class="subtitle">${method.subtitle}</p>
            ${stepsHtml}
            ${notesHtml}
        `);

        // Ensure Prism is fully loaded and applied
        if (window.Prism) {
            Prism.highlightAll();
        }

        // Initialize buttons and other functionality
        initializeCodeBlockButtons();
        setCodeBlockContainerBackground();
        checkCodeOverflow();
    }

    function initializeCodeBlockButtons() {
        // Copy button logic
        $('.copy-btn').off('click').on('click', function() {
            const codeText = $(this).closest('.code-wrapper').find('code').text();

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

        // Expand button logic
        $('.expand-btn').off('click').on('click', function() {
            const codeWrapper = $(this).closest('.code-wrapper');
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
            const expandBtn = codeWrapper.find('.expand-btn');

            // Check if content overflows (vertical or horizontal)
            const isVerticalOverflow = preElement[0].scrollHeight > codeWrapper.height();
            const isHorizontalOverflow = preElement[0].scrollWidth > codeWrapper.width();

            // Show or hide expand button based on overflow
            if (isVerticalOverflow || isHorizontalOverflow) {
                expandBtn.addClass('overflow-available');
            } else {
                expandBtn.removeClass('overflow-available');
            }
        });
    }

    setTimeout(checkCodeOverflow, 300);
    $(window).on('resize', function() {
        setTimeout(checkCodeOverflow, 100);
    });

    {{ .Button1Function }}
});