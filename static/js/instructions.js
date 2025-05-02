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
const instructionData = [
    {
        id: 'promtail',
        title: 'Promtail',
    },
    {
        id: 'opentelemetry',
        title: 'OpenTelemetry Collector',
    },
    {
        id: 'filebeat',
        title: 'Filebeat',
    },
    {
        id: 'fluentd',
        title: 'Fluentd',
    },
    {
        id: 'fluent-bit',
        title: 'Fluent-bit',
    },
    {
        id: 'logstash',
        title: 'Logstash',
    },
    {
        id: 'vector',
        title: 'Vector',
    },
    {
        id: 'elasticbulk',
        title: 'Elastic Bulk',
    },
    {
        id: 'splunkhec',
        title: 'Splunk HEC',
    },
    {
        id: 'vector-metrics',
        title: 'Vector Metrics',
    },
    {
        id: 'open-telemetry',
        title: 'OpenTelemetry Collector',
    },
    {
        id: 'go-app',
        title: 'Go App',
    },
    {
        id: 'dotnet-app',
        title: '.Net App',
    },
    {
        id: 'java-app',
        title: 'Java App',
    },
    {
        id: 'js-app',
        title: 'Javascript App',
    },
    {
        id: 'python-app',
        title: 'Python App',
    },
];

const languageMap = {
    powershell: 'bash',
    ps: 'bash',
    ps1: 'bash',
    yml: 'yaml',
    sh: 'bash',
};

async function loadInstruction(instructionId) {
    try {
        document.getElementById('content-container').innerHTML = '<div class="loading-indicator">Loading...</div>';

        // Determine the path based on instructionId
        let path;
        if (['promtail', 'opentelemetry', 'filebeat', 'fluentd', 'fluent-bit', 'logstash', 'vector', 'elasticbulk', 'splunkhec'].includes(instructionId)) {
            path = `../../content/log-ingestion-methods/${instructionId}.md`;
        } else if (['vector-metrics', 'open-telemetry'].includes(instructionId)) {
            path = `/content/metric-ingestion-methods/${instructionId}.md`;
        } else if (['dotnet-app', 'go-app', 'java-app', 'js-app', 'python-app', 'opentelemetry'].includes(instructionId)) {
            path = `/content/trace-ingestion-methods/${instructionId}.md`;
        } else {
            throw new Error(`Unknown instruction ID: ${instructionId}`);
        }

        const response = await fetch(path);
        if (!response.ok) {
            throw new Error(`Failed to load instruction: ${response.status}`);
        }

        const markdownContent = await response.text();
        let processedMarkdown = preprocessMarkdown(markdownContent);

        marked.setOptions({
            highlight: function (code, language) {
                if (language && languageMap[language]) {
                    language = languageMap[language];
                }

                if (!language || language === '') {
                    language = 'plaintext';
                }
                try {
                    return hljs.highlight(code, { language: language }).value;
                } catch (e) {
                    console.warn(`Highlight.js error for language "${language}":`, e);
                    return hljs.highlightAuto(code).value;
                }
            },
            langPrefix: 'hljs language-',
            renderer: createCustomRenderer(),
        });

        const htmlContent = marked.parse(processedMarkdown);

        document.getElementById('content-container').innerHTML = htmlContent;
        processCustomComponents();

        // Inject CSS for markdown images
        const styleId = 'markdown-image-styles';
        if (!document.getElementById(styleId)) {
            const style = document.createElement('style');
            style.id = styleId;
            style.textContent = `
                .markdown-image {
                    width: 100%;
                    max-width: 800px;
                    height: auto;
                    display: block;
                    margin: 0 auto;
                }
            `;
            document.head.appendChild(style);
        }
    } catch (error) {
        console.error('Failed to load instruction:', error);
        document.getElementById('content-container').innerHTML = `
            <div class="error-message">
                <h3>Error Loading Content</h3>
                <p>${error.message}</p>
            </div>
        `;
    }
}
function processNestedMarkdown(content) {
    // Process admonitions (:::note, :::tip, etc.) within TabItem content
    content = content.replace(/:::(note|tip|info|warning|danger|caution)\n([\s\S]*?):::/g, function (match, type, innerContent) {
        const cleanContent = innerContent.replace(/^\s+/gm, '').trim();
        return `<div class="admonition ${type}"><div class="admonition-heading">${type.charAt(0).toUpperCase() + type.slice(1)}</div><div class="admonition-content">${cleanContent}</div></div>`;
    });

    // Parse nested markdown (e.g., headings, lists, code blocks) using marked
    return marked.parse(content, {
        renderer: createCustomRenderer(),
        highlight: function (code, language) {
            if (language && languageMap[language]) {
                language = languageMap[language];
            }
            if (!language || language === '') {
                language = 'plaintext';
            }
            try {
                return hljs.highlight(code, { language: language }).value;
            } catch (e) {
                console.warn(`Highlight.js error for language "${language}":`, e);
                return hljs.highlightAuto(code).value;
            }
        },
        langPrefix: 'hljs language-',
    });
}

// Pre-process markdown content to handle custom components and image paths
function preprocessMarkdown(markdown) {
    // Process import statements (remove them)
    let processed = markdown.replace(/import\s+.*?from\s+['"].*?['"];?\n?/g, '');

    // Process :::note blocks (outside of tabs)
    processed = processed.replace(/:::(note|tip|info|warning|danger|caution)\n([\s\S]*?):::/g, function (match, type, content) {
        const cleanContent = content.replace(/^\s+/gm, '').trim();
        return `<div class="admonition ${type}"><div class="admonition-heading">${type.charAt(0).toUpperCase() + type.slice(1)}</div><div class="admonition-content">${cleanContent}</div></div>`;
    });

    // Process image paths
    processed = processed.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, function (match, alt, imgPath) {
        if (!imgPath.startsWith('http') && !imgPath.startsWith('data:')) {
            const correctedPath = `../content/tutorials/${imgPath.split('/').pop()}`;
            return `![${alt}](${correctedPath})`;
        }
        return match;
    });

    // Process Tabs component
    processed = processed.replace(/<Tabs[\s\S]*?<\/Tabs>/gs, function (tabsBlock) {
        const defaultValueMatch = tabsBlock.match(/defaultValue=["']([^"']+)["']/);
        const defaultValue = defaultValueMatch ? defaultValueMatch[1] : '';

        let valuesArray = [];
        const valuesStartIdx = tabsBlock.indexOf('values={[');
        if (valuesStartIdx !== -1) {
            let openBrackets = 1;
            let closeBrackets = 0;
            let endIdx = valuesStartIdx + 9;

            while (endIdx < tabsBlock.length && openBrackets !== closeBrackets) {
                if (tabsBlock[endIdx] === '[') openBrackets++;
                if (tabsBlock[endIdx] === ']') closeBrackets++;
                endIdx++;
            }

            const valuesContent = tabsBlock.substring(valuesStartIdx + 9, endIdx - 1);

            const pairs = valuesContent.split(/,(?![^{]*})/).map((s) => s.trim());

            pairs.forEach((pair) => {
                const labelMatch = pair.match(/label:\s*['"]([^'"]+)['"]/);
                const valueMatch = pair.match(/value:\s*['"]([^'"]+)['"]/);

                if (labelMatch && valueMatch) {
                    valuesArray.push({
                        label: labelMatch[1],
                        value: valueMatch[1],
                    });
                }
            });
        }

        const tabItems = [];
        const tabItemRegex = /<TabItem\s+value=["']([^"']+)["']>([\s\S]*?)<\/TabItem>/gs;
        let tabMatch;

        while ((tabMatch = tabItemRegex.exec(tabsBlock)) !== null) {
            const value = tabMatch[1];
            let content = tabMatch[2].trim();

            const valueInfo = valuesArray.find((v) => v.value === value);
            const label = valueInfo ? valueInfo.label : value;

            // Process nested markdown within TabItem content
            content = processNestedMarkdown(content);

            tabItems.push({ value, label, content });
        }

        // HTML for the tabs
        let tabsHtml = '\n\n<div class="tabs">\n  <div class="tabs-header">\n';

        tabItems.forEach((tab) => {
            const isActive = tab.value === defaultValue ? ' active' : '';
            tabsHtml += `    <button class="tab-button${isActive}" data-tab="${tab.value}">${tab.label}</button>\n`;
        });

        tabsHtml += '  </div>\n\n';

        tabItems.forEach((tab) => {
            const isActive = tab.value === defaultValue ? ' active' : '';
            tabsHtml += `  <div class="tab-content${isActive}" data-tab="${tab.value}">\n${tab.content}\n  </div>\n`;
        });

        tabsHtml += '</div>\n\n';

        return tabsHtml;
    });

    return processed;
}

function createCustomRenderer() {
    const renderer = new marked.Renderer();

    // Customize image rendering to add CSS class
    renderer.image = function (href, title, text) {
        const escapedHref = href.replace(/"/g, '&quot;');
        const escapedText = text.replace(/"/g, '&quot;');
        const titleAttr = title ? ` title="${title.replace(/"/g, '&quot;')}"` : '';
        return `<img src="${escapedHref}" alt="${escapedText}" class="markdown-image"${titleAttr}>`;
    };

    // Handle titles in code blocks
    const originalCodeRenderer = renderer.code;
    renderer.code = function (code, language, isEscaped) {
        let title = '';

        if (language && language.includes(' title=')) {
            const titleMatch = language.match(/title="([^"]+)"/);
            if (titleMatch) {
                title = titleMatch[1];
                language = language.replace(/\stitle="([^"]+)"/, '').trim();
            }
        }

        if (language && languageMap[language]) {
            language = languageMap[language];
        }

        if (!language) language = 'plaintext';

        try {
            let html = originalCodeRenderer.call(this, code, language, isEscaped);

            if (title) {
                html = html.replace('<pre>', `<pre data-title="${title}">`);
            }

            return html;
        } catch (error) {
            console.warn(`Error rendering code block with language '${language}':`, error);
            return `<pre><code class="language-plaintext">${code}</code></pre>`;
        }
    };

    return renderer;
}

function processCustomComponents() {
    // Set up tab functionality
    const tabButtons = document.querySelectorAll('.tab-button');
    tabButtons.forEach((button) => {
        button.addEventListener('click', () => {
            const tabId = button.getAttribute('data-tab');
            const tabContainer = button.closest('.tabs');

            tabContainer.querySelectorAll('.tab-button').forEach((btn) => {
                btn.classList.remove('active');
            });
            button.classList.add('active');

            tabContainer.querySelectorAll('.tab-content').forEach((content) => {
                content.classList.remove('active');
            });
            tabContainer.querySelector(`.tab-content[data-tab="${tabId}"]`).classList.add('active');
        });
    });

    document.querySelectorAll('pre code').forEach((block) => {
        hljs.highlightElement(block);
    });

    // Add copy buttons
    addCopyButtonsToCodeBlocks();
}

function addCopyButtonsToCodeBlocks() {
    document.querySelectorAll('pre').forEach(function (pre) {
        if (!pre.parentNode.classList.contains('pre-wrapper')) {
            const wrapper = document.createElement('div');
            wrapper.className = 'pre-wrapper';

            pre.parentNode.insertBefore(wrapper, pre);

            wrapper.appendChild(pre);

            const copyButton = document.createElement('button');
            copyButton.className = 'copy-button';
            copyButton.title = 'Copy to clipboard';
            copyButton.innerHTML = '<span class="copy-icon"></span>';

            wrapper.appendChild(copyButton);

            if (pre.hasAttribute('data-title')) {
                copyButton.style.top = '44px';
            }

            copyButton.addEventListener('click', function () {
                const code = pre.querySelector('code') ? pre.querySelector('code').innerText : pre.innerText;

                navigator.clipboard
                    .writeText(code)
                    .then(function () {
                        $('.copy-icon').addClass('success');
                        copyButton.classList.add('success');

                        setTimeout(function () {
                            $('.copy-icon').removeClass('success');
                        }, 1500);
                    })
                    .catch(function (err) {
                        console.error('Failed to copy: ', err);

                        const textarea = document.createElement('textarea');
                        textarea.value = code;
                        textarea.style.position = 'fixed';
                        document.body.appendChild(textarea);
                        textarea.select();

                        try {
                            document.execCommand('copy');
                            copyButton.classList.add('success');
                            setTimeout(function () {
                                copyButton.classList.remove('success');
                            }, 1500);
                        } catch (err) {
                            console.error('Fallback copy failed:', err);
                        }

                        document.body.removeChild(textarea);
                    });
            });
        }
    });
}

function initApp() {
    const urlParams = new URLSearchParams(window.location.search);
    const type = urlParams.get('type') || 'logs';
    const methodParam = urlParams.get('method');

    initializeBreadcrumbsForType(type, methodParam);
    loadInstruction(methodParam);
}

function initializeBreadcrumbsForType(type, methodName) {
    let breadcrumbConfig = [];
    let title = '';

    const instructionItem = instructionData.find((item) => item.id === methodName);
    const methodTitle = instructionItem ? instructionItem.title : methodName;

    // Determine type dynamically if not provided
    let resolvedType = type;
    if (!resolvedType && methodName) {
        if (['promtail', 'opentelemetry', 'filebeat', 'fluentd', 'fluent-bit', 'logstash', 'vector', 'elasticbulk', 'splunkhec'].includes(methodName)) {
            resolvedType = 'logs';
        } else if (['vector-metrics', 'open-telemetry'].includes(methodName)) {
            resolvedType = 'metrics';
        } else if (['dotnet-app', 'go-app', 'java-app', 'js-app', 'python-app', 'opentelemetry'].includes(methodName)) {
            resolvedType = 'traces';
        }
    }

    switch (resolvedType) {
        case 'logs':
            title = 'Log Ingestion Methods';
            breadcrumbConfig = [{ name: 'Ingestion', url: './ingestion.html' }, { name: title, url: './log-ingestion.html' }, { name: methodTitle }];
            break;

        case 'metrics':
            title = 'Metrics Ingestion Methods';
            breadcrumbConfig = [{ name: 'Ingestion', url: './ingestion.html' }, { name: title, url: './metrics-ingestion.html' }, { name: methodTitle }];
            break;

        case 'traces':
            title = 'Traces Ingestion Methods';
            breadcrumbConfig = [{ name: 'Ingestion', url: './ingestion.html' }, { name: title, url: './traces-ingestion.html' }, { name: methodTitle }];
            break;

        default:
            title = 'Ingestion Methods';
            breadcrumbConfig = [{ name: 'Ingestion', url: './ingestion.html' }, { name: methodTitle }];
            break;
    }

    initializeBreadcrumbs(breadcrumbConfig);
}

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);
    initApp();
});
