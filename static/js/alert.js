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

let alertData = {};
let isEditMode = 0;
let isFromMetrics = 0;
let alertChartInstance = null;

let messageTemplateInfo = '<i class="fa fa-info-circle position-absolute info-icon sendMsg" rel="tooltip" id="info-icon-msg" style="display: block;" title = "You can use following template variables:' + '\n' + inDoubleBrackets('alert_rule_name') + '\n' + inDoubleBrackets('query_string') + '\n' + inDoubleBrackets('condition') + '\n' + inDoubleBrackets('queryLanguage') + '"></i>';
let messageInputBox = document.getElementById('message-info');
if (messageInputBox) messageInputBox.innerHTML += messageTemplateInfo;

function inDoubleBrackets(str) {
    return '{' + '{' + str + '}' + '}';
}

let mapConditionTypeToIndex = new Map([
    ['Is above', 0],
    ['Is below', 1],
    ['Equal to', 2],
    ['Not equal to', 3],
]);

let mapIndexToConditionType = new Map([
    [0, 'Is above'],
    [1, 'Is below'],
    [2, 'Equal to'],
    [3, 'Not equal to'],
]);

const alertForm = $('#alert-form');

let originalIndexValues;
//eslint-disable-next-line no-unused-vars
let indexValues;

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);
    $('.theme-btn').on('click', updateChartColorsBasedOnTheme);

    let startTime = 'now-30m';
    let endTime = 'now';
    datePickerHandler(startTime, endTime, startTime);
    setupEventHandlers();

    setupAlertEventHandlers();
    setupValidationHandlers();
    setupTooltips();
    initializeBreadcrumbs([
        { name: 'Alerting', url: './alerting.html' },
        { name: 'Alert Rules', url: './all-alerts.html' },
        { name: 'New Alert Rule', url: '#' },
    ]);

    await initializeFromUrl();
});

function setupAlertEventHandlers() {
    $('.alert-condition-options li').on('click', function () {
        $('.alert-condition-option').removeClass('active');
        $('#alert-condition span').html($(this).html());
        $(this).addClass('active');
    });

    $('#cancel-alert-btn').on('click', function () {
        window.location.href = '../all-alerts.html';
        resetAddAlertForm();
    });

    $('#contact-points-dropdown').on('click', contactPointsDropdownHandler);

    // Enable the save button when a contact point is selected
    $('.contact-points-options li').on('click', function () {
        $('#contact-points-dropdown span').text($(this).text());
        $('#save-alert-btn').prop('disabled', false);
        $('#contact-point-error').css('display', 'none');
    });

    $('.contact-points-options').on('click', 'li', function () {
        $('.contact-points-option').removeClass('active');
        $('#contact-points-dropdown span').html($(this).html());
        $('#contact-points-dropdown span').attr('id', $(this).attr('id'));
        $(this).addClass('active');

        if ($(this).html() === 'Add New') {
            $('.popupOverlay, .popupContent').addClass('active');
            $('#contact-form-container').css('display', 'block');
        }
    });

    $('#all-alerts-text').click(function () {
        window.location.href = '../all-alerts.html';
    });

    $('.add-label-container').on('click', function () {
        var newLabelContainer = `
            <div class="label-container d-flex align-items-center">
                <input type="text" id="label-key" class="form-control" placeholder="Label name" tabindex="7" value="">
                <span class="label-equal"> = </span>
                <input type="text" id="label-value" class="form-control" placeholder="Value" value="" tabindex="8">
                <button class="btn-simple delete-icon" type="button" id="delete-alert-label"></button>
            </div>
        `;
        $('.label-main-container').append(newLabelContainer);
    });

    $('.label-main-container').on('click', '.delete-icon', function () {
        $(this).closest('.label-container').remove();
    });
}

function setupValidationHandlers() {
    $('#evaluate-for').tooltip({
        title: 'Evaluate For must be greater than or equal to Evaluate Interval',
        placement: 'top',
        trigger: 'manual',
    });
    let evaluateForValue = 0;

    function checkEvaluateConditions() {
        let evaluateEveryValue = parseInt($('#evaluate-every').val());
        evaluateForValue = parseInt($('#evaluate-for').val());
        let submitbtn = $('#save-alert-btn');
        let errorMessage = $('.evaluation-error-message');

        if (evaluateForValue < evaluateEveryValue) {
            $('#evaluate-for').addClass('error-border');
            errorMessage.show();
            $('#evaluate-for').tooltip('show');
            submitbtn.prop('disabled', true);
        } else {
            $('#evaluate-for').removeClass('error-border');
            errorMessage.hide();
            $('#evaluate-for').tooltip('hide');
            submitbtn.prop('disabled', false);
        }
    }

    $('#evaluate-for').on('input', function () {
        checkEvaluateConditions();
    });

    $('#evaluate-every').on('input', function () {
        checkEvaluateConditions();
    });
}

function setupTooltips() {
    const tooltipIds = ['info-icon-spl', 'info-icon-msg', 'info-evaluate-every', 'info-evaluate-for'];

    tooltipIds.forEach((id) => {
        if ($(`#${id}`).length) {
            $(`#${id}`)
                .tooltip({
                    delay: { show: 0, hide: 300 },
                    trigger: 'click',
                })
                .on('click', function () {
                    $(`#${id}`).tooltip('show');
                });
        }
    });

    $(document).mouseup(function (e) {
        if ($(e.target).closest('.tooltip-inner').length === 0) {
            tooltipIds.forEach((id) => $(`#${id}`).tooltip('hide'));
        }
    });
}

async function toggleAlertTypeUI(type) {
    const isLogs = type === 'logs';
    $('.query-container, .logs-lang-container, .index-box, #logs-explorer').toggle(isLogs);
    $('#metrics-explorer, #metrics-graphs').toggle(!isLogs);

    if (isLogs) {
        let indexes = await getListIndices();
        if (indexes) {
            originalIndexValues = indexes.map((item) => item.index);
            indexValues = [...originalIndexValues];
        }
        initializeIndexAutocomplete();
        setIndexDisplayValue(selectedSearchIndex);

        initializeFilterInputEvents();

        //Show empty chart initially
        const logsExplorer = document.getElementById('logs-explorer');
        showEmptyChart(logsExplorer);
        $('#query').attr('required', 'required');
    } else {
        $('#query').removeAttr('required');
    }
}

async function initializeFromUrl() {
    const params = new URLSearchParams(window.location.search);
    let alertType = params.get('type') || 'logs';

    // Edit Mode
    if (params.has('id')) {
        alertType = await loadExistingAlert(params.get('id'));
    }
    // If alert created from metrics
    else if (params.has('queryString')) {
        let dataParam = getUrlParameter('queryString');
        let jsonString = decodeURIComponent(dataParam);
        let obj = JSON.parse(jsonString);
        isFromMetrics = 1;
        fillAlertForm(obj);
        alertData.alert_type = 2;
        alertType = 'metrics';
    }
    // If alert created from logs
    else if (params.has('queryLanguage')) {
        const queryLanguage = params.get('queryLanguage');
        const searchText = params.get('searchText');
        const startEpoch = params.get('startEpoch');
        const endEpoch = params.get('endEpoch');
        const filterTab = params.get('filterTab');
        createAlertFromLogs(queryLanguage, searchText, startEpoch, endEpoch, filterTab);
        alertData.alert_type = 2;
        alertType = 'logs';
    }

    if (!isEditMode && !isFromMetrics && alertType !== 'logs') {
        addQueryElement();
    }

    handleFormValidationTooltip(alertType);
    toggleAlertTypeUI(alertType);
}

async function loadExistingAlert(alertId) {
    const res = await $.ajax({
        method: 'get',
        url: 'api/alerts/' + alertId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    });
    isEditMode = true;
    isFromMetrics = 0;
    fillAlertForm(res.alert);
    const alertType = res.alert.alert_type === 1 ? 'logs' : 'metrics';
    alertData.alert_type = res.alert.alert_type;

    return alertType;
}

function contactPointsDropdownHandler() {
    $.ajax({
        method: 'get',
        url: 'api/alerts/allContacts',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    })
        .then(function (res) {
            if (res.contacts && Array.isArray(res.contacts)) {
                let dropdown = $('.contact-points-options');

                res.contacts.forEach((cp) => {
                    if (cp && cp.contact_name && !$(`.contact-points-option:contains(${cp.contact_name})`).length) {
                        dropdown.append(`<li class="contact-points-option" id="${cp.contact_id}">${cp.contact_name}</li>`);
                    }
                });
            }
        })
        .catch(function (error) {
            console.error('Error fetching contacts:', error);
        });
}

$(document).keyup(function (e) {
    if (e.key === 'Escape' || e.key === 'Esc') {
        $('.popupOverlay, .popupContent').removeClass('active');
    }
});

function submitAddAlertForm(e) {
    e.preventDefault();
    setAlertRule();
    saveAlert();
}

function saveAlert() {
    alertData.alert_type = alertData.alert_type || 1;

    const url = isEditMode && !isFromMetrics ? 'api/alerts/update' : 'api/alerts/create';

    $.ajax({
        method: 'post',
        url: url,
        headers: {
            'Content-Type': 'application/json',
            Accept: '*/*',
        },
        data: JSON.stringify(alertData),
        dataType: 'json',
        crossDomain: true,
    })
        .then(() => {
            $('#alert-form')[0].reset();
            window.location.href = '../all-alerts.html';
        })
        .catch((err) => {
            showToast(err.responseJSON?.error || 'Failed to save alert', 'error');
        });
}

function setAlertRule() {
    let alertType;
    if (isEditMode || isFromMetrics) {
        // For edit mode
        alertType = alertData.alert_type === 1 ? 'logs' : 'metrics';
    } else {
        // For new alerts
        const urlParams = new URLSearchParams(window.location.search);
        alertType = urlParams.get('type') || 'logs';
    }
    if (alertType === 'logs') {
        let searchText, queryMode;
        if (isQueryBuilderSearch) {
            searchText = getQueryBuilderCode();
            queryMode = 'Builder';
        } else {
            searchText = $('#filter-input').val();
            queryMode = 'Code';
        }
        alertData.alert_type = 1;
        alertData.queryParams = {
            data_source: alertType,
            queryLanguage: 'Splunk QL',
            queryText: searchText,
            startTime: filterStartDate,
            endTime: filterEndDate,
            index: selectedSearchIndex,
            queryMode: queryMode,
        };
    } else if (alertType === 'metrics') {
        alertData.alert_type = 2;
        alertData.metricsQueryParams = JSON.stringify(metricsQueryParams);
    }
    alertData.alert_name = $('#alert-rule-name').val();
    alertData.condition = mapConditionTypeToIndex.get($('#alert-condition span').text());
    alertData.eval_interval = parseInt($('#evaluate-every').val());
    alertData.eval_for = parseInt($('#evaluate-for').val());
    alertData.contact_name = $('#contact-points-dropdown span').text();
    alertData.contact_id = $('#contact-points-dropdown span').attr('id');
    alertData.message = $('.message').val();
    alertData.value = parseFloat($('#threshold-value').val());
    alertData.message = $('.message').val();
    alertData.labels = [];

    $('.label-container').each(function () {
        let labelName = $(this).find('#label-key').val();
        let labelVal = $(this).find('#label-value').val();
        if (labelName && labelVal) {
            let labelEntry = {
                label_name: labelName,
                label_value: labelVal,
            };
            alertData.labels.push(labelEntry);
        }
    });
}

function resetAddAlertForm() {
    alertForm[0].reset();
}

async function fillAlertForm(res) {
    $('#alert-rule-name').val(res.alert_name);

    // Alert Type: Logs
    if (res.alert_type === 1) {
        const { startTime, endTime, queryText, queryMode, index } = res.queryParams;

        $('#info-icon-spl').show();

        $('.ranges .inner-range .range-item').removeClass('active');
        $(`.ranges .inner-range #${startTime}`).addClass('active');
        datePickerHandler(startTime, endTime, startTime);

        if (index === '') {
            setIndexDisplayValue('*');
        } else {
            setIndexDisplayValue(index);
        }

        if (queryMode === 'Builder') {
            codeToBuilderParsing(queryText);
            $('#filter-input').val(queryText);
            isQueryBuilderSearch = true;
        } else if (queryMode === 'Code' || queryMode === '') {
            $('#custom-code-tab').tabs('option', 'active', 1);
            $('#filter-input').val(queryText);
        }

        let data = {
            state: wsState,
            searchText: queryText,
            startEpoch: startTime,
            endEpoch: endTime,
            indexName: index,
            queryLanguage: 'Splunk QL',
        };

        fetchLogsPanelData(data, -1)
            .then((res) => {
                alertChart(res);
            })
            .catch(function (xhr, _err) {
                handleErrors(xhr);
            });
    }
    // Alert Type: Metrics
    else if (res.alert_type === 2) {
        let metricsQueryParams;
        if (isFromMetrics) {
            metricsQueryParams = res;
        } else {
            metricsQueryParams = JSON.parse(res.metricsQueryParams);
        }
        // eslint-disable-next-line no-undef
        populateMetricsQueryElement(metricsQueryParams);
    }

    let conditionType = mapIndexToConditionType.get(res.condition);

    $('.alert-condition-option').removeClass('active');
    $(`.alert-condition-options #option-${res.condition}`).addClass('active');

    $('#alert-condition span').text(conditionType);
    $('#threshold-value').val(res.value || 0);
    $('#evaluate-every').val(res.eval_interval || 1);
    $('#evaluate-for').val(res.eval_for || 1);
    $('.message').val(res.message);

    if (isEditMode && !isFromMetrics) {
        alertData.alert_id = res.alert_id;
        $('#alert-name').empty().text(res.alert_name);
    }

    initializeBreadcrumbs([
        { name: 'Alerting', url: './alerting.html' },
        { name: 'Alert Rules', url: './all-alerts.html' },
        { name: res.alert_name ? res.alert_name : 'New Alert Rule', url: '#' },
    ]);

    $('#contact-points-dropdown span').html(res.contact_name).attr('id', res.contact_id);

    res.labels.forEach(function (label) {
        var labelContainer = $(`
        <div class="label-container d-flex align-items-center">
            <input type="text" id="label-key" class="form-control" placeholder="Label name" tabindex="7" value="">
            <span class="label-equal"> = </span>
            <input type="text" id="label-value" class="form-control" placeholder="Value" value="" tabindex="8">
            <button class="btn-simple delete-icon" type="button" id="delete-alert-label"></button>
        </div>
    `);
        labelContainer.find('#label-key').val(label.label_name);
        labelContainer.find('#label-value').val(label.label_value);
        labelContainer.appendTo('.label-main-container');
    });
}

function createAlertFromLogs(queryLanguage, searchText, startEpoch, endEpoch, filterTab) {
    const urlParams = new URLSearchParams(window.location.search);
    const ruleName = decodeURIComponent(urlParams.get('ruleName'));
    $('#alert-rule-name').val(ruleName);

    initializeBreadcrumbs([
        { name: 'Alerting', url: './alerting.html' },
        { name: 'Alert Rules', url: './all-alerts.html' },
        { name: ruleName ? ruleName : 'New Alert Rule', url: '#' },
    ]);

    if (filterTab === '0') {
        codeToBuilderParsing(searchText);
        $('#filter-input').val(searchText);
        isQueryBuilderSearch = true;
    } else if (filterTab === '1') {
        $('#custom-code-tab').tabs('option', 'active', 1);
        $('#filter-input').val(searchText);
    }
    datePickerHandler(startEpoch, endEpoch, startEpoch);
    let data = {
        searchText: searchText,
        startEpoch: startEpoch,
        endEpoch: endEpoch,
        indexName: selectedSearchIndex,
        queryLanguage: queryLanguage,
    };
    fetchLogsPanelData(data, -1)
        .then((res) => {
            alertChart(res);
        })
        .catch(function (xhr, _err) {
            handleErrors(xhr);
        });
}

function handleFormValidationTooltip(alertType) {
    $('#save-alert-btn').off('click');

    $('#save-alert-btn').on('click', function (event) {
        event.preventDefault();

        let isLogsCodeMode = $('#custom-code-tab').tabs('option', 'active');
        let isMetricsCodeMode = $('.raw-query-input').is(':visible');

        // Logs validation
        if (alertType === 'logs' && !isLogsCodeMode) {
            if (thirdBoxSet.size === 0 && secondBoxSet.size === 0 && firstBoxSet.size === 0) {
                $('#logs-error').css('display', 'inline-block');
                $('#metric-error').removeClass('visible');
                $('#contact-point-error').css('display', 'none');
                document.getElementById('logs-error').scrollIntoView({
                    behavior: 'smooth',
                    block: 'center',
                });
                return false;
            }
        }
        // Metrics validation
        else if (alertType === 'metrics' && !isMetricsCodeMode) {
            if ($('#select-metric-input').val().trim() === '') {
                $('#metric-error').css('display', 'inline-block');
                $('#contact-point-error').css('display', 'none');
                document.getElementById('select-metric-input').scrollIntoView({
                    behavior: 'smooth',
                    block: 'center',
                });
                return false;
            }
        }

        // Contact point validation
        if ($('#contact-points-dropdown span').text() === 'Choose' || $('#contact-points-dropdown span').text() === 'Add New') {
            $('#contact-point-error').css('display', 'inline-block');
            document.getElementById('contact-points-dropdown').scrollIntoView({
                behavior: 'smooth',
                block: 'center',
            });
            return false;
        }

        // If all validations pass, submit the form
        submitAddAlertForm(event);
    });

    $('#select-metric-input').on('focus', function () {
        $('#metric-error').css('display', 'none');
    });

    $(document).on('click', function (event) {
        if (!$(event.target).closest('#select-metric-input, #save-alert-btn').length) {
            $('#metric-error').css('display', 'none');
            $('#logs-error').css('display', 'none');
            $('#contact-point-error').css('display', 'none');
        }
    });
}

function alertChart(res) {
    const logsExplorer = document.getElementById('logs-explorer');
    logsExplorer.style.display = 'flex';
    logsExplorer.innerHTML = '';

    if (res.qtype === 'logs-query') {
        showEmptyChart(logsExplorer);
        return;
    }

    // Handle both aggs-query and segstats-query
    if (res.qtype === 'aggs-query' || res.qtype === 'segstats-query') {
        if (!res.measure || res.measure.length === 0) {
            showEmptyChart(logsExplorer);
            return;
        }
        let hits = res.measure;
        const thresholdValue = parseFloat($('#threshold-value').val()) || 0;
        const conditionType = $('#alert-condition span').text();

        const canvas = document.createElement('canvas');
        canvas.style.width = '100%';
        canvas.style.height = '400px';
        logsExplorer.appendChild(canvas);

        const chartData = prepareLogsChartData(res, hits);
        const ctx = canvas.getContext('2d');

        // Destroy existing chart if it exists
        if (alertChartInstance) {
            alertChartInstance.destroy();
        }

        // Calculate the maximum data value for y-axis scaling
        const maxDataValue = Math.max(...chartData.datasets.map((d) => Math.max(...d.data)));
        const maxYTick = maxDataValue * 1.2; // Add 20% padding

        let operator = '>';
        let boxConfig = {};
        let visibleThreshold = Math.min(thresholdValue, maxYTick);
        let thresholdLabel = `y ${operator} ${thresholdValue}`;

        if (conditionType === 'Is above') {
            thresholdLabel = `y > ${thresholdValue}`;
            boxConfig = {
                type: 'box',
                yMin: visibleThreshold,
                yMax: maxYTick,
                backgroundColor: 'rgb(255, 218, 224, 0.8)',
                borderWidth: 0,
            };
        } else if (conditionType === 'Is below') {
            thresholdLabel = `y < ${thresholdValue}`;
            boxConfig = {
                type: 'box',
                yMin: 0,
                yMax: visibleThreshold,
                backgroundColor: 'rgb(255, 218, 224, 0.8)',
                borderWidth: 0,
            };
        } else {
            operator = conditionType === 'Equal to' ? '=' : '≠';
            thresholdLabel = `y ${operator} ${thresholdValue}`;
            boxConfig = {
                borderWidth: 0,
            };
        }

        alertChartInstance = new Chart(ctx, {
            type: 'bar',
            data: {
                labels: chartData.labels,
                datasets: chartData.datasets.map((dataset) => ({
                    ...dataset,
                    backgroundColor: 'rgba(99, 102, 241, 0.6)',
                    borderColor: 'rgba(99, 102, 241, 1)',
                    borderWidth: 1,
                    barPercentage: 0.3,
                    categoryPercentage: 0.8,
                })),
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                layout: {
                    padding: {
                        top: 0,
                    },
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        ticks: {
                            color: '#718096',
                            callback: function (value, index, values) {
                                // Hide label for the maximum tick value
                                if (index === values.length - 1) return '';
                                return value;
                            },
                        },
                        grid: {
                            drawTicks: true,
                            color: function (context) {
                                const maxValue = Math.max(...context.chart.scales.y.ticks.map((t) => t.value));
                                // Hide the top grid line
                                if (context.tick.value === maxValue) return 'rgba(0, 0, 0, 0)';
                                return '#E2E8F0';
                            },
                        },
                        suggestedMin: 0,
                        suggestedMax: maxYTick,
                    },
                    x: {
                        grid: {
                            display: false,
                        },
                        ticks: {
                            color: '#718096',
                            maxRotation: 45,
                            minRotation: 45,
                            font: {
                                size: 10,
                            },
                            padding: 2,
                        },
                    },
                },
                plugins: {
                    legend: {
                        display: true,
                        position: 'bottom',
                        padding: 0,
                        labels: {
                            boxWidth: 10,
                            boxHeight: 8,
                            padding: 4,
                            font: {
                                size: 10,
                            },
                        },
                    },
                    annotation: {
                        annotations: {
                            thresholdLine: {
                                type: 'line',
                                scaleID: 'y',
                                value: visibleThreshold,
                                borderColor: 'rgb(255, 107, 107)',
                                borderWidth: 2,
                                borderDash: [5, 5],
                                label: {
                                    display: true,
                                    content: thresholdLabel,
                                    position: 'start',
                                    backgroundColor: 'rgb(255, 107, 107)',
                                    color: '#fff',
                                    padding: {
                                        x: 6,
                                        y: 4,
                                    },
                                    font: {
                                        size: 12,
                                    },
                                    z: 100,
                                },
                            },
                            thresholdBox: boxConfig,
                        },
                    },
                },
            },
        });

        $('#threshold-value').on('input', updateThreshold);
        $('.alert-condition-options li').on('click', updateThreshold);

        function updateThreshold() {
            const newThresholdValue = parseFloat($('#threshold-value').val()) || 0;
            const newConditionType = $('#alert-condition span').text();
            const maxDataValue = Math.max(...chartData.datasets.map((d) => Math.max(...d.data)));
            const maxYTick = maxDataValue * 1.2;
            const visibleThreshold = Math.min(newThresholdValue, maxYTick);

            let newOperator = '>';
            let newBoxConfig = {};
            let thresholdLabel = '';

            if (newConditionType === 'Is above') {
                thresholdLabel = `y > ${newThresholdValue}`;
                newBoxConfig = {
                    type: 'box',
                    yMin: visibleThreshold,
                    yMax: maxYTick,
                    backgroundColor: 'rgb(255, 218, 224, 0.8)',
                    borderWidth: 0,
                };
            } else if (newConditionType === 'Is below') {
                thresholdLabel = `y < ${newThresholdValue}`;
                newBoxConfig = {
                    type: 'box',
                    yMin: 0,
                    yMax: visibleThreshold,
                    backgroundColor: 'rgb(255, 218, 224, 0.8)',
                    borderWidth: 0,
                };
            } else {
                newOperator = newConditionType === 'Equal to' ? '=' : '≠';
                thresholdLabel = `y ${newOperator} ${newThresholdValue}`;
                newBoxConfig = {
                    borderWidth: 0,
                };
            }

            alertChartInstance.options.plugins.annotation.annotations.thresholdLine.value = visibleThreshold;
            alertChartInstance.options.plugins.annotation.annotations.thresholdLine.label.content = thresholdLabel;
            alertChartInstance.options.plugins.annotation.annotations.thresholdBox = newBoxConfig;

            alertChartInstance.update();
        }
    }
}

function prepareLogsChartData(res, hits) {
    const measureFunctions = res.measureFunctions;

    let labels;
    if (res.qtype === 'segstats-query') {
        labels = ['Total'];
    } else {
        // For aggs queries
        labels = hits.map((item) => {
            const groupByValues = item.GroupByValues || item.IGroupByValues?.map((v) => v.CVal);
            if (!Array.isArray(groupByValues)) {
                return groupByValues || 'NULL';
            }
            // If there's only one group by value
            if (groupByValues.length === 1) {
                return groupByValues[0] || 'NULL';
            }
            //eslint-disable-next-line no-undef
            return formatGroupByValues(groupByValues, true) || 'NULL';
        });
    }

    const datasets = measureFunctions.map((measureFunction) => {
        const data = hits.map((item) => item.MeasureVal[measureFunction] || 0);
        return {
            label: measureFunction,
            data: data,
            backgroundColor: 'rgba(99, 102, 241, 0.8)',
            borderColor: 'rgba(99, 102, 241, 1)',
            borderWidth: 1,
        };
    });

    return { labels, datasets };
}

function handleErrors(error) {
    const logsExplorer = document.getElementById('logs-explorer');
    const errorText = error.responseJSON?.error || error.statusText || 'Failed to fetch logs data';
    
    logsExplorer.style.display = 'flex';
    logsExplorer.innerHTML = `
        <div style="color: #666; text-align: center; padding: 20px; font-size: 16px; font-style: italic;">
            ${errorText}
        </div>
    `;
}

function showEmptyChart(logsExplorer) {
    const canvas = document.createElement('canvas');
    canvas.style.width = '100%';
    canvas.style.height = '400px';
    logsExplorer.appendChild(canvas);

    // Destroy existing chart if it exists
    if (alertChartInstance) {
        alertChartInstance.destroy();
    }

    const ctx = canvas.getContext('2d');
    alertChartInstance = new Chart(ctx, {
        type: 'bar',
        data: {
            labels: [],
            datasets: [
                {
                    data: [],
                    backgroundColor: 'rgba(0, 0, 0, 0.1)',
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            scales: {
                y: {
                    beginAtZero: true,
                    ticks: {
                        color: '#718096',
                    },
                },
                x: {
                    grid: {
                        display: false,
                    },
                    ticks: {
                        color: '#718096',
                    },
                },
            },
            plugins: {
                legend: {
                    display: false,
                },
            },
        },
    });
}
