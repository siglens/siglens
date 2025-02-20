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
let alertEditFlag = 0;
let alertFromMetricsExplorerFlag = 0;
let messageTemplateInfo = `<i class="fa fa-info-circle position-absolute info-icon sendMsg" rel="tooltip" id="info-icon-msg" style="display: block;" title = "You can use following template variables:' + '\n' + inDoubleBrackets('alert_rule_name') + '\n' + inDoubleBrackets('query_string') + '\n' + inDoubleBrackets('condition') + '\n' + inDoubleBrackets('queryLanguage') + '"></i>`;
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

let mapIndexToAlertState = new Map([
    [0, 'Inactive'],
    [1, 'Normal'],
    [2, 'Pending'],
    [3, 'Firing'],
]);

const alertForm = $('#alert-form');

let originalIndexValues;
//eslint-disable-next-line no-unused-vars
let indexValues;

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);

    $('.theme-btn').on('click', updateChartColorsBasedOnTheme);

    // $('#logs-language-btn').show();
    let startTime = 'now-30m';
    let endTime = 'now';
    datePickerHandler(startTime, endTime, startTime);
    setupEventHandlers();

    await getAlertIdFromURL();

    $('.alert-condition-options li').on('click', setAlertConditionHandler);
    $('#contact-points-dropdown').on('click', contactPointsDropdownHandler);
    // $('#logs-language-options li').on('click', setLogsLangHandler);
    // $('#data-source-options li').on('click', function () {
    //     let alertType;
    //     if ($(this).html() === 'Logs') {
    //         alertType = 1;
    //     } else {
    //         alertType = 2;
    //         $('#save-alert-btn').on('click', function () {
    //             if ($('#select-metric-input').val === '') {
    //                 $('#save-alert-btn').prop('disabled', true);
    //             } else {
    //                 $('#save-alert-btn').prop('disabled', false);
    //             }
    //         });
    //     }
    //     setDataSourceHandler(alertType);
    // });

    $('#cancel-alert-btn').on('click', function () {
        window.location.href = '../all-alerts.html';
        resetAddAlertForm();
    });

    // alertForm.on('submit', (e) => {
    //     console.log('form submit');
    //     submitAddAlertForm(e);
    // });

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

    // Enable the save button when a contact point is selected
    $('.contact-points-options li').on('click', function () {
        $('#contact-points-dropdown span').text($(this).text());
        $('#save-alert-btn').prop('disabled', false);
        $('#contact-point-error').css('display', 'none'); // Hide error message when a contact point is selected
    });

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

    $('#all-alerts-text').click(function () {
        window.location.href = '../all-alerts.html';
    });

    // $(document).on('submit', '#alert-form', function (e) {
    //     console.log('form submit via delegation');
    //     e.preventDefault();
    //     submitAddAlertForm(e);
    // });
});

async function getAlertIdFromURL() {
    const urlParams = new URLSearchParams(window.location.search);
    let alertType = urlParams.get('type') || 'logs';
    console.log('getAlertIdFromURL', alertType);

    // Alert Creation
    if (alertType === 'logs') {
        $('.query-container, .logs-lang-container, .index-box, #logs-explorer').show();
        $('#metrics-explorer, #metrics-graphs').hide();
        let indexes = await getListIndices();
        if (indexes) {
            originalIndexValues = indexes.map((item) => item.index);
            indexValues = [...originalIndexValues];
        }
        initializeIndexAutocomplete();
        setIndexDisplayValue(selectedSearchIndex);

        //Show empty chart initially
        const logsExplorer = document.getElementById('logs-explorer');
        showEmptyChart(logsExplorer);
    } else if (alertType === 'metrics') {
        $('.query-container, .logs-lang-container, .index-box, #logs-explorer').hide();
        $('#metrics-explorer, #metrics-graphs').show();
    }

    // If we have id in URL , alert editing
    if (urlParams.has('id')) {
        const id = urlParams.get('id');
        alertType = await editAlert(id);
    }
    // If alert created from metrics
    else if (urlParams.has('queryString')) {
        console.log('Alert created from metrics');
        let dataParam = getUrlParameter('queryString');
        let jsonString = decodeURIComponent(dataParam);
        let obj = JSON.parse(jsonString);
        alertFromMetricsExplorerFlag = 1;

        // createAlertFromMetrics(obj);
        fillAlertForm(obj);
        alertData.alert_type = 2;
        alertType = 'metrics';
    }
    // If alert created from logs
    else if (urlParams.has('queryLanguage')) {
        const queryLanguage = urlParams.get('queryLanguage');
        const searchText = urlParams.get('searchText');
        const startEpoch = urlParams.get('startEpoch');
        const endEpoch = urlParams.get('endEpoch');
        const filterTab = urlParams.get('filterTab');
        createAlertFromLogs(queryLanguage, searchText, startEpoch, endEpoch, filterTab);
        alertData.alert_type = 2;
        alertType = 'logs';

    }

    if (!alertEditFlag && !alertFromMetricsExplorerFlag) {
        addQueryElement();
    }
    handleFormValidationTooltip(alertType);
}

async function editAlert(alertId) {
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

    alertEditFlag = true;
    alertFromMetricsExplorerFlag = 0;
    fillAlertForm(res.alert);
    alertData.alert_type = res.alert.alert_type;
    const alertType = res.alert.alert_type === 1 ? 'logs' : 'metrics';

    return alertType;
}

function setAlertConditionHandler(_e) {
    $('.alert-condition-option').removeClass('active');
    $('#alert-condition span').html($(this).html());
    $(this).addClass('active');
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

$(document).keyup(function (e) {
    if (e.key === 'Escape' || e.key === 'Esc') {
        $('.popupOverlay, .popupContent').removeClass('active');
    }
});

function submitAddAlertForm(e) {
    e.preventDefault();
    console.log('Submit Add Alert Form');
    console.log('alertEditFlag:', alertEditFlag);
    console.log('alertFromMetricsExplorerFlag:', alertFromMetricsExplorerFlag);

    setAlertRule();
    console.log('Alert data after setAlertRule:', alertData);

    if (alertEditFlag && !alertFromMetricsExplorerFlag) {
        console.log('Updating existing alert');
        updateAlertRule(alertData);
    } else {
        console.log('Creating new alert');
        createNewAlertRule(alertData);
    }
}

function setAlertRule() {
    let alertType;
    if (alertEditFlag || alertFromMetricsExplorerFlag) {
        // For edit mode, get type from alertData's alert_type
        alertType = alertData.alert_type === 1 ? 'logs' : 'metrics';
    } else {
        // For new alerts, get from URL or default to 'logs'
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

    console.log(alertData);
}

function createNewAlertRule(alertData) {
    if (!alertData.alert_type) {
        alertData.alert_type = 1;
    }
    $.ajax({
        method: 'post',
        url: 'api/alerts/create',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(alertData),
        dataType: 'json',
        crossDomain: true,
    })
        .then((_res) => {
            resetAddAlertForm();
            window.location.href = '../all-alerts.html';
        })
        .catch((err) => {
            showToast(err.responseJSON.error, 'error');
        });
}

// update alert rule
function updateAlertRule(alertData) {
    if (!alertData.alert_type) {
        alertData.alert_type = 1;
    }
    $.ajax({
        method: 'post',
        url: 'api/alerts/update',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(alertData),
        dataType: 'json',
        crossDomain: true,
    })
        .then((_res) => {
            // resetAddAlertForm();
            // window.location.href = '../all-alerts.html';
        })
        .catch((err) => {
            showToast(err.responseJSON.error, 'error');
        });
}

function resetAddAlertForm() {
    alertForm[0].reset();
}

async function fillAlertForm(res) {
    console.log(res);
    $('#alert-rule-name').val(res.alert_name);
    setDataSourceHandler(res.alert_type);

    // Alert Type: Logs
    if (res.alert_type === 1) {
        const { data_source, queryLanguage, startTime, endTime, queryText, queryMode, index } = res.queryParams;

        // $('#alert-data-source span').html(data_source);
        // $('#logs-language-btn span').text(queryLanguage);
        // $('.logs-language-option').removeClass('active');
        // $(`.logs-language-option:contains(${queryLanguage})`).addClass('active');
        // displayQueryToolTip();

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

        initializeFilterInputEvents();

        let data = {
            state: wsState,
            searchText: queryText,
            startEpoch: startTime,
            endEpoch: endTime,
            indexName: index,
            queryLanguage: 'Splunk QL',
        };

        fetchLogsPanelData(data, -1).then((res) => {
            alertChart(res);
        });

        // $('#query').val(res.queryParams.queryText);

        // $(`.ranges .inner-range #${res.queryParams.startTime}`).addClass('active');
        // datePickerHandler(res.queryParams.startTime, res.queryParams.endTime, res.queryParams.startTime);
    }
    // Alert Type: Metrics
    else if (res.alert_type === 2) {
        let metricsQueryParams;
        if (alertFromMetricsExplorerFlag) {
            metricsQueryParams = res;
        } else {
            metricsQueryParams = JSON.parse(res.metricsQueryParams);
        } // eslint-disable-next-line no-undef
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

    if (alertEditFlag && !alertFromMetricsExplorerFlag) {
        alertData.alert_id = res.alert_id;
        $('#alert-name').empty().text(res.alert_name);
    }

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

// function setLogsLangHandler(_e) {
//     $('.logs-language-option').removeClass('active');
//     $('#logs-language-btn span').html($(this).html());
//     $(this).addClass('active');
//     displayQueryToolTip($(this).html());
// }

function setDataSourceHandler(alertType) {
    // $('.data-source-option').removeClass('active');
    const isLogs = alertType === 1;
    // const sourceText = isLogs ? 'Logs' : 'Metrics';
    // const $span = $('#alert-data-source span');

    // $span.html(sourceText);
    // $(`.data-source-option:contains("${sourceText}")`).addClass('active');

    $('.query-container, .logs-lang-container, .index-box, #logs-explorer').toggle(isLogs);
    $('#metrics-explorer, #metrics-graphs').toggle(!isLogs);

    if (isLogs) {
        $('#query').attr('required', 'required');
    } else {
        $('#query').removeAttr('required');
    }
}

// function displayQueryToolTip(selectedQueryLang) {
//     $('#info-icon-pipeQL, #info-icon-spl').hide();
//     if (selectedQueryLang === 'Pipe QL') {
//         $('#info-icon-pipeQL').show();
//     } else if (selectedQueryLang === 'Splunk QL') {
//         $('#info-icon-spl').show();
//     }
// }

// Add Label
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

function createAlertFromLogs(queryLanguage, searchText, startEpoch, endEpoch, filterTab) {
    const urlParams = new URLSearchParams(window.location.search);
    $('#alert-rule-name').val(decodeURIComponent(urlParams.get('ruleName')));

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
    console.log(data);
    fetchLogsPanelData(data, -1).then((res) => {
        alertChart(res);
    });
}

function createAlertFromMetrics(data) {
    $('#metrics-explorer, #metrics-graphs').show();
    $('.index-box, #logs-explorer, .query-builder-container').hide();
    populateMetricsQueryElement(data);
}
function handleFormValidationTooltip(alertType) {
    // Remove any existing click handlers from the save button
    $('#save-alert-btn').off('click');

    // Add our consolidated handler
    $('#save-alert-btn').on('click', function (event) {
        event.preventDefault();
        console.log('Form Validation Tooltip clicked saved button');
        console.log('handleFormValidationTooltip: alertType', alertType);
        let isLogsCodeMode = $('#custom-code-tab').tabs('option', 'active');
        let isMetricsCodeMode = $('.raw-query-input').is(':visible');
        // Logs validation
        if (alertType === 'logs' && !isLogsCodeMode) {
            console.log('Form Validation Tooltip');
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
            console.log('handleFormValidationTooltip: alertType=' + alertType);
            if ($('#select-metric-input').val().trim() === '') {
                console.log('handleFormValidationTooltip: metrics if condition');
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
            console.log('contact point validation');
            $('#contact-point-error').css('display', 'inline-block');
            document.getElementById('contact-points-dropdown').scrollIntoView({
                behavior: 'smooth',
                block: 'center',
            });
            return false;
        }

        // If all validations pass, call submitAddAlertForm directly
        console.log('Validation passed, calling submitAddAlertForm');
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

let alertChartInstance = null;

function alertChart(res) {
    const logsExplorer = document.getElementById('logs-explorer');
    logsExplorer.style.display = 'flex';
    logsExplorer.innerHTML = '';

    if (res.qtype === 'logs-query' || res.qtype === 'segstats-query') {
        showEmptyChart(logsExplorer);
        return;
    }

    if (res.qtype === 'aggs-query') {
        let columnOrder = getColumnOrder(res);
        if (handleErrors(res, logsExplorer) || !hasValidData(res)) {
            showEmptyChart(logsExplorer);
            return;
        }
        let hits = res.measure;
        const thresholdValue = parseFloat($('#threshold-value').val()) || 0;
        const conditionType = $('#alert-condition span').text();

        if (columnOrder.length > 1) {
            const canvas = document.createElement('canvas');
            canvas.style.width = '100%';
            canvas.style.height = '400px';
            logsExplorer.appendChild(canvas);

            const { labels, datasets } = prepareLogsChartData(res, hits);
            const ctx = canvas.getContext('2d');

            // Destroy existing chart if it exists
            if (alertChartInstance) {
                alertChartInstance.destroy();
            }

            // Calculate the maximum data value for y-axis scaling
            const maxDataValue = Math.max(...datasets.map((d) => Math.max(...d.data)));
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
                    labels: labels,
                    datasets: datasets,
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    scales: {
                        y: {
                            beginAtZero: true,
                            ticks: {
                                color: '#718096',
                                callback: function (value, index, values) {
                                    // Hide label for the maximum tick value
                                    if (index === values.length - 1) {
                                        return '';
                                    }
                                    return value;
                                },
                            },
                            grid: {
                                drawTicks: true,
                                color: function (context) {
                                    const maxValue = Math.max(...context.chart.scales.y.ticks.map((t) => t.value));

                                    // Hide the top grid line
                                    if (context.tick.value === maxValue) {
                                        return 'rgba(0, 0, 0, 0)';
                                    }
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
                            },
                        },
                    },
                    plugins: {
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
                const maxDataValue = Math.max(...datasets.map((d) => Math.max(...d.data)));
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
}
function prepareLogsChartData(res, hits) {
    const multipleGroupBy = hits[0].GroupByValues.length > 1;
    const measureFunctions = res.measureFunctions;

    const labels = hits.map((item) => formatGroupByValues(item.GroupByValues, multipleGroupBy) || 'NULL');

    const datasets = measureFunctions.map((measureFunction) => {
        const data = hits.map((item) => item.MeasureVal[measureFunction] || 0);
        return {
            label: measureFunction,
            data: data,
        };
    });

    return { labels, datasets };
}

// Helper functions
function getColumnOrder(res) {
    let columnOrder = [];
    if (res.columnsOrder?.length > 0) {
        columnOrder = res.columnsOrder;
    } else {
        if (res.groupByCols) {
            columnOrder = _.uniq(_.concat(res.groupByCols));
        }
        if (res.measureFunctions) {
            columnOrder = _.uniq(_.concat(columnOrder, res.measureFunctions));
        }
    }
    return columnOrder;
}

function handleErrors(res, logsExplorer) {
    if (res.errors) {
        const errorMsg = document.createElement('div');
        errorMsg.textContent = res.errors[0];
        logsExplorer.appendChild(errorMsg);
        return true;
    }
    return false;
}

function hasValidData(res) {
    const hits = res.measure;
    if (!hits || hits.length === 0) {
        $('#logs-explorer').hide();
        return false;
    }
    return true;
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

function updateChartColorsBasedOnTheme() {
    //eslint-disable-next-line no-undef
    const { gridLineColor, tickColor } = getGraphGridColors();
    //eslint-disable-next-line no-undef
    if (mergedGraph) {
        //eslint-disable-next-line no-undef
        mergedGraph.options.scales.x.ticks.color = tickColor;
        //eslint-disable-next-line no-undef
        mergedGraph.options.scales.y.ticks.color = tickColor;
        //eslint-disable-next-line no-undef
        mergedGraph.options.scales.y.grid.color = gridLineColor;
        //eslint-disable-next-line no-undef
        mergedGraph.update();
    }

    for (const queryName in chartDataCollection) {
        if (Object.prototype.hasOwnProperty.call(chartDataCollection, queryName)) {
            //eslint-disable-next-line no-undef
            const lineChart = lineCharts[queryName];

            lineChart.options.scales.x.ticks.color = tickColor;
            lineChart.options.scales.y.ticks.color = tickColor;
            lineChart.options.scales.y.grid.color = gridLineColor;
            lineChart.update();
        }
    }
}
