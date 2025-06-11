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
let alertID;
let alertHistoryData = [];
let autoRefreshInterval = null;
let historyPagination;

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

$(document).ready(async function () {
    $('.theme-btn').on('click', themePickerHandler);

    if ($('#properties-grid').length) {
        //eslint-disable-next-line no-undef
        new agGrid.Grid(document.querySelector('#properties-grid'), propertiesGridOptions);
    }
    if ($('#history-grid').length) {
        //eslint-disable-next-line no-undef
        new agGrid.Grid(document.querySelector('#history-grid'), historyGridOptions);
    }

    await getAlertIdFromURl();
    alertDetailsFunctions();

    startAutoRefresh();

    historyPagination = createSimplePagination('history-pagination', {
        pageSize: 20,
        pageSizeOptions: [10, 20, 50, 100],
        onPageChange: (page, pageSize) => {
            displayHistoryDataPaginated(page, pageSize);
        },
        onPageSizeChange: (pageSize) => {
            displayHistoryDataPaginated(1, pageSize);
        },
    });
});

function startAutoRefresh() {
    if (autoRefreshInterval) {
        clearInterval(autoRefreshInterval);
    }

    autoRefreshInterval = setInterval(() => {
        if (alertID) {
            fetchAlertHistory();
        }
    }, 30000); // 30 seconds
}

async function getAlertIdFromURl() {
    const urlParams = new URLSearchParams(window.location.search);
    if (urlParams.has('id')) {
        const id = urlParams.get('id');
        alertID = id;
        await getAlert(id);
    }
}

async function getAlert(id) {
    const res = await $.ajax({
        method: 'get',
        url: 'api/alerts/' + id,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    });
    initializeBreadcrumbs([
        { name: 'Alerting', url: './alerting.html' },
        { name: 'Alert Rules', url: './all-alerts.html' },
        { name: res.alert.alert_name, url: '#' },
    ]);
    fetchAlertProperties(res);
    fetchAlertHistory();
}

const propertiesBtn = document.getElementById('properties-btn');
const historyBtn = document.getElementById('history-btn');

if (propertiesBtn) {
    propertiesBtn.addEventListener('click', function () {
        document.getElementById('properties-grid').style.display = 'block';
        document.getElementById('history-grid').style.display = 'none';
        document.getElementById('history-search-container').style.display = 'none';
        document.getElementById('history-pagination').style.display = 'none';
        propertiesBtn.classList.add('active');
        historyBtn.classList.remove('active');
        historyPagination.hide();
        $('#alert-details .btn-container').show();
    });
}
if (historyBtn) {
    historyBtn.addEventListener('click', function () {
        document.getElementById('properties-grid').style.display = 'none';
        document.getElementById('history-grid').style.display = 'block';
        document.getElementById('history-search-container').style.display = 'block';
        document.getElementById('history-pagination').style.display = 'block';
        historyBtn.classList.add('active');
        propertiesBtn.classList.remove('active');

        displayHistoryDataPaginated(1, historyPagination.getPageSize());

        $('#alert-details .btn-container').hide();
    });
}

const propertiesGridOptions = {
    columnDefs: [
        { headerName: 'Config Variable Name', field: 'name', sortable: true, filter: true, cellStyle: { 'white-space': 'normal', 'word-wrap': 'break-word' }, width: 200 },
        { headerName: 'Config Variable Value', field: 'value', sortable: true, filter: true, cellStyle: { 'white-space': 'normal', 'word-wrap': 'break-word' }, autoHeight: true },
    ],
    defaultColDef: {
        cellClass: 'align-center-grid',
        resizable: true,
        flex: 1,
        minWidth: 150,
    },
    rowData: [],
    domLayout: 'autoHeight',
    headerHeight: 26,
    rowHeight: 34,
    suppressDragLeaveHidesColumns: true,
};

const historyGridOptions = {
    columnDefs: [
        { headerName: 'Timestamp', field: 'timestamp', sortable: true, filter: true },
        { headerName: 'Action', field: 'action', sortable: true, filter: true },
        { headerName: 'State', field: 'state', sortable: true, filter: true },
    ],
    defaultColDef: {
        cellClass: 'align-center-grid',
        resizable: true,
        flex: 1,
        minWidth: 150,
    },
    rowData: [],
    headerHeight: 26,
    rowHeight: 34,
    suppressDragLeaveHidesColumns: true,
};

$('#history-filter-input').on('keypress', function (e) {
    if (e.which === 13) {
        performSearch();
    }
});

$('#history-filter-input').on('input', function () {
    if ($(this).val().trim() === '') {
        historyPagination.updateState(alertHistoryData.length, 1);
        displayHistoryDataPaginated(1, historyPagination.getPageSize());
    } else {
        performSearch();
    }
});

function performSearch() {
    const searchTerm = $('#history-filter-input').val().trim().toLowerCase();
    const dataToShow = searchTerm ? getFilteredHistoryData(searchTerm) : alertHistoryData;

    // Update pagination with filtered data count
    historyPagination.updateState(dataToShow.length, 1);
    displayHistoryDataPaginated(1, historyPagination.getPageSize());
}

function fetchAlertProperties(res) {
    const alert = res.alert;
    let propertiesData = [];

    if (alert.alert_type === 1) {
        propertiesData.push({ name: 'Query', value: alert.queryParams.queryText }, { name: 'Type', value: alert.queryParams.data_source }, { name: 'Query Language', value: alert.queryParams.queryLanguage });
    } else if (alert.alert_type === 2) {
        const metricsQueryParams = JSON.parse(alert.metricsQueryParams || '{}');
        let formulaString = metricsQueryParams.formulas && metricsQueryParams.formulas.length > 0 ? metricsQueryParams.formulas[0].formula : 'No formula';

        // Replace a, b, etc., with actual query values
        metricsQueryParams.queries.forEach((query) => {
            const regex = new RegExp(`\\b${query.name}\\b`, 'g');
            formulaString = formulaString.replace(regex, query.query);
        });

        propertiesData.push({ name: 'Query', value: formulaString }, { name: 'Type', value: 'Metrics' }, { name: 'Query Language', value: 'PromQL' });
    }

    propertiesData.push({ name: 'Status', value: mapIndexToAlertState.get(alert.state) }, { name: 'Condition', value: `${mapIndexToConditionType.get(alert.condition)}  ${alert.value}` }, { name: 'Evaluate', value: `every ${alert.eval_interval} minutes for ${alert.eval_for} minutes` }, { name: 'Contact Point', value: alert.contact_name });
    if (alert.silence_end_time > Math.floor(Date.now() / 1000)) {
        //eslint-disable-next-line no-undef
        let mutedFor = calculateMutedFor(alert.silence_end_time);
        propertiesData.push({ name: 'Silenced For', value: mutedFor });
    }
    if (alert.labels && alert.labels.length > 0) {
        const labelsValue = alert.labels.map((label) => `${label.label_name}:${label.label_value}`).join(', ');
        propertiesData.push({ name: 'Label', value: labelsValue });
    }

    if (propertiesGridOptions.api) {
        propertiesGridOptions.api.setRowData(propertiesData);
    } else {
        console.error('propertiesGridOptions.api is not defined');
    }
}

function displayHistoryDataPaginated(page, pageSize) {
    const searchTerm = $('#history-filter-input').val().trim().toLowerCase();
    const dataToShow = searchTerm ? getFilteredHistoryData(searchTerm) : alertHistoryData;

    const startIndex = (page - 1) * pageSize;
    const endIndex = startIndex + pageSize;
    const pageData = dataToShow.slice(startIndex, endIndex);

    if (historyGridOptions.api) {
        historyGridOptions.api.setRowData(pageData);
    }
}

function fetchAlertHistory() {
    if (alertID) {
        $.ajax({
            method: 'get',
            url: `api/alerts/${alertID}/history`,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            dataType: 'json',
            crossDomain: true,
        })
            .then(function (res) {
                // Store the data locally
                alertHistoryData = res.alertHistory.map((item) => ({
                    timestamp: new Date(item.event_triggered_at).toLocaleString(),
                    action: item.event_description,
                    state: mapIndexToAlertState.get(item.alert_state),
                }));

                const searchTerm = $('#history-filter-input').val().trim().toLowerCase();
                const dataToShow = searchTerm ? getFilteredHistoryData(searchTerm) : alertHistoryData;
                historyPagination.updateState(dataToShow.length, historyPagination.getCurrentPage());
                displayHistoryDataPaginated(historyPagination.getCurrentPage(), historyPagination.getPageSize());
            })
            .catch(function (err) {
                console.error('Error fetching alert history:', err);
            });
    }
}
function getFilteredHistoryData(searchTerm) {
    return alertHistoryData.filter((item) => {
        const action = item.action.toLowerCase();
        const state = item.state.toLowerCase();
        return action.includes(searchTerm) || state.includes(searchTerm);
    });
}
function alertDetailsFunctions() {
    function getAlert(event) {
        var queryString = '?id=' + alertID;
        window.location.href = '../alert.html' + queryString;
        event.stopPropagation();
    }

    function deleteAlert() {
        $.ajax({
            method: 'delete',
            url: 'api/alerts/delete',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            data: JSON.stringify({
                alert_id: alertID,
            }),
            crossDomain: true,
        })
            .then(function (res) {
                showToast(res.message);
                window.location.href = '../all-alerts.html';
            })
            .catch((err) => {
                showToast(err.responseJSON.error, 'error');
            });
    }

    function showPrompt(event) {
        event.stopPropagation();
        $('.popupOverlay, .popupContent').addClass('active');

        $('#cancel-btn, .popupOverlay, #delete-btn').click(function () {
            $('.popupOverlay, .popupContent').removeClass('active');
        });
        $('#delete-btn').click(deleteAlert);
    }

    $('#edit-alert-btn').on('click', getAlert);
    $('#delete-alert').on('click', showPrompt);
    $('#cancel-alert-details').on('click', function () {
        window.location.href = '../all-alerts.html';
    });
}
