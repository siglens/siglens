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

let alertGridDiv = null;
let alertRowData = [];

let mapIndexToAlertState = new Map([
    [0, 'Inactive'],
    [1, 'Normal'],
    [2, 'Pending'],
    [3, 'Firing'],
]);

let mapIndexToAlertType = new Map([
    [1, 'Logs'],
    [2, 'Metrics'],
]);

$(document).ready(function () {
    $('.theme-btn').on('click', themePickerHandler);
    getAllAlerts();

    $('#new-alert-rule').on('click', function () {
        window.location.href = '../alert.html';
    });
});

//get all alerts
function getAllAlerts() {
    $.ajax({
        method: 'get',
        url: 'api/allalerts',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        displayAllAlerts(res.alerts);
    });
}
// Custom cell renderer for State field
function getCssVariableValue(variableName) {
    return getComputedStyle(document.documentElement).getPropertyValue(variableName).trim();
}

function stateCellRenderer(params) {
    let state = params.value;
    let color;
    switch (state) {
        case 'Normal':
            color = getCssVariableValue('--color-normal');
            break;
        case 'Pending':
            color = getCssVariableValue('--color-pending');
            break;
        case 'Firing':
            color = getCssVariableValue('--color-firing');
            break;
        default:
            color = getCssVariableValue('--color-inactive');
    }
    return `<div style="background-color: ${color}; padding: 5px; border-radius: 5px; color: white">${state}</div>`;
}

class btnRenderer {
    init(params) {
        this.eGui = document.createElement('span');
        this.eGui.innerHTML = `<div id="alert-grid-btn">
				<button class='btn' id="editbutton" title="Edit Alert Rule"></button>
                <button class="btn-simple" id="delbutton" title="Delete Alert Rule"></button>
				</div>`;
        this.eButton = this.eGui.querySelector('#editbutton');
        this.dButton = this.eGui.querySelector('.btn-simple');

        function editAlert(event) {
            var queryString = '?id=' + params.data.alertId;
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
                    alert_id: params.data.alertId,
                }),
                crossDomain: true,
            }).then(function (res) {
                let deletedRowID = params.data.rowId;
                alertGridOptions.api.applyTransaction({
                    remove: [{ rowId: deletedRowID }],
                });
                showToast(res.message, 'success');
            });
        }

        function showPrompt(event) {
            event.stopPropagation();
            const alertRuleName = params.data.alertName;
            const confirmationMessage = `Are you sure you want to delete the "<strong>${alertRuleName}</strong>" alert?`;

            $('.popupOverlay, .popupContent').addClass('active');
            $('#delete-alert-name').html(confirmationMessage);

            $('#cancel-btn, .popupOverlay, #delete-btn').click(function () {
                $('.popupOverlay, .popupContent').removeClass('active');
            });
            $('#delete-btn').click(deleteAlert);
        }

        this.eButton.addEventListener('click', editAlert);
        this.dButton.addEventListener('click', showPrompt);
    }

    getGui() {
        return this.eGui;
    }
    refresh() {
        return false;
    }
}

let alertColumnDefs = [
    {
        field: 'rowId',
        hide: true,
    },
    {
        field: 'alertId',
        hide: true,
    },
    {
        headerName: 'State',
        field: 'alertState',
        width: 100,
        cellRenderer: stateCellRenderer,
    },
    {
        headerName: 'Alert Name',
        field: 'alertName',
        width: 200,
    },
    {
        headerName: 'Alert Type',
        field: 'alertType',
        width: 100,
    },
    {
        headerName: 'Labels',
        field: 'labels',
        width: 200,
    },
    {
        headerName: 'Actions',
        cellRenderer: btnRenderer,
        width: 100,
    },
];

const alertGridOptions = {
    columnDefs: alertColumnDefs,
    rowData: alertRowData,
    animateRows: true,
    rowHeight: 44,
    headerHeight: 32,
    defaultColDef: {
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        cellClass: 'align-center-grid',
        resizable: true,
        sortable: true,
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    getRowId: (params) => params.data.rowId,
    onGridReady(params) {
        this.gridApi = params.api;
    },
    onRowClicked: onRowClicked,
};

function displayAllAlerts(res) {
    if (alertGridDiv === null) {
        alertGridDiv = document.querySelector('#ag-grid');
        //eslint-disable-next-line no-undef
        new agGrid.Grid(alertGridDiv, alertGridOptions);
    }
    alertGridOptions.api.setColumnDefs(alertColumnDefs);
    let newRow = new Map();
    $.each(res, function (key, value) {
        newRow.set('rowId', key);
        newRow.set('alertId', value.alert_id);
        newRow.set('alertName', value.alert_name);
        let labels = [];
        value.labels.forEach(function (label) {
            labels.push(label.label_name + '=' + label.label_value);
        });
        let allLabels = labels.join(', ');

        newRow.set('labels', allLabels);
        newRow.set('alertState', mapIndexToAlertState.get(value.state));
        newRow.set('alertType', mapIndexToAlertType.get(value.alert_type));
        alertRowData = _.concat(alertRowData, Object.fromEntries(newRow));
    });
    alertGridOptions.api.setRowData(alertRowData);
    alertGridOptions.api.sizeColumnsToFit();
}

function onRowClicked(event) {
    var queryString = '?id=' + event.data.alertId;
    window.location.href = '../alert-details.html' + queryString;
    event.stopPropagation();
}
