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
    static activeDropdown = null;
    static globalListenerAdded = false;

    init(params) {
        this.params = params;
        this.eGui = document.createElement('span');
        this.eGui.innerHTML = `
            <div id="alert-grid-btn">
                <button class='btn' id="editbutton" title="Edit Alert Rule"></button>
                <button class="btn-simple" id="delbutton" title="Delete Alert Rule"></button>
                <div class="custom-alert-dropdown">
                    <button class="btn mute-icon" id="mute-icon" title="Mute"></button>
                </div>
            </div>`;

        this.eButton = this.eGui.querySelector('#editbutton');
        this.dButton = this.eGui.querySelector('.btn-simple');
        this.mButton = this.eGui.querySelector('#mute-icon');

        this.eButton.addEventListener('click', this.editAlert.bind(this));
        this.dButton.addEventListener('click', this.showPrompt.bind(this));
        this.mButton.addEventListener('click', this.toggleMuteDropdown.bind(this));

        // Create dropdown element
        this.dropdown = document.createElement('div');
        this.dropdown.className = 'custom-alert-dropdown-menu daterangepicker dropdown-menu';
        this.dropdown.id = 'daterangepicker-' + params.data.alertId;
        this.dropdown.style.display = 'none';
        this.dropdown.style.position = 'absolute';
        this.dropdown.innerHTML = `
            <p class="dt-header">Silence for</p>
            <div class="ranges">
                <div class="inner-range">
                    <div id="now-5m" class="range-item">5 Mins</div>
                    <div id="now-3h" class="range-item">3 Hrs</div>
                    <div id="now-2d" class="range-item">2 Days</div>
                </div>
                <div class="inner-range">
                    <div id="now-15m" class="range-item">15 Mins</div>
                    <div id="now-6h" class="range-item">6 Hrs</div>
                    <div id="now-7d" class="range-item">7 Days</div>
                </div>
                <div class="inner-range">
                    <div id="now-30m" class="range-item">30 Mins</div>
                    <div id="now-12h" class="range-item">12 Hrs</div>
                    <div id="now-30d" class="range-item">30 Days</div>
                </div>
                <div class="inner-range">
                    <div id="now-1h" class="range-item">1 Hr</div>
                    <div id="now-24h" class="range-item">24 Hrs</div>
                    <div id="now-90d" class="range-item">90 Days</div>
                </div>
            </div>`;

        this.dropdown.querySelectorAll('.range-item').forEach(item => {
            item.addEventListener('click', this.handleSilenceSelection.bind(this));
        });

        const gridContainer = document.querySelector('.ag-root-wrapper');
        gridContainer.appendChild(this.dropdown);

        this.gridApi = params.api;
        this.gridApi.addEventListener('bodyScroll', this.updateDropdownPosition.bind(this));

        if (!btnRenderer.globalListenerAdded) {
            document.addEventListener('click', btnRenderer.handleGlobalClick);
            btnRenderer.globalListenerAdded = true;
        }
    }

    static handleGlobalClick(event) {
        if (!event.target.closest('.custom-alert-dropdown') && !event.target.closest('.custom-alert-dropdown-menu')) {
            btnRenderer.closeAllDropdowns();
        }
    }

    static closeAllDropdowns() {
        if (btnRenderer.activeDropdown) {
            btnRenderer.activeDropdown.style.display = 'none';
            btnRenderer.activeDropdown = null;
        }
    }

    updateDropdownPosition() {
        if (this.dropdown.style.display === 'block') {
            const buttonRect = this.mButton.getBoundingClientRect();
            const gridContainer = document.querySelector('.ag-root-wrapper');
            const gridRect = gridContainer.getBoundingClientRect();

            // Calculate position relative to grid container
            const top = buttonRect.bottom - gridRect.top + gridContainer.scrollTop;
            const left = buttonRect.right - gridRect.left + gridContainer.scrollLeft;

            this.dropdown.style.top = `${top}px`;
            this.dropdown.style.left = `${left-300}px`;
            this.dropdown.style.zIndex = '9999';
        }
    }

    editAlert(event) {
        event.stopPropagation();
        var queryString = '?id=' + this.params.data.alertId;
        window.location.href = '../alert.html' + queryString;
    }

    deleteAlert() {
        $.ajax({
            method: 'delete',
            url: 'api/alerts/delete',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            data: JSON.stringify({
                alert_id: this.params.data.alertId,
            }),
            crossDomain: true,
        }).then((res) => {
            let deletedRowID = this.params.data.rowId;
            alertGridOptions.api.applyTransaction({
                remove: [{ rowId: deletedRowID }],
            });
            showToast(res.message, 'success');
        }).catch((err) => {
            showToast('Failed to delete alert', 'error');
        });
    }

    showPrompt(event) {
        event.stopPropagation();
        const alertRuleName = this.params.data.alertName;
        const confirmationMessage = `Are you sure you want to delete the "<strong>${alertRuleName}</strong>" alert?`;

        $('.popupOverlay, .popupContent').addClass('active');
        $('#delete-alert-name').html(confirmationMessage);

        $('#cancel-btn, .popupOverlay').off('click').on('click', () => {
            $('.popupOverlay, .popupContent').removeClass('active');
        });

        $('#delete-btn').off('click').on('click', () => {
            $('.popupOverlay, .popupContent').removeClass('active');
            this.deleteAlert();
        });
    }

    toggleMuteDropdown(event) {
        event.stopPropagation();
        
        if (btnRenderer.activeDropdown && btnRenderer.activeDropdown !== this.dropdown) {
            btnRenderer.activeDropdown.style.display = 'none';
        }

        if (this.dropdown.style.display === 'block') {
            this.dropdown.style.display = 'none';
            btnRenderer.activeDropdown = null;
        } else {
            this.dropdown.style.display = 'block';
            btnRenderer.activeDropdown = this.dropdown;
            this.updateDropdownPosition();
        }
    }

    handleSilenceSelection(event) {
        event.stopPropagation();
        const id = event.target.id;
        const minutesMap = {
            'now-5m': 5, 'now-15m': 15, 'now-30m': 30, 'now-1h': 60,
            'now-3h': 180, 'now-6h': 360, 'now-12h': 720, 'now-24h': 1440,
            'now-2d': 2880, 'now-7d': 10080, 'now-30d': 43200, 'now-90d': 129600
        };
        const minutes = minutesMap[id] || 0;
        this.silenceAlert(minutes);
        btnRenderer.closeAllDropdowns();
    }

    silenceAlert(minutes) {
        $.ajax({
            method: 'PUT',
            url: 'api/alerts/silenceAlert',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            data: JSON.stringify({
                alert_id: this.params.data.alertId,
                silence_minutes: minutes
            }),
            crossDomain: true,
        })
        .done((res) => {
            showToast(res.message, 'success');
        })
        .fail((err) => {
            showToast('Failed to silence alert', 'error');
        })
        .always(() => {
            this.dropdown.style.display = 'none';
            btnRenderer.activeDropdown = null;
        });
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
        width: 150,
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
