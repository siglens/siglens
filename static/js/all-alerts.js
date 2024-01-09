/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

'use strict';

let alertGridDiv = null;
let alertRowData = [];

let mapIndexToAlertState=new Map([
    [0,"Normal"],
    [1,"Pending"],
    [2,"Firing"],
]);

$(document).ready(function () {
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    $('.theme-btn').on('click', themePickerHandler);
    getAllAlerts();

    $('#new-alert-rule').on('click',function(){
        window.location.href = "../alert.html";
    });
});

//get all alerts
function getAllAlerts(){
    $.ajax({
        method: "get",
        url: "api/allalerts",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        displayAllAlerts(res.alerts);
    })
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

        function editAlert(event){        
            var queryString = "?id=" + params.data.alertId;
            window.location.href = "../alert.html" + queryString;
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
                    alert_id: params.data.alertId
                }),
				crossDomain: true,
			}).then(function (res) {
				let deletedRowID = params.data.rowId;
				alertGridOptions.api.applyTransaction({
					remove: [{ rowId: deletedRowID }],
				});
                showToast(res.message)
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
            $('#delete-btn').click(deleteAlert)
		}

		
		this.eButton.addEventListener('click', editAlert);
		this.dButton.addEventListener('click', showPrompt);
	}

	getGui() {
		return this.eGui;
	}
	refresh(params) {
		return false;
	}
}

let alertColumnDefs = [
    {
        field: "rowId",
		hide: true
    },
    {
        field: "alertId",
		hide: true
    },
    {
        headerName: "State",
        field: "alertState",
        width:50,
    },
    {
        headerName: "Alert Name",
        field: "alertName",
        width: 100,
    },
    {
        headerName: "Labels",
        field: "labels",
        width:100,
    },
    {
        headerName: "Actions",
        cellRenderer: btnRenderer,
        width:50,
    },
];

const alertGridOptions = {
    columnDefs: alertColumnDefs,
	rowData: alertRowData,
	animateRows: true,
	rowHeight: 44,
    headerHeight:32,
	defaultColDef: {
		icons: {
			sortAscending: '<i class="fa fa-sort-alpha-up"/>',
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

function displayAllAlerts(res){
    if (alertGridDiv === null) {
        alertGridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(alertGridDiv, alertGridOptions);
    }
    alertGridOptions.api.setColumnDefs(alertColumnDefs);
    let newRow = new Map()
    $.each(res, function (key, value) {
        newRow.set("rowId", key);
        newRow.set("alertId", value.alert_id);
        newRow.set("alertName", value.alert_name);
        let labels= [];
        value.labels.forEach(function (label) {
            labels.push(label.label_name + '=' + label.label_value);
        });
        let allLabels = labels.join(', ');
    
        newRow.set("labels", allLabels);
        newRow.set("alertState", mapIndexToAlertState.get(value.state));
        alertRowData = _.concat(alertRowData, Object.fromEntries(newRow));
    })
    alertGridOptions.api.setRowData(alertRowData);
    alertGridOptions.api.sizeColumnsToFit();
}

function showToast(msg) {
    let toast =
        `<div class="div-toast" id="save-db-modal"> 
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast)
    setTimeout(removeToast, 2000);
}

function removeToast() {
    $('.div-toast').remove();
}


function onRowClicked(event) {
    var queryString = "?id=" + event.data.alertId;
    window.location.href = "../alert-details.html" + queryString;
    event.stopPropagation();
}