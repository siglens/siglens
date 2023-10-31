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

let dbgridDiv = null;
let dbRowData = [];

async function getAllDashboards() {
	let serverResponse = []
	await $.ajax({
		method: 'get',
		url: 'api/dashboards/listall',
		headers: {
			'Content-Type': 'application/json; charset=utf-8',
			'Accept': '*/*'
		},
		crossDomain: true,
		dataType: 'json',
	}).then(function (res) {
		serverResponse = res;
	})
	return serverResponse
}

async function getAllDefaultDashboards() {
	let serverResponse = []
	await $.ajax({
		method: 'get',
		url: 'api/dashboards/defaultlistall',
		headers: {
			'Content-Type': 'application/json; charset=utf-8',
			'Accept': '*/*'
		},
		crossDomain: true,
		dataType: 'json',
	}).then(function (res) {
		serverResponse = res;
	})
	return serverResponse
}

function createDashboard() {
	$('.popupOverlay, .popupContent').addClass('active');
	$('#new-dashboard-modal').show();
	$('#delete-db-prompt').hide();
  
	function createDashboardWithInput() {
	  var inputdbname = $("#db-name").val();
	  var inputdbdescription = $("#db-description").val();
	  var timeRange = "Last 1 Hour";
	  var refresh = "";
  
	  if (!inputdbname) {
		$('.error-tip').addClass('active');
		$('.popupOverlay, .popupContent').addClass('active');
		$('#new-dashboard-modal').show();
	  } else {
		$('#save-dbbtn').off('click');
		$(document).off('keypress');
		
		$.ajax({
		  method: "post",
		  url: "api/dashboards/create",
		  headers: {
			'Content-Type': 'application/json; charset=utf-8',
			'Accept': '*/*'
		  },
		  data: JSON.stringify(inputdbname),
		  dataType: 'json',
		  crossDomain: true,
		}).then(function (res) {
		  $("#db-name").val("");
		  $("#db-description").val("");
		  $('.error-tip').removeClass('active');
		  $('.popupOverlay, .popupContent').removeClass('active');
  
		  var updateDashboard = {
			"id": Object.keys(res)[0],
			"name": Object.values(res)[0],
			"details": {
			  "name": Object.values(res)[0],
			  "description": inputdbdescription,
			  "timeRange": timeRange,
			  "refresh": refresh,
			}
		  }
  
		  $.ajax({
			method: "post",
			url: "api/dashboards/update",
			headers: {
			  'Content-Type': 'application/json; charset=utf-8',
			  'Accept': '*/*'
			},
			data: JSON.stringify(updateDashboard),
			dataType: 'json',
			crossDomain: true,
		  }).then(function (msg) {
			console.log("done:", msg)
		  })
  
		  var queryString = "?id=" + Object.keys(res)[0];
		  window.location.href = "../dashboard.html" + queryString;
		});
	  }
	}
  
	$('#save-dbbtn').click(function () {
	  createDashboardWithInput();
	});
  
	$(document).keypress(function(event){
		if(event.keyCode == '13'){
			createDashboardWithInput();
		}
	});

	$('#cancel-dbbtn, .popupOverlay').click(function () {
	  $('.popupOverlay, .popupContent').removeClass('active');
	  $('.error-tip').removeClass('active');
	});
}

function createSiglensDashboard(inputdbname) {
	var inputdbdescription = "A pre-created Dashboard to monitor the cluster";
	var timeRange = "Last 1 Hour"

	$.ajax({
		method: "post",
		url: "api/dashboards/create",
		headers: {
			'Content-Type': 'application/json; charset=utf-8',
			'Accept': '*/*'
		},
		data: JSON.stringify(inputdbname),
		dataType: 'json',
		crossDomain: true,
	}).then(function (res) {
		var updateDashboard = {
			"id": Object.keys(res)[0],
			"name": Object.values(res)[0],
			"details": {
				"name": Object.values(res)[0],
				"description": inputdbdescription,
				"timeRange": timeRange,
			}
		}
		$.ajax({
			method: "post",
			url: "api/dashboards/update",
			headers: {
				'Content-Type': 'application/json; charset=utf-8',
				'Accept': '*/*'
			},
			data: JSON.stringify(updateDashboard),
			dataType: 'json',
			crossDomain: true,
		}).then(function (msg) {
			console.log("done:", msg)
		})
	});
}

class btnRenderer {
	init(params) {
		this.eGui = document.createElement('span');
		this.eGui.innerHTML = `<div id="dashboard-grid-btn">
			 
				<button class='btn' id="viewbutton" title="Open dashboard"></button>
				<button class="btn-simple" id="delbutton" title="Delete dashboard"></button>
				<button class="btn-duplicate" id="duplicateButton" title="Duplicate dashboard"></button>
				</div>`;
		this.vButton = this.eGui.querySelector('.btn');
		this.dButton = this.eGui.querySelector('.btn-simple');
		this.duplicateButton = this.eGui.querySelector('.btn-duplicate');

		function view() {
			$.ajax({
				method: 'get',
				url: 'api/dashboards/' + params.data.uniqId,
				headers: {
					'Content-Type': 'application/json; charset=utf-8',
					Accept: '*/*',
				},
				crossDomain: true,
				dataType: 'json',
			}).then(function (res) {
				var queryString = "?id=" + params.data.uniqId;
				window.location.href = "../dashboard.html" + queryString;
			});
		}

		function deletedb() {
			$.ajax({
				method: 'get',
				url: 'api/dashboards/delete/' + params.data.uniqId,
				headers: {
					'Content-Type': 'application/json; charset=utf-8',
					Accept: '*/*',
				},
				crossDomain: true,
			}).then(function () {
				let deletedRowID = params.data.rowId;
				dbgridOptions.api.applyTransaction({
					remove: [{ rowId: deletedRowID }],
				});
			});
		}

		function duplicatedb() {
			$.ajax({
				method: 'get',
				url: 'api/dashboards/' + params.data.uniqId,
				headers: {
					'Content-Type': 'application/json; charset=utf-8',
					Accept: '*/*',
				},
				crossDomain: true,
				dataType: 'json',
			}).then(function (res) {
				let duplicatedDBName = res.name + "-Copy";
				let duplicatedDescription = res.description;
				let duplicatedPanels = res.panels;
				let duplicateTimeRange = res.timeRange;
				let duplicateRefresh = res.refresh;
				let uniqIDdb;
				$.ajax({
					method: "post",
					url: "api/dashboards/create",
					headers: {
						'Content-Type': 'application/json; charset=utf-8',
						'Accept': '*/*'
					},
					data: JSON.stringify(duplicatedDBName),
					dataType: 'json',
					crossDomain: true,
				}).then((res) => {
					uniqIDdb = Object.keys(res)[0];
					$.ajax(
						{
							method: 'POST',
							url: '/api/dashboards/update',
							data: JSON.stringify({
								"id": uniqIDdb,
								"name": duplicatedDBName,
								"details": {
									"name": duplicatedDBName,
									"description": duplicatedDescription,
									"panels": duplicatedPanels,
									"timeRange": duplicateTimeRange,
									"refresh": duplicateRefresh,
								}
							})
						}
					)
				}).then(function () {
					dbgridOptions.api.applyTransaction({
						add: [{
							dbname: duplicatedDBName,
							uniqId: uniqIDdb,
						}],
					});
				})
			})
		}

		function showPrompt() {
			$('#delete-db-prompt').css('display', 'flex');
			$('.popupOverlay, .popupContent').addClass('active');
			$('#new-dashboard-modal').hide();

			$('#cancel-db-prompt, .popupOverlay').click(function () {
				$('.popupOverlay, .popupContent').removeClass('active');
				$('#delete-db-prompt').hide();
			});

			$('#delete-dbbtn').click(function () {
				deletedb();
				$('.popupOverlay, .popupContent').removeClass('active');
				$('#delete-db-prompt').hide();
			});
		}

		this.vButton.addEventListener('click', view);
		this.dButton.addEventListener('click', showPrompt);
		this.duplicateButton.addEventListener('click', duplicatedb);
	}

	getGui() {
		return this.eGui;
	}
	refresh(params) {
		return false;
	}
}

let dashboardColumnDefs = [
	{
		field: "rowId",
		hide: true
	},
	{
		headerName: "Dashboard Name",
		field: "dbname",
		sortable: true,
		sort: 'desc',
		cellClass: "",
		cellRenderer: (params) => {
			var link = document.createElement('a');
			link.href = '#';
			link.innerText = params.value;
			link.addEventListener('click', (e) => {
				e.preventDefault();
				view()
			});
			return link;

			function view() {
				$.ajax({
					method: 'get',
					url: 'api/dashboards/' + params.data.uniqId,
					headers: {
						'Content-Type': 'application/json; charset=utf-8',
						Accept: '*/*',
					},
					crossDomain: true,
					dataType: 'json',
				}).then(function (res) {
					var queryString = "?id=" + params.data.uniqId;
					window.location.href = "../dashboard.html" + queryString;
				});
			}
		}

	},
	{
		cellRenderer: btnRenderer,
		width: 5,
	},

];

// let the grid know which columns and what data to use
const dbgridOptions = {
	columnDefs: dashboardColumnDefs,
	rowData: dbRowData,
	animateRows: true,
	rowHeight: 64,
	defaultColDef: {
		icons: {
			sortAscending: '<i class="fa fa-sort-alpha-up"/>',
			sortDescending: '<i class="fa fa-sort-alpha-down"/>',
		},
	},
	enableCellTextSelection: true,
	suppressScrollOnNewData: true,
	suppressAnimationFrame: true,
	getRowId: (params) => params.data.rowId,
	onGridReady(params) {
		this.gridApi = params.api; // To access the grids API
	},
};


function displayDashboards(res, flag) {
	if (flag == -1) {
		// show search results
		let dbFilteredRowData = [];
		if (dbgridDiv === null) {
			dbgridDiv = document.querySelector('#dashboard-grid');
			new agGrid.Grid(dbgridDiv, dbgridOptions);
		}
		dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
		let idx = 0;
		let newRow = new Map()
		$.each(res, function (key, value) {
			newRow.set("rowId", idx)
			newRow.set("uniqId", key)
			newRow.set("dbname", value)

			dbFilteredRowData = _.concat(dbFilteredRowData, Object.fromEntries(newRow));
			idx = idx + 1;
		})
		dbgridOptions.api.setRowData(dbFilteredRowData);
		dbgridOptions.api.sizeColumnsToFit();
	} else {
		if (dbgridDiv === null) {
			dbgridDiv = document.querySelector('#dashboard-grid');
			new agGrid.Grid(dbgridDiv, dbgridOptions);
		}
		dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
		let idx = 0;
		let newRow = new Map()
		$.each(res, function (key, value) {
			newRow.set("rowId", idx)
			newRow.set("uniqId", key)
			newRow.set("dbname", value)

			dbRowData = _.concat(dbRowData, Object.fromEntries(newRow));
			idx = idx + 1;
		})
		dbgridOptions.api.setRowData(dbRowData);
		dbgridOptions.api.sizeColumnsToFit();
	}
}

function searchDB() {
	let searchText = $('.search-db-input').val();
	var tokens = searchText.toLowerCase()
		.split(' ')
		.filter(function (token) {
			return token.trim() !== '';
		});

	let dbNames = [];
	dbRowData.forEach(rowData => {
		dbNames.push(rowData.dbname)
	})

	let dbFilteredRowsObject = {};
	if (tokens.length) {
		var searchTermRegex = new RegExp(tokens.join('|'), 'gi');
		var filteredList = dbNames.filter(function (dbName, i) {
			if (dbName.match(searchTermRegex)) {
				let uniqIdDB = dbRowData[i].uniqId;
				dbFilteredRowsObject[`${uniqIdDB}`] = dbRowData[i].dbname;
			}
			return dbName.match(searchTermRegex);
		});

		if (Object.keys(dbFilteredRowsObject).length === 0) {
			displayDashboards(dbFilteredRowsObject, -1);
			showDBNotFoundMsg();
		} else {
			$('#dashboard-grid-container').show();
			$('#empty-response').hide();
			displayDashboards(dbFilteredRowsObject, -1);
		}
	}
}

function displayOriginalDashboards() {
	let searchText = $('.search-db-input').val();

	if (searchText.length === 0) {
		if (dbgridDiv === null) {
			dbgridDiv = document.querySelector('#dashboard-grid');
			new agGrid.Grid(dbgridDiv, dbgridOptions);
		}
		$('#dashboard-grid-container').show();
		$('#empty-response').hide();
		dbgridOptions.api.setColumnDefs(dashboardColumnDefs);
		dbgridOptions.api.setRowData(dbRowData);
		dbgridOptions.api.sizeColumnsToFit();
	}
}

function showDBNotFoundMsg() {
	$('#dashboard-grid-container').hide();
	$('#empty-response').show();
}

$(document).ready(async function () {
	displayNavbar();
	if (Cookies.get('theme')) {
		theme = Cookies.get('theme');
		$('body').attr('data-theme', theme);
	}
	$('.theme-btn').on('click', themePickerHandler);

	let normalDBs = await getAllDashboards();
	let allDefaultDBs = await getAllDefaultDashboards();
	let allDBs = {...normalDBs, ...allDefaultDBs}
	displayDashboards(allDBs)

	$('#create-db-btn').click(createDashboard);
	$('#run-search').click(searchDB);
	$('.search-db-input').on('keyup', displayOriginalDashboards);

	let stDate = "now-1h";
	let endDate = "now";
	datePickerHandler(stDate, endDate, stDate);
}
);