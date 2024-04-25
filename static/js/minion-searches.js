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

'use strict';

let gridDiv = null;
let resultRowData=[];

$(document).ready(function () {

    $('.theme-btn').on('click', themePickerHandler);
    getAllMinionSearches()
});

function getAllMinionSearches(){
  fetch("/api/minionsearch/allMinionSearches")
  .then(res => res.json())
  .then(data=> displayLogsResults(data.minionSearches));
}

const columnDefs=[
    { headerName: "Log Statement", field: "log_text", initialWidth:400,autoHeight: true,},
    { headerName: "Filename", field: "filename",initialWidth:400,autoHeight: true,},
    { headerName: "Line Number", field: "line_number",initialWidth:140},
    { headerName: "Level", field: "log_level",initialWidth: 140 },
    { headerName: "State", field: "state",initialWidth: 140 },
];

const gridOptions = {
    defaultColDef: {
      cellStyle: { 'text-align': "left" },
      resizable: true,
      sortable: true,
      minWidth: 120,
      animateRows: true,
      readOnlyEdit: true,
      autoHeight: true,
    },
    headerHeight:32,
    columnDefs:columnDefs,
    pagination: true,
    paginationAutoPageSize: true,
    onRowClicked: onRowClicked,
};

function displayLogsResults(res){
    if (gridDiv === null) {
        gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, gridOptions);
    }
    gridOptions.api.setColumnDefs(columnDefs);
    let newRow = new Map();
    $.each(res, function (key, value) {
        const details = value.minionSearchDetails;
        //later implement from backend
        const state = value.alertInfo.state;
        const stateLabel = state === 0 ? "Normal" : state === 1 ? "Firing" : null;
        
        newRow.set("log_text", details.log_text);
        newRow.set("filename", details.filename);
        newRow.set("line_number", details.line_number);
        newRow.set("log_level", details.log_level);
        newRow.set("state", stateLabel);
        
        resultRowData.push(Object.fromEntries(newRow));
    })
    gridOptions.api.setRowData(resultRowData);
    gridOptions.api.sizeColumnsToFit();
}

function onRowClicked(event) {
  const resultBody = $('.result-pane .result-body');
  resultBody.empty();
  const rowData = event.data;
  const logText = rowData.log_text;
  const filename = rowData.filename;
  const lineNumber = rowData.line_number;
  const logLevel = rowData.log_level;

  const rowDetailsDiv = document.createElement('div');
  rowDetailsDiv.classList.add('row-details');

  const content = `
      <p><strong>Log Text:</strong> ${logText}</p>
      <p><strong>Filename:</strong> ${filename}</p>
      <p><strong>Line Number:</strong> ${lineNumber}</p>
      <p><strong>Log Level:</strong> ${logLevel}</p>
  `;

  rowDetailsDiv.innerHTML = content;
  resultBody.append(rowDetailsDiv);
}