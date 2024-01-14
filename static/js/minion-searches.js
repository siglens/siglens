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

let gridDiv = null;
let resultRowData=[];

$(document).ready(function () {
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
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