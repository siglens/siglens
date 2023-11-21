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

$(document).ready(() => {
    displayNavbar();
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $(".theme-btn").on("click", themePickerHandler);
    displayServiceHealthTable();
});

let gridDiv = null;

const columnDefs=[
    { headerName: "Service", field: "service"},
    { headerName: "Rate", field: "rate"},
    { headerName: "Error", field: "error"},
    { headerName: "Duration", field: "duration" },
];

const gridOptions = {
    defaultColDef: {
      cellStyle: { 'text-align': "left" },
      resizable: true,
      sortable: true,
      animateRows: true,
      readOnlyEdit: true,
      autoHeight: true,
    },
    columnDefs:columnDefs,
};

function displayServiceHealthTable(res){
    if (gridDiv === null) {
        gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, gridOptions);
    }
    gridOptions.api.sizeColumnsToFit();
}
