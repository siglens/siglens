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

let cellEditingClass = '';

class ReadOnlyCellEditor {
    // gets called once before the renderer is used
    init(params) {
      // create the cell
        this.eInput = document.createElement('textarea');
        cellEditingClass = params.rowIndex%2===0 ? 'even-popup-textarea' : 'odd-popup-textarea'
        this.eInput.classList.add(cellEditingClass);
        this.eInput.readOnly = true;
        this.eInput.cols = params.cols;
        this.eInput.rows = params.rows;
        this.eInput.maxLength = params.maxLength;
        this.eInput.value = params.value;
    }
    // gets called once when grid ready to insert the element
    getGui() {
        return this.eInput;
    }
    // returns the new value after editing
    getValue() {
        return this.eInput.value;
    }   
    isPopup() {
        return true
    }
    refresh(params) {
        return true;
      }
    destroy() {
        this.eInput.classList.remove(cellEditingClass);
    }
  }

const cellEditorParams = (params) => {
    const jsonLog = JSON.stringify(JSON.unflatten(params.data), null, 2) 
    return {
        value :jsonLog,
        cols: 100,
        rows: 10
    };
  };
// initial columns
let logsColumnDefs = [
    {
        field: "timestamp",
        headerName: "timestamp",
        editable: true, 
        cellEditor: ReadOnlyCellEditor,
        cellEditorPopup: true,
        cellEditorPopupPosition: 'under',
        cellRenderer: (params) => {
            return moment(params.value).format(timestampDateFmt);
        },
        cellEditorParams:cellEditorParams,
        maxWidth: 216,
        minWidth: 216,
    },
    {
        field: "logs",
        headerName: "logs",
        minWidth: 1128,
        cellRenderer: (params) => {
            let logString = '';
            let counter = 0;
            if (updatedSelFieldList){
                selectedFieldsList = _.intersection(selectedFieldsList, availColNames);
            }else{
                selectedFieldsList = _.union(selectedFieldsList, availColNames);
            }

            if (selectedFieldsList.length != 0) {
                availColNames.forEach((colName, index) => {
                    if(selectedFieldsList.includes(colName)){
                        $(`.toggle-${string2Hex(colName)}`).addClass('active');
                    } else {
                        $(`.toggle-${string2Hex(colName)}`).removeClass('active');
                    }
                });
            } else {
                selectedFieldsList = [];
                availColNames.forEach((colName, index) => {
                    $(`.toggle-${string2Hex(colName)}`).addClass('active');
                    selectedFieldsList.push(colName);
                });
            }
            _.forEach(params.data, (value, key) => {
                let colSep = counter > 0 ? '<span class="col-sep"> | </span>' : '';
                if (key != 'logs' && selectedFieldsList.includes(key)) {
                    logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}${key}=`+ JSON.stringify(JSON.unflatten(value), null, 2)+`</span>`;                    

                    counter++;
                }
                if (key === 'timestamp'){
                    logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}${key}=${value}</span>`;
                }
            });
            return logString;
        },
    }
];

// initial dataset
let logsRowData = [];
let allLiveTailColumns = [];
let total_liveTail_searched = 0;
 // let the grid know which columns and what data to use
const gridOptions = {
    columnDefs: logsColumnDefs,
    rowData: logsRowData,
    animateRows: true,
    readOnlyEdit: true,
    singleClickEdit: true,
    headerHeight:32,
    defaultColDef: {
        initialWidth: 100,
        sortable: true,
        resizable: true,
        minWidth: 200,
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-down"/>',
            sortDescending: '<i class="fa fa-sort-alpha-up"/>',
          },
    },
    icons: {
        sortAscending: '<i class="fa fa-sort-alpha-down"/>',
        sortDescending: '<i class="fa fa-sort-alpha-up"/>',
      },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    suppressFieldDotNotation: true,
    onBodyScroll(evt){
        if(evt.direction === 'vertical' && canScrollMore == true){
            let diff = logsRowData.length - evt.api.getLastDisplayedRow();
            // if we're less than 1 items from the end...fetch more data
            if(diff <= 5) {
                // Show loading indicator
                showLoadingIndicator();
                
                let scrollingTrigger = true;
                data = getSearchFilter(false, scrollingTrigger);
                if (data && data.searchText == "error") {
                  alert("Error");
                  hideLoadingIndicator(); // Hide loading indicator on error
                  return;
                }
                doSearch(data).then(() => {
                    hideLoadingIndicator(); // Hide loading indicator once data is fetched
                });
            }
        }
    },
    overlayLoadingTemplate: '<div class="ag-overlay-loading-center"><div class="loading-icon"></div><div class="loading-text">Loading...</div></div>',
};

function showLoadingIndicator() {
    gridOptions.api.showLoadingOverlay();
}

function hideLoadingIndicator() {
    gridOptions.api.hideOverlay();
}

const myCellRenderer= (params) => {
    let logString = '';
    if (typeof params.data === 'object' && params.data !== null){
        let value = params.data[params.colName]
        if (value !== ""){
            if (Array.isArray(value)){
                logString= JSON.stringify(JSON.unflatten(value), null, 2)
            }else{
                logString= value
            }
        }
    }
    return logString;
};

let gridDiv = null;

function renderLogsGrid(columnOrder, hits){
    if (sortByTimestampAtDefault) {
        logsColumnDefs[0].sort = "desc";
    }else {
        logsColumnDefs[0].sort = undefined;
    }
    if (gridDiv == null){
        gridDiv = document.querySelector('#LogResultsGrid');
        new agGrid.Grid(gridDiv, gridOptions);
    }

    let logview = getLogView();

    let cols = columnOrder.map((colName, index) => {
        let hideCol = false;
        if (index >= defaultColumnCount) {
            hideCol = true;
        }
       
        if (logview != 'single-line' && colName == 'logs'){
            hideCol = true;
        }

        if (index > 1) {
            if (selectedFieldsList.indexOf(colName) != -1){
                hideCol = true;
            } else{
                hideCol = false;
            }
        }
        return {
            field: colName,
            hide: hideCol,
            headerName: colName,
            cellRenderer: myCellRenderer,
            cellRendererParams : {
                colName: colName
             }
        };
    });
    if(hits.length != 0){
        logsRowData = _.concat(hits, logsRowData);
        if (liveTailState && logsRowData.length > 500){
            logsRowData = logsRowData.slice(0, 500);
        }
            
    }
    logsColumnDefs = _.chain(logsColumnDefs).concat(cols).uniqBy('field').value();
    gridOptions.api.setColumnDefs(logsColumnDefs);

    const allColumnIds = [];
    gridOptions.columnApi.getColumns().forEach((column) => {
        allColumnIds.push(column.getId());
    });
    gridOptions.columnApi.autoSizeColumns(allColumnIds, false);
    gridOptions.api.setRowData(logsRowData);
    
    switch (logview){
        case 'single-line':
            logOptionSingleHandler();
            break;
        case 'multi-line':
            logOptionMultiHandler();
            break;
        case 'table':
            logOptionTableHandler();
            break;
    }
}

function updateColumns() {
    // Always show timestamp
    gridOptions.columnApi.setColumnVisible("timestamp", true);
    let isAnyColActive = false;
    availColNames.forEach((colName, index) => {
        if ($(`.toggle-${string2Hex(colName)}`).hasClass('active')) {
            isAnyColActive = true;
            gridOptions.columnApi.setColumnVisible(colName, true);
        } else {
            gridOptions.columnApi.setColumnVisible(colName, false);
        }
    });

    if (isAnyColActive) {
        // Always hide logs column if we have some fields selected
        gridOptions.columnApi.setColumnVisible("logs", false);
    }
    gridOptions.api.sizeColumnsToFit();
}

function getLogView(){
    let logview = Cookies.get('log-view') || 'table';
    return logview
}

JSON.unflatten = function (data) {
    "use strict";
    if (Object(data) !== data || Array.isArray(data)) return data;
    let regex = /\.?([^.\[\]]+)|\[(\d+)\]/g,
        resultholder = {};
    for (let p in data) {
        let cur = resultholder,
            prop = "",
            m;
        while (m = regex.exec(p)) {
            cur = cur[prop] || (cur[prop] = (m[2] ? [] : {}));
            prop = m[2] || m[1];
        }
        cur[prop] = data[p];
    }
    return resultholder[""] || resultholder;
};
