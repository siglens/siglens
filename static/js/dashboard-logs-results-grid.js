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

let panelGridDiv = null;

function getGridPanelRows() {
    // initial dataset
    let panelLogsRowData = [];
    return panelLogsRowData;
}
function getGridPanelCols(){
    // initial columns
    let panelLogsColumnDefs = [
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
            cellEditorParams: cellEditorParams,
            maxWidth: 250,
            minWidth: 250,
            sort: "desc"
        },
        {
            field: "logs",
            headerName: "logs",
            cellRenderer: (params) => {
                let logString = '';
                let counter = 0;
                    
                _.forEach(params.data, (value, key) => {
                    let colSep = counter > 0 ? '<span class="col-sep"> | </span>' : '';
                   
                        logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}${key}=` + JSON.stringify(JSON.unflatten(value), null, 2) + `</span>`;
                        counter++;
                    })
                return logString;
            },
        }
    ];
    return panelLogsColumnDefs;
}

// let the grid know which columns and what data to use
function getPanelGridOptions() {
    const panelGridOptions = {
        columnDefs: getGridPanelCols(),
        rowData: getGridPanelRows(),
        animateRows: true,
        readOnlyEdit: true,
        singleClickEdit: true,
        rowHeight: 35,
        defaultColDef: {
            initialWidth: 100,
            sortable: true,
            resizable: true,
            minWidth: 200,
            icons: {
                sortAscending: '<i class="fa fa-sort-alpha-up"/>',
                sortDescending: '<i class="fa fa-sort-alpha-down"/>',
            },
        },
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-up"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        enableCellTextSelection: true,
        suppressScrollOnNewData: true,
        suppressAnimationFrame: true,
        suppressFieldDotNotation: true,
        onBodyScroll(evt) {
            if (evt.direction === 'vertical' && canScrollMore == true) {
                let diff = getGridPanelRows().length - evt.api.getLastDisplayedRow();
                // if we're less than 1 items from the end...fetch more data
                if (diff <= 5) {
                    let scrollingTrigger = true;
                    data = getQueryParamsData(scrollingTrigger);
                    runPanelLogsQuery(data);
                }
            }
        },
    };
    return panelGridOptions;
}


function renderPanelLogsGrid(columnOrder, hits, panelId,logLinesViewType) {
    $(`.panelDisplay .big-number-display-container`).hide();
    let panelLogsRowData = getGridPanelRows();
    let panelLogsColumnDefs = getGridPanelCols();
    let panelGridOptions = getPanelGridOptions();

    if(panelId == -1) // for panel on the editPanelScreen page
        panelGridDiv = document.querySelector('.panelDisplay #panelLogResultsGrid');
    else // for panels on the dashboard page
        panelGridDiv = document.querySelector(`#panel${panelId} #panelLogResultsGrid`);

    new agGrid.Grid(panelGridDiv, panelGridOptions);

    let cols = columnOrder.map((colName, index) => {
        let hideCol = false;
        if (index >= defaultColumnCount) {
            hideCol = true;
        }

        if (logLinesViewType != 'single-line' && colName == 'logs') {
            hideCol = true;
        }

        if (index > 1) {
            if (selectedFieldsList.indexOf(colName) != -1) {
                hideCol = true;
            } else {
                hideCol = false;
            }
        }
        return {
            field: colName,
            hide: hideCol,
            headerName: colName,
            cellRenderer: myCellRenderer,
            cellRendererParams: {
                colName: colName
            }
        };
    });
    panelLogsRowData = _.concat(panelLogsRowData, hits);
    panelLogsColumnDefs = _.chain(panelLogsColumnDefs).concat(cols).uniqBy('field').value();

    const allColumnIds = [];
    panelGridOptions.columnApi.getColumns().forEach((column) => {
        allColumnIds.push(column.getId());
    });
    panelGridOptions.columnApi.autoSizeColumns(allColumnIds, false);
    panelGridOptions.api.setRowData(panelLogsRowData);

    switch (logLinesViewType){
        case 'Single line display view':
            panelLogOptionSingleHandler(panelGridOptions,panelLogsColumnDefs);
            break;
        case 'Multi line display view':
            panelLogOptionMultiHandler(panelGridOptions,panelLogsColumnDefs,panelLogsRowData);
            break;
        case 'Table view':
            panelLogOptionTableHandler(panelGridOptions,panelLogsColumnDefs);
            break;
    }
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}

function panelLogOptionSingleHandler(panelGridOptions,panelLogsColumnDefs){
    panelLogsColumnDefs.forEach(function (colDef, index) {
        if (colDef.field === "logs"){
            colDef.cellStyle = null;
            colDef.autoHeight = null;
        }
    });
    panelGridOptions.api.setColumnDefs(panelLogsColumnDefs);
    panelGridOptions.api.resetRowHeights()
    panelLogsColumnDefs.forEach((colDef, index) => {
        panelGridOptions.columnApi.setColumnVisible(colDef.field, false);
    });
    panelGridOptions.columnApi.setColumnVisible("logs", true);
    panelGridOptions.columnApi.setColumnVisible("timestamp", true);
    
    panelGridOptions.columnApi.autoSizeColumn(panelGridOptions.columnApi.getColumn("logs"), false);
}

function panelLogOptionMultiHandler(panelGridOptions,panelLogsColumnDefs,panelLogsRowData) {

        panelLogsColumnDefs.forEach(function (colDef, index) {
            if (colDef.field === "logs"){
                colDef.cellStyle = {'white-space': 'normal'};
                colDef.autoHeight = true;
            }
        });
        panelGridOptions.api.setColumnDefs(panelLogsColumnDefs);
        
        panelLogsColumnDefs.forEach((colDef, index) => {
            panelGridOptions.columnApi.setColumnVisible(colDef.field, false);
        });
        panelGridOptions.columnApi.setColumnVisible("logs", true);
        panelGridOptions.columnApi.setColumnVisible("timestamp", true);
        
        panelGridOptions.columnApi.autoSizeColumn(panelGridOptions.columnApi.getColumn("logs"), false);
        panelGridOptions.api.setRowData(panelLogsRowData);
        panelGridOptions.api.sizeColumnsToFit();
}

function panelLogOptionTableHandler(panelGridOptions,panelLogsColumnDefs) {

        panelLogsColumnDefs.forEach(function (colDef, index) {
            if (colDef.field === "logs") {
                colDef.cellStyle = null;
                colDef.autoHeight = null;
            }
        });
        panelGridOptions.api.setColumnDefs(panelLogsColumnDefs);
        panelGridOptions.api.resetRowHeights();
        // Always show timestamp
        panelGridOptions.columnApi.setColumnVisible("timestamp", true);
        panelGridOptions.columnApi.setColumnVisible("logs", false);
}


function renderPanelAggsGrid(columnOrder, hits,panelId) {
    let aggsColumnDefs = [];
    let segStatsRowData=[];
    const aggGridOptions = {
        columnDefs: aggsColumnDefs,
        rowData: [],
        animateRows: true,
        defaultColDef: {
            flex: 1,
            minWidth: 100,
            resizable: true,
            sortable: true,
            icons: {
                sortAscending: '<i class="fa fa-sort-alpha-up"/>',
                sortDescending: '<i class="fa fa-sort-alpha-down"/>',
            },
            cellRenderer: params => params.value ? params.value : 'null',
        },
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-up"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        }
    };
    $(`.panelDisplay .big-number-display-container`).hide();
    if(panelId == -1)
        panelGridDiv = document.querySelector('.panelDisplay #panelLogResultsGrid');
    else 
        panelGridDiv = document.querySelector(`#panel${panelId} #panelLogResultsGrid`);

    new agGrid.Grid(panelGridDiv, aggGridOptions);

    let colDefs = aggGridOptions.api.getColumnDefs();
    colDefs.length = 0;
    colDefs = columnOrder.map((colName, index) => {
        let title =  colName;
        let resize = index + 1 == columnOrder.length ? false : true;
        let maxWidth = Math.max(displayTextWidth(colName, "italic 19pt  DINpro "), 200)         //200 is approx width of 1trillion number
        return {
            field: title,
            headerName: title,
            resizable: resize,
            minWidth: maxWidth,
        };
    });
    aggsColumnDefs = _.chain(aggsColumnDefs).concat(colDefs).uniqBy('field').value();
    aggGridOptions.api.setColumnDefs(aggsColumnDefs);
    let newRow = new Map()
    $.each(hits, function (key, resMap) {
       newRow.set("id", 0)
       columnOrder.map((colName, index) => {
           if (resMap.GroupByValues!=null && resMap.GroupByValues[index]!="*" && index< (resMap.GroupByValues).length){
               newRow.set(colName, resMap.GroupByValues[index])
           }else{
               newRow.set(colName, resMap.MeasureVal[colName])
           }
       })
        segStatsRowData = _.concat(segStatsRowData, Object.fromEntries(newRow));
    })
    aggGridOptions.api.setRowData(segStatsRowData);
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}