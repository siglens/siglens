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

let panelGridDiv = null;
$('.panEdit-navBar #available-fields .select-unselect-header').on('click','.select-unselect-checkbox', toggleAllAvailableFieldsHandler);
$('.panEdit-navBar #available-fields .select-unselect-header').on('click','.select-unselect-checkmark', toggleAllAvailableFieldsHandler);

let panelLogsColumn = [
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
var panelLogsRow = [];
const panelGridOption = {
        columnDefs: panelLogsColumn,
        rowData: panelLogsRow,
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
                sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
                sortDescending: '<i class="fa fa-sort-alpha-down"/>',
            },
        },
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
        enableCellTextSelection: true,
        suppressScrollOnNewData: true,
        suppressAnimationFrame: true,
        suppressFieldDotNotation: true,
        onBodyScroll(evt) {
            if (evt.direction === 'vertical' && canScrollMore && !isFetching) {
                let diff = panelLogsRow.length - evt.api.getLastDisplayedRow();
                if (diff <= 1) {
                    let scrollingTrigger = true;
                    data = getQueryParamsData(scrollingTrigger);                
                    isFetching = true;  
                    runPanelLogsQuery(data)
                    .then(() => {
                        isFetching = false;
                    })
                    .catch((error) => {
                        console.warn("Error fetching data", error);
                        isFetching = false;
                    })
                    .finally(() => {
                        isFetching = false;
                    });
                }
            }
        },
    };


function renderPanelLogsGrid(columnOrder, hits, panelId,currentPanel) {
    $(`.panelDisplay .big-number-display-container`).hide();
    let logLinesViewType = currentPanel.logLinesViewType;

    if(panelId == -1) // for panel on the editPanelScreen page
        panelGridDiv = document.querySelector('.panelDisplay #panelLogResultsGrid');
    else // for panels on the dashboard page
        panelGridDiv = document.querySelector(`#panel${panelId} #panelLogResultsGrid`);

    new agGrid.Grid(panelGridDiv, panelGridOption);

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
    panelLogsRow = _.concat(panelLogsRow, hits);
    panelLogsColumn = _.chain(panelLogsColumn).concat(cols).uniqBy('field').value();

    const allColumnIds = [];
    panelGridOption.columnApi.getColumns().forEach((column) => {
        allColumnIds.push(column.getId());
    });
    panelGridOption.columnApi.autoSizeColumns(allColumnIds, false);
    panelGridOption.api.setRowData(panelLogsRow);

    switch (logLinesViewType){
        case 'Single line display view':
            panelLogOptionSingleHandler(panelGridOption,panelLogsColumn);
            break;
        case 'Multi line display view':
            panelLogOptionMultiHandler(panelGridOption,panelLogsColumn,panelLogsRow);
            break;
        case 'Table view':
            panelLogOptionTableHandler(panelGridOption,panelLogsColumn);
            if (currentPanel?.selectedFields) {
                updateColumns(currentPanel.selectedFields);
            }
            break;
    }
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}

function panelLogOptionSingleHandler(panelGridOption,panelLogsColumn){
    panelLogsColumn.forEach(function (colDef, index) {
        if (colDef.field === "logs"){
            colDef.cellStyle = null;
            colDef.autoHeight = null;
        }
    });
    panelGridOption.api.setColumnDefs(panelLogsColumn);
    panelGridOption.api.resetRowHeights()
    panelLogsColumn.forEach((colDef, index) => {
        panelGridOption.columnApi.setColumnVisible(colDef.field, false);
    });
    panelGridOption.columnApi.setColumnVisible("logs", true);
    panelGridOption.columnApi.setColumnVisible("timestamp", true);
    
    panelGridOption.columnApi.autoSizeColumn(panelGridOption.columnApi.getColumn("logs"), false);
}

function panelLogOptionMultiHandler(panelGridOption,panelLogsColumn,panelLogsRow) {

        panelLogsColumn.forEach(function (colDef, index) {
            if (colDef.field === "logs"){
                colDef.cellStyle = {'white-space': 'normal'};
                colDef.autoHeight = true;
            }
        });
        panelGridOption.api.setColumnDefs(panelLogsColumn);
        
        panelLogsColumn.forEach((colDef, index) => {
            panelGridOption.columnApi.setColumnVisible(colDef.field, false);
        });
        panelGridOption.columnApi.setColumnVisible("logs", true);
        panelGridOption.columnApi.setColumnVisible("timestamp", true);
        
        panelGridOption.columnApi.autoSizeColumn(panelGridOption.columnApi.getColumn("logs"), false);
        panelGridOption.api.setRowData(panelLogsRow);
        panelGridOption.api.sizeColumnsToFit();
}

function panelLogOptionTableHandler(panelGridOption,panelLogsColumn) {

        panelLogsColumn.forEach(function (colDef, index) {
            if (colDef.field === "logs") {
                colDef.cellStyle = null;
                colDef.autoHeight = null;
                colDef.hide = true; 
            } else 
                colDef.hide = false;
        });
        panelGridOption.api.setColumnDefs(panelLogsColumn);
        panelGridOption.api.resetRowHeights();
        // Always show timestamp
        panelGridOption.columnApi.setColumnVisible("timestamp", true);
        panelGridOption.columnApi.setColumnVisible("logs", false);
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
                sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
                sortDescending: '<i class="fa fa-sort-alpha-down"/>',
            },
            cellRenderer: params => params.value ? params.value : 'null',
        },
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
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
     $.each(hits.measure, function (key, resMap) {
        newRow.set("id", 0)
        columnOrder.map((colName, index) => {
            let ind=-1
            if (hits.groupByCols !=undefined && hits.groupByCols.length > 0) {
                ind = findColumnIndex(hits.groupByCols,colName)
            }
            //group by col
            if (ind !=-1  && resMap.GroupByValues.length != 1 && resMap.GroupByValues[ind]!="*"){
                newRow.set(colName, resMap.GroupByValues[ind])
            }else if (ind !=-1 && resMap.GroupByValues.length === 1 && resMap.GroupByValues[0]!="*"){
                newRow.set(colName, resMap.GroupByValues[0])
            }else{
            // Check if MeasureVal is undefined or null and set it to 0
                if (resMap.MeasureVal[colName] === undefined || resMap.MeasureVal[colName] === null) {
                    newRow.set(colName, "0");
                } else {
                    newRow.set(colName, resMap.MeasureVal[colName]);
                }
            }
        })
        segStatsRowData = _.concat(segStatsRowData, Object.fromEntries(newRow));
    })
    aggGridOptions.api.setRowData(segStatsRowData);
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}

function updateColumns(selectedFieldsList = null) {

    panelGridOption.columnApi.setColumnVisible("timestamp", true);
    
    let isAnyColActive = false;
    const selectedFieldsSet = selectedFieldsList ? new Set(selectedFieldsList) : null;
    availColNames.forEach((colName) => {
        const colElement = $(`.toggle-${string2Hex(colName)}`);
        const shouldBeVisible = selectedFieldsSet ? selectedFieldsSet.has(colName) : colElement.hasClass('active');
        
        if (shouldBeVisible) {
            colElement.addClass('active');
            isAnyColActive = true;
            panelGridOption.columnApi.setColumnVisible(colName, true);
        } else {
            colElement.removeClass('active');
            panelGridOption.columnApi.setColumnVisible(colName, false);
        }
    });

    if (isAnyColActive) {
        panelGridOption.columnApi.setColumnVisible("logs", false);
    }
    
    panelGridOption.api.sizeColumnsToFit();
}

function toggleAllAvailableFieldsHandler(evt) {
    let el = $('#available-fields .select-unselect-header');
    let isChecked = el.find('.select-unselect-checkmark');

    if (isChecked.length === 0) {
        if (theme === "light") {
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        } else {
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }

        availColNames.forEach((colName) => {
            $(`.toggle-${string2Hex(colName)}`).addClass('active');
            panelGridOption.columnApi.setColumnVisible(colName, true);
        });
        
        selectedFieldsList = [...availColNames];
    } else {
        isChecked.remove();

        availColNames.forEach((colName) => {
            $(`.toggle-${string2Hex(colName)}`).removeClass('active');
            panelGridOption.columnApi.setColumnVisible(colName, false);
        });

        selectedFieldsList = [];
    }

    panelGridOption.columnApi.setColumnVisible("logs", false);

    updatedSelFieldList = true;
}
function scrollingErrorPopup(){
    $('.popupOverlay').addClass('active');
    $('#error-popup.popupContent').addClass('active');

    $('#okay-button').on('click', function(){
        $('.popupOverlay').removeClass('active');
        $('#error-popup.popupContent').removeClass('active');
    });
}