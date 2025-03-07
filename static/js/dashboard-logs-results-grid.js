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
let panelGridDiv = null;
let panelID = null;
let isFetching = false;
$('.panEdit-navBar #available-fields .select-unselect-header').on('click', '.select-unselect-checkbox', toggleAllAvailableFieldsHandler);
$('.panEdit-navBar #available-fields .select-unselect-header').on('click', '.select-unselect-checkmark', toggleAllAvailableFieldsHandler);

let panelLogsColumnDefs = [
    {
        field: 'timestamp',
        headerName: 'timestamp',
        cellRenderer: (params) => {
            return moment(params.value).format(timestampDateFmt);
        },
        maxWidth: 250,
        minWidth: 250,
    },
    {
        field: 'logs',
        headerName: 'logs',
        cellRenderer: (params) => {
            let logString = '';
            let counter = 0;

            _.forEach(params.data, (value, key) => {
                let colSep = counter > 0 ? '<span class="col-sep"> | </span>' : '';

                logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}<b>${key}</b>` + JSON.stringify(JSON.unflatten(value), null, 2) + `</span>`;
                counter++;
            });
            return logString;
        },
    },
];

var panelLogsRowData = [];
let panelGridOptions;

function createPanelGridOptions(currentPanel) {
    panelGridOptions = {
        columnDefs: panelLogsColumnDefs,
        rowData: panelLogsRowData,
        animateRows: true,
        readOnlyEdit: true,
        singleClickEdit: true,
        headerHeight: 26,
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
            if (panelID == -1 || panelID == null || panelID == undefined) {
                //eslint-disable-next-line no-undef
                if (evt.direction === 'vertical' && canScrollMore && !isFetching) {
                    let diff = panelLogsRowData.length - evt.api.getLastDisplayedRow();
                    if (diff <= 1) {
                        let scrollingTrigger = true;
                        data = getQueryParamsData(scrollingTrigger);
                        //eslint-disable-next-line no-undef
                        if (data.searchText !== initialSearchDashboardData.searchText || data.indexName !== initialSearchDashboardData.indexName || data.startEpoch !== initialSearchDashboardData.startEpoch || data.endEpoch !== initialSearchDashboardData.endEpoch || data.queryLanguage !== initialSearchDashboardData.queryLanguage) {
                            scrollingErrorPopup();
                            return; // Prevent further scrolling
                        }
                        //eslint-disable-next-line no-undef
                        isFetching = true;
                        showLoadingIndicator();
                        if (data && data.searchText == 'error') {
                            alert('Error');
                            hideLoadingIndicator(); // Hide loading indicator on error
                            //eslint-disable-next-line no-undef
                            isFetching = false;
                            return;
                        }
                        runPanelLogsQuery(data, panelID, currentPanel)
                            .then(() => {
                                //eslint-disable-next-line no-undef
                                isFetching = false;
                            })
                            .catch((error) => {
                                console.warn('Error fetching data', error);
                                //eslint-disable-next-line no-undef
                                isFetching = false;
                            })
                            .finally(() => {
                                hideLoadingIndicator();
                                //eslint-disable-next-line no-undef
                                isFetching = false;
                            });
                    }
                }
            }
        },
        onColumnResized: function (params) {
            if (params.finished && params.column) {
                const resizedColumn = params.column;
                const columnId = resizedColumn.getColId();
                const newWidth = Math.round(resizedColumn.getActualWidth());

                if (!currentPanel.customColumnWidths) {
                    currentPanel.customColumnWidths = {};
                }

                currentPanel.customColumnWidths[columnId] = newWidth;

                if (Object.keys(currentPanel.customColumnWidths).length === 0) {
                    delete currentPanel.customColumnWidths;
                }
            }
        },
        onGridReady: function (params) {
            if (currentPanel.chartType === 'Data Table' && currentPanel.customColumnWidths) {
                // Get the current column order from panelLogsColumnDefs
                const orderedColumnIds = panelLogsColumnDefs.map((colDef) => colDef.field);

                // Preserve the column order
                const columnStateOrder = orderedColumnIds.map((colId) => ({
                    colId: colId,
                }));

                // Apply the column order without modifying widths
                params.columnApi.applyColumnState({
                    state: columnStateOrder,
                    applyOrder: true,
                });

                // Set the column widths
                const columnStateWidths = orderedColumnIds
                    .filter((colId) => Object.prototype.hasOwnProperty.call(currentPanel.customColumnWidths, colId))
                    .map((colId) => ({
                        colId: colId,
                        width: currentPanel.customColumnWidths[colId],
                    }));

                // Apply only the widths without applying order
                params.columnApi.applyColumnState({
                    state: columnStateWidths,
                    applyOrder: false, // Apply widths only
                });
            }
            params.api.refreshCells({ force: true });
        },
        overlayLoadingTemplate: '<div class="ag-overlay-loading-center"><div class="loading-icon"></div><div class="loading-text">Loading...</div></div>',
    };
    return panelGridOptions;
}

function showLoadingIndicator() {
    panelGridOptions.api.showLoadingOverlay();
}

function hideLoadingIndicator() {
    panelGridOptions.api.hideOverlay();
}
//eslint-disable-next-line no-unused-vars
function renderPanelLogsGrid(columnOrder, hits, panelId, currentPanel) {
    panelID = panelId;
    $(`.panelDisplay .big-number-display-container`).hide();
    let logLinesViewType = currentPanel.logLinesViewType;

    if (panelId == -1 && panelGridDiv == null) {
        // for panel on the editPanelScreen page
        panelGridDiv = document.querySelector('.panelDisplay #panelLogResultsGrid');        
        panelGridOptions = createPanelGridOptions(currentPanel);

        //eslint-disable-next-line no-undef
        new agGrid.Grid(panelGridDiv, panelGridOptions);
    }
    if (panelId != -1) {
        panelGridDiv = document.querySelector(`#panel${panelId} #panelLogResultsGrid`);
        panelGridOptions = createPanelGridOptions(currentPanel);

        //eslint-disable-next-line no-undef
        new agGrid.Grid(panelGridDiv, panelGridOptions);
    }

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
                colName: colName,
            },
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

    switch (logLinesViewType) {
        case 'Single line display view':
            panelLogOptionSingleHandler(panelGridOptions, panelLogsColumnDefs);
            break;
        case 'Multi line display view':
            panelLogOptionMultiHandler(panelGridOptions, panelLogsColumnDefs, panelLogsRowData);
            break;
        case 'Table view':
            panelLogOptionTableHandler(panelGridOptions, panelLogsColumnDefs);
            if (currentPanel?.selectedFields) {
                updateColumns(currentPanel.selectedFields);
            }
            break;
    }
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}

function panelLogOptionSingleHandler(panelGridOptions, panelLogsColumnDefs) {
    panelLogsColumnDefs.forEach(function (colDef, _index) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = null;
            colDef.autoHeight = null;
        }
    });
    panelGridOptions.api.setColumnDefs(panelLogsColumnDefs);
    panelGridOptions.api.resetRowHeights();
    panelLogsColumnDefs.forEach((colDef, _index) => {
        panelGridOptions.columnApi.setColumnVisible(colDef.field, false);
    });
    panelGridOptions.columnApi.setColumnVisible('logs', true);
    panelGridOptions.columnApi.setColumnVisible('timestamp', true);

    panelGridOptions.columnApi.autoSizeColumn(panelGridOptions.columnApi.getColumn('logs'), false);
}

function panelLogOptionMultiHandler(panelGridOptions, panelLogsColumnDefs, panelLogsRowData) {
    panelLogsColumnDefs.forEach(function (colDef, _index) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = { 'white-space': 'normal' };
            colDef.autoHeight = true;
        }
    });
    panelGridOptions.api.setColumnDefs(panelLogsColumnDefs);

    panelLogsColumnDefs.forEach((colDef, _index) => {
        panelGridOptions.columnApi.setColumnVisible(colDef.field, false);
    });
    panelGridOptions.columnApi.setColumnVisible('logs', true);
    panelGridOptions.columnApi.setColumnVisible('timestamp', true);

    panelGridOptions.columnApi.autoSizeColumn(panelGridOptions.columnApi.getColumn('logs'), false);
    panelGridOptions.api.setRowData(panelLogsRowData);
    panelGridOptions.api.sizeColumnsToFit();
}

function panelLogOptionTableHandler(panelGridOptions, panelLogsColumnDefs) {
    panelLogsColumnDefs.forEach(function (colDef, _index) {
        if (colDef.field === 'logs') {
            colDef.cellStyle = null;
            colDef.autoHeight = null;
            colDef.hide = true;
        } else colDef.hide = false;
    });
    panelGridOptions.api.setColumnDefs(panelLogsColumnDefs);
    panelGridOptions.api.resetRowHeights();
    // Always show timestamp
    panelGridOptions.columnApi.setColumnVisible('timestamp', true);
    panelGridOptions.columnApi.setColumnVisible('logs', false);
}
//eslint-disable-next-line no-unused-vars
function renderPanelAggsGrid(columnOrder, hits, panelId) {
    let aggsColumnDefs = [];
    let segStatsRowData = [];
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
            cellRenderer: (params) => (params.value ? params.value : 'null'),
        },
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-desc"/>',
            sortDescending: '<i class="fa fa-sort-alpha-down"/>',
        },
    };
    $(`.panelDisplay .big-number-display-container`).hide();
    if (panelId == -1) panelGridDiv = document.querySelector('.panelDisplay #panelLogResultsGrid');
    else panelGridDiv = document.querySelector(`#panel${panelId} #panelLogResultsGrid`);
    //eslint-disable-next-line no-undef
    new agGrid.Grid(panelGridDiv, aggGridOptions);

    let colDefs = aggGridOptions.api.getColumnDefs();
    colDefs.length = 0;
    colDefs = columnOrder.map((colName, index) => {
        let title = colName;
        let fieldId = colName.replace(/\s+/g, '_').replace(/[^\w\s]/gi, ''); // Replace special characters and spaces
        let resize = index + 1 !== columnOrder.length;
        let maxWidth = Math.max(displayTextWidth(colName, 'italic 19pt DINpro'), 200); //200 is approx width of 1trillion number
        return {
            field: fieldId,
            headerName: title,
            resizable: resize,
            minWidth: maxWidth,
        };
    });
    aggsColumnDefs = _.chain(aggsColumnDefs).concat(colDefs).uniqBy('field').value();
    aggGridOptions.api.setColumnDefs(aggsColumnDefs);
    let newRow = new Map();
    $.each(hits.measure, function (key, resMap) {
        newRow.set('id', 0);
        columnOrder.map((colName, _index) => {
            let fieldId = colName.replace(/\s+/g, '_').replace(/[^\w\s]/gi, ''); // Replace special characters and spaces
            let ind = -1;
            if (hits.groupByCols != undefined && hits.groupByCols.length > 0) {
                ind = findColumnIndex(hits.groupByCols, colName);
            }
            //group by col
            if (ind != -1 && resMap.GroupByValues.length != 1 && resMap.GroupByValues[ind] != '*') {
                newRow.set(fieldId, resMap.GroupByValues[ind]);
            } else if (ind != -1 && resMap.GroupByValues.length === 1 && resMap.GroupByValues[0] != '*') {
                newRow.set(fieldId, resMap.GroupByValues[0]);
            } else {
                // Check if MeasureVal is undefined or null and set it to 0
                if (resMap.MeasureVal[colName] === undefined || resMap.MeasureVal[colName] === null) {
                    newRow.set(fieldId, '0');
                } else {
                    newRow.set(fieldId, resMap.MeasureVal[colName]);
                }
            }
        });
        segStatsRowData = _.concat(segStatsRowData, Object.fromEntries(newRow));
    });
    aggGridOptions.api.setRowData(segStatsRowData);
    $(`#panel${panelId} .panel-body #panel-loading`).hide();
}

function updateColumns(selectedFieldsList = null) {
    panelGridOptions.columnApi.setColumnVisible('timestamp', true);

    let isAnyColActive = false;
    const selectedFieldsSet = selectedFieldsList ? new Set(selectedFieldsList) : null;
    availColNames.forEach((colName) => {
        const colElement = $(`.toggle-${string2Hex(colName)}`);
        const shouldBeVisible = selectedFieldsSet ? selectedFieldsSet.has(colName) : colElement.hasClass('active');

        if (shouldBeVisible) {
            colElement.addClass('active');
            isAnyColActive = true;
            panelGridOptions.columnApi.setColumnVisible(colName, true);
        } else {
            colElement.removeClass('active');
            panelGridOptions.columnApi.setColumnVisible(colName, false);
        }
    });

    if (isAnyColActive) {
        panelGridOptions.columnApi.setColumnVisible('logs', false);
    }

    panelGridOptions.api.sizeColumnsToFit();
}

function toggleAllAvailableFieldsHandler(_evt) {
    let el = $('#available-fields .select-unselect-header');
    let isChecked = el.find('.select-unselect-checkmark');

    if (isChecked.length === 0) {
        if (theme === 'light') {
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        } else {
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }

        availColNames.forEach((colName) => {
            $(`.toggle-${string2Hex(colName)}`).addClass('active');
            panelGridOptions.columnApi.setColumnVisible(colName, true);
        });

        selectedFieldsList = [...availColNames];
    } else {
        isChecked.remove();

        availColNames.forEach((colName) => {
            $(`.toggle-${string2Hex(colName)}`).removeClass('active');
            panelGridOptions.columnApi.setColumnVisible(colName, false);
        });

        selectedFieldsList = [];
    }

    panelGridOptions.columnApi.setColumnVisible('logs', false);

    updatedSelFieldList = true;
}
function scrollingErrorPopup() {
    $('.mypopupOverlay').addClass('active');
    $('#error-popup.popupContent').addClass('active');

    $('#okay-button').on('click', function () {
        $('.mypopupOverlay').removeClass('active');
        $('#error-popup.popupContent').removeClass('active');
    });
}


function resetPanelLogsColumnDefs() {
    panelLogsColumnDefs = [
        {
            field: 'timestamp',
            headerName: 'timestamp',
            cellRenderer: (params) => {
                return moment(params.value).format(timestampDateFmt);
            },
            maxWidth: 250,
            minWidth: 250,
        },
        {
            field: 'logs',
            headerName: 'logs',
            cellRenderer: (params) => {
                let logString = '';
                let counter = 0;

                _.forEach(params.data, (value, key) => {
                    let colSep = counter > 0 ? '<span class="col-sep"> | </span>' : '';

                    logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}<b>${key} </b>` + JSON.stringify(JSON.unflatten(value), null, 2) + `</span>`;
                    counter++;
                });
                return logString;
            },
        },
    ];
    
    panelLogsRowData = [];
}