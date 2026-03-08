// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

let eGridDiv = null;
//eslint-disable-next-line no-unused-vars
function renderMeasuresGrid(columnOrder, hits) {
    if (eGridDiv === null) {
        eGridDiv = document.querySelector('#measureAggGrid');
        //eslint-disable-next-line no-undef
        new agGrid.Grid(eGridDiv, aggGridOptions);
    }
    // set the column headers from the data
    let colDefs = columnOrder.map((colName, index) => {
        let title = colName;
        let fieldId = colName.replace(/\s+/g, '_').replace(/[^\w\s]/gi, ''); // Replace special characters and spaces
        let resize = index + 1 !== columnOrder.length;
        let maxWidth = Math.max(displayTextWidth(colName, 'italic 19pt  DINpro '), 200); //200 is approx width of 1trillion number
        //preserving white space for every column
        let cellRenderer = (params) => {
            const span = document.createElement('span');
            span.style.whiteSpace = 'pre';
            if (typeof params.value === 'number') {
                span.textContent = formatNumber(params.value);
            } else {
                span.textContent = params.value;
            }
            return span;
        };
        return {
            field: fieldId,
            headerName: title,
            resizable: resize,
            minWidth: maxWidth,
            cellRenderer: cellRenderer,
        };
    });

    aggsColumnDefs = _.chain(aggsColumnDefs).concat(colDefs).uniqBy('field').value();
    aggGridOptions.api.setColumnDefs(aggsColumnDefs);

    segStatsRowData = [];

    hits.measure.forEach((resMap, rowIndex) => {
        let rowData = {};
        rowData.id = rowIndex;

        columnOrder.forEach((colName) => {
            let fieldId = colName.replace(/\s+/g, '_').replace(/[^\w\s]/gi, '');
            let ind = -1;
            if (hits.groupByCols != undefined && hits.groupByCols.length > 0) {
                ind = findColumnIndex(hits.groupByCols, colName);
            }

            if (ind != -1 && resMap.GroupByValues.length != 1 && resMap.GroupByValues[ind] != '*') {
                rowData[fieldId] = resMap.GroupByValues[ind];
            } else if (ind != -1 && resMap.GroupByValues.length === 1 && resMap.GroupByValues[0] != '*') {
                rowData[fieldId] = resMap.GroupByValues[0];
            } else {
                rowData[fieldId] = resMap.MeasureVal[colName] ?? '';
            }
        });

        segStatsRowData.push(rowData);
    });

    paginateAggsData(segStatsRowData);
}

function displayTextWidth(text, font) {
    let canvas = displayTextWidth.canvas || (displayTextWidth.canvas = document.createElement('canvas'));
    let context = canvas.getContext('2d');
    context.font = font;
    let metrics = context.measureText(text);
    return metrics.width;
}

function paginateAggsData(fullData) {
    pageSize = parseInt(pageSize);

    // Calculate start and end indices for current page
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, fullData.length);

    const paginatedData = fullData.slice(startIndex, endIndex);

    totalLoadedRecords = fullData.length;

    aggGridOptions.api.setRowData(paginatedData);

    updatePaginationDisplay();
}
