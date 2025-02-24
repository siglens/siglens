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
            span.textContent = params.value;
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
        // Use forEach with index
        let rowData = {}; // Use plain object instead of Map

        // Add unique row ID
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
    console.log('Paginate Agg Groups');
    // Ensure pageSize is a number
    pageSize = parseInt(pageSize);

    // Calculate start and end indices for current page
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, fullData.length);

    // Get the slice of data for current page
    const paginatedData = fullData.slice(startIndex, endIndex);

    console.log(`Paginating data: Page ${currentPage}, showing records ${startIndex + 1}-${endIndex} of ${fullData.length}`);
    console.log('First row of current page:', paginatedData[0]);

    // Update total records count
    totalLoadedRecords = fullData.length;

    // Update the grid with paginated data
    aggGridOptions.api.setRowData(paginatedData);

    // Update pagination display
    updatePaginationDisplay();
}
