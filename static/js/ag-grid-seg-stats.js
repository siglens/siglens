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

 let eGridDiv = null;
 
 function renderMeasuresGrid(columnOrder, hits) {
     if (eGridDiv === null) {
         eGridDiv = document.querySelector('#measureAggGrid');
         new agGrid.Grid(eGridDiv, aggGridOptions);
     }
     // set the column headers from the data
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
 
 
 }
 
 function displayTextWidth(text, font) {
    let canvas = displayTextWidth.canvas || (displayTextWidth.canvas = document.createElement("canvas"));
    let context = canvas.getContext("2d");
    context.font = font;
    let metrics = context.measureText(text);
    return metrics.width;
  }
