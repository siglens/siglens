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

/**
 * siglens
 * (c) 2022 - All rights reserved.
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
