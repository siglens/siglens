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

function renderAvailableFields(columnOrder) {
    let el = $('#available-fields .fields');
    let columnsToIgnore = ['timestamp', 'logs'];
    el.empty();
    columnOrder.forEach((colName, index) => {
        if (columnsToIgnore.indexOf(colName) == -1) {
            if (!availColNames.includes(colName)){
                availColNames.push(colName);
                selectedFieldsList.push(colName)
            }
        }
    });
    
    // Render all the available fields
    availColNames.forEach((colName, index) => {
        el.append(`<div class="available-fields-dropdown-item toggle-field toggle-${string2Hex(colName)}" data-index="${colName}">
                        <span class="fieldname-text">${colName}</span>
                        <img src="/assets/index-selection-check.svg">
                        </div>`);
    });

let afieldDropDownItem = $(".fields .available-fields-dropdown-item");
afieldDropDownItem.each(function (idx, li) {
    li.style.width = "auto"; 
});

let afieldDropDown = document.getElementById("available-fields");
afieldDropDown.style.width = "auto"; 

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
}

function resetAvailableFields() {
    $('#available-fields .fields').html('');
}