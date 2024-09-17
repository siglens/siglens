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

//eslint-disable-next-line no-unused-vars
function renderAvailableFields(columnOrder, columnCount) {
    let el = $('#available-fields .fields');
    let columnsToIgnore = ['timestamp', 'logs'];
    el.empty();
    $('.column-count').html(columnCount);
    columnOrder.forEach((colName, _index) => {
        if (columnsToIgnore.indexOf(colName) == -1) {
            if (!availColNames.includes(colName)) {
                availColNames.push(colName);
            }
        }
    });

    // Render all the available fields
    availColNames.forEach((colName, _index) => {
        el.append(`<div class="available-fields-dropdown-item toggle-field toggle-${string2Hex(colName)}" data-index="${colName}">
                        <span class="fieldname-text">${colName}</span>
                        <img src="/assets/index-selection-check.svg">
                        </div>`);
    });

    let afieldDropDownItem = $('.fields .available-fields-dropdown-item');
    afieldDropDownItem.each(function (idx, li) {
        li.style.width = 'auto';
    });

    let afieldDropDown = document.getElementById('available-fields');
    afieldDropDown.style.width = 'auto';

    if (updatedSelFieldList) {
        selectedFieldsList = _.intersection(selectedFieldsList, availColNames);
    } else {
        selectedFieldsList = _.union(selectedFieldsList, availColNames);
    }

    if (selectedFieldsList.length != 0) {
        availColNames.forEach((colName, _index) => {
            if (selectedFieldsList.includes(colName)) {
                $(`.toggle-${string2Hex(colName)}`).addClass('active');
            } else {
                $(`.toggle-${string2Hex(colName)}`).removeClass('active');
            }
        });
    }
}
//eslint-disable-next-line no-unused-vars
function resetAvailableFields() {
    $('#available-fields .fields').html('');
}
