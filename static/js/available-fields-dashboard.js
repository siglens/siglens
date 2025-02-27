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

$('#available-fields').off('click').on('click', availableFieldsClickHandler);
$('#available-fields .fields').off('click').on('click', '.available-fields-dropdown-item', availableFieldsSelectHandler);

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

// prevent the available fields popup from closing when you toggle an available field
function availableFieldsClickHandler(evt) {
    evt.stopPropagation();
}

function availableFieldsSelectHandler(evt) {
    let colName = evt.currentTarget.dataset.index;

    let encColName = string2Hex(colName);
    // don't toggle the timestamp column
    if (colName !== 'timestamp') {
        // toggle the column visibility
        $(`.toggle-${encColName}`).toggleClass('active');
        const isSelected = $(`.toggle-${encColName}`).hasClass('active');

        if (isSelected) {
            // Update the selectedFieldsList everytime a field is selected
            if (!selectedFieldsList.includes(colName)) {
                selectedFieldsList.push(colName);
            }
        } else {
            // Everytime the field is unselected, remove it from selectedFieldsList
            selectedFieldsList = selectedFieldsList.filter((field) => field !== colName);
        }
    }

    let visibleColumns = 0;
    let totalColumns = -1;

    availColNames.forEach((colName, _index) => {
        if (selectedFieldsList.includes(colName)) {
            visibleColumns++;
            totalColumns++;
        }
    });

    if (visibleColumns == 1) {
        shouldCloseAllDetails = true;
    } else {
        if (shouldCloseAllDetails) {
            shouldCloseAllDetails = false;
        }
    }
    let el = $('#available-fields .select-unselect-header');

    // uncheck the toggle-all fields if the selected columns count is different
    if (visibleColumns < totalColumns) {
        let cmClass = el.find('.select-unselect-checkmark');
        cmClass.remove();
    }
    // We do not count time and log column
    if (visibleColumns == totalColumns - 2) {
        if (theme === 'light') {
            el.append(`<img class="select-unselect-checkmark" src="assets/available-fields-check-light.svg">`);
        } else {
            el.append(`<img class="select-unselect-checkmark" src="assets/index-selection-check.svg">`);
        }
    }

    if (window.location.pathname.includes('dashboard.html')) {
        updateColumns();
        currentPanel.selectedFields = selectedFieldsList;
        panelGridOptions.api.sizeColumnsToFit();
    }

    updatedSelFieldList = true;
}
