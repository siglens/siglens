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

function initializeAvailableFieldsSidebar(columnOrder) {
    const columnsToIgnore = ['timestamp', 'logs'];

    columnOrder.forEach((colName) => {
        if (columnsToIgnore.indexOf(colName) === -1 && !availColNames.includes(colName)) {
            availColNames.push(colName);
        }
    });

    if (!updatedSelFieldList) {
        selectedFieldsList = [];
    }

    renderAvailableFields();
    renderSelectedFields();

    setupSectionToggling();
    setupFieldsEventHandlers();

    refreshColumnVisibility();
}

function renderAvailableFields() {
    const el = $('#available-fields-list');
    el.empty();

    // Render all available fields
    availColNames.forEach((colName) => {
        const isSelected = selectedFieldsList.includes(colName);
        el.append(`
            <div class="field-item" data-field="${colName}">
                <div class="field-name">${colName}</div>
                <div class="field-action ${isSelected ? 'field-action-remove' : 'field-action-add'}">
                    ${isSelected ? '×' : '+'}
                </div>
            </div>
        `);
    });

    updateFieldCounts();
}

function renderSelectedFields() {
    let el = $('#selected-fields-list');
    if (!el.length) return;

    el.empty();

    // Render only fields that are in selectedFieldsList
    selectedFieldsList.forEach((field) => {
        el.append(`
            <div class="field-item" data-field="${field}">
                <div class="field-name">${field}</div>
                <div class="field-action field-action-remove">×</div>
            </div>
        `);
    });

    updateFieldCounts();
}

function setupFieldsEventHandlers() {
    $('#available-fields-list .field-action')
        .off('click')
        .on('click', function (e) {
            e.stopPropagation();
            const fieldName = $(this).closest('.field-item').data('field');
            const isSelected = $(this).hasClass('field-action-remove');

            if (isSelected) {
                removeFieldFromSelected(fieldName);
            } else {
                addFieldToSelected(fieldName);
            }
        });

    $('#selected-fields-list .field-action-remove')
        .off('click')
        .on('click', function (e) {
            e.stopPropagation();
            const fieldName = $(this).closest('.field-item').data('field');
            removeFieldFromSelected(fieldName);
        });

    $('.field-item')
        .off('click')
        .on('click', function (e) {
            e.stopPropagation();
        });
}

function updateFieldCounts() {
    $('#selected-fields-header .field-count').text(selectedFieldsList.length);
    $('#available-fields-header .field-count').text(availColNames.length);
}

function setupSectionToggling() {
    const sections = [
        { header: '#selected-fields-header', list: '#selected-fields-list' },
        { header: '#available-fields-header', list: '#available-fields-list' },
    ];

    sections.forEach((section) => {
        const headerElement = $(section.header);
        const listElement = $(section.list);
        const chevronIcon = headerElement.find('.chevron-icon');

        if (headerElement.length && listElement.length && chevronIcon.length) {
            headerElement.off('click').on('click', function () {
                const isVisible = listElement.is(':visible');
                listElement.toggle(!isVisible);
                chevronIcon.text(isVisible ? '▶' : '▼');
            });
        }
    });
}

function addFieldToSelected(fieldName) {
    if (fieldName === 'timestamp' || fieldName === 'logs') {
        return;
    }

    if (selectedFieldsList.includes(fieldName)) {
        return;
    }

    selectedFieldsList.push(fieldName);

    renderSelectedFields();
    updateAvailableFieldsUI();

    setupFieldsEventHandlers();

    updatedSelFieldList = true;

    refreshColumnVisibility();
}

function removeFieldFromSelected(fieldName) {
    selectedFieldsList = selectedFieldsList.filter((field) => field !== fieldName);

    renderSelectedFields();
    updateAvailableFieldsUI();

    setupFieldsEventHandlers();

    updatedSelFieldList = true;

    refreshColumnVisibility();
}

// Handler for hiding a column from grid header 'X' icon
function hideColumnHandler(evt, isCloseIcon = false) {
    evt.preventDefault();
    evt.stopPropagation();

    let colName;
    if (isCloseIcon) {
        const outerDiv = evt.currentTarget.closest('.ag-header-cell');
        colName = outerDiv.getAttribute('col-id');
    } else {
        colName = evt.currentTarget.dataset.index;
    }

    removeFieldFromSelected(colName);
}

function resetAvailableFields() {
    $('#available-fields-list').empty();
    $('#selected-fields-list').empty();
    availColNames = [];
    updateFieldCounts();
}

function updateAvailableFieldsUI() {
    const availableFieldsList = $('#available-fields-list');

    availableFieldsList.find('.field-item').each(function () {
        const fieldName = $(this).data('field');
        const actionButton = $(this).find('.field-action');

        if (selectedFieldsList.includes(fieldName)) {
            // Field is selected - show remove icon
            actionButton.removeClass('field-action-add').addClass('field-action-remove');
            actionButton.text('×');
        } else {
            // Field is not selected - show add icon
            actionButton.removeClass('field-action-remove').addClass('field-action-add');
            actionButton.text('+');
        }
    });
}
