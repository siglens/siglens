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

    // Sort column names alphabetically
    const sortedColNames = [...availColNames].sort((a, b) => a.localeCompare(b));

    sortedColNames.forEach((colName) => {
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
        const chevronIcon = headerElement.find('.fa-chevron-down');

        if (headerElement.length && listElement.length && chevronIcon.length) {
            headerElement.off('click').on('click', function () {
                const isVisible = listElement.is(':visible');
                listElement.toggle(!isVisible);
                if (isVisible) {
                    chevronIcon.removeClass('fa-chevron-down').addClass('fa-chevron-right');
                } else {
                    chevronIcon.removeClass('fa-chevron-right').addClass('fa-chevron-down');
                }
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

//eslint-disable-next-line no-unused-vars
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

jQuery(document).ready(function () {
    //Resizing Fields Sidebar
    const fieldsSidebar = jQuery('.fields-sidebar');
    const chartContainer = jQuery('.custom-chart-container');

    if (fieldsSidebar.length === 0 || chartContainer.length === 0) return;

    const resizer = jQuery('<div>', {
        class: 'fields-resizer',
    });
    fieldsSidebar.after(resizer);

    let initialWidth = localStorage.getItem('fieldsSidebarWidth') || 250;
    fieldsSidebar.css('width', initialWidth + 'px');

    resizer.on('mousedown', function (e) {
        e.preventDefault();
        resizer.addClass('active');

        const startX = e.clientX;
        const sidebarWidth = fieldsSidebar.outerWidth();

        function handleMouseMove(e) {
            const newWidth = sidebarWidth + (e.clientX - startX);

            if (newWidth >= 190 && newWidth <= 500) {
                fieldsSidebar.css('width', newWidth + 'px');
                localStorage.setItem('fieldsSidebarWidth', newWidth);
            }
        }

        function handleMouseUp() {
            resizer.removeClass('active');
            jQuery(document).off('mousemove', handleMouseMove);
            jQuery(document).off('mouseup', handleMouseUp);
        }

        jQuery(document).on('mousemove', handleMouseMove);
        jQuery(document).on('mouseup', handleMouseUp);
    });

    const savedWidth = localStorage.getItem('fieldsSidebarWidth');
    if (savedWidth) {
        fieldsSidebar.css('width', savedWidth + 'px');
    }

    jQuery(window).on('resize', function () {
        const savedWidth = localStorage.getItem('fieldsSidebarWidth');
        if (savedWidth) {
            fieldsSidebar.css('width', savedWidth + 'px');
        }
    });
});

const expandSvg = `
<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-panel-left-open">
    <rect width="16" height="16" x="4" y="4" rx="1.5"/>
    <path d="M8 4v16"/>
    <path d="m14 13-2-2 2-2"/>
</svg>`;

const collapseSvg = `
<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round" class="lucide lucide-panel-left-close">
    <rect width="16" height="16" x="4" y="4" rx="1.5"/>
    <path d="M8 4v16"/>
    <path d="m12 9 2 2-2 2"/>
</svg>`;

let isFieldsSidebarHidden = false;
let tooltipInstance = null;
let toggleButton = null;

// Apply the sidebar visibility state to the DOM
function applyFieldsSidebarState(isHidden) {
    const fieldsSidebar = document.querySelector('.fields-sidebar');
    const resizer = document.querySelector('.fields-resizer');

    if (!fieldsSidebar) return;

    if (isHidden) {
        fieldsSidebar.classList.add('hidden');
        if (resizer) {
            resizer.style.display = 'none';
        }
    } else {
        fieldsSidebar.classList.remove('hidden');
        if (resizer) {
            console.log('resizer hide');
            resizer.style.display = 'block';
        }
    }

    updateToggleButton(isHidden);
}

// Update the toggle button's icon and tooltip
function updateToggleButton(isHidden) {
    if (!toggleButton) return;

    toggleButton.innerHTML = isHidden ? collapseSvg : expandSvg;

    const tooltipText = isHidden ? 'Show Fields' : 'Hide Fields';
    if (tooltipInstance) {
        tooltipInstance.setContent(tooltipText);
    } else if (typeof tippy !== 'undefined') {
        tooltipInstance = tippy(toggleButton, {
            content: tooltipText,
            trigger: 'mouseenter focus',
        });
    }
}

// Update the URL with the current sidebar state
function updateUrlState(isHidden) {
    const searchParams = new URLSearchParams(window.location.search);

    if (searchParams.has('searchText') && searchParams.has('indexName')) {
        const url = new URL(window.location);
        url.searchParams.set('fieldsHidden', isHidden);
        window.history.pushState({}, document.title, url);
    }
}

function toggleFieldsSidebar(event) {
    if (event) {
        event.stopPropagation();
    }

    isFieldsSidebarHidden = !isFieldsSidebarHidden;

    applyFieldsSidebarState(isFieldsSidebarHidden);

    const buttonElement = document.querySelector('.expand-svg-container');
    if (buttonElement) {
        positionToggleButton(buttonElement, isFieldsSidebarHidden);
    }

    updateUrlState(isFieldsSidebarHidden);
}

function createToggleButton() {
    const buttonContainer = document.createElement('div');
    buttonContainer.className = 'expand-svg-container';

    buttonContainer.innerHTML = `
        <span class="expand-svg-box">
            <button id="expand-toggle-svg" class="btn expand-svg-button below-btn-img">
                ${isFieldsSidebarHidden ? collapseSvg : expandSvg}
            </button>
        </span>
    `;

    toggleButton = buttonContainer.querySelector('#expand-toggle-svg');
    toggleButton.addEventListener('click', toggleFieldsSidebar);

    return buttonContainer;
}

function positionToggleButton(buttonElement, isHidden) {
    if (!buttonElement) return;

    if (buttonElement.parentNode) {
        buttonElement.parentNode.removeChild(buttonElement);
    }

    if (isHidden) {
        // Position before tabs when sidebar is hidden
        const tabList = document.querySelector('.tab-chart-list');
        if (tabList) {
            tabList.parentNode.insertBefore(buttonElement, tabList);
            buttonElement.classList.add('before-tabs');
            // Make sure it's visible
            buttonElement.style.display = 'block';
        }
    } else {
        // Position near the selected fields header when sidebar is shown
        const selectedFieldsHeader = document.getElementById('selected-fields-header');
        if (selectedFieldsHeader) {
            selectedFieldsHeader.parentNode.insertBefore(buttonElement, selectedFieldsHeader);
            buttonElement.classList.remove('before-tabs');
            // Make sure it's visible
            buttonElement.style.display = 'block';
        }
    }

    setTimeout(() => {
        if (buttonElement) {
            buttonElement.style.display = 'block';
        }
    }, 50);
}

function initSidebarToggle() {
    // Check URL for initial state
    const searchParams = new URLSearchParams(window.location.search);
    const isSearchActive = searchParams.has('searchText') && searchParams.has('indexName');

    if (isSearchActive && searchParams.has('fieldsHidden')) {
        isFieldsSidebarHidden = searchParams.get('fieldsHidden') === 'true';
    }

    const toggleButtonElement = createToggleButton();

    positionToggleButton(toggleButtonElement, isFieldsSidebarHidden);

    applyFieldsSidebarState(isFieldsSidebarHidden);
}
