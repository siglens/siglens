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

let cellEditingClass = '';
let isFetching = false;

class ReadOnlyCellEditor {
    // gets called once before the renderer is used
    init(params) {
        // create the cell
        this.eInput = document.createElement('textarea');
        cellEditingClass = params.rowIndex % 2 === 0 ? 'even-popup-textarea' : 'odd-popup-textarea';
        this.eInput.classList.add(cellEditingClass);
        this.eInput.classList.add('copyable');
        this.eInput.readOnly = true;

        // Set styles to ensure the textarea fits within its container
        this.eInput.style.width = '100%';
        this.eInput.style.height = '100%';
        this.eInput.style.maxWidth = '100%';
        this.eInput.style.maxHeight = '100%';
        this.eInput.style.boxSizing = 'border-box';
        this.eInput.style.overflow = 'auto';

        this.eInput.cols = params.cols;
        this.eInput.rows = params.rows;
        this.eInput.maxLength = params.maxLength;
        this.eInput.value = params.value;

        this.gridApi = params.api;

        // event listener for clicks outside the popup
        this.onClickOutside = this.onClickOutside.bind(this);
        document.addEventListener('mousedown', this.onClickOutside);
    }
    // gets called once when grid ready to insert the element
    getGui() {
        this.gridApi.addEventListener('cellEditingStarted', (event) => {
            if (event.rowIndex === this.gridApi.getDisplayedRowAtIndex(event.rowIndex).rowIndex) {
                this.addCopyIcon();
            }
        });

        return this.eInput;
    }
    // returns the new value after editing
    getValue() {
        return this.eInput.value;
    }
    isPopup() {
        return true;
    }
    refresh() {
        return true;
    }
    destroy() {
        this.eInput.classList.remove(cellEditingClass);
        document.removeEventListener('mousedown', this.onClickOutside);
    }
    addCopyIcon() {
        // Remove any existing copy icons
        $('.copy-icon').remove();

        // Add copy icon to the textarea
        $('.copyable').each(function () {
            var copyIcon = $('<span class="copy-icon"></span>');
            $(this).after(copyIcon);
        });

        // Attach click event handler to the copy icon
        $('.copy-icon').on('click', function (_event) {
            var copyIcon = $(this);
            var inputOrTextarea = copyIcon.prev('.copyable');
            var inputValue = inputOrTextarea.val();

            var tempInput = document.createElement('textarea');
            tempInput.value = inputValue;
            document.body.appendChild(tempInput);
            tempInput.select();
            document.execCommand('copy');
            document.body.removeChild(tempInput);

            copyIcon.addClass('success');
            setTimeout(function () {
                copyIcon.removeClass('success');
            }, 1000);
        });
    }

    onClickOutside(event) {
        if (this.eInput && !this.eInput.contains(event.target) && !document.querySelector('.ag-popup-editor').contains(event.target)) {
            this.gridApi.stopEditing();
        }
    }
}

const cellEditorParams = (params) => {
    const jsonLog = JSON.stringify(JSON.unflatten(params.data), null, 2);
    return {
        value: jsonLog,
        cols: 100,
        rows: 10,
    };
};
// initial columns
let logsColumnDefs = [
    {
        field: 'timestamp',
        headerName: 'timestamp',
        editable: true,
        cellEditor: ReadOnlyCellEditor,
        cellEditorPopup: true,
        cellEditorPopupPosition: 'under',
        cellRenderer: (params) => {
            return moment(params.value).format(timestampDateFmt);
        },
        cellEditorParams: cellEditorParams,
        maxWidth: 216,
        minWidth: 216,
    },
    {
        field: 'logs',
        headerName: 'logs',
        minWidth: 1128,
        cellRenderer: (params) => {
            let logString = '';
            let counter = 0;
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
            _.forEach(params.data, (value, key) => {
                let colSep = counter > 0 ? '<span class="col-sep"> | </span>' : '';
                if (key != 'logs' && selectedFieldsList.includes(key)) {
                    logString += `<span class="cname-hide-${string2Hex(key)}">${colSep}${key}=` + JSON.stringify(JSON.unflatten(value), null, 2) + `</span>`;
                    counter++;
                }
            });
            return logString;
        },
    },
];

// initial dataset
let logsRowData = [];
//eslint-disable-next-line no-unused-vars
let allLiveTailColumns = [];
//eslint-disable-next-line no-unused-vars
let total_liveTail_searched = 0;
// let the grid know which columns and what data to use
const gridOptions = {
    columnDefs: logsColumnDefs,
    rowData: logsRowData,
    readOnlyEdit: true,
    singleClickEdit: true,
    headerHeight: 32,
    suppressDragLeaveHidesColumns: true,
    defaultColDef: {
        initialWidth: 100,
        sortable: true,
        resizable: true,
        suppressSizeToFit: true,
        suppressDragLeaveHidesColumns: true,
        minWidth: 200,
        icons: {
            sortAscending: '<i class="fa fa-sort-alpha-down"/>',
            sortDescending: '<i class="fa fa-sort-alpha-desc"/>',
        },
        headerComponentParams: {
            template: `<div class="ag-cell-label-container" role="presentation">
                  <span ref="eMenu" class="ag-header-icon ag-header-cell-menu-button"></span>
                  <span ref="eFilterButton" class="ag-header-icon ag-header-cell-filter-button"></span>
                  <div ref="eLabel" class="ag-header-cell-label" role="presentation">
                    <div style="display: flex; align-items: center; justify-content: space-between; gap: 3px; width: 100%;">
                        <div style="display: flex; align-items: center; gap: 3px;">
                            <span ref="eText" class="ag-header-cell-text" role="columnheader"></span>
                            <span ref="eSortOrder" class="ag-header-icon ag-sort-order"></span>
                            <span ref="eSortAsc" class="ag-header-icon ag-sort-ascending-icon"></span>
                            <span ref="eSortDesc" class="ag-header-icon ag-sort-descending-icon"></span>
                            <span ref="eSortNone" class="ag-header-icon ag-sort-none-icon"></span>
                            <span ref="eFilter" class="ag-header-icon ag-filter-icon"></span>
                        </div>
                        <i onclick="hideColumnHandler(event, true)" class="fa fa-close close-icon"></i>
                    </div>
                  </div>
                </div>`,
        },
    },
    icons: {
        sortAscending: '<i class="fa fa-sort-alpha-down"/>',
        sortDescending: '<i class="fa fa-sort-alpha-desc"/>',
    },
    enableCellTextSelection: true,
    suppressScrollOnNewData: true,
    suppressAnimationFrame: true,
    suppressFieldDotNotation: true,
    animateRows: false,
    suppressColumnVirtualisation: false,
    suppressRowVirtualisation: false,
    onBodyScroll: _.debounce(function (evt) {
        if (evt.direction === 'vertical' && canScrollMore && !isFetching) {
            let diff = logsRowData.length - evt.api.getLastDisplayedRow();
            // if we're less than 5 items from the end...fetch more data
            if (diff <= 5) {
                let scrollingTrigger = true;
                data = getSearchFilter(false, scrollingTrigger);
                if (data.searchText !== initialSearchData.searchText || data.indexName !== initialSearchData.indexName || data.startEpoch !== initialSearchData.startEpoch || data.endEpoch !== initialSearchData.endEpoch || data.queryLanguage !== initialSearchData.queryLanguage) {
                    scrollingErrorPopup();
                    return; // Prevent further scrolling
                }

                isFetching = true;
                showLoadingIndicator();
                if (data && data.searchText == 'error') {
                    alert('Error');
                    hideLoadingIndicator(); // Hide loading indicator on error
                    isFetching = false;
                    return;
                }

                doSearch(data)
                    .then(() => {
                        isFetching = false;
                    })
                    .catch((error) => {
                        console.warn('Error fetching data', error);
                        isFetching = false;
                    })
                    .finally(() => {
                        hideLoadingIndicator(); // Hide loading indicator once data is fetched
                        isFetching = false;
                    });
            }
        }
    }, 250),
    overlayLoadingTemplate: '<div class="ag-overlay-loading-center"><div class="loading-icon"></div><div class="loading-text">Loading...</div></div>',
    onGridReady: function (_params) {
        const eGridDiv = document.querySelector('#LogResultsGrid');
        const style = document.createElement('style');
        style.textContent = `
            .close-icon {
                cursor: pointer;
                color: #888;
                margin-left: 5px;
                display: none;
            }
              
            .ag-header-cell:not([col-id="timestamp"]):not([col-id="logs"]):hover .close-icon
                display: inline-block;
            }
            
        `;
        eGridDiv.appendChild(style);
    },
};

function showLoadingIndicator() {
    gridOptions.api.showLoadingOverlay();
}

function hideLoadingIndicator() {
    gridOptions.api.hideOverlay();
}
//eslint-disable-next-line no-unused-vars
const myCellRenderer = (params) => {
    if (typeof params.data !== 'object' || params.data === null) return '';
    const value = params.data[params.colName];
    if (value == null || value === '') return '';
    if (Array.isArray(value)) {
        return JSON.stringify(JSON.unflatten(value));
    }
    return value;
};
//eslint-disable-next-line no-unused-vars
function updateColumns() {
    // Always show timestamp
    gridOptions.columnApi.setColumnVisible('timestamp', true);
    let isAnyColActive = false;
    availColNames.forEach((colName, _index) => {
        if ($(`.toggle-${string2Hex(colName)}`).hasClass('active')) {
            isAnyColActive = true;
            gridOptions.columnApi.setColumnVisible(colName, true);
        } else {
            gridOptions.columnApi.setColumnVisible(colName, false);
        }
    });

    if (isAnyColActive) {
        // Always hide logs column if we have some fields selected
        gridOptions.columnApi.setColumnVisible('logs', false);
    }
    gridOptions.api.sizeColumnsToFit();
}

JSON.unflatten = function (data) {
    if (Object(data) !== data || Array.isArray(data)) return data;
    //eslint-disable-next-line no-useless-escape
    let regex = /\.?([^.\[\]]+)|\[(\d+)\]/g,
        resultholder = {};
    for (let p in data) {
        let cur = resultholder,
            prop = '',
            m;
        while ((m = regex.exec(p))) {
            cur = cur[prop] || (cur[prop] = m[2] ? [] : {});
            prop = m[2] || m[1];
        }
        cur[prop] = data[p];
    }
    return resultholder[''] || resultholder;
};

function scrollingErrorPopup() {
    $('.popupOverlay').addClass('active');
    $('#error-popup.popupContent').addClass('active');

    $('#okay-button').on('click', function () {
        $('.popupOverlay').removeClass('active');
        $('#error-popup.popupContent').removeClass('active');
    });
}
