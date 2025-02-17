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


class JsonOverlayCellEditor {
    constructor() {
        this.eGui = document.createElement("div");
    }

    init(params) {
        this.params = params;

        // Ensure the cell remains visually empty
        this.eGui.style.width = "100%";
        this.eGui.style.height = "100%";

        // Attach click event listener to show the overlay
        this.eGui.addEventListener("click", (event) => {
            event.stopPropagation(); // Prevents row selection
            this.showOverlay();
        });

        // Automatically show the overlay when editing starts
        this.showOverlay();
    }

    getGui() {
        return this.eGui;
    }

    getValue() {
        return this.params.value; // Preserve the original value
    }

    isPopup() {
        return true; // Prevents cell content from affecting layout
    }
    
    showOverlay() {
        // Remove existing overlay before adding a new one
        this.destroy();
    
        // Create overlay container
        this.overlay = document.createElement("div");
        this.overlay.classList.add("json-overlay");
    
        function syntaxHighlight(json) {
            if (typeof json !== "string") {
                json = JSON.stringify(json, null, 2);
            }
            
            json = json.replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
            
            return json.replace(
                /("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(\.\d*)?([eE][+-]?\d+)?)/g,
                function (match) {
                    let cls = "json-number"; // Default color for numbers

                    if (/:$/.test(match)) {
                        cls = "json-key"; // Blue for keys
                    } else {
                        cls = "json-value"; // Green for strings
                    }

                    return `<span class="${cls}">${match}</span>`;
                }
            );
        }
        // Convert row data to formatted and highlighted JSON
        const jsonContent = syntaxHighlight(this.params.data);
    
        // Set inner HTML for overlay
        this.overlay.innerHTML = `
            <div class="json-overlay-content">
                <div class="close-overlay">
                    <span class="close-overlay-icon">
                        <i class="fa-solid fa-xmark"></i> <!-- Close Icon -->
                    </span>
                </div>
                <pre>${jsonContent}</pre>
            </div>
        `;
    
        // Close overlay when clicking outside
        this.overlay.addEventListener("click", (event) => {
            if (event.target === this.overlay) {
                this.destroy();
            }
        });
    
        // Close overlay when clicking the close button
        this.overlay.querySelector(".close-overlay").addEventListener("click", () => {
            this.destroy();
        });
    
        // Append overlay to body
        document.body.appendChild(this.overlay);
    }
    
    destroy() {
        if (this.overlay && document.body.contains(this.overlay)) {
            document.body.removeChild(this.overlay);
            this.params.api.stopEditing(); // Exit edit mode
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
        cellEditor: JsonOverlayCellEditor,
        cellEditorPopup: true,
        cellEditorPopupPosition: 'under',
        cellRenderer: (params) => {
            let timeString = '';
            let timestamp = moment(params.value).format(timestampDateFmt);

            timeString = `<span class="expand-icon-box">
                <button class="expand-icon-button">
                    <i class="fa-solid fa-up-right-and-down-left-from-center">
                </button></i>
            </span>
            <span>${timestamp}</span>`;

            return timeString;
        },
        cellEditorParams: cellEditorParams,
        maxWidth: 250,
        minWidth: 250,
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
    headerHeight: 26,
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
