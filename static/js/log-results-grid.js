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

let isFetching = false;

class TimestampCellRenderer {
    init(params) {
        this.params = params;
        this.eGui = document.createElement('div');
        this.eGui.style.display = 'flex';

        const timestamp = typeof params.value === 'number' ? moment(params.value).format(timestampDateFmt) : params.value;

        this.eGui.innerHTML = `
        <span class="expand-icon-box">
        <button class="expand-icon-button">
            <i class="fa-solid fa-up-right-and-down-left-from-center">
        </button></i>
    </span>
    <span>${timestamp}</span>
        `;

        const expandBtn = this.eGui.querySelector('.expand-icon-box');
        expandBtn.addEventListener('click', this.showJsonPanel.bind(this));
    }

    showJsonPanel(event) {
        event.stopPropagation();
        let trace_id = '';
        let time_stamp = '';
        const jsonPopup = document.querySelector('.json-popup');
        const rowData = this.params.node.data;

        window.copyJsonToClipboard = function copyJsonToClipboard() {
            const jsonContent = document.querySelector("#json-tab div").innerText; // Get JSON text
            navigator.clipboard.writeText(jsonContent).then(() => {
                alert("Copied to clipboard!");
            }).catch(err => {
                console.error("Failed to copy: ", err);
            });
        }

        window.switchTab = function (tab) {  // Attach to global scope
            document.querySelectorAll('.tab-content').forEach(el => el.classList.remove('active'));
            document.querySelectorAll('.tab-button').forEach(el => el.classList.remove('active'));

            document.getElementById(tab + '-tab').classList.add('active');
            document.querySelector(`[onclick="switchTab('${tab}')"]`).classList.add('active');

            if (tab === 'table') {
                populateTable();
            }
        };

        function flattenJson(data, parentKey = '', result = {}) {
            Object.entries(data).forEach(([key, value]) => {
                const newKey = parentKey ? `${parentKey}.${key}` : key; // Create dot-separated key

                if (typeof value === 'object' && value !== null && !Array.isArray(value)) {
                    flattenJson(value, newKey, result);
                } else {
                    result[newKey] = value;
                }
            });
            return result;
        }

        // Convert JSON Data to Table
        function populateTable() {
            const tableBody = document.getElementById('table-content');
            tableBody.innerHTML = ''; // Clear old rows

            const jsonData = JSON.unflatten(rowData);
            const flattenedData = flattenJson(jsonData);

            if (!flattenedData || Object.keys(flattenedData).length === 0) {
                tableBody.innerHTML = `<tr><td colspan="2" style="text-align:center;">No data available</td></tr>`;
                return;
            }

            Object.entries(flattenedData).forEach(([key, value]) => {
                let formattedValue = (typeof value === "object") ? JSON.stringify(value, null, 2) : value;

                let row = document.createElement("tr");

                let keyCell = document.createElement("td");
                keyCell.textContent = key;
                keyCell.style.border = "1px solid #ddd";
                keyCell.style.padding = "6px";

                let valueCell = document.createElement("td");
                valueCell.textContent = formattedValue;
                valueCell.style.border = "1px solid #ddd";
                valueCell.style.padding = "6px";

                row.appendChild(keyCell);
                row.appendChild(valueCell);
                tableBody.appendChild(row);
            });
        }

        const jsonData = JSON.unflatten(rowData);
        const flattenedData = flattenJson(jsonData);

        // Check if "trace_id" exists and is not empty or null
        const showRelatedTraceButton = Object.keys(flattenedData).some(key => {
            if (key.toLowerCase() === "timestamp") {
                time_stamp = flattenedData[key];
            }
            if (key.toLowerCase() === "trace_id") {
                trace_id = flattenedData[key];
                return trace_id !== null && trace_id !== "";
            }
            return false;
        });

        // Generate the HTML for the popup
        jsonPopup.innerHTML = `
            <div class="json-popup-header">
                <div class="json-popup-header-buttons">
                    ${showRelatedTraceButton ? `
                        <button class="btn-related-trace btn btn-purple" onclick="handleRelatedTraces('${trace_id}', ${time_stamp}, true)">
                            <i class="fa fa-file-text"></i>&nbsp; Related Trace
                        </button>
                    ` : ""}
                    <button class="json-popup-close">×</button>
                </div>
            </div>

            <!-- Tabs for JSON and Table -->
            <div class="json-content-type-box">
                <button class="tab-button active" onclick="switchTab('json')">JSON</button>
                <button class="tab-button" onclick="switchTab('table')">Table</button>

                <!-- Copy Icon Button -->
                <button class="copy-json-button" onclick="copyJsonToClipboard()">
                    <i class="fa fa-clipboard"></i>
                </button>
            </div>

            <!-- JSON and Table Content -->
            <div class="json-popup-content">
                <div id="json-tab" class="tab-content active">
                    <div class="json-key-values">${syntaxHighlight(JSON.unflatten(rowData))}</div>
                </div>
                <div id="table-tab" class="tab-content">
                    <table border="1" class="json-table">
                        <thead>
                            <tr>
                                <th>Key</th>
                                <th>Value</th>
                            </tr>
                        </thead>
                        <tbody id="table-content"></tbody>
                    </table>
                </div>
            </div>
        `;

        // Show JSON panel
        jsonPopup.classList.add('active');

        const closeBtn = jsonPopup.querySelector('.json-popup-close');
        closeBtn.onclick = () => {
            jsonPopup.classList.remove('active');
            this.params.api.sizeColumnsToFit();
        };

        this.params.api.sizeColumnsToFit();
    }

    getGui() {
        return this.eGui;
    }

    refresh() {
        return false;
    }
}


function syntaxHighlight(json) {
    if (typeof json !== 'string') {
        json = JSON.stringify(json, null, 2);
    }
    json = json.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
    return json.replace(/("(\\u[a-zA-Z0-9]{4}|\\[^u]|[^\\"])*"(\s*:)?|\b(true|false|null)\b|-?\d+(\.\d*)?([eE][+-]?\d+)?)/g, function (match) {
        let cls = 'json-value';
        if (/^"/.test(match) && /:$/.test(match)) {
            cls = 'json-key';
        }
        return `<span class="${cls}">${match}</span>`;
    });
}

// initial columns
let logsColumnDefs = [
    {
        field: 'timestamp',
        headerName: 'timestamp',
        cellRenderer: TimestampCellRenderer,
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
