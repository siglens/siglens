// Copyright (c) 2026 The SigScalr Authors.
// SPDX-License-Identifier: Apache-2.0

let logsColumnDefs = [
    {
        field: 'timestamp',
        headerName: 'timestamp',
        cellRenderer: ExpandableJsonCellRenderer('logs'),
        maxWidth: 250,
        minWidth: 250,
    },
    {
        field: 'logs',
        headerName: 'logs',
        minWidth: 1128,
    },
];

//eslint-disable-next-line no-unused-vars
let allLiveTailColumns = [];
//eslint-disable-next-line no-unused-vars
let total_liveTail_searched = 0;

// let the grid know which columns and what data to use
//eslint-disable-next-line no-unused-vars
const gridOptions = {
    columnDefs: logsColumnDefs,
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
};

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
