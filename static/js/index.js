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

'use strict';
let sortedListIndices;
function getListIndices() {
    return fetch('api/listIndices', {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
    })
    .then(response => {
        return response.json();
    })
    .then(function (res) {
        processListIndicesResult(res);
        return res;
    });
}

function processListIndicesResult(res) {
    if(res)
        renderIndexDropdown(res)
    $("body").css("cursor", "default");
}

function renderIndexDropdown(listIndices) {
    sortedListIndices = listIndices.sort();
    let el = $('#index-listing');
    el.html(``);
    if (sortedListIndices) {
        sortedListIndices.forEach((index, i) => {
            el.append(`<div class="index-dropdown-item" data-index="${index.index}">
                            <span class="indexname-text">${index.index}</span>
                            <img src="/assets/index-selection-check.svg">
                       </div>`);
        });
    }
    if (Cookies.get('IndexList')) {
        selectedSearchIndex = Cookies.get('IndexList');
    }else {
        selectedSearchIndex = sortedListIndices[0].index;
        $("#index-btn span").html(sortedListIndices[0].index);
    }
}