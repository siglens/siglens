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
    selectedSearchIndex = sortedListIndices[0].index;
    $("#index-btn span").html(sortedListIndices[0].index);
}