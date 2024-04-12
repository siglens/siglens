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

function getListIndices() {
    $('body').css('cursor', 'progress');
    $.ajax({
        method: 'get',
        url: 'api/listIndices',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        crossDomain: true,
        dataType: 'json',
    })
        .then(processListIndicesResult)
        .catch(function () {
            $('body').css('cursor', 'default');
        });
}

function processListIndicesResult(res) {
    if(res)
        renderIndexDropdown(res)
    $("body").css("cursor", "default");
}

function renderIndexDropdown(listIndices) {
    let sortedListIndices = listIndices.sort((a, b) => parseInt(a.index.split('-')[1]) - parseInt(b.index.split('-')[1]));
    let el = $('#index-listing');
    el.html(``);
    if (sortedListIndices) {
        sortedListIndices.forEach((index, i) => {
            const isActive = i === 0 ? 'active' : ''; // Setting First Index Active
            el.append(`<div class="index-dropdown-item ${isActive}" data-index="${index.index}">
                            <span class="indexname-text">${index.index}</span>
                            <img src="/assets/index-selection-check.svg">
                       </div>`);
        });
    }
    $("#index-btn span").html(sortedListIndices[0].index);
}