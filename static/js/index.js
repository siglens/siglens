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
    renderIndexDropdown(res)
    $("body").css("cursor", "default");
}

function renderIndexDropdown(listIndices) {
    let el = $('#index-listing');
    el.html(``);
    if (listIndices) {
        el.append(`<div class="index-dropdown-item" data-index="*">
                       <span class="indexname-text">*</span>
                       <img src="/assets/index-selection-check.svg">
                   </div>`);
        listIndices.forEach((index) => {
            el.append(`<div class="index-dropdown-item" data-index="${index.index}">
                            <span class="indexname-text">${index.index}</span>
                            <img src="/assets/index-selection-check.svg">
                       </div>`);
        });
    }
}