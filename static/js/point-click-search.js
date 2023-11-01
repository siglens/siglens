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

function selectedTextHandler() {
    $('.popover').hide();
    let selection = window.getSelection();
    let dataTableNode = $(selection.anchorNode).parents('#logs-result-container');
    let selectedText = selection.toString();

    if (dataTableNode.length && selectedText.length) {
        let range = selection.getRangeAt(0).cloneRange();
        range.collapse(false);

        let marker = document.createElement('i');
        marker.setAttribute('data-bs-trigger', 'focus');
        marker.setAttribute('data-bs-toggle', 'popover');
        marker.setAttribute('data-bs-custom-class', 'point-click-search');
        range.insertNode(marker);

        let content = `<div>
            <div class="fw-bold pb-2">Update search filter with:</div>
            <div class="selected-text pb-3">${selectedText}</div>
            <div>
                <span class="btn btn-md" id="replace-pcs-btn">replace</span>&nbsp;
                <span class="btn btn-md" id="append-pcs-btn">append</span>&nbsp;
            </div
        </div>`;

        let popover = new bootstrap.Popover(marker, {
            trigger: 'focus',
            content: content,
            html: true,
            placement: 'top'
        });
        popover.show();
    }
}

function replaceSearchQueryInputHandler(evt) {
    let selectedText = $(evt.currentTarget).parents('.popover-body').find('.selected-text').text();

    $('#filter-input').val(selectedText);
    closePcsPopover(evt.currentTarget);
}

function appendSearchQueryInputHandler(evt) {
    let selectedText = $(evt.currentTarget).parents('.popover-body').find('.selected-text').text();

    $('#filter-input').val($('#filter-input').val() + ' AND ' + selectedText);
    closePcsPopover(evt.currentTarget);
}


function closePcsPopover(target) {
    $('.popover').remove();
}