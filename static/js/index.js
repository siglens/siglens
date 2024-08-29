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

let sortedListIndices;
//eslint-disable-next-line no-unused-vars
async function getListIndices() {
    return fetch('api/listIndices', {
        method: 'GET',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
    })
        .then((response) => {
            return response.json();
        })
        .then(function (res) {
            if (!res) {
                return null;
            }
            sortedListIndices = res.sort();
            if (Cookies.get('IndexList')) {
                selectedSearchIndex = Cookies.get('IndexList');
            } else {
                selectedSearchIndex = sortedListIndices[0].index;
            }
            return res;
        });
}

// Dashboards Index Dropdown
// Function to initialize autocomplete with original index values
//eslint-disable-next-line no-unused-vars
async function initializeIndexAutocomplete() {
    $('#index-listing')
        .autocomplete({
            source: indexValues,
            minLength: 0,
            select: function (event, ui) {
                event.preventDefault();
                const selectedValue = ui.item.value;
                addSelectedIndex(selectedValue);

                const index = indexValues.indexOf(selectedValue);
                if (index !== -1) {
                    indexValues.splice(index, 1);
                    indexValues = indexValues.filter((option) => !option.includes('*')); // Remove options including '*'
                    $(this).autocomplete('option', 'source', indexValues);
                }

                // Update selectedSearchIndex
                if (!selectedSearchIndex.split(',').includes(selectedValue)) {
                    selectedSearchIndex += selectedSearchIndex ? ',' + selectedValue : selectedValue;
                }

                $(this).val('');
                $(this).blur();
            },
            open: function (_event, _ui) {
                var containerPosition = $(this).closest('.index-container').offset();

                $(this)
                    .autocomplete('widget')
                    .css({
                        position: 'absolute',
                        top: containerPosition.top + $(this).closest('.index-container').outerHeight(),
                        left: containerPosition.left,
                        'z-index': 1000,
                    });
            },
        })
        .on('click', function () {
            if ($(this).autocomplete('widget').is(':visible')) {
                $(this).autocomplete('close');
            } else {
                $(this).autocomplete('search', '');
            }
        })
        .on('click', function () {
            $(this).select();
        })
        .on('input', function () {
            let typedValue = $(this).val();
            const minWidth = 3;
            const inputWidth = Math.max(typedValue.length * 10, minWidth);
            this.style.width = inputWidth + 'px';

            // Check if the typed value matches any index exactly
            const exactMatch = indexValues.includes(typedValue);

            // Filter out options with "*" from indexValues
            const filteredIndexValues = indexValues.filter(function (option) {
                return !option.includes('*');
            });

            // If the typed value is empty or matches exactly, remove the "*" options
            if (typedValue.trim() === '' || exactMatch) {
                $(this).autocomplete('option', 'source', filteredIndexValues);
                return;
            }

            indexValues = filteredIndexValues;
            $(this).autocomplete('option', 'source', filteredIndexValues);
        })
        .on('change', function () {
            // Clear the input field if the typed value does not match any options
            let typedValue = $(this).val();
            if (!indexValues.includes(typedValue)) {
                $(this).val('');
                this.style.width = '5px';
            }
            const filteredIndexValues = indexValues.filter(function (option) {
                return !option.includes('*');
            });
            $(this).autocomplete('option', 'source', filteredIndexValues);
        })
        .on('keypress', async function (event) {
            // Clear the input field if the typed value does not match any options when Enter is pressed
            if (event.keyCode === 13) {
                let typedValue = $(this).val();
                if (indexValues.includes(typedValue)) {
                    addSelectedIndex(typedValue);
                    if (!selectedSearchIndex.split(',').includes(typedValue)) {
                        selectedSearchIndex += selectedSearchIndex ? ',' + typedValue : typedValue;
                    }
                    $(this).val('');
                    this.style.width = '5px';

                    // Remove the selected value from indexValues
                    const index = indexValues.indexOf(typedValue);
                    if (index !== -1) {
                        indexValues.splice(index, 1);
                        indexValues = indexValues.filter((option) => !option.includes('*')); // Remove options including '*'
                        $(this).autocomplete('option', 'source', indexValues);
                    }
                } else {
                    var matcher = new RegExp(typedValue.replace('*', '.*'), 'gi');
                    function hasMatchingString(arr, regex) {
                        return arr.some((element) => regex.test(element.toLowerCase()));
                    }
                    const matchesIndex = hasMatchingString(indexValues, matcher);
                    // If the typed value matches any index, add the option to index list
                    if (matchesIndex) {
                        addSelectedIndex(typedValue);
                        if (!selectedSearchIndex.split(',').includes(typedValue)) {
                            selectedSearchIndex += selectedSearchIndex ? ',' + typedValue : typedValue;
                        }
                    }
                    $(this).autocomplete('option', 'source', indexValues);
                    $(this).val('');
                    this.style.width = '5px';
                }
                $(this).autocomplete('close');
                $(this).blur();
            }
        });
}

// Remove selected index from container when remove icon is clicked
$('.index-container').on('click', '.remove-icon', function (_e) {
    const removedValue = $(this)
        .parent()
        .contents()
        .filter(function () {
            return this.nodeType === 3;
        })
        .text()
        .trim();

    if ($('.index-container .selected-index').length === 1) {
        // If there's only one tag left, add * as selected index
        if (removedValue === '*') {
            return;
        } else {
            addSelectedIndex('*');
            selectedSearchIndex += selectedSearchIndex ? ',' + '*' : '*';
        }
    }

    $(this).parent().remove();

    if (!removedValue.includes('*')) {
        // If the removed value is not a wildcard option
        indexValues.push(removedValue);
        indexValues.sort();
        $('#index-listing').autocomplete('option', 'source', indexValues);
    }

    selectedSearchIndex = selectedSearchIndex
        .split(',')
        .map(function (value) {
            return value.trim();
        })
        .filter(function (value) {
            return value !== removedValue;
        })
        .join(',');

    // Update the input width and placeholder if necessary
    if ($('.index-container').find('.selected-index').length === 0) {
        $('#index-listing').css('width', '100%');
    }
});

function addSelectedIndex(index) {
    const indexElement = $("<div class='selected-index'></div>").text(index);
    const removeIcon = $("<span class='remove-icon'>×</span>");
    indexElement.append(removeIcon);
    $('#index-listing').before(indexElement);

    if ($('.index-container').find('.selected-index').length === 0) {
        $('#index-listing').css('width', '100%');
    } else {
        $('#index-listing').css('width', '5px');
    }
}

$('#add-index').click(function (e) {
    e.preventDefault();
    $('#index-listing').focus();
    $('#index-listing').autocomplete('search', '');
});
