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

// Dashboards Index Dropdown 
// Function to initialize autocomplete with original index values
async function initializeIndexAutocomplete() {
    $("#index-listing").autocomplete({
        source: indexValues,
        minLength: 0,
        select: function(event, ui) {
            event.preventDefault();
            const selectedValue = ui.item.value;
            addSelectedIndex(selectedValue);


            if (selectedValue.endsWith('*')) {
                const prefix = selectedValue.slice(0, -1); // Remove the '*'
                let filteredIndexValues = indexValues.filter(function(option) {
                    return !option.startsWith(prefix);
                });
                indexValues = filteredIndexValues;
                $(this).autocomplete("option", "source", filteredIndexValues);
            }

            const index = indexValues.indexOf(selectedValue);
            if (index !== -1) {
                indexValues.splice(index, 1);
                $(this).autocomplete("option", "source", indexValues);
            }
            
            // Update selectedSearchIndex
            if (!selectedSearchIndex.includes(selectedValue)) {
                selectedSearchIndex += selectedSearchIndex ? ',' + selectedValue : selectedValue;
            }
            
            $(this).val('');
            runQueryBtnHandler();
        },
        open: function(event, ui) {
            var containerPosition = $(this).closest('.index-container').offset();
    
            $(this).autocomplete("widget").css({
                "position": "absolute",
                "top": containerPosition.top + $(this).closest('.index-container').outerHeight(),
                "left": containerPosition.left,
                "z-index": 1000
            });
        }
    }).on('click', function() {
        if ($(this).autocomplete('widget').is(':visible')) {
            $(this).autocomplete('close');
        } else {
            $(this).autocomplete('search', '');
        }
    }).on('click', function() {
        $(this).select();
    }).on('input', function() {
        let typedValue = $(this).val();
        const minWidth = 3; 
        const inputWidth = Math.max(typedValue.length * 10, minWidth);
        this.style.width = inputWidth + 'px'; 
    
        // Check if the typed value matches any index exactly
        const exactMatch = indexValues.includes(typedValue);
        
        // Filter out options with "*" from indexValues
        const filteredIndexValues = indexValues.filter(function(option) {
            return !option.includes('*');
        });
    
        // If the typed value is empty or matches exactly, remove the "*" options
        if (typedValue.trim() === '' || exactMatch) {
            $(this).autocomplete("option", "source", filteredIndexValues);
            return;
        }
    
        // Check if the typed value matches any index
        const matchesIndex = indexValues.some(function(option) {
            return option.startsWith(typedValue);
        });
    
        // If the typed value matches any index, add the option with "*"
        if (matchesIndex && !typedValue.includes('*')) {
            const wildcardOption = typedValue + '*'; // Create the option with "*"
            filteredIndexValues.unshift(wildcardOption); // Add the option with "*" to the beginning of the array
        }
        $(this).autocomplete("option", "source", filteredIndexValues);
    }).on('change', function() {
        console.log("close");
        // Clear the input field if the typed value does not match any options
        let typedValue = $(this).val();
        if (!indexValues.includes(typedValue)) {
            $(this).val('');
            this.style.width = '5px';
        }
    }).on('keypress', function(event) {
        // Clear the input field if the typed value does not match any options when Enter is pressed
        if (event.keyCode === 13) {
            let typedValue = $(this).val();
            if (!indexValues.includes(typedValue)) {
                $(this).val('');
                this.style.width = '5px';
            }
        }
    });
}

// Remove selected index from container when remove icon is clicked
$(".index-container").on("click", ".remove-icon", function() {
    if ($('.index-container .selected-index').length === 1) {
        return; // If there's only one tag left, do not remove it
    }

    const removedValue = $(this).parent().contents().filter(function() {
        return this.nodeType === 3;
    }).text().trim();
    $(this).parent().remove();

    // If the removed value is a wildcard option
    if (removedValue.endsWith('*')) {
        const prefix = removedValue.slice(0, -1); // Remove the '*'
        
        // Remove wildcard options from indexValues
        indexValues = indexValues.filter(function(option) {
            return !option.startsWith(prefix);
        });
        
        // Add back only the non-wildcard options that match the prefix
        const matchingOptions = originalIndexValues.filter(function(option) {
            return option.startsWith(prefix) && !selectedSearchIndex.includes(option);
        });
        
        indexValues.push(...matchingOptions);
        indexValues.sort();
        $("#index-listing").autocomplete('option', 'source', indexValues);
    } else {
        // If the removed value is not a wildcard option
        const prefix = removedValue;

        // Check if any wildcard options exist that match the removed value
        const wildcardExists = selectedSearchIndex.split(',').some(function(value) {
            const strippedValue = value.replace('*', '');
            return value.endsWith('*') && prefix.startsWith(strippedValue);
        });

        // If no wildcard options match the removed value's prefix, add it back to indexValues
        if (!wildcardExists) {
            indexValues.push(removedValue);
            indexValues.sort();
            $("#index-listing").autocomplete('option', 'source', indexValues);
        }
    }

    // Update selectedSearchIndex
    selectedSearchIndex = selectedSearchIndex.split(',').filter(function(value) {
        return value !== removedValue;
    }).join(',');

    // Update the input width and placeholder if necessary
    if ($('.index-container').find('.selected-index').length === 0) {
        $("#index-listing").css('width', '100%');
    }

    runQueryBtnHandler();
});


function addSelectedIndex(index) {
    const indexElement = $("<div class='selected-index'></div>").text(index);
    const removeIcon = $("<span class='remove-icon'>×</span>");
    indexElement.append(removeIcon);
    $("#index-listing").before(indexElement);

    if ($('.index-container').find('.selected-index').length === 0) {
        $("#index-listing").css('width', '100%');
    } else {
        $("#index-listing").css('width', '5px');
    }
}
