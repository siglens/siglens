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

$(function () {
    $('#custom-code-tab').tabs();
    $('#custom-chart-tab').tabs();
    $('#save-query-div').children().show();
});

$('#custom-code-tab').tabs({
    activate: function (_event, _ui) {
        let currentTab = $('#custom-code-tab').tabs('option', 'active');
        if (currentTab == 0) {
            // Query Builder Tab
            if (!isQueryBuilderSearch) {
                // Clear input boxes of the query builder when a query is searched from the free text
                $('.tags-list').empty();
                [firstBoxSet, secondBoxSet, thirdBoxSet] = [new Set(), new Set(), new Set()];
                $('#aggregations, #aggregate-attribute-text, #search-filter-text').show();
            }
            $('.query-language-option').removeClass('active');
            $('#query-language-options #option-3').addClass('active');
            $('#query-language-btn span').html('Splunk QL');
            displayQueryLangToolTip('3');
        } else {
            // Free Text Tab
            if (isQueryBuilderSearch) {
                let filterValue = getQueryBuilderCode();
                if (filterValue != '') $('#filter-input').val(filterValue);
                toggleClearButtonVisibility();
            }
        }
    },
});

$('#custom-chart-tab').tabs({
    activate: function (_event, _ui) {
        let currentTab = $('#custom-chart-tab').tabs('option', 'active');
        if (currentTab == 0) {
            $('#save-query-div').children().show();
            if (lastQType === 'logs-query') {
                $('#views-container, .fields-sidebar, .expand-svg-container').show();
            }
            $('#pagination-container').show();
        } else {
            $('#save-query-div').children().hide();
            $('#views-container, .fields-sidebar, .fields-resizer, .expand-svg-container , #pagination-container').hide();
        }
    },
});

$(document).ready(function () {
    $('#add-con')
        .off('click')
        .on('click', function (e) {
            e.stopPropagation();
            filterStart(e);
        });
    $('#filter-box-1').off('click').on('click', filterStart);
    $('#add-con-second').on('click', secondFilterStart);
    $('#filter-box-2').on('click', secondFilterStart);
    $('#add-con-third')
        .off('click')
        .on('click', function (e) {
            e.stopPropagation();
            ThirdFilterStart(e);
        });
    $('#filter-box-3').off('click').on('click', ThirdFilterStart);
    $('#completed').on('click', filterComplete);
    $('#completed-second').on('click', secondFilterComplete);
    $('#cancel-enter').on('click', cancelInfo);
    $('#cancel-enter-second').on('click', secondCancelInfo);
    $('#cancel-enter-third').on('click', ThirdCancelInfo);

    $('#add-con').show();
    $('#add-con-second').show();
    $('#completed').hide();
    $('#completed-second').hide();
    $('#cancel-enter').hide();
    $('#cancel-enter-second').hide();
    $('#add-filter').hide();
    $('#add-filter-second').hide();

    if (thirdBoxSet.size > 0) $('#aggregations').hide();
    else $('#aggregations').show();
    if (secondBoxSet.size > 0) $('#aggregate-attribute-text').hide();
    else $('#aggregate-attribute-text').show();
    if (firstBoxSet.size > 0) $('#search-filter-text').hide();
    else $('#search-filter-text').show();

    setShowColumnInfoDialog();

    updateResetButtonVisibility();
});

const tags = document.getElementById('tags');
const tagSecond = document.getElementById('tags-second');
const tagThird = document.getElementById('tags-third');

tags.addEventListener('click', function (event) {
    // If the clicked element has the class 'delete-button'
    if (event.target.classList.contains('delete-button')) {
        // Remove the parent element (the tag)
        let str = event.target.parentNode.textContent;
        firstBoxSet.delete(str.substring(0, str.length - 1));
        event.target.parentNode.remove();
        getSearchText();
        updateResetButtonVisibility();
        if (firstBoxSet.size > 0) $('#search-filter-text').hide();
        else $('#search-filter-text').show();
        cancelInfo(event);
    }
});

tagSecond.addEventListener('click', function (event) {
    // If the clicked element has the class 'delete-button'
    if (event.target.classList.contains('delete-button')) {
        // Remove the parent element (the tag)
        let str = event.target.parentNode.textContent;
        secondBoxSet.delete(str.substring(0, str.length - 1));
        event.target.parentNode.remove();
        getSearchText();
        updateResetButtonVisibility();
        if (secondBoxSet.size > 0) $('#aggregate-attribute-text').hide();
        else $('#aggregate-attribute-text').show();
        secondCancelInfo(event);
    }
});

tagThird.addEventListener('click', function (event) {
    // If the clicked element has the class 'delete-button'
    if (event.target.classList.contains('delete-button')) {
        // Remove the parent element (the tag)
        let str = event.target.parentNode.textContent;
        thirdBoxSet.delete(str.substring(0, str.length - 1));
        event.target.parentNode.remove();
        getSearchText();
        updateResetButtonVisibility();
        if (thirdBoxSet.size > 0) $('#aggregations').hide();
        else $('#aggregations').show();
        ThirdCancelInfo(event);
    }
});

$(document).mouseup(function (e) {
    var firstCon = $('#add-filter');
    var secondCon = $('#add-filter-second');
    var thirdCon = $('#add-filter-third');
    var dropInfo = $('.ui-autocomplete');
    if (!firstCon.is(e.target) && firstCon.has(e.target).length === 0 && !dropInfo.is(e.target) && dropInfo.has(e.target).length === 0 && !secondCon.is(e.target) && secondCon.has(e.target).length === 0 && !thirdCon.is(e.target) && thirdCon.has(e.target).length === 0) {
        cancelInfo(e);
        secondCancelInfo(e);
        ThirdCancelInfo(e);
    }
});

var calculations = ['min', 'max', 'count', 'avg', 'sum', 'sumsq', 'var', 'varp'];
var ifCurIsNum = false;
var availSymbol = [];

async function filterStart(evt) {
    evt.preventDefault();
    $('#column-first').attr('type', 'text');
    $('#add-con').hide();
    $('#cancel-enter').show();
    $('#add-filter').show();
    $('#add-filter').css({ visibility: 'visible' });
    $('#filter-box-1').addClass('select-box');

    $('#column-first').prop('disabled', true).attr('placeholder', 'Loading columns...');

    const columnsNames = await getColumns();

    $('#column-first').prop('disabled', false).attr('placeholder', '');

    if ($('#column-first').hasClass('ui-autocomplete-input')) {
        $('#column-first').autocomplete('destroy');
    }

    $('#column-first')
        .autocomplete({
            source: columnsNames.sort(),
            minLength: 0,
            maxheight: 100,
            select: async function (event, ui) {
                $('#symbol').val('').attr('type', 'text');
                $('#value-first').val('').attr('type', 'text');

                $('#symbol').prop('disabled', true).attr('placeholder', 'Loading...');
                $('#value-first').prop('disabled', true).attr('placeholder', '');

                let chooseColumn = ui.item.value.trim();
                const columnData = await getValuesForColumn(chooseColumn);
                const columnValues = columnData.values;
                const isNumericColumn = columnData.isNumeric;

                // Set available symbols based on column type
                if (isNumericColumn) {
                    availSymbol = ['=', '!=', '<=', '>=', '>', '<'];
                    ifCurIsNum = true;
                } else {
                    availSymbol = ['=', '!='];
                    ifCurIsNum = false;
                }

                $('#symbol').prop('disabled', false).attr('placeholder', '');

                if ($('#symbol').hasClass('ui-autocomplete-input')) {
                    $('#symbol').autocomplete('destroy');
                }
                if ($('#value-first').hasClass('ui-autocomplete-input')) {
                    $('#value-first').autocomplete('destroy');
                }

                $('#symbol')
                    .autocomplete({
                        source: availSymbol,
                        minLength: 0,
                        select: function (_event, _ui) {
                            $('#completed').show();
                            checkFirstBox(1);

                            setTimeout(() => {
                                $('#value-first').focus();
                                $('#value-first').autocomplete('search', '');
                            }, 100);
                        },
                    })
                    .on('focus', function () {
                        if (!$(this).val().trim()) $(this).keydown();
                    });

                $('#value-first')
                    .autocomplete({
                        source: columnValues,
                        minLength: 0,
                        select: function (_event, _ui) {
                            checkFirstBox(2);
                        },
                    })
                    .on('focus', function () {
                        if (!$(this).val().trim()) $(this).keydown();
                    });

                $('#value-first').prop('disabled', false);

                $('#symbol').focus();

                setTimeout(() => {
                    $('#symbol').autocomplete('search', '');
                }, 100);

                checkFirstBox(0);
            },
        })
        .on('focus', function () {
            if (!$(this).val().trim()) $(this).keydown();
        });

    $('#column-first').focus();

    setTimeout(() => {
        $('#column-first').autocomplete('search', '');
    }, 100);
}

async function secondFilterStart(evt) {
    evt.preventDefault();
    $('#filter-box-2').addClass('select-box');
    $('#column-second').attr('type', 'text');
    $('#add-con-second').hide();
    $('#cancel-enter-second').show();
    $('#add-filter-second').show();
    $('#add-filter-second').css({ visibility: 'visible' });

    if ($('#column-second').hasClass('ui-autocomplete-input')) {
        $('#column-second').autocomplete('destroy');
    }

    $('#column-second')
        .autocomplete({
            source: calculations.sort(),
            minLength: 0,
            maxheight: 100,
            select: async function (event, ui) {
                $('#value-second').attr('type', 'text');
                $('#completed-second').show();
                $('#value-second').val('');
                $('#completed-second').attr('disabled', true);

                $('#value-second').prop('disabled', true).attr('placeholder', 'Loading columns...');

                const columnsNames = ui.item.value === 'count' ? await getColumns() : await getNumericColumns();

                $('#value-second').prop('disabled', false).attr('placeholder', '');

                if ($('#value-second').hasClass('ui-autocomplete-input')) {
                    $('#value-second').autocomplete('destroy');
                }

                $('#value-second')
                    .autocomplete({
                        source: columnsNames.sort(),
                        minLength: 0,
                        select: function (_event, _ui) {
                            let secVal = $('#column-second').val();
                            if (secVal == null || secVal.trim() == '') {
                                $('#completed-second').attr('disabled', true);
                            } else {
                                $('#completed-second').attr('disabled', false);
                            }
                        },
                    })
                    .on('focus', function () {
                        if (!$(this).val().trim()) $(this).keydown();
                    });

                $('#value-second').focus();

                setTimeout(() => {
                    $('#value-second').autocomplete('search', '');
                }, 100);
            },
        })
        .on('focus', function () {
            if (!$(this).val().trim()) $(this).keydown();
        });

    $('#column-second').focus();
}

async function ThirdFilterStart(evt) {
    evt.preventDefault();
    $('#filter-box-3').addClass('select-box');
    $('#column-third').attr('type', 'text');
    $('#add-con-third').hide();
    $('#add-filter-third').show();
    $('#add-filter-third').css({ visibility: 'visible' });

    $('#column-third').prop('disabled', true).attr('placeholder', 'Loading columns...');

    const columnsNames = await getColumns();

    $('#column-third').prop('disabled', false).attr('placeholder', '');

    if ($('#column-third').hasClass('ui-autocomplete-input')) {
        $('#column-third').autocomplete('destroy');
    }

    $('#column-third')
        .autocomplete({
            source: columnsNames,
            minLength: 0,
            maxheight: 100,
            select: function (event, ui) {
                event.preventDefault();
                let tag = document.createElement('li');
                if (ui.item.value !== '') {
                    if (thirdBoxSet.has(ui.item.value)) {
                        alert('Duplicate filter!');
                        return;
                    } else thirdBoxSet.add(ui.item.value);

                    // Set the text content of the tag to the trimmed value
                    tag.innerText = ui.item.value;
                    // Add a delete button to the tag
                    tag.innerHTML += '<button class="delete-button">×</button>';
                    // Append the tag to the tags list
                    tagThird.appendChild(tag);

                    var dom = $('#tags-third');
                    var x = dom[0].scrollWidth;
                    dom[0].scrollLeft = x;

                    $('#column-third').val('');
                    $(this).blur();
                    getSearchText();
                    updateResetButtonVisibility();
                }

                if (thirdBoxSet.size > 0) $('#aggregations').hide();
                else $('#aggregations').show();

                ThirdCancelInfo(event);
                return false;
            },
        })
        .on('focus', function () {
            if (!$(this).val().trim()) $(this).keydown();
        });

    $('#column-third').focus();

    setTimeout(() => {
        $('#column-third').autocomplete('search', '');
    }, 100);
}
/**
 * check first box
 * @param {*} obj
 */
//eslint-disable-next-line no-unused-vars
function checkContent(obj) {
    if ($(obj).val() === '' || $(obj).val() === null) {
        $('#completed').attr('disabled', true);
    } else {
        $('#completed').attr('disabled', false);
    }
}

function checkFirstBox(curSelect) {
    let num = 0;
    if (($('#column-first').val() == null || $('#column-first').val().trim() == '') && curSelect != 0) num++;
    if (($('#symbol').val() == null || $('#symbol').val().trim() == '') && curSelect != 1) num++;
    if (($('#value-first').val() == null || $('#value-first').val().trim() == '') && curSelect != 2) num++;
    if (num != 0) {
        $('#completed').attr('disabled', true);
    } else {
        $('#completed').attr('disabled', false);
    }
}

//eslint-disable-next-line no-unused-vars
function checkSecondContent(obj) {
    if ($(obj).val() === '' || $(obj).val() === null) {
        $('#completed-second').attr('disabled', true);
    } else {
        $('#completed-second').attr('disabled', false);
    }
}

/**
 * first box complete one filter info
 * @param {*} evt
 */
function filterComplete(evt) {
    evt.preventDefault();
    let val = $('#value-first').val().trim();
    if ($('#column-first').val() == null || $('#column-first').val().trim() == '' || $('#symbol').val() == null || $('#symbol').val().trim() == '' || $('#value-first').val() == null || $('#value-first').val().trim() == '') {
        alert('Please select one of the values below');
        return;
    }
    $('#filter-box-1').removeClass('select-box');
    let tagContent = $('#column-first').val().trim() + $('#symbol').val().trim();
    if (ifCurIsNum) tagContent += val;
    else tagContent += '"' + val + '"';
    $('#column-first').val('');
    $('#symbol').val('');
    $('#value-first').val('');
    $('#column-first').attr('type', 'hidden');
    $('#symbol').attr('type', 'hidden');
    $('#value-first').attr('type', 'hidden');
    $('#completed').hide();
    $('#cancel-enter').hide();
    $('#add-filter').hide();
    $('#add-con').show();
    let tag = document.createElement('li');
    if (tagContent !== '') {
        if (firstBoxSet.has(tagContent)) {
            alert('Duplicate filter!');
            return;
        } else firstBoxSet.add(tagContent);
        // Set the text content of the tag to
        // the trimmed value
        tag.innerText = tagContent;
        // Add a delete button to the tag
        tag.innerHTML += '<button class="delete-button">×</button>';
        // Append the tag to the tags list
        tags.appendChild(tag);
        var dom = $('#tags');
        var x = dom[0].scrollWidth;
        dom[0].scrollLeft = x;
        getSearchText();
        updateResetButtonVisibility();
        if (firstBoxSet.size > 0) $('#search-filter-text').hide();
        else $('#search-filter-text').show();
    }
}

function secondFilterComplete(evt) {
    evt.preventDefault();
    if ($('#column-second').val() == null || $('#column-second').val().trim() == '' || $('#value-second').val() == null || $('#value-second').val().trim() == '') {
        alert('Please select one of the values below');
        return;
    }
    $('#filter-box-2').removeClass('select-box');
    let tagContent = $('#column-second').val().trim() + '(' + $('#value-second').val().trim() + ')';
    $('#column-second').val('');
    $('#value-second').val('');
    $('#column-second').attr('type', 'hidden');
    $('#value-second').attr('type', 'hidden');
    $('#completed-second').hide();
    $('#cancel-enter-second').hide();
    $('#add-filter-second').hide();
    $('#add-con-second').show();
    let tag = document.createElement('li');
    if (tagContent !== '') {
        if (secondBoxSet.has(tagContent)) {
            alert('Duplicate filter!');
            return;
        } else secondBoxSet.add(tagContent);
        // Set the text content of the tag to
        // the trimmed value
        tag.innerText = tagContent;
        // Add a delete button to the tag
        tag.innerHTML += '<button class="delete-button">×</button>';
        // Append the tag to the tags list
        tagSecond.appendChild(tag);
        var dom = $('#tags-second');
        var x = dom[0].scrollWidth;
        dom[0].scrollLeft = x;
        getSearchText();
        updateResetButtonVisibility();
        if (secondBoxSet.size > 0) $('#aggregate-attribute-text').hide();
        else $('#aggregate-attribute-text').show();
    }
}

// Todo - update this function show error message of we do not build correct query
function getSearchText() {
    let filterValue = getQueryBuilderCode();
    if (filterValue != '') {
        $('#query-input').val(filterValue);
    }
    if (filterValue === 'Searches with a Search Criteria must have an Aggregate Attribute') {
        $('#query-builder-btn').addClass('stop-search').prop('disabled', true);
    } else {
        $('#query-builder-btn').removeClass('stop-search').prop('disabled', false);
    }
}

function cancelInfo(evt) {
    evt.preventDefault();
    evt.stopPropagation();
    $('#filter-box-1').removeClass('select-box');
    $('#column-first').val('');
    $('#symbol').val('');
    $('#value-first').val('');
    $('#column-first').attr('type', 'hidden');
    $('#symbol').attr('type', 'hidden');
    $('#value-first').attr('type', 'hidden');
    $('#completed').hide();
    $('#add-filter').hide();
    $('#cancel-enter').hide();
    $('#add-con').show();
}

function secondCancelInfo(evt) {
    evt.preventDefault();
    evt.stopPropagation();
    $('#filter-box-2').removeClass('select-box');
    $('#column-second').val('');
    $('#value-second').val('');
    $('#column-second').attr('type', 'hidden');
    $('#value-second').attr('type', 'hidden');
    $('#completed-second').hide();
    $('#add-filter-second').hide();
    $('#cancel-enter-second').hide();
    $('#add-con-second').show();
}

function ThirdCancelInfo(event) {
    event.preventDefault();
    event.stopPropagation();
    $('#filter-box-3').removeClass('select-box');
    $('#column-third').val('');
    $('#add-filter-third').hide();
    $('#column-third').attr('type', 'hidden');
    $('#add-con-third').show();
}

/**
 * get cur column names from back-end for first input box
 *
 */
async function getColumns() {
    const data = {
        startEpoch: filterStartDate,
        endEpoch: filterEndDate,
        indexName: selectedSearchIndex,
    };

    try {
        const res = await $.ajax({
            method: 'post',
            url: 'api/listColumnNames',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(data),
        });

        if (res) {
            const columnsList = res.filter((column) => column !== '_index' && column !== 'timestamp');
            if (columnsList.length === 0) {
                showToast('No columns found in the selected time range.', 'error', 5000);
            }
            return columnsList;
        }
        showToast('No columns found in the selected time range.', 'error', 5000);
        return [];
    } catch (error) {
        console.error('Error fetching columns:', error);
        showToast('Error fetching columns: ' + error.message, 'error', 5000);
        return [];
    }
}

/**
 * get numeric column names from back-end for second input box (aggregation)
 *
 */
async function getNumericColumns() {
    const data = {
        searchText: '* | head 10',
        startEpoch: filterStartDate,
        endEpoch: filterEndDate,
        indexName: selectedSearchIndex,
        queryLanguage: 'Splunk QL',
    };

    try {
        const res = await $.ajax({
            method: 'post',
            url: 'api/search/',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(data),
        });

        if (!res.allColumns || !res.hits || !res.hits.records || res.hits.records.length === 0) {
            return [];
        }

        const numericColumns = [];

        for (const column of res.allColumns) {
            if (column === '_index' || column === 'timestamp') continue;

            let isNumeric = true;

            for (const record of res.hits.records) {
                const value = record[column];
                if (value !== null && value !== undefined) {
                    if (typeof value !== 'number') {
                        isNumeric = false;
                        break;
                    }
                }
            }

            if (isNumeric) {
                numericColumns.push(column);
            }
        }

        return numericColumns;
    } catch (error) {
        console.error('Error determining numeric columns:', error);
        return [];
    }
}
/**
 * get values of cur column names from back-end for first input box
 *
 */
async function getValuesForColumn(chooseColumn) {
    const param = {
        searchText: `*| dedup ${chooseColumn} | fields ${chooseColumn}`,
        startEpoch: filterStartDate,
        endEpoch: filterEndDate,
        indexName: selectedSearchIndex,
        queryLanguage: 'Splunk QL',
        from: 0,
        size: 100,
    };

    try {
        const res = await $.ajax({
            method: 'post',
            url: 'api/search',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
            data: JSON.stringify(param),
        });

        const columnValues = new Set();
        let isNumeric = true;

        if (res && res.hits && res.hits.records) {
            for (let i = 0; i < res.hits.records.length; i++) {
                let cur = res.hits.records[i][chooseColumn];

                // Check if cur is not null or undefined before processing
                if (cur !== null && cur !== undefined) {
                    if (typeof cur === 'string') {
                        isNumeric = false; // Found a string value, mark as non-numeric
                        columnValues.add(cur);
                    } else if (typeof cur === 'number') {
                        columnValues.add(cur.toString());
                    } else {
                        // Any other type (object, boolean, etc.) is considered non-numeric
                        isNumeric = false;
                        columnValues.add(cur.toString());
                    }
                }
            }
        }

        if (columnValues.size === 0) {
            showToast(`No values found for column "${chooseColumn}" in the selected time range.`, 'error', 5000);
        }

        return {
            values: Array.from(columnValues).sort(),
            isNumeric: isNumeric,
        };
    } catch (error) {
        console.error('Error fetching column values:', error);
        showToast('Error fetching column values: ' + error.message, 'error', 5000);
        return {
            values: [],
            isNumeric: false,
        };
    }
}

function setShowColumnInfoDialog() {
    $('#show-record-popup').dialog({
        autoOpen: false,
        resizable: false,
        title: 'Query Results Information',
        maxHeight: 307,
        height: 307,
        width: 464,
        modal: true,
        position: {
            my: 'center',
            at: 'center',
            of: window,
        },
        buttons: {
            Cancel: {
                class: 'cancelqButton cancel-record-btn btn btn-secondary',
                text: 'OK',
                click: function () {
                    $('#show-record-popup').dialog('close');
                    const tooltipInstance = $('#show-record-intro-btn')[0]?._tippy;
                    if (tooltipInstance) {
                        tooltipInstance.hide();
                    }
                },
            },
        },
        create: function () {
            $(this).parent().find('.ui-dialog-titlebar').show().addClass('border-bottom p-4');
        },
    });
    $('#show-record-intro-btn').on('click', function () {
        $('#show-record-popup').dialog('open');
        $('.ui-widget-overlay').addClass('opacity-75');
        // return false;
    });
}

$('#logs-settings').click(function () {
    event.stopPropagation();
    $('#setting-container').fadeToggle('fast');
});

$(document).click(function (event) {
    if (!$(event.target).closest('#setting-container').length) {
        $('#setting-container').hide();
    }
});

// Allow Editing Existing Filters in Query Builder
let originalTagContent = null;
let isCurrentlyEditing = false;

$('#tags').on('click', 'li', function (event) {
    if (!$(event.target).hasClass('delete-button') && event.target.tagName !== 'BUTTON') {
        event.stopPropagation();

        if (isCurrentlyEditing) return;
        isCurrentlyEditing = true;

        const tagElement = $(this);
        originalTagContent = tagElement.text();
        originalTagContent = originalTagContent.substring(0, originalTagContent.length - 1);

        let column, symbol, value;

        if (originalTagContent.includes('=') || originalTagContent.includes('!=') || originalTagContent.includes('<=') || originalTagContent.includes('>=') || originalTagContent.includes('>') || originalTagContent.includes('<')) {
            const symbols = ['!=', '<=', '>=', '=', '>', '<'];
            let foundSymbol = '';
            for (const sym of symbols) {
                if (originalTagContent.includes(sym)) {
                    foundSymbol = sym;
                    break;
                }
            }

            const parts = originalTagContent.split(foundSymbol);
            column = parts[0].trim();
            symbol = foundSymbol;

            value = parts[1].trim();
            if (value.startsWith('"') && value.endsWith('"')) {
                value = value.substring(1, value.length - 1);
                ifCurIsNum = false;
            } else {
                ifCurIsNum = true;
            }
        }

        firstBoxSet.delete(originalTagContent);
        tagElement.hide();

        $('#filter-box-1').off('click');
        $('#add-con').off('click');

        $('#filter-box-1').addClass('select-box');
        $('#add-con').hide();
        $('#cancel-enter').show();
        $('#add-filter').show();
        $('#add-filter').css({ visibility: 'visible' });
        $('#completed').show();

        $('#column-first').attr('type', 'text').val(column);
        $('#symbol').attr('type', 'text').val(symbol);
        $('#value-first').attr('type', 'text').val(value);

        getColumns().then((columnsNames) => {
            $('#column-first')
                .autocomplete({
                    source: columnsNames.sort(),
                    minLength: 0,
                    maxheight: 100,
                    select: async function (_event, ui) {
                        $('#symbol').attr('type', 'text');

                        let chooseColumn = ui.item.value.trim();
                        const columnData = await getValuesForColumn(chooseColumn);
                        const columnValues = columnData.values;
                        const isNumericColumn = columnData.isNumeric;

                        if (isNumericColumn) {
                            availSymbol = ['=', '!=', '<=', '>=', '>', '<'];
                            ifCurIsNum = true;
                        } else {
                            availSymbol = ['=', '!='];
                            ifCurIsNum = false;
                        }

                        setupSymbolField();
                        setupValueField(columnValues);

                        checkFirstBox(0);
                    },
                })
                .on('focus', function () {
                    if (!$(this).val().trim()) $(this).keydown();
                });

            function setupSymbolField() {
                $('#symbol')
                    .autocomplete({
                        source: availSymbol,
                        minLength: 0,
                        select: function (_event, _ui) {
                            $('#value-first').attr('type', 'text');
                            $('#completed').show();

                            checkFirstBox(1);
                        },
                    })
                    .on('focus', function () {
                        if (!$(this).val().trim()) $(this).keydown();
                    });
            }

            function setupValueField(columnValues) {
                $('#value-first')
                    .autocomplete({
                        source: columnValues || [],
                        minLength: 0,
                        select: function (_event, _ui) {
                            checkFirstBox(2);
                        },
                    })
                    .on('focus', function () {
                        if (!$(this).val().trim()) $(this).keydown();
                    });
            }

            if (column) {
                getValuesForColumn(column).then((columnData) => {
                    const columnValues = columnData.values;

                    setupSymbolField();
                    setupValueField(columnValues);
                });
            } else {
                $('#column-first').focus();
            }
        });

        checkFirstBox(3);

        $('#completed')
            .off('click')
            .on('click', function (e) {
                if (originalTagContent) {
                    firstBoxSet.delete(originalTagContent);
                    originalTagContent = null;
                }
                tagElement.remove();
                filterComplete(e);
                $('#add-con, #filter-box-1').off('click').on('click', filterStart);
                isCurrentlyEditing = false;
            });

        $('#cancel-enter')
            .off('click')
            .on('click', function (e) {
                restoreOriginalFilter(tagElement);
                cancelInfo(e);
                $('#add-con, #filter-box-1').off('click').on('click', filterStart);
                isCurrentlyEditing = false;
            });

        $(document)
            .off('mousedown.filterEdit')
            .on('mousedown.filterEdit', function (e) {
                if (!$(e.target).closest('#add-filter').length && !$(e.target).closest('.ui-autocomplete').length && !$(e.target).closest('#tags li').length) {
                    restoreOriginalFilter(tagElement);
                    cancelInfo(e);
                    $('#add-con, #filter-box-1').off('click').on('click', filterStart);
                    isCurrentlyEditing = false;

                    $(document).off('mousedown.filterEdit');
                }
            });

        getSearchText();
        updateResetButtonVisibility();
        if (firstBoxSet.size > 0) $('#search-filter-text').hide();
        else $('#search-filter-text').show();
    }
});

function restoreOriginalFilter(tagElement) {
    if (originalTagContent) {
        firstBoxSet.add(originalTagContent);
        tagElement.show();
        originalTagContent = null;

        if (firstBoxSet.size > 0) $('#search-filter-text').hide();
        else $('#search-filter-text').show();
    }
}

let originalSecondTagContent = null;

$('#tags-second').on('click', 'li', function (event) {
    if (!$(event.target).hasClass('delete-button') && event.target.tagName !== 'BUTTON') {
        event.stopPropagation();

        if (isCurrentlyEditing) return;
        isCurrentlyEditing = true;

        const tagElement = $(this);
        originalSecondTagContent = tagElement.text();
        originalSecondTagContent = originalSecondTagContent.substring(0, originalSecondTagContent.length - 1);

        let func, column;
        if (originalSecondTagContent.includes('(') && originalSecondTagContent.includes(')')) {
            const funcPart = originalSecondTagContent.split('(')[0].trim();
            const columnPart = originalSecondTagContent.substring(originalSecondTagContent.indexOf('(') + 1, originalSecondTagContent.indexOf(')')).trim();

            func = funcPart;
            column = columnPart;
        }

        secondBoxSet.delete(originalSecondTagContent);
        tagElement.hide();

        $('#filter-box-2').off('click');
        $('#add-con-second').off('click');

        $('#column-second').attr('type', 'text').val(func);
        $('#value-second').attr('type', 'text').val(column);

        $('#filter-box-2').addClass('select-box');
        $('#add-con-second').hide();
        $('#cancel-enter-second').show();
        $('#add-filter-second').show();
        $('#add-filter-second').css({ visibility: 'visible' });
        $('#completed-second').show();
        $('#completed-second').attr('disabled', false);

        async function setupColumnField() {
            const columnsPromise = func === 'count' ? getColumns() : getNumericColumns();

            const columnInfo = await columnsPromise;

            $('#value-second')
                .autocomplete({
                    source: columnInfo.sort(),
                    minLength: 0,
                    select: function (_event, _ui) {
                        $('#completed-second').attr('disabled', false);
                    },
                })
                .on('focus', function () {
                    if (!$(this).val().trim()) $(this).keydown();
                });

            $('#value-second').focus();
        }

        $('#column-second')
            .autocomplete({
                source: calculations.sort(),
                minLength: 0,
                maxheight: 100,
                select: async function (_event, ui) {
                    $('#value-second').attr('type', 'text');
                    $('#completed-second').show();
                    $('#value-second').val('');

                    func = ui.item.value;

                    await setupColumnField();
                },
            })
            .on('focus', function () {
                if (!$(this).val().trim()) $(this).keydown();
            });

        setupColumnField();

        $('#column-second').focus();

        $('#completed-second')
            .off('click')
            .on('click', function (e) {
                if (originalSecondTagContent) {
                    secondBoxSet.delete(originalSecondTagContent);
                    originalSecondTagContent = null;
                }
                tagElement.remove();
                secondFilterComplete(e);
                $('#add-con-second, #filter-box-2').off('click').on('click', secondFilterStart);
                isCurrentlyEditing = false;
            });

        $('#cancel-enter-second')
            .off('click')
            .on('click', function (e) {
                restoreOriginalSecondFilter(tagElement);
                secondCancelInfo(e);
                $('#add-con-second, #filter-box-2').off('click').on('click', secondFilterStart);
                isCurrentlyEditing = false;
            });

        $(document)
            .off('mousedown.filterEdit2')
            .on('mousedown.filterEdit2', function (e) {
                if (!$(e.target).closest('#add-filter-second').length && !$(e.target).closest('.ui-autocomplete').length && !$(e.target).closest('#tags-second li').length) {
                    restoreOriginalSecondFilter(tagElement);
                    secondCancelInfo(e);
                    $('#add-con-second, #filter-box-2').off('click').on('click', secondFilterStart);
                    isCurrentlyEditing = false;

                    $(document).off('mousedown.filterEdit2');
                }
            });

        getSearchText();
        updateResetButtonVisibility();
        if (secondBoxSet.size > 0) $('#aggregate-attribute-text').hide();
        else $('#aggregate-attribute-text').show();
    }
});

function restoreOriginalSecondFilter(tagElement) {
    if (originalSecondTagContent) {
        secondBoxSet.add(originalSecondTagContent);
        tagElement.show();
        originalSecondTagContent = null;

        if (secondBoxSet.size > 0) $('#aggregate-attribute-text').hide();
        else $('#aggregate-attribute-text').show();
    }
}

let originalThirdTagContent = null;

$('#tags-third').on('click', 'li', function (event) {
    if (!$(event.target).hasClass('delete-button') && event.target.tagName !== 'BUTTON') {
        event.stopPropagation();

        if (isCurrentlyEditing) return;
        isCurrentlyEditing = true;

        const tagElement = $(this);
        originalThirdTagContent = tagElement.text();
        originalThirdTagContent = originalThirdTagContent.substring(0, originalThirdTagContent.length - 1);

        thirdBoxSet.delete(originalThirdTagContent);
        tagElement.hide();

        $('#filter-box-3').addClass('select-box');
        $('#column-third').attr('type', 'text').val(originalThirdTagContent);
        $('#add-con-third').hide();
        $('#add-filter-third').show();
        $('#add-filter-third').css({ visibility: 'visible' });

        getColumns().then((columnsNames) => {
            $('#filter-box-3').off('click');
            $('#add-con-third').off('click');

            if ($('#column-third').hasClass('ui-autocomplete-input')) {
                $('#column-third').autocomplete('destroy');
            }

            $('#column-third')
                .autocomplete({
                    source: columnsNames,
                    minLength: 0,
                    maxheight: 100,
                    select: function (event, ui) {
                        event.preventDefault();
                        tagElement.remove();
                        originalThirdTagContent = null;

                        let tag = document.createElement('li');
                        if (ui.item.value !== '') {
                            if (thirdBoxSet.has(ui.item.value)) {
                                alert('Duplicate filter!');
                                return;
                            } else {
                                thirdBoxSet.add(ui.item.value);
                            }

                            tag.innerText = ui.item.value;
                            tag.innerHTML += '<button class="delete-button">×</button>';
                            tagThird.appendChild(tag);

                            var dom = $('#tags-third');
                            var x = dom[0].scrollWidth;
                            dom[0].scrollLeft = x;

                            $('#column-third').val('');
                            $(this).blur();
                            getSearchText();
                            updateResetButtonVisibility();
                        }

                        if (thirdBoxSet.size > 0) $('#aggregations').hide();
                        else $('#aggregations').show();

                        ThirdCancelInfo(event);

                        $('#add-con-third, #filter-box-3').off('click').on('click', ThirdFilterStart);
                        isCurrentlyEditing = false;

                        return false;
                    },
                })
                .on('focus', function () {
                    if (!$(this).val().trim()) $(this).keydown();
                });

            $('#column-third').focus();
        });

        $(document)
            .off('mousedown.filterEdit3')
            .on('mousedown.filterEdit3', function (e) {
                if (!$(e.target).closest('#add-filter-third').length && !$(e.target).closest('.ui-autocomplete').length && !$(e.target).closest('#tags-third li').length) {
                    restoreOriginalThirdFilter(tagElement);
                    ThirdCancelInfo(e);
                    $('#add-con-third, #filter-box-3').off('click').on('click', ThirdFilterStart);
                    isCurrentlyEditing = false;

                    $(document).off('mousedown.filterEdit3');
                }
            });

        $('#cancel-enter-third')
            .off('click')
            .on('click', function (e) {
                restoreOriginalThirdFilter(tagElement);
                ThirdCancelInfo(e);
                $('#add-con-third, #filter-box-3').off('click').on('click', ThirdFilterStart);
                isCurrentlyEditing = false;
            });

        getSearchText();
        updateResetButtonVisibility();
        if (thirdBoxSet.size > 0) $('#aggregations').hide();
        else $('#aggregations').show();
    }
});

function restoreOriginalThirdFilter(tagElement) {
    if (originalThirdTagContent) {
        thirdBoxSet.add(originalThirdTagContent);
        tagElement.show();
        originalThirdTagContent = null;

        if (thirdBoxSet.size > 0) $('#aggregations').hide();
        else $('#aggregations').show();

        getSearchText();
        updateResetButtonVisibility();
    }
}

// Reset the query builder
$('.custom-reset-button').on('click', function (e) {
    e.stopPropagation();

    $(this).addClass('active');

    firstBoxSet = new Set();
    let tags = document.getElementById('tags');
    if (tags) {
        while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
        }
        $('#search-filter-text').show();
    }

    secondBoxSet = new Set();
    let tagsSecond = document.getElementById('tags-second');
    if (tagsSecond) {
        while (tagsSecond.firstChild) {
            tagsSecond.removeChild(tagsSecond.firstChild);
        }
        $('#aggregate-attribute-text').show();
    }

    thirdBoxSet = new Set();
    let tagsThird = document.getElementById('tags-third');
    if (tagsThird) {
        while (tagsThird.firstChild) {
            tagsThird.removeChild(tagsThird.firstChild);
        }
        $('#aggregations').show();
    }

    $('#query-builder-btn').removeClass('stop-search').prop('disabled', false);
    $('#filter-input').val('*');
});

function updateResetButtonVisibility() {
    const hasFiltersInVariables = (firstBoxSet && firstBoxSet.size > 0) || (secondBoxSet && secondBoxSet.size > 0) || (thirdBoxSet && thirdBoxSet.size > 0);

    const hasFiltersInDOM = $('#tags li').length > 0 || $('#tags-second li').length > 0 || $('#tags-third li').length > 0;

    const hasFilters = hasFiltersInVariables || hasFiltersInDOM;

    if (hasFilters) {
        $('.custom-reset-button').show();
    } else {
        $('.custom-reset-button').hide();
    }
}
