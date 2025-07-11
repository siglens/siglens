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

let localPanels = [],
    dbData,
    dbName,
    dbDescr,
    dbId,
    allResultsDisplayed = 0;
let panelIndex;
let isFavorite = false;
//eslint-disable-next-line no-unused-vars
let flagDBSaved = true;
let timeRange = 'Last 1 Hr';
let dbRefresh = '';
let originalIndexValues = [];
//eslint-disable-next-line no-unused-vars
let indexValues = [];
let isDefaultDashboard = false;
let searchTippy;
let isFilterApplied = false;
let originalQueries = {};

$(document).ready(async function () {
    let indexes = await getListIndices();
    if (indexes) {
        originalIndexValues = indexes.map((item) => item.index);
        indexValues = [...originalIndexValues];
    }
    initializeIndexAutocomplete();

    initializeFilterInputEvents();

    $('#dbSet-save-json').on('click', saveJsonChanges);
    $('.panelEditor-container').hide();
    $('.dbSet-container').hide();
    setupEventHandlers();
    dbId = getDashboardId();
    if (defaultDashboardIds.includes(dbId)) {
        isDefaultDashboard = true;
        $('#save-db-btn, #add-panel-btn').hide();
    }

    $('#add-panel-btn, .close-widget-popup').click(() => {
        let pflag = `{{ .PanelFlag }}`;
        if (pflag === 'false' && localPanels.length >= 10) {
            return;
        }
        $('#add-widget-options').toggle();
        $('.add-icon').toggleClass('rotate-icon');
        $('#add-panel-btn').toggleClass('active');
        $('.add-panel-div .plus-icon').toggle();
        $('.default-item').toggleClass('active');

        // Check if .add-panel-div is active and update text accordingly
        if ($('.default-item').hasClass('active')) {
            $('.add-panel-div .text').text('Select the panel type');
            $('.add-panel-div .plus-icon').hide();
        } else {
            $('.add-panel-div .text').text('Add Panel');
            $('.add-panel-div .plus-icon').show();
        }
    });

    $('.widget-option').on('click', (event) => {
        let dataIndex = $(event.currentTarget).data('index');
        addPanel(dataIndex);
    });

    // Event handler for add-panel-div click
    $(document).on('click', '.default-item', function () {
        if ($(this).hasClass('active')) {
            return;
        } else {
            let pflag = `{{ .PanelFlag }}`;
            if (pflag === 'false' && localPanels.length >= 10) {
                return;
            }
            $(this).addClass('active');
            $('#add-widget-options').toggle();
            $('.add-icon').toggleClass('rotate-icon');
            $('#add-panel-btn').toggleClass('active');
            $(this).find('.text').text('Select the panel type');
            $('.add-panel-div .plus-icon').hide();
        }
    });

    // // Event handler to remove active class when clicking outside
    $('#new-dashboard').on('click', function (event) {
        if (!$(event.target).closest('.default-item').length && !$(event.target).closest('#add-widget-options').length && !$(event.target).closest('#add-panel-btn').length && !$(event.target).closest('.grid-stack-item').length && !$(event.target).closest('.panel-view-li').length) {
            $('.default-item').removeClass('active');
            $('.add-panel-div .text').text('Add Panel');
            $('.add-panel-div .plus-icon').show();
            $('#add-widget-options').hide();
            $('.add-icon').removeClass('rotate-icon');
            $('#add-panel-btn').removeClass('active');
        }
    });

    $('#theme-btn').click(() => updateDashboardTheme());
    getDashboardData();

    $('#favbutton').on('click', toggleFavorite);
});
function saveJsonChanges() {
    try {
        const jsonText = aceEditor.getValue();
        const updatedData = JSON.parse(jsonText); // Parse to validate

        // Update local variables
        dbName = updatedData.name;
        dbDescr = updatedData.description;
        timeRange = updatedData.timeRange;
        localPanels = updatedData.panels;
        dbRefresh = updatedData.refresh;
        const isFavorite = updatedData.isFavorite;

        // Update the dbData object
        dbData = updatedData;

        return fetch('/api/dashboards/update', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
            },
            body: JSON.stringify({
                id: dbId,
                name: dbName,
                details: {
                    name: dbName,
                    description: dbDescr,
                    timeRange: timeRange,
                    panels: localPanels.map((panel) => ({
                        ...panel,
                        style: {
                            display: panel.style.display,
                            color: panel.style.color,
                            lineStyle: panel.style.lineStyle,
                            lineStroke: panel.style.lineStroke,
                        },
                    })),
                    refresh: dbRefresh,
                    isFavorite: isFavorite,
                    panelFlag: `{{ .PanelFlag }}`,
                    folder: dbData.folder,
                },
            }),
        })
            .then((res) => {
                if (res.status === 409) {
                    showToast('Dashboard name already exists', 'error');
                    throw new Error('Dashboard name already exists');
                }
                if (res.status == 200) {
                    showToast('Dashboard Updated Successfully', 'success');
                    return true;
                }
                return res.json().then((err) => {
                    showToast('Request failed: ' + err.message, 'error');
                    throw new Error('Request failed: ' + err.message);
                });
            })
            .catch((error) => {
                console.error(error);
                alert('Failed to save the dashboard. Please try again.');
                return false;
            });
    } catch (e) {
        console.error(e);
        alert('Invalid JSON format. Please correct the JSON and try again.');
    }
}

// Initialize Gridstack
var options = {
    resizable: {
        handles: 'e, se, s, sw, w',
    },
    draggable: {
        handle: '.grid-stack-item-content .panel-header',
    },
    animate: false,
};
var grid = GridStack.init(options, '#panel-container');

$('#save-db-btn').on('click', updateDashboard);
$('.refresh-btn').on('click', refreshDashboardHandler);
$('#db-settings-btn').on('click', handleDbSettings);
$('#dbSet-save').on('click', saveDbSetting);
$('#dbSet-discard').on('click', discardDbSetting);
$('.refresh-range-item').on('click', refreshRangeItemHandler);

async function updateDashboard() {
    timeRange = $('#date-picker-btn').text().trim().replace(/\s+/g, ' ');
    flagDBSaved = true;
    let tempPanels = JSON.parse(JSON.stringify(localPanels));
    for (let i = 0; i < tempPanels.length; i++) delete tempPanels[i].queryRes;
    return fetch('/api/dashboards/update', {
        method: 'POST',
        body: JSON.stringify({
            id: dbId,
            name: dbName,
            details: {
                name: dbName,
                description: dbDescr,
                timeRange: timeRange,
                panels: tempPanels.map((panel) => ({
                    ...panel,
                    style: {
                        display: panel.style?.display || 'Line chart',
                        color: panel.style?.color || 'Classic',
                        lineStyle: panel.style?.lineStyle || 'Solid',
                        lineStroke: panel.style?.lineStroke || 'Normal',
                    },
                })),
                refresh: dbRefresh,
                panelFlag: `{{ .PanelFlag }}`,
                isFavorite: isFavorite,
                folder: dbData.folder,
            },
        }),
    })
        .then((res) => {
            if (res.status === 409) {
                showToast('Dashboard name already exists', 'error');
                throw new Error('Dashboard name already exists');
            }
            if (res.status == 200) {
                $('.name-dashboard').text(dbName);
                showToast('Dashboard Updated Successfully', 'success');
                return true;
            }
            return res.json().then((err) => {
                showToast('Request failed: ' + err.message, 'error');
                throw new Error('Request failed: ' + err.message);
            });
        })
        .catch((error) => {
            console.error(error);
            return false;
        });
}

function refreshDashboardHandler() {
    if ($('#viewPanel-container').css('display') !== 'none') {
        displayPanelView(panelIndex);
    } else {
        for (let i = 0; i < localPanels.length; i++) {
            localPanels[i].queryRes = undefined;
        }
        displayPanels();
    }
}

function handlePanelView() {
    $('.panel-view-li').unbind('click');
    $('.panel-view-li').on('click', function () {
        panelIndex = $(this).closest('.panel').attr('panel-index');
        pauseRefreshInterval();
        viewPanelInit();
        displayPanelView(panelIndex);
    });
}

function viewPanelInit() {
    $('.panelEditor-container').css('display', 'flex');
    $('.panelDisplay #panelLogResultsGrid').empty();
    $('.panelDisplay .big-number-display-container').hide();
    $('.popupOverlay').addClass('active');
    $('.panelDisplay #empty-response').hide();
    editPanelInit(-1);
}

function handlePanelEdit() {
    $('.panel-edit-li').unbind('click');
    $('.panel-edit-li').on('click', function () {
        panelIndex = $(this).closest('.panel').attr('panel-index');
        editPanelInit();
        $('.panelEditor-container').css('display', 'flex');
        $('.popupOverlay').addClass('active');
        $('.panelDisplay #panelLogResultsGrid').empty();
        $('.panelDisplay .big-number-display-container').hide();
        $('.panelDisplay #empty-response').hide();
    });
}
function handlePanelRemove(panelId) {
    $(`#panel${panelId} .panel-remove-li`).unbind('click');
    $(`#panel${panelId} .panel-remove-li`).on('click', function () {
        showPrompt(panelId);
    });

    function showPrompt(panelId) {
        $('.popupOverlay, .popupContent').addClass('active');
        $('#delete-btn-panel').on('click', function () {
            deletePanel(panelId);
            $('.popupOverlay, .popupContent').removeClass('active');
        });
        $('#cancel-btn-panel').on('click', function () {
            $('.popupOverlay, .popupContent').removeClass('active');
        });
    }

    async function deletePanel(panelId) {
        flagDBSaved = false;

        // Remove the panel element
        const panel = $(`#panel${panelId}`);

        // Remove the panel data from the localPanels array
        let panelIndex = panel.attr('panel-index');
        localPanels = localPanels.filter(function (el) {
            return el.panelIndex != panelIndex;
        });
        panel.remove();
        resetPanelIndices();

        // Remove the corresponding grid-stack-item
        const gridItem = $(`#${panelId}`);
        if (gridItem.length > 0) {
            grid.removeWidget(gridItem.get(0));
        }
        await updateDashboard();
    }
}

function handleDescriptionTooltip(panelId, description, searchText) {
    const panelInfoCorner = $(`#panel${panelId} .panel-info-corner`);
    const panelDescIcon = $(`#panel${panelId} .panel-info-corner #panel-desc-info`);
    panelInfoCorner.show();
    let tooltipText = '';

    // Check if description is provided
    if (description) {
        tooltipText += `Description: ${description}`;
    }

    // Check if both description and searchText are provided, add line break if needed
    if (description && searchText) {
        tooltipText += '\n';
    }

    // Check if searchText is provided
    if (searchText) {
        tooltipText += `Query: ${searchText}`;
    }

    panelDescIcon.attr('title', tooltipText);

    panelDescIcon.tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'hover',
    });
    panelInfoCorner.hover(
        function () {
            panelDescIcon.tooltip('show');
        },
        function () {
            panelDescIcon.tooltip('hide');
        }
    );
}

function handlePanelDuplicate() {
    $('.panel-dupl-li').unbind('click');
    $('.panel-dupl-li').on('click', async function () {
        flagDBSaved = false;
        let duplicatedPanelIndex = $(this).closest('.panel').attr('panel-index');
        addDuplicatePanel(JSON.parse(JSON.stringify(localPanels[duplicatedPanelIndex])));
        renderDuplicatePanel(duplicatedPanelIndex);
        await updateDashboard();
    });
}

function renderDuplicatePanel(duplicatedPanelIndex) {
    let localPanel = localPanels[localPanels.length - 1];
    let panelId = localPanels[localPanels.length - 1].panelId;
    // only render the duplicated panel
    $(`#panel${localPanels[localPanels.length - 1].panelId} .panel-header p`).html(localPanels[duplicatedPanelIndex].name + 'Copy');

    if (localPanel.description || localPanel.queryData.searchText) {
        handleDescriptionTooltip(panelId, localPanel.description, localPanel.queryData.searchText);
    }

    if (localPanel.queryType === 'logs') {
        // Handle all chart types for logs
        if (localPanel.chartType == 'Data Table' || localPanel.chartType == 'loglines') {
            let panEl = $(`#panel${panelId} .panel-body`);
            let responseDiv = `<div id="panelLogResultsGrid" class="panelLogResultsGrid ag-theme-mycustomtheme"></div>
            <div id="empty-response"></div>`;
            panEl.append(responseDiv);
            $('#panelLogResultsGrid').show();
            if (localPanel.queryRes) runPanelLogsQuery(localPanel.queryData, panelId, localPanel, localPanel.queryRes);
            else runPanelLogsQuery(localPanel.queryData, panelId, localPanel);
        } else if (localPanel.chartType == 'Line Chart' || localPanel.chartType == 'Bar Chart' || localPanel.chartType == 'Pie Chart' || localPanel.chartType == 'number') {
            let panEl = $(`#panel${panelId} .panel-body`);
            let responseDiv = '';

            if (localPanel.chartType == 'number') {
                responseDiv = `<div class="big-number-display-container"></div>
                <div id="empty-response"></div><div id="corner-popup"></div>`;
                panEl.append(responseDiv);
                $('.big-number-display-container').show();
            } else {
                responseDiv = `<div id="empty-response"></div><div id="corner-popup"></div>`;
                panEl.append(responseDiv);
            }

            if (localPanel.queryRes) runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
            else runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
        }
    } else if (localPanel.queryType === 'metrics') {
        // Only Line Chart for metrics
        if (localPanel.chartType == 'Line Chart') {
            let panEl = $(`#panel${panelId} .panel-body`);
            let responseDiv = `<div id="empty-response"></div></div><div id="corner-popup"></div>`;
            panEl.append(responseDiv);
            if (localPanel.queryRes) runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel, localPanel.queryRes);
            else runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel);
        }
    }
}

function resetPanelIndices() {
    for (let i = 0; i < localPanels.length; i++) {
        localPanels[i].panelIndex = i;
    }
}

async function getDashboardData() {
    try {
        const response = await fetch(`/api/dashboards/${dbId}`);

        if (!response.ok) {
            handleDashboardError(response.status, response.statusText);
            return;
        }

        const data = await response.json();
        dbData = data;

        const breadcrumb = new Breadcrumb();
        const urlParams = new URLSearchParams(window.location.search);
        const mode = urlParams.get('mode');

        breadcrumb.render(
            dbData.folder?.breadcrumbs,
            dbData.name,
            true, // Show favorite button for dashboard
            dbData.isFavorite,
            mode === 'settings',
            false
        );
        breadcrumb.onFavoriteClick(() => toggleFavorite(dbId));

        dbName = dbData.name;
        dbDescr = dbData.description;
        dbRefresh = dbData.refresh;
        isFavorite = dbData.isFavorite;

        if (dbData.panels != undefined) {
            localPanels = JSON.parse(JSON.stringify(dbData.panels));
            originalQueries = {};
            localPanels.forEach((panel) => {
                if (panel.queryData && panel.queryData.searchText) {
                    originalQueries[panel.panelId] = panel.queryData.searchText;
                }
            });
        } else {
            localPanels = [];
        }

        if (localPanels != undefined) {
            if (mode === 'settings') {
                // When page loads and mode=settings is in URL, open settings
                handleDbSettings();
            } else {
                displayPanels();
            }
            setFavoriteValue(dbData.isFavorite);
            setTimePickerValue();
            setRefreshItemHandler();
        }
    } catch (error) {
        console.error('Failed to fetch dashboard data:', error);
        handleDashboardError(0, 'Network Error');
    }
}

function setTimePickerValue() {
    let start, end;
    if (localPanels.length > 0) {
        localPanels.some((panel) => {
            if (panel.queryData) {
                if (panel.queryType === 'metrics') {
                    if (Array.isArray(panel.queryData.queriesData)) {
                        let query = panel.queryData.queriesData[0];
                        start = query.start;
                        end = query.end;
                        return true;
                    }
                } else {
                    start = panel.queryData.startEpoch;
                    end = panel.queryData.endEpoch;
                    return true;
                }
            }
            return false;
        });
        if (!isNaN(start)) {
            let stDate = Number(start);
            let endDate = Number(end);
            datePickerHandler(stDate, endDate, 'custom');
            $('.inner-range .db-range-item').removeClass('active');

            loadCustomDateTimeFromEpoch(stDate, endDate);
        } else {
            datePickerHandler(start, end, dbData.timeRange);
            $('.inner-range .db-range-item').removeClass('active');
            $('.inner-range #' + start).addClass('active');
        }
    }
}
//eslint-disable-next-line no-unused-vars
function updateTimeRangeForAllPanels(filterStartDate, filterEndDate) {
    localPanels.forEach((panel) => {
        delete panel.queryRes;

        if (panel.queryData) {
            if (panel.queryType === 'metrics') {
                if (panel.queryData) {
                    // Update start and end for each item in queriesData
                    if (Array.isArray(panel.queryData.queriesData)) {
                        panel.queryData.queriesData.forEach((query) => {
                            query.start = filterStartDate;
                            query.end = filterEndDate;
                        });
                    }

                    // Update start and end for each item in formulasData
                    if (Array.isArray(panel.queryData.formulasData)) {
                        panel.queryData.formulasData.forEach((formula) => {
                            formula.start = filterStartDate;
                            formula.end = filterEndDate;
                        });
                    }
                }
            } else {
                // logs
                panel.queryData.startEpoch = filterStartDate;
                panel.queryData.endEpoch = filterEndDate;
            }
        }
    });
    $('.inner-range .db-range-item').removeClass('active');

    if (!isNaN(filterStartDate)) {
        let stDate = Number(filterStartDate);
        let endDate = Number(filterEndDate);
        datePickerHandler(stDate, endDate, 'custom');
        loadCustomDateTimeFromEpoch(stDate, endDate);
    } else {
        $('.inner-range #' + filterStartDate).addClass('active');
        datePickerHandler(filterStartDate, filterEndDate, filterStartDate);
    }
}

// Event listener for Gridstack resize and drag events
grid.on('change', function (event, items) {
    items.forEach(function (item) {
        // Find the panel in localPanels array using its ID
        let panelIndex = localPanels.findIndex((panel) => panel.panelId === item.el.id);
        if (panelIndex !== -1) {
            // Update the position and size of the panel in localPanels array
            localPanels[panelIndex].gridpos.x = item.y;
            localPanels[panelIndex].gridpos.y = item.x;
            localPanels[panelIndex].gridpos.w = item.width;
            localPanels[panelIndex].gridpos.h = item.height;
        }
    });
});

grid.on('dragstart', function (_event, _items) {
    $('.default-item').hide();
});

grid.on('resizestart', function (_event, _items) {
    $('.default-item').hide();
});

grid.on('dragstop', function (_event, _items) {
    $('.default-item').show();
});

grid.on('resizestop', function (_event, ui) {
    var gridStackItemId = ui.id;
    var panelIndex = $('#' + gridStackItemId)
        .find('.panel')
        .attr('panel-index');
    const panelChartType = localPanels[panelIndex].chartType;
    switch (panelChartType) {
        case 'number':
            var newSize = $('#' + gridStackItemId).width() / 8;
            $('#' + gridStackItemId)
                .find('.big-number, .unit')
                .css('font-size', newSize + 'px');
            break;
        case 'Bar Chart':
        case 'Pie Chart':
            var echartsInstanceId = $('#' + gridStackItemId)
                .find('.panEdit-panel')
                .attr('_echarts_instance_');
            if (echartsInstanceId) {
                var echartsInstance = echarts.getInstanceById(echartsInstanceId);
                if (echartsInstance) {
                    echartsInstance.resize();
                }
            }
            break;
    }
    // Show the default-item when resizing stops
    $('.default-item').show();
});

async function displayPanels() {
    allResultsDisplayed = localPanels.length;
    grid.removeAll();
    let panelContainerMinHeight = 0;
    $('body').css('cursor', 'progress');

    // Variable to store the maximum coordinates of existing panels
    let maxCoord = { x: 0, y: 0 };

    // Loop through existing panels to find the maximum coordinates
    localPanels.forEach((localPanel) => {
        let panelEndX = localPanel.gridpos.x + localPanel.gridpos.w;
        let panelEndY = localPanel.gridpos.y + localPanel.gridpos.h;
        if (panelEndX > maxCoord.x) maxCoord.x = panelEndX;
        if (panelEndY > maxCoord.y) maxCoord.y = panelEndY;
    });

    for (const localPanel of localPanels) {
        let idpanel = localPanel.panelId;
        let widgetOptions = {
            width: parseInt(localPanel.gridpos.w),
            height: parseInt(localPanel.gridpos.h),
            x: parseInt(localPanel.gridpos.y),
            y: parseInt(localPanel.gridpos.x),
        };
        if (isDefaultDashboard) {
            widgetOptions.noResize = true;
            widgetOptions.noMove = true;
        }
        var newItem = grid.addWidget(`<div class="grid-stack-item" id="${idpanel}"><div class="grid-stack-item-content"></div></div>`, widgetOptions);
        let panelDiv;
        // Append panel layout to the new grid-stack-item
        if (isDefaultDashboard) {
            panelDiv = $('<div>').append(defaultPanelLayout).addClass('panel temp').attr('id', `panel${idpanel}`).attr('panel-index', localPanel.panelIndex);
        } else {
            panelDiv = $('<div>').append(panelLayout).addClass('panel temp').attr('id', `panel${idpanel}`).attr('panel-index', localPanel.panelIndex);
        }
        newItem.firstChild.appendChild(panelDiv[0]);

        $('#panel' + idpanel).on('mouseenter', function () {
            $('#panel' + idpanel + ' .panel-icons').addClass('active');
        });
        $('#panel' + idpanel).on('mouseleave', function () {
            $('#panel' + idpanel + ' .panel-icons').removeClass('active');
            $('#panel' + idpanel + ' .dropdown-style').addClass('hidden');
        });
        $('#panel' + idpanel + ' .dropdown-btn').click(function (e) {
            e.stopPropagation();
            $('#panel' + idpanel + ' .dropdown-btn').toggleClass('active');
            $('#panel' + idpanel + ' .dropdown-style').toggleClass('hidden');
        });

        $(`.grid-stack-item .grid-stack-item-content #panel${idpanel} .panel-header p`).html(localPanel.name);

        if (localPanel.description || (localPanel.queryData && localPanel.queryData.searchText)) {
            handleDescriptionTooltip(idpanel, localPanel.description, localPanel.queryData ? localPanel.queryData.searchText : '');
        } else {
            $(`#panel${idpanel} .panel-info-corner`).hide();
        }
        let val = localPanel.gridpos.y + localPanel.gridpos.h;
        if (val > panelContainerMinHeight) panelContainerMinHeight = val;

        handlePanelRemove(idpanel);

        if (localPanel.queryType === 'logs') {
            // Handle all chart types for logs
            if (localPanel.chartType === 'Data Table' || localPanel.chartType === 'loglines') {
                let panEl = $(`#panel${idpanel} .panel-body`);
                let responseDiv = `<div id="panelLogResultsGrid" class="panelLogResultsGrid ag-theme-mycustomtheme"></div>
                <div id="empty-response"></div></div><div id="corner-popup"></div>
                <div id="panel-loading"></div>`;
                panEl.append(responseDiv);

                $('#panelLogResultsGrid').show();
                if (!isFilterApplied && localPanel.queryRes) {
                    runPanelLogsQuery(localPanel.queryData, idpanel, localPanel, localPanel.queryRes);
                } else {
                    runPanelLogsQuery(localPanel.queryData, idpanel, localPanel);
                }
            } else if (localPanel.chartType === 'Bar Chart' || localPanel.chartType === 'Pie Chart' || localPanel.chartType === 'Line Chart' || localPanel.chartType === 'number') {
                let panEl = $(`#panel${idpanel} .panel-body`);
                let responseDiv = ``;
                if (localPanel.chartType === 'number') {
                    responseDiv = `<div class="big-number-display-container"></div>
                    <div id="empty-response"></div><div id="corner-popup"></div>
                    <div id="panel-loading"></div>`;
                    panEl.append(responseDiv);

                    $('.big-number-display-container').show();
                } else {
                    responseDiv = `<div id="empty-response"></div><div id="corner-popup"></div>
                    <div id="panel-loading"></div>`;
                    panEl.append(responseDiv);
                }

                if (!isFilterApplied && localPanel.queryRes) {
                    runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
                } else {
                    runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
                }
            } else {
                allResultsDisplayed--;
            }
        } else if (localPanel.queryType === 'metrics') {
            if (localPanel.chartType === 'Line Chart') {
                let panEl = $(`#panel${idpanel} .panel-body`);
                let responseDiv = `<div id="empty-response"></div></div><div id="corner-popup"></div>
                <div id="panel-loading"></div>`;
                panEl.append(responseDiv);
                if (localPanel.queryRes) {
                    runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel, localPanel.queryRes);
                } else {
                    delete localPanel.queryData.startEpoch;
                    delete localPanel.queryData.endEpoch;
                    await runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel);
                }
            } else {
                allResultsDisplayed--;
            }
        } else {
            allResultsDisplayed--;
        }
        handlePanelEdit();
        handlePanelView();
        handlePanelRemove(idpanel);
        handlePanelDuplicate();
    }
    if (allResultsDisplayed === 0) {
        $('body').css('cursor', 'default');
    }
    // Remove the Add Panel Widgets from the default dashboards
    if (!isDefaultDashboard) {
        $('.default-item').remove();
        addDefaultPanel();
    }
    // Show the Add Panel Widget if there is no panel
    if (localPanels.length === 0) {
        $('.default-item').addClass('active').find('.text').text('Select the panel type');
        $('#add-widget-options').show();
        $('.add-icon').addClass('rotate-icon');
        $('#add-panel-btn').addClass('active');
        $('.add-panel-div .plus-icon').hide();
    } else {
        $('.default-item').removeClass('active');
        $('#add-widget-options').hide();
        $('.add-icon').removeClass('rotate-icon');
        $('#add-panel-btn').removeClass('active');
        $('.add-panel-div .text').text('Add Panel');
        $('.add-panel-div .plus-icon').show();
    }
}

function getDashboardId() {
    const urlParams = new URLSearchParams(window.location.search);
    const dashboardId = urlParams.get('id');

    return dashboardId;
}

var panelLayout =
    '<div class="panel-header">' +
    '<div>' +
    '<p>Panel Title</p>' +
    '</div>' +
    '<div class="panel-icons">' +
    '<div><img src="../assets/edit-panel-icon.svg" alt="" class="panel-edit-li" /></div>' +
    '<div><img src="../assets/resize-icon.svg" alt="" class="panel-view-li" /></div>' +
    '<div>' +
    '<span class="dropdown-btn" id="panel-options-btn"></span>' +
    '<ul class="dropdown-style hidden" id="panel-dropdown-modal">' +
    '<li data-value="edit" class="panel-edit-li"><span class="edit"></span>Edit</li>' +
    '<li data-value="duplicate" class="panel-dupl-li"><span class="duplicate"></span>Clone</li>' +
    '<li data-value="remove" class="panel-remove-li"><span class="remove"></span>Remove</li>' +
    '</ul>' +
    '</div>' +
    '</div>' +
    '</div>' +
    `<div class="panel-body">
    <div class="panEdit-panel"></div>
    </div>
    <div class="panel-info-corner"><i class="fa fa-info" aria-hidden="true" id="panel-desc-info"></i></div>
`;

var defaultPanelLayout =
    '<div class="panel-header">' +
    '<div>' +
    '<p>Panel Title</p>' +
    '</div>' +
    '<div class="panel-icons">' +
    '<div><img src="../assets/resize-icon.svg" alt="" class="panel-view-li" /></div>' +
    '</div>' +
    '</div>' +
    `<div class="panel-body">
    <div class="panEdit-panel"></div>
    </div>
    <div class="panel-info-corner"><i class="fa fa-info" aria-hidden="true" id="panel-desc-info"></i></div>
`;

function addPanel(chartIndex) {
    flagDBSaved = false;
    panelIndex = localPanels.length;
    var defaultWidget = $('.default-item').get(0); // Get the DOM element
    // Remove the default widget from the grid
    grid.removeWidget(defaultWidget);
    let idpanel = uuidv4();
    let panel = $('<div>').append(panelLayout).addClass('panel temp').attr('id', `panel${idpanel}`).attr('panel-index', panelIndex);
    $('#panel-container').append(panel);
    $(`#panel${idpanel} .panel-header p`).html(`panel${panelIndex}`);
    $('#panel' + idpanel).on('mouseenter', function () {
        $('#panel' + idpanel + ' .panel-icons').addClass('active');
    });
    $('#panel' + idpanel).on('mouseleave', function () {
        $('#panel' + idpanel + ' .panel-icons').removeClass('active');
        $('#panel' + idpanel + ' .dropdown-style').addClass('hidden');
    });
    $('#panel' + idpanel + ' .dropdown-btn').click(function (e) {
        e.stopPropagation();
        $('#panel' + idpanel + ' .dropdown-btn').toggleClass('active');
        $('#panel' + idpanel + ' .dropdown-style').toggleClass('hidden');
    });
    $(`#panel${idpanel} .panel-info-corner`).hide();
    var newItem = grid.addWidget(`<div class="grid-stack-item" id="${idpanel}"><div class="grid-stack-item-content"></div></div>`, { width: 4, height: 2 });

    // Insert panel content into grid-stack-item-content
    newItem.firstChild.appendChild(panel[0]);

    let panelTop = newItem.getAttribute('data-gs-x');
    let panelLeft = newItem.getAttribute('data-gs-y');
    let panelWidth = newItem.getAttribute('data-gs-width');
    let panelHeight = newItem.getAttribute('data-gs-height');
    let chartType = '';
    let queryType = '';
    let queryData = {};
    let logLinesViewType = '';
    let unit = '';

    switch (chartIndex) {
        case 0: // Bar chart
            chartType = 'Bar Chart';
            queryType = 'logs';
            queryData = {
                state: 'query',
                searchText: '',
                startEpoch: filterStartDate,
                endEpoch: filterEndDate,
                indexName: selectedSearchIndex,
                from: 0,
                queryLanguage: 'Splunk QL',
            };
            break;
        case 1: // Line chart
            chartType = 'Line Chart';
            queryType = 'metrics';
            queryData = {
                formulasData: [],
                queriesData: [
                    {
                        end: 'now',
                        formulas: [
                            {
                                formula: 'a',
                            },
                        ],
                        queries: [
                            {
                                name: 'a',
                                qlType: 'promql',
                                query: '((testmetric0))',
                            },
                        ],
                        start: 'now-1h',
                    },
                ],
                start: 'now-1h',
            };
            break;
    }

    localPanels.push({
        name: `panel${panelIndex}`,
        panelIndex: panelIndex,
        panelId: idpanel,
        description: '',
        chartType: chartType,
        dataType: '',
        gridpos: {
            h: panelHeight,
            w: panelWidth,
            x: panelLeft,
            y: panelTop,
        },
        queryType: queryType,
        queryData: queryData,
        logLinesViewType: logLinesViewType,
        unit: unit,
    });

    editPanelInit(panelIndex, true);
    $('.panelEditor-container').css('display', 'flex');
    $('.popupOverlay').addClass('active');
    handlePanelEdit();
    handlePanelRemove(idpanel);
    handlePanelDuplicate();
}

function addDuplicatePanel(panelToDuplicate) {
    flagDBSaved = false;
    panelIndex = localPanels.length;

    var defaultWidget = $('.default-item').get(0);
    // Remove the default widget from the grid
    grid.removeWidget(defaultWidget);

    let idpanel = uuidv4();
    let panel = $('<div>').append(panelLayout).addClass('panel temp').attr('id', `panel${idpanel}`).attr('panel-index', panelIndex);
    $('#panel-container').append(panel);
    $(`#panel${idpanel} .panel-header p`).html(`panel${panelIndex}`);
    $('#panel' + idpanel).on('mouseenter', function () {
        $('#panel' + idpanel + ' .panel-icons').addClass('active');
    });
    $('#panel' + idpanel).on('mouseleave', function () {
        $('#panel' + idpanel + ' .panel-icons').removeClass('active');
        $('#panel' + idpanel + ' .dropdown-style').addClass('hidden');
    });
    $('#panel' + idpanel + ' .dropdown-btn').click(function (e) {
        e.stopPropagation();
        $('#panel' + idpanel + ' .dropdown-btn').toggleClass('active');
        $('#panel' + idpanel + ' .dropdown-style').toggleClass('hidden');
    });
    $(`#panel${idpanel} .panel-info-corner`).hide();
    var newItem = grid.addWidget(`<div class="grid-stack-item" id="${idpanel}"><div class="grid-stack-item-content"></div></div>`, { width: panelToDuplicate.gridpos.w, height: panelToDuplicate.gridpos.h });

    // Insert panel content into grid-stack-item-content
    newItem.firstChild.appendChild(panel[0]);

    let panelTop = newItem.getAttribute('data-gs-x');
    let panelLeft = newItem.getAttribute('data-gs-y');
    let panelWidth = newItem.getAttribute('data-gs-width');
    let panelHeight = newItem.getAttribute('data-gs-height');

    panelToDuplicate.panelId = idpanel;
    panelToDuplicate.name += 'Copy';
    panelToDuplicate.panelIndex = panelIndex;
    panelToDuplicate.gridpos.x = panelLeft;
    panelToDuplicate.gridpos.y = panelTop;
    panelToDuplicate.gridpos.h = panelHeight;
    panelToDuplicate.gridpos.w = panelWidth;
    if (panelToDuplicate.description) {
        handleDescriptionTooltip(panelToDuplicate.panelId, panelToDuplicate.description);
    }
    localPanels.push(JSON.parse(JSON.stringify(panelToDuplicate)));
    handlePanelView();
    handlePanelEdit();
    handlePanelRemove(idpanel);
    handlePanelDuplicate();
    $(`#panel${idpanel}`).get(0).scrollIntoView({ behavior: 'smooth' });

    // Remove the Add Panel Widgets from the default dashboards
    if (!isDefaultDashboard) {
        addDefaultPanel();
    }
}

function addDefaultPanel() {
    grid.addWidget(
        `<div class="grid-stack-item default-item"><div class="add-panel-div">
    <div class="plus-icon">+</div>
    <div class="text">Add Panel</div>
    </div></div>`,
        { width: 4, height: 2, noResize: true, noMove: true }
    );
}

// DASHBOARD SETTINGS PAGE

let aceEditor;

function handleDbSettings() {
   
    $('#new-dashboard').hide();
    $('.dbSet-container').show();

    const currentUrl = new URL(window.location.href);
    currentUrl.searchParams.set('mode', 'settings');
    window.history.pushState({}, '', currentUrl);

    const breadcrumb = new Breadcrumb();
    breadcrumb.render(dbData.folder?.breadcrumbs, dbData.name, false, dbData.isFavorite, true, false);

    $('.dbSet-dbName').val(dbName);
    $('.dbSet-dbDescr').val(dbDescr);
    initializeFolderDropdown();

    if (isDefaultDashboard) {
        $('.dbSet-dbName').prop('readonly', true);
        $('.dbSet-dbDescr').prop('readonly', true);
        $('#dbSet-edit-json').hide();
        $('#dbSet-discard,#dbSet-save').hide();
    }

    //get dashboard data from database
    $.ajax({
        method: 'get',
        url: 'api/dashboards/' + dbId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        $('.dbSet-dbName').val(res.name);
        $('.dbSet-dbDescr').val(res.description);
        initAceEditor(JSON.unflatten(res));
    });

    showGeneralDbSettings();
    addDbSettingsEventListeners();
}

function showGeneralDbSettings() {
    $('.dbSet-general').addClass('active');
    $('.dbSet-generalHTML').removeClass('hide');

    $('.dbSet-jsonModel').removeClass('active');
    $('.dbSet-jsonModelHTML').addClass('hide');
}

function showJsonModelDbSettings() {
    $('.dbSet-general').removeClass('active');
    $('.dbSet-generalHTML').addClass('hide');

    $('.dbSet-jsonModel').addClass('active');
    $('.dbSet-jsonModelHTML').removeClass('hide');
}

function addDbSettingsEventListeners() {
    $('.dbSet-general').on('click', showGeneralDbSettings);
    $('.dbSet-jsonModel').on('click', showJsonModelDbSettings);
}

function saveDbSetting() {
    if ($('.dbSet-generalHTML').is(':visible')) {
        let trimmedDbName = $('.dbSet-dbName').val().trim();
        let trimmedDbDescription = $('.dbSet-dbDescr').val().trim();

        if (!trimmedDbName) {
            $('.error-tip').addClass('active');
            $('.popupOverlay, .popupContent').addClass('active');
            $('#error-message').text('Dashboard name cannot be empty.');
            return;
        }

        dbName = trimmedDbName;
        dbDescr = trimmedDbDescription;
    }

    if ($('.dbSet-jsonModelHTML').is(':visible')) {
        const jsonText = aceEditor.getValue().trim();
        let dbSettings;
        try {
            dbSettings = JSON.parse(jsonText);
        } catch (e) {
            console.error(e);
            alert('Invalid JSON format. Please correct the JSON and try again.');
            return;
        }

        dbName = dbSettings.name || dbName;
        dbDescr = dbSettings.description || dbDescr;
        timeRange = dbSettings?.timeRange || timeRange;
        localPanels = dbSettings?.panels || localPanels;
        dbRefresh = dbSettings?.refresh || dbRefresh;
        isFavorite = dbSettings?.isFavorite !== undefined ? dbSettings.isFavorite : isFavorite;
    }

    updateDashboard().then((updateSuccessful) => {
        if (updateSuccessful) {
            const currentUrl = new URL(window.location.href);
            currentUrl.searchParams.delete('mode');
            window.history.pushState({}, '', currentUrl);

            $('#new-dashboard').show();
            $('.dbSet-container').hide();
            // Refresh the dashboard data to reflect changes immediately
            getDashboardData();
        }
    });
}

$('#error-ok-btn').click(function () {
    $('.popupOverlay, .popupContent').removeClass('active');
    $('.error-tip').removeClass('active');
});

function initializeFolderDropdown() {
    new FolderDropdown('folder-dropdown-container', {
        showRoot: true,
        initialFolder: dbData.folder
            ? {
                  id: dbData.folder.id,
                  name: dbData.folder.id === 'root-folder' ? 'Dashboards' : dbData.folder.name,
              }
            : null,
        onSelect: (selectedFolder) => {
            dbData.folder = selectedFolder;
        },
    });
}

function discardDbSetting() {
    const currentUrl = new URL(window.location.href);
    currentUrl.searchParams.delete('mode');
    window.history.pushState({}, '', currentUrl);

    const breadcrumb = new Breadcrumb();
    breadcrumb.render(dbData.folder?.breadcrumbs, dbData.name, false, dbData.isFavorite, false, false);


    $('#new-dashboard').show();
    displayPanels();

    $('.dbSet-dbName').val('');
    $('.dbSet-dbDescr').val('');
    $('.dbSet-jsonModelData').val('');
    $('.dbSet-container').hide();
    dbName = dbData.name;
    dbDescr = dbData.description;
}

function initAceEditor(jsonData) {
    if (!aceEditor) {
        //eslint-disable-next-line no-undef
        aceEditor = ace.edit('json-editor');
        aceEditor.session.setMode('ace/mode/json');
        aceEditor.setOptions({
            fontSize: '12px',
            showPrintMargin: false,
            showGutter: true,
            highlightActiveLine: false,
            wrap: true,
        });
    }

    aceEditor.setValue(JSON.stringify(jsonData, null, 2), -1);

    $('#copy-json-btn')
        .off('click')
        .on('click', function () {
            const jsonText = aceEditor.getValue();
            navigator.clipboard.writeText(jsonText).then(() => {
                $(this).text('Copied!');
                setTimeout(() => {
                    $(this).text('Copy JSON');
                }, 1000);
            });
        });
}

// Refresh handler
function setRefreshItemHandler() {
    $('.refresh-range-item').removeClass('active');
    if (dbRefresh) {
        $(`.refresh-range-item:contains('${dbRefresh}')`).addClass('active');
        $('.refresh-container #refresh-picker-btn span').text(dbRefresh);
        startRefreshInterval(dbRefresh);
    } else {
        $('.refresh-container #refresh-picker-btn span').text('');
        $(`.refresh-range-item:contains('Off')`).addClass('active');
    }
}

function refreshRangeItemHandler(evt) {
    $.each($('.refresh-range-item.active'), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    let refreshInterval = $(evt.currentTarget).attr('id');
    if (refreshInterval === '0') {
        dbRefresh = '';
        $('.refresh-container #refresh-picker-btn span').html('');
    } else {
        dbRefresh = refreshInterval;
        $('.refresh-container #refresh-picker-btn span').html(refreshInterval);
    }
    startRefreshInterval(refreshInterval);
}

let intervalId;

function startRefreshInterval(refreshInterval) {
    let parsedRefreshInterval = parseInterval(refreshInterval);
    clearInterval(intervalId);
    if (parsedRefreshInterval > 0) {
        intervalId = setInterval(function () {
            refreshDashboardHandler();
        }, parsedRefreshInterval);
    } else {
        pauseRefreshInterval();
    }
}

function pauseRefreshInterval() {
    clearInterval(intervalId);
    return 0;
}

function parseInterval(interval) {
    if (interval === '0') {
        pauseRefreshInterval();
        return;
    }
    const regex = /(\d+)([smhd])/;
    const match = interval.match(regex);
    const value = parseInt(match[1]);
    const unit = match[2];

    switch (unit) {
        case 'm':
            return value * 60 * 1000;
        case 'h':
            return value * 60 * 60 * 1000;
        case 'd':
            return value * 24 * 60 * 60 * 1000;
        default:
            throw new Error('Invalid interval unit');
    }
}

// Favorite Handler
function toggleFavorite() {
    $.ajax({
        method: 'put',
        url: 'api/dashboards/favorite/' + dbId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
    }).then((response) => {
        isFavorite = response.isFavorite;
        setFavoriteValue(response.isFavorite);
    });
}

function setFavoriteValue(favValue) {
    if (favValue) {
        $('#favbutton').addClass('active');
    } else {
        $('#favbutton').removeClass('active');
    }
}

// Resizing handler
$(window).on('resize', function () {
    setTimeout(resizeCharts, 100);
    var windowWidth = window.innerWidth;

    // If window width is less than a certain threshold, disable resizing and dragging
    if (windowWidth < 978) {
        // Adjust the threshold as needed
        grid.movable('.grid-stack-item', false);
        grid.resizable('.grid-stack-item', false);
    } else {
        // Enable resizing and dragging
        grid.movable('.grid-stack-item', true);
        grid.resizable('.grid-stack-item', true);
    }
});

function resizeCharts() {
    $('.grid-stack-item-content .panEdit-panel').each(function () {
        var echartsInstanceId = $(this).attr('_echarts_instance_');
        if (echartsInstanceId) {
            var echartsInstance = echarts.getInstanceById(echartsInstanceId);
            if (echartsInstance) {
                echartsInstance.resize();
            }
        }
    });
}

//eslint-disable-next-line no-unused-vars
function setDashboardQueryModeHandler(panelQueryMode) {
    // Determine which mode to use: panel > queryMode > default (Builder)
    const mode = panelQueryMode || Cookies.get('queryMode') || 'Builder';

    // Trigger appropriate tab
    const tabSelector = mode === 'Builder' ? '.custom-code-tab a:first' : '.custom-code-tab a[href="#tabs-2"]';

    $(tabSelector).trigger('click');
    updateQueryModeUI(mode);
}

// Search across the panels based on the search input
$('#run-dashboard-fliter').on('click', function () {
    const filterValue = $('.search-db-input').val();
    if (!validateFilterInput(filterValue)) {
        if (!searchTippy) {
            searchTippy = tippy(this, {
                content: 'Invalid filter input. Please enter a valid filter search.',
                trigger: 'manual',
                placement: 'top',
                theme: 'error',
            });
        } else {
            searchTippy.setContent('Invalid filter input. Please enter a valid filter search.');
        }

        searchTippy.show();

        setTimeout(() => {
            searchTippy.hide();
        }, 3000);
    } else {
        if (searchTippy) {
            searchTippy.hide();
        }
        applyDashboardFilter(filterValue);
    }
});

function validateFilterInput(input) {
    input = input.trim();

    const disallowedPatterns = [
        /\|/, // Pipe character
        /\bBY\b/i, // BY keyword
        /\bstats\b/i, // stats keyword
        /\bregex\b/i, // regex keyword
        /\bfields\b/i, // fields keyword
        /\s(AND|OR)$/i, // AND or OR at the end of the input
    ];

    for (let pattern of disallowedPatterns) {
        if (pattern.test(input)) {
            return false;
        }
    }
    return true;
}

function applyDashboardFilter(filterValue) {
    if (!filterValue.trim()) {
        // Empty string
        resetToOriginalQueries();
        displayPanels();
        return;
    }

    isFilterApplied = true;
    localPanels.forEach((panel, index) => {
        if (panel.queryType === 'logs' && panel.queryData.queryLanguage === 'Splunk QL') {
            updatePanelQuery(panel, filterValue, index);
            delete panel.queryRes;
        }
    });
    displayPanels();
    isFilterApplied = false;
}

function updatePanelQuery(panel, filterInput, panelIndex) {
    let originalQuery = panel.queryData.searchText;
    let newQuery;

    if (originalQuery === '' || originalQuery === '*') {
        newQuery = filterInput;
    } else if (originalQuery.startsWith('* |')) {
        newQuery = filterInput + originalQuery.substring(2);
    } else if (originalQuery.includes('|')) {
        // If the query already has a pipe, add the filter before the first pipe
        let parts = originalQuery.split('|');
        if (parts[0].trim() === '') {
            newQuery = `${filterInput} | ${parts.slice(1).join('|')}`.trim();
        } else {
            newQuery = `${filterInput} AND ${parts[0].trim()} | ${parts.slice(1).join('|')}`.trim();
        }
    } else {
        newQuery = `${filterInput} AND ${originalQuery}`.trim();
    }
    panel.queryData.searchText = newQuery;
    localPanels[panelIndex] = panel;
}

function resetToOriginalQueries() {
    localPanels.forEach((panel) => {
        if (originalQueries[panel.panelId]) {
            panel.queryData.searchText = originalQueries[panel.panelId];
            delete panel.queryRes;
        }
    });
    originalQueries = {};
}

function handleDashboardError(status, statusText) {
    const mainContent = document.querySelector('.dashboard-container');
    if (mainContent) {
        mainContent.style.display = 'none';
    }

    const breadcrumb = new Breadcrumb();
    const dashboardName = status === 400 ? 'Dashboard Not Found' : status === 404 ? 'Dashboard Not Found' : 'Error Loading Dashboard';

    breadcrumb.render([{ id: 'root-folder', name: 'Root' }], dashboardName);
    showDashboardErrorState(status, statusText);
}

function showDashboardErrorState(status, statusText) {
    const errorHTML = `
        <div class="dashboard-error-container">
            <div class="error-card">
                <h2 class="error-title fw-bold">Dashboard Not Found</h2>
                <p>
                    ${status === 400 ? "The dashboard you're looking for doesn't exist or may have been deleted." : status === 404 ? 'The requested dashboard could not be found.' : 'There was an error loading the dashboard.'}
                </p>
                
                <div class="error-details">
                    <strong>Error:</strong> ${status} ${statusText}
                    <br>
                    <small>Dashboard ID: ${dbId}</small>
                </div>
                
                <div class="error-actions d-flex flex-column gap-3">
                    <button class="btn btn-primary" onclick="retryLoadDashboard()">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M3 12a9 9 0 0 1 9-9 9.75 9.75 0 0 1 6.74 2.74L21 8"/>
                            <path d="M21 3v5h-5"/>
                            <path d="M21 12a9 9 0 0 1-9 9 9.75 9.75 0 0 1-6.74-2.74L3 16"/>
                            <path d="M3 21v-5h5"/>
                        </svg>
                        Retry
                    </button>
                    <button class="btn btn-secondary" onclick="goToDashboards()">
                        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="m3 9 9-7 9 7v11a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/>
                            <polyline points="9,22 9,12 15,12 15,22"/>
                        </svg>
                        Go to Dashboards
                    </button>
                </div>
            </div>
        </div>
    `;

    const container = document.querySelector('#new-dashboard') || document.body;
    const errorDiv = document.createElement('div');
    errorDiv.innerHTML = errorHTML;
    container.appendChild(errorDiv);
}

//eslint-disable-next-line no-unused-vars
function retryLoadDashboard() {
    const errorContainer = document.querySelector('.dashboard-error-container');
    if (errorContainer) {
        errorContainer.remove();
    }

    getDashboardData();
}

//eslint-disable-next-line no-unused-vars
function goToDashboards() {
    window.location.href = '/dashboards-home.html';
}

function updateDashboardTheme() {
    const { gridLineColor, tickColor } = getGraphGridColors();

    // Update Chart.js charts (Line and Bar Chart)
    function updateChartJS(selector) {
        $(selector).each(function () {
            const chart = Chart.getChart(this);
            if (chart?.options?.scales) {
                const axes = ['x', 'y', 'y1'];

                axes.forEach((axis) => {
                    if (chart.options.scales[axis]) {
                        chart.options.scales[axis].ticks.color = tickColor;

                        if (!chart.options.scales[axis].grid) {
                            chart.options.scales[axis].grid = {};
                        }
                        chart.options.scales[axis].grid.color = gridLineColor;
                    }
                });

                if (chart.options.plugins?.legend?.labels) {
                    chart.options.plugins.legend.labels.color = tickColor;
                }

                chart.update();
            }
        });
    }

    updateChartJS('.bar-chart-canvas');
    updateChartJS('.metrics-canvas');

    // Update ECharts instances (Pie charts)
    $('.panEdit-panel').each(function () {
        const instanceId = $(this).attr('_echarts_instance_');
        if (instanceId) {
            const instance = echarts.getInstanceById(instanceId);
            if (instance) {
                const option = instance.getOption();

                // Update legend and label colors
                if (option.legend?.[0]) {
                    option.legend[0].textStyle = { ...option.legend[0].textStyle, color: tickColor };
                }
                if (option.series?.[0]?.label) {
                    option.series[0].label = { ...option.series[0].label, color: tickColor };
                }

                instance.setOption(option);
            }
        }
    });
}
