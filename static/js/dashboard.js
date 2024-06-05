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

let localPanels = [], dbData, dbName, dbDescr, dbId, panelIndex, flagDBSaved = true, allResultsDisplayed = 0;
let timeRange = "Last 1 Hr";
let dbRefresh ="";
let panelContainer;
let panelContainerWidthGlobal;
let originalIndexValues = [];
let indexValues = [];
$(document).ready(async function () {
    let indexes = await getListIndices();
    if (indexes){
        originalIndexValues = indexes.map(item => item.index);
        indexValues = [...originalIndexValues];
    }
    initializeIndexAutocomplete();
    
    $('#new-dashboard').css("transform", "translate(170px)")
    $('#new-dashboard').css("width", "calc(100% - 170px)")

    $('.panelEditor-container').hide();
    $('.dbSet-container').hide();

    $('.theme-btn').on('click', themePickerHandler);
    setupEventHandlers();
    dbId = getDashboardId();

    $("#add-panel-btn, .close-widget-popup").click(() => {
      
        $('#add-widget-options').toggle();
        $('.add-icon').toggleClass('rotate-icon');
        $('#add-panel-btn').toggleClass('active'); 
        $('.plus-icon').toggle();
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
    $(document).on('click', '.default-item', function() {
        if ($(this).hasClass('active')) {
            return;
        } else {
            $(this).addClass('active');
            $('#add-widget-options').toggle();
            $('.add-icon').toggleClass('rotate-icon');
            $('#add-panel-btn').toggleClass('active'); 
            $(this).find('.text').text('Select the panel type');
            $('.plus-icon').hide();
        }
    });

    // // Event handler to remove active class when clicking outside
    $('#new-dashboard').on('click', function(event) {
        if (
            !$(event.target).closest('.default-item').length &&
            !$(event.target).closest('#add-widget-options').length &&
            !$(event.target).closest('#add-panel-btn').length &&
            !$(event.target).closest('.grid-stack-item').length &&
            !$(event.target).closest('.panel-view-li').length 
        ) {
            $('.default-item').removeClass('active');
            $('.add-panel-div .text').text('Add Panel');
            $('.plus-icon').show();
            $('#add-widget-options').hide();
            $('.add-icon').removeClass('rotate-icon');
            $('#add-panel-btn').removeClass('active'); 
        }
    });

    
    $(".all-dashboards").click(function () {
        window.location.href = "../dashboards-home.html";
    })

    $("#theme-btn").click(() => displayPanels());
    getDashboardData();

    setTimePicker();

    $(`.dbSet-textareaContainer .copy`).tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'hover'
    });
    $('#favbutton').on('click',toggleFavorite);
})

// Initialize Gridstack
var options = {
    resizable: {
        handles: 'e, se, s, sw, w'
    },
    draggable: {
        handle: '.grid-stack-item-content'
    },
    animate: false 
};
var grid = GridStack.init(options, '#panel-container');

$(`.dbSet-textareaContainer .copy`).click(function() {
    $(this).tooltip('dispose');
    $(this).attr('title', 'Copied!').tooltip('show');
    navigator.clipboard.writeText($(`.dbSet-jsonModelData`).val())
        .then(() => {
            setTimeout(() => {
                $(this).tooltip('dispose');
                $(this).attr('title', 'Copy').tooltip({
                    delay: { show: 0, hide: 300 },
                    trigger: 'hover',
                  });
            }, 1000);
        })
});

$('#save-db-btn').on("click", updateDashboard);
$('.refresh-btn').on("click", refreshDashboardHandler);
$('.settings-btn').on('click', handleDbSettings);
$('#dbSet-save').on('click', saveDbSetting);
$('#dbSet-discard').on('click', discardDbSetting);
$('.dbSet-goToDB').on('click', discardDbSetting);
$('.refresh-range-item').on('click', refreshRangeItemHandler);


async function updateDashboard() {
    timeRange = $('#date-picker-btn').text().trim().replace(/\s+/g, ' ');
    resetPanelTimeRanges();
    flagDBSaved = true;
    let tempPanels = JSON.parse(JSON.stringify(localPanels));
    for (let i = 0; i < tempPanels.length; i++)
        delete tempPanels[i].queryRes;
    return fetch('/api/dashboards/update',
        {
            method: 'POST',
            body: JSON.stringify({
                "id": dbId,
                "name": dbName,
                "details": {
                    "name": dbName,
                    "description": dbDescr,
                    "timeRange": timeRange,
                    "panels": tempPanels,
                    "refresh": dbRefresh,
                },
            })
        }
    )
        .then(res => {
            if (res.status === 409) {
                showToast('Dashboard name already exists');
                throw new Error('Dashboard name already exists');
            }    
            if (res.status == 200) {
                $(".name-dashboard").text(dbName);
                showToast('Dashboard Updated Successfully');
                return true;
            } else {
                showToast('Error saving dashboard');
                throw new Error('Error saving dashboard');
            }
        })
        .catch(error => {
            console.error(error);
            return false;
        });
}

function refreshDashboardHandler() {
    if ($('#viewPanel-container').css('display') !== 'none') {
        displayPanelView(panelIndex);
    }
    else {
        for (let i = 0; i < localPanels.length; i++) {
            localPanels[i].queryRes = undefined;
        }
        displayPanels();
    }
}

function handlePanelView() {
    $(".panel-view-li").unbind("click");
    $(".panel-view-li").on("click", function () {
        panelIndex = $(this).closest(".panel").attr("panel-index");
        pauseRefreshInterval();
        viewPanelInit();
        displayPanelView(panelIndex);
    })
}

function viewPanelInit() {
    $('.panelEditor-container').css('display', 'flex');
    $('.popupOverlay').addClass('active');
    $('.panelDisplay #panelLogResultsGrid').empty();
    $('.panelDisplay .big-number-display-container').hide();
    $('.panelDisplay #empty-response').hide();
    editPanelInit(-1);
    setTimePicker();
}

function handlePanelEdit() {
    $(".panel-edit-li").unbind("click");
    $(".panel-edit-li").on("click", function () {
        panelIndex = $(this).closest(".panel").attr("panel-index");
        editPanelInit();
        $('.panelEditor-container').css('display', 'flex');
        $('.popupOverlay').addClass('active');
        $('.panelDisplay #panelLogResultsGrid').empty();
        $('.panelDisplay .big-number-display-container').hide();
        $('.panelDisplay #empty-response').hide();
    })
}
function handlePanelRemove(panelId) {
    $(`#panel${panelId} .panel-remove-li`).unbind("click");
    $(`#panel${panelId} .panel-remove-li`).on("click", function () {
        showPrompt(panelId);
    });

    function showPrompt(panelId) {
        $('.popupOverlay, .popupContent').addClass('active');
        $('#delete-btn-panel').on("click", function () {
            deletePanel(panelId);
            $('.popupOverlay, .popupContent').removeClass('active');
        });
        $('#cancel-btn-panel').on("click", function () {
            $('.popupOverlay, .popupContent').removeClass('active');
        });
    }

    async function deletePanel(panelId) {
        flagDBSaved = false;
    
        // Remove the panel element
        const panel = $(`#panel${panelId}`);

        // Remove the panel data from the localPanels array
        let panelIndex = panel.attr("panel-index");
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

function handleDescriptionTooltip(panelId,description,searchText) {
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
        trigger: 'hover'});
    panelInfoCorner.hover(function () {panelDescIcon.tooltip('show');},
    function () {panelDescIcon.tooltip('hide');});
}

function handlePanelDuplicate() {
    $(".panel-dupl-li").unbind("click");
    $(".panel-dupl-li").on("click", async function () {
        flagDBSaved = false;
        let duplicatedPanelIndex = $(this).closest(".panel").attr("panel-index");
        addDuplicatePanel(JSON.parse(JSON.stringify(localPanels[duplicatedPanelIndex])));
        renderDuplicatePanel(duplicatedPanelIndex);
        await updateDashboard();
    })
}

function renderDuplicatePanel(duplicatedPanelIndex) {
    let localPanel = localPanels[localPanels.length - 1];
    let panelId = localPanels[localPanels.length - 1].panelId;
    // only render the duplicated panel
    $(`#panel${localPanels[localPanels.length - 1].panelId} .panel-header p`).html(localPanels[duplicatedPanelIndex].name + "Copy");

    if (localPanel.description||localPanel.queryData.searchText) {
        handleDescriptionTooltip(panelId,localPanel.description,localPanel.queryData.searchText)
    }
    
    if (localPanel.chartType == 'Data Table' || localPanel.chartType == 'loglines') {
        let panEl = $(`#panel${panelId} .panel-body`)
        let responseDiv = `<div id="panelLogResultsGrid" class="panelLogResultsGrid ag-theme-mycustomtheme"></div>
        <div id="empty-response"></div>`
        panEl.append(responseDiv)
        $("#panelLogResultsGrid").show();

        if (localPanel.queryRes)
            runPanelLogsQuery(localPanel.queryData, panelId,localPanel, localPanel.queryRes);
        else
            runPanelLogsQuery(localPanel.queryData, panelId,localPanel);
    } else if (localPanel.chartType == 'Line Chart') {
        let panEl = $(`#panel${panelId} .panel-body`)
        let responseDiv = `<div id="empty-response"></div></div><div id="corner-popup"></div>`
        panEl.append(responseDiv)
        if (localPanel.queryRes)
            runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel, localPanel.queryRes)
        else
            runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel)
    } else if (localPanel.chartType == 'number') {
        let panEl = $(`#panel${panelId} .panel-body`)
        let responseDiv = `<div class="big-number-display-container"></div>
        <div id="empty-response"></div><div id="corner-popup"></div>`
        panEl.append(responseDiv)
        if(localPanel.queryType ==='metrics') {
            if (localPanel.queryRes)
                runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel, localPanel.queryRes)
            else
                runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel)
        }else{
            if (localPanel.queryRes)
                runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
            else
                runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
        }
        } else if (localPanel.chartType == 'Pie Chart' || localPanel.chartType == 'Bar Chart') {
        // generic for both bar and pie chartTypes.
        let panEl = $(`#panel${panelId} .panel-body`)
        let responseDiv = `<div id="empty-response"></div><div id="corner-popup"></div>`
        panEl.append(responseDiv)
        if (localPanel.queryRes)
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
        else
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
    }
}

function resetPanelIndices() {
    for (let i = 0; i < localPanels.length; i++) {
        localPanels[i].panelIndex = i;
    }
}

async function getDashboardData() {
    await fetch(`/api/dashboards/${dbId}`)
        .then(res => {
            return res.json();
        })
        .then(data => {
            dbData = data;
        })
    $(".name-dashboard").text(dbData.name);
    dbName = dbData.name;
    dbDescr = dbData.description;
    dbRefresh = dbData.refresh;
    if (dbData.panels != undefined) {
        localPanels = JSON.parse(JSON.stringify(dbData.panels));
    } else localPanels = [];
    if (localPanels != undefined) {
        displayPanels();
        setFavoriteValue(dbData.isFavorite);
        updateTimeRangeForPanels();
        setRefreshItemHandler();
    }
}

function updateTimeRangeForPanels() {
    localPanels.forEach(panel => {
        delete panel.queryRes;
        if(panel.queryData) {
            if(panel.chartType === "Line Chart" || panel.queryType === "metrics") {
                datePickerHandler(panel.queryData.start, panel.queryData.end, panel.queryData.start)
                panel.queryData.start = filterStartDate.toString();
                panel.queryData.end = filterEndDate.toString();
            } else {
                datePickerHandler(panel.queryData.startEpoch, panel.queryData.endEpoch, panel.queryData.startEpoch)
                panel.queryData.startEpoch = filterStartDate
                panel.queryData.endEpoch = filterEndDate
            }
            $('.inner-range .db-range-item').removeClass('active');
            $('.inner-range #' + filterStartDate).addClass('active');
        }
    })
}

function updateTimeRangeForPanel(panelIndex) {
    delete localPanels[panelIndex].queryRes;
    if(localPanels[panelIndex].queryData) {
        if(localPanels[panelIndex].chartType === "Line Chart" && localPanels[panelIndex].queryType === "metrics") {
            localPanels[panelIndex].queryData.start = filterStartDate.toString();
            localPanels[panelIndex].queryData.end = filterEndDate.toString();
        } else {
            localPanels[panelIndex].queryData.startEpoch = filterStartDate
            localPanels[panelIndex].queryData.endEpoch = filterEndDate
        }
    }
}


// Event listener for Gridstack resize and drag events
grid.on('change', function(event, items) {
    items.forEach(function(item) {
        // Find the panel in localPanels array using its ID
        let panelIndex = localPanels.findIndex(panel => panel.panelId === item.el.id);
        if (panelIndex !== -1) {
            // Update the position and size of the panel in localPanels array
            localPanels[panelIndex].gridpos.x = item.y;
            localPanels[panelIndex].gridpos.y = item.x;
            localPanels[panelIndex].gridpos.w = item.width;
            localPanels[panelIndex].gridpos.h = item.height;
        }
    });
});

grid.on('dragstart', function(event, items) {
    $('.default-item').hide();
});

grid.on('resizestart', function(event, items) {
    $('.default-item').hide();
});

grid.on('dragstop', function(event, items) {
    $('.default-item').show();
});

grid.on('resizestop', function(event, ui) {
    var gridStackItemId = ui.id;
    var panelIndex = $('#' + gridStackItemId).find('.panel').attr('panel-index');
    const panelChartType = localPanels[panelIndex].chartType;
    switch (panelChartType) {
        case 'number' : 
            var newSize = $('#' + gridStackItemId).width() / 8;
            $('#' + gridStackItemId).find('.big-number, .unit').css('font-size', newSize + 'px');
            break;
        case 'Bar Chart' :
        case 'Pie Chart' :
            var echartsInstanceId = $('#' + gridStackItemId).find('.panEdit-panel').attr('_echarts_instance_');
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

function displayPanels() {
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

    localPanels.forEach((localPanel) => {
        let idpanel = localPanel.panelId;
        
        var newItem = grid.addWidget(`<div class="grid-stack-item" id="${idpanel}"><div class="grid-stack-item-content"></div></div>`, {
            width: parseInt(localPanel.gridpos.w),
            height: parseInt(localPanel.gridpos.h),
            x: parseInt(localPanel.gridpos.y),
            y: parseInt(localPanel.gridpos.x)
        });
        
        // Append panel layout to the new grid-stack-item
        var panelDiv = $("<div>").append(panelLayout).addClass("panel temp").attr("id", `panel${idpanel}`).attr("panel-index", localPanel.panelIndex);
        newItem.firstChild.appendChild(panelDiv[0]);

        $("#panel" + idpanel).on('mouseenter',function(){
            $("#panel" + idpanel + " .panel-icons").addClass("active")
        });
        $("#panel" + idpanel).on('mouseleave',function(){
            $("#panel" + idpanel + " .panel-icons").removeClass("active");
            $("#panel" + idpanel + " .dropdown-style").addClass("hidden");
        });
        $("#panel" + idpanel + " .dropdown-btn").click(function (e) {
            e.stopPropagation();
            $("#panel" + idpanel + " .dropdown-btn").toggleClass("active");
            $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
        });
        
        $(`.grid-stack-item .grid-stack-item-content #panel${idpanel} .panel-header p`).html(localPanel.name);

        if (localPanel.description || (localPanel.queryData && localPanel.queryData.searchText)) {
            handleDescriptionTooltip(idpanel, localPanel.description, localPanel.queryData ? localPanel.queryData.searchText : '');
        } else {
            $(`#panel${idpanel} .panel-info-corner`).hide();
        }
        let val = localPanel.gridpos.y + localPanel.gridpos.h;
        if (val > panelContainerMinHeight) panelContainerMinHeight = val;

        handlePanelRemove(idpanel)

        if (localPanel.chartType == 'Data Table'||localPanel.chartType == 'loglines') {
            let panEl = $(`#panel${idpanel} .panel-body`)
            let responseDiv = `<div id="panelLogResultsGrid" class="panelLogResultsGrid ag-theme-mycustomtheme"></div>
            <div id="empty-response"></div></div><div id="corner-popup"></div>
            <div id="panel-loading"></div>`
            panEl.append(responseDiv)

            $("#panelLogResultsGrid").show();
            if (localPanel.queryRes)
                runPanelLogsQuery(localPanel.queryData, idpanel,localPanel, localPanel.queryRes);
            else
                runPanelLogsQuery(localPanel.queryData, idpanel,localPanel);
        } else if (localPanel.chartType == 'Line Chart') {
            let panEl = $(`#panel${idpanel} .panel-body`)
            let responseDiv = `<div id="empty-response"></div></div><div id="corner-popup"></div>
            <div id="panel-loading"></div>`
            panEl.append(responseDiv)
            if (localPanel.queryRes){
                runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel, localPanel.queryRes)
            }
            else {
                //remove startEpoch from from localPanel.queryData
                delete localPanel.queryData.startEpoch
                delete localPanel.queryData.endEpoch
                runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel)
            }
        } else if (localPanel.chartType == 'number') {
            let panEl = $(`#panel${idpanel} .panel-body`)
            let responseDiv = `<div class="big-number-display-container"></div>
            <div id="empty-response"></div><div id="corner-popup"></div>
            <div id="panel-loading"></div>`
            panEl.append(responseDiv)

            $('.big-number-display-container').show();
            if (localPanel.queryType === "metrics"){

                if (localPanel.queryRes){
                    delete localPanel.queryData.startEpoch
                    delete localPanel.queryData.endEpoch
                    runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel, localPanel.queryRes)
                }
                else {
                    //remove startEpoch from from localPanel.queryData
                    delete localPanel.queryData.startEpoch
                    delete localPanel.queryData.endEpoch
                    runMetricsQuery(localPanel.queryData, localPanel.panelId, localPanel)
                }
            }else {
                if (localPanel.queryRes)
                    runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
                else
                    runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
            }
        } else if (localPanel.chartType == 'Bar Chart' || localPanel.chartType == 'Pie Chart') {
            // generic for both bar and pie chartTypes.
            let panEl = $(`#panel${idpanel} .panel-body`)
            let responseDiv = `<div id="empty-response"></div><div id="corner-popup"></div>
            <div id="panel-loading"></div>`
            panEl.append(responseDiv)
            if (localPanel.queryRes)
                runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
            else
                runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
        } else{
            allResultsDisplayed--;
        }
            handlePanelEdit();
            handlePanelView();
            handlePanelRemove(idpanel);
            handlePanelDuplicate();
    })
    if(allResultsDisplayed === 0) {
        $('body').css('cursor', 'default');
    }
    
    addDefaultPanel();
}   

function showToast(msg) {
    let toast =
        `<div class="div-toast" id="save-db-modal">
        ${msg}
        <button type="button" aria-label="Close" class="toast-close">✖</button>
    <div>`
    $('body').prepend(toast);
    $('.toast-close').on('click', removeToast)
    setTimeout(removeToast, 1000);
}

function removeToast() {
    $('.div-toast').remove();
}

function getDashboardId() {
    let queryString = decodeURIComponent(window.location.search); //parsing
    queryString = queryString.substring(1).split("=");
    let uniq = queryString[1];
    return uniq;
}

var panelLayout =
    '<div class="panel-header">' +
        '<div>'+
            '<p>Panel Title</p>'+
        '</div>' +
        '<div class="panel-icons">'+
            '<div><img src="../assets/edit-panel-icon.svg" alt="" class="panel-edit-li" /></div>'+
            '<div><img src="../assets/resize-icon.svg" alt="" class="panel-view-li" /></div>'+
            '<div>'+
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

function addPanel(chartIndex) {
    flagDBSaved = false;
    panelIndex = localPanels.length;
    var defaultWidget = $('.default-item').get(0); // Get the DOM element
    // Remove the default widget from the grid
    grid.removeWidget(defaultWidget);
    let idpanel = uuidv4();
    let panel = $("<div>").append(panelLayout).addClass("panel temp").attr("id", `panel${idpanel}`).attr("panel-index", panelIndex);
    $("#panel-container").append(panel);
    $(`#panel${idpanel} .panel-header p`).html(`panel${panelIndex}`);
    $("#panel" + idpanel).on('mouseenter',function(){
        $("#panel" + idpanel + " .panel-icons").addClass("active")
    });
    $("#panel" + idpanel).on('mouseleave',function(){
        $("#panel" + idpanel + " .panel-icons").removeClass("active");
        $("#panel" + idpanel + " .dropdown-style").addClass("hidden");
    });
    $("#panel" + idpanel + " .dropdown-btn").click(function (e) {
        e.stopPropagation();
        $("#panel" + idpanel + " .dropdown-btn").toggleClass("active")
        $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
    });
    $(`#panel${idpanel} .panel-info-corner`).hide();
    var newItem = grid.addWidget(`<div class="grid-stack-item" id="${idpanel}"><div class="grid-stack-item-content"></div></div>`, { width: 4, height: 2 });

    // Insert panel content into grid-stack-item-content
    newItem.firstChild.appendChild(panel[0]);

    let panelTop = newItem.getAttribute('data-gs-x');
    let panelLeft = newItem.getAttribute('data-gs-y');
    let panelWidth = newItem.getAttribute('data-gs-width');
    let panelHeight = newItem.getAttribute('data-gs-height')
    let chartType = "";
    let queryType = "";
    let queryData = {};
    let logLinesViewType = "";
    let unit = "";

    switch (chartIndex) {
        case 0: // Line chart
            chartType = "Line Chart";
            queryType = "metrics";
            queryData = {
                start: "now-1h",
                end: "now",
                formulas: [
                    {
                      "formula": "a"
                    }
                  ],
                  "queries": [
                    {
                      "name": "a",
                      "qlType": "promql",
                      "query": "testmetric0"
                    }
                  ],
            };
            break;
        case 1: // Bar chart
            chartType = "Bar Chart";
            queryType = "logs";
            queryData = {
                state: "query",
                searchText: "city=Boston | stats count AS Count BY weekday",
                startEpoch: filterStartDate,
                endEpoch: filterEndDate,
                indexName: selectedSearchIndex,
                from: 0,
                queryLanguage: "Splunk QL"
            };
            break;
        case 2: // Pie chart
            chartType = "Pie Chart";
            queryType = "logs";
            queryData = {
                state: "query",
                searchText: "city=Boston | stats count AS Count BY http_status",
                startEpoch: filterStartDate,
                endEpoch: filterEndDate,
                indexName: selectedSearchIndex,
                from: 0,
                queryLanguage: "Splunk QL"
            };
            break;
        case 3: // Data Table
            chartType = "Data Table";
            queryType = "logs";
            queryData = {
                state: "query",
                searchText: "*",
                startEpoch: filterStartDate,
                endEpoch: filterEndDate,
                indexName: selectedSearchIndex,
                from: 0,
                queryLanguage: "Splunk QL"
            };
            break;
        case 4: // Number
            chartType = "number";
            queryType = "logs";
            queryData = {
                state: "query",
                searchText: "city=Boston | stats avg(latency)",
                startEpoch: filterStartDate,
                endEpoch: filterEndDate,
                indexName: selectedSearchIndex,
                from: 0,
                queryLanguage: "Splunk QL"
            };
            unit = "misc";
            break;
        case 5: // Log Lines
            chartType = "loglines";
            queryType = "logs";
            queryData = {
                state: "query",
                searchText: "*",
                startEpoch: filterStartDate,
                endEpoch: filterEndDate,
                indexName: selectedSearchIndex,
                from: 0,
                queryLanguage: "Splunk QL"
            };
            logLinesViewType = "Single line display view";
            break;
    }

    localPanels.push({
        "name": `panel${panelIndex}`,
        "panelIndex": panelIndex,
        "panelId": idpanel,
        "description": "",
        "chartType": chartType,
        "unit": "",
        "dataType": "",
        "gridpos": {
            "h": panelHeight,
            "w": panelWidth,
            "x": panelLeft,
            "y": panelTop,
        },
        "queryType": queryType,
        "queryData": queryData,
        "logLinesViewType": logLinesViewType,
        "unit": unit,
    });

    editPanelInit(panelIndex);
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
    let panel = $("<div>").append(panelLayout).addClass("panel temp").attr("id", `panel${idpanel}`).attr("panel-index", panelIndex);
    $("#panel-container").append(panel);
    $(`#panel${idpanel} .panel-header p`).html(`panel${panelIndex}`);
    $("#panel" + idpanel).on('mouseenter',function(){
        $("#panel" + idpanel + " .panel-icons").addClass("active")
    });
    $("#panel" + idpanel).on('mouseleave',function(){
        $("#panel" + idpanel + " .panel-icons").removeClass("active");
        $("#panel" + idpanel + " .dropdown-style").addClass("hidden");
    });
    $("#panel" + idpanel + " .dropdown-btn").click(function (e) {
        e.stopPropagation();
        $("#panel" + idpanel + " .dropdown-btn").toggleClass("active")
        $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
    });
    $(`#panel${idpanel} .panel-info-corner`).hide();
    var newItem = grid.addWidget(`<div class="grid-stack-item" id="${idpanel}"><div class="grid-stack-item-content"></div></div>`, { width: panelToDuplicate.gridpos.w, height: panelToDuplicate.gridpos.h });

    // Insert panel content into grid-stack-item-content
    newItem.firstChild.appendChild(panel[0]);

    let panelTop = newItem.getAttribute('data-gs-x');
    let panelLeft = newItem.getAttribute('data-gs-y');
    let panelWidth = newItem.getAttribute('data-gs-width');
    let panelHeight = newItem.getAttribute('data-gs-height')


    panelToDuplicate.panelId = idpanel;
    panelToDuplicate.name += "Copy";
    panelToDuplicate.panelIndex = panelIndex;
    panelToDuplicate.gridpos.x = panelLeft;
    panelToDuplicate.gridpos.y = panelTop;
    panelToDuplicate.gridpos.h = panelHeight;
    panelToDuplicate.gridpos.w = panelWidth;
    if (panelToDuplicate.description){
        handleDescriptionTooltip(panelToDuplicate.panelId,panelToDuplicate.description)
    }
    localPanels.push(JSON.parse(JSON.stringify(panelToDuplicate)))
    handlePanelView();
    handlePanelEdit();
    handlePanelRemove(idpanel);
    handlePanelDuplicate();
    $(`#panel${idpanel}`).get(0).scrollIntoView({ behavior: 'smooth' });

    addDefaultPanel();
}

function addDefaultPanel(){
    var defaultItem = grid.addWidget(`<div class="grid-stack-item default-item active"><div class="add-panel-div">
    <div class="plus-icon">+</div>
    <div class="text">Select the Panel Type</div>
    </div></div>`, 
    {   width: 4, 
        height:2,  
        noResize: true,
        noMove: true,
    });
    $('#add-widget-options').show();
}


// DASHBOARD SETTINGS PAGE
let editPanelFlag = false;
function handleDbSettings() {
    if ($('.panelEditor-container').css('display') !== 'none') {
        $('.panelEditor-container').hide();
        $('#app-container').hide();
        $('.popupOverlay').removeClass('active');
        editPanelFlag =true;
    } else {
        $('#app-container').hide();
    }
    $('.dbSet-container').show();

    $('.dbSet-name').html(dbName)
    $('.dbSet-dbName').val(dbName)
    $('.dbSet-dbDescr').val(dbDescr)
    $('.dbSet-jsonModelData').val(JSON.stringify(JSON.unflatten({
        description: dbDescr,
        name: dbName,
        timeRange: timeRange,
        panels: localPanels,
        refresh: dbRefresh,
    }), null, 2))
    $('.dbSet-dbName').on("change keyup paste", function () {
        dbName = $('.dbSet-dbName').val()
        $('.dbSet-name').html(dbName)
    })
    $('.dbSet-dbDescr').on("change keyup paste", function () {
        dbDescr = $('.dbSet-dbDescr').val()
        $('.dbSet-dbDescr').html(dbDescr)
        $('.dbSet-jsonModelData').val(JSON.stringify(JSON.unflatten({
            description: dbDescr,
            name: dbName,
            timeRange: timeRange,
            panels: localPanels,
            refresh: dbRefresh,
        }), null, 2))
    })

    $.ajax({
        method: "get",
        url: "api/dashboards/defaultlistall",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        for (let [key, value] of Object.entries(res)) {
            if (key == dbId ){
                $(".dbSet-dbName").prop('readonly', true);
                $('.dbSet-dbDescr').prop('readonly', true);
		    
                break
            }
        }
    })

    //get dashboard data from database
    $.ajax({
        method: "get",
        url: "api/dashboards/" + dbId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        console.log(JSON.stringify(res))
        $(".dbSet-dbName").val(res.name);
        $(".dbSet-dbDescr").val(res.description);
        $('.dbSet-jsonModelData').val(JSON.stringify(JSON.unflatten(res), null, 2))
    })

    showGeneralDbSettings();
    addDbSettingsEventListeners();
}

function showGeneralDbSettings() {
    $('.dbSet-general').addClass('selected')
    $('.dbSet-generalHTML').removeClass('hide');

    $('.dbSet-jsonModel').removeClass('selected');
    $('.dbSet-jsonModelHTML').addClass('hide')
}

function showJsonModelDbSettings() {
    $('.dbSet-general').removeClass('selected')
    $('.dbSet-generalHTML').addClass('hide');

    $('.dbSet-jsonModel').addClass('selected');
    $('.dbSet-jsonModelHTML').removeClass('hide')
}

function addDbSettingsEventListeners() {
    $('.dbSet-general').on('click', showGeneralDbSettings);
    $('.dbSet-jsonModel').on('click', showJsonModelDbSettings)
}

function saveDbSetting() {
    let trimmedDbName = $('.dbSet-dbName').val().trim();
    let trimmedDbDescription = $(".dbSet-dbDescr").val().trim();
    if (!trimmedDbName) {
        // Show error message using error-tip and popupOverlay
        $('.error-tip').addClass('active');
        $('.popupOverlay, .popupContent').addClass('active');
        $('#error-message').text('Dashboard name cannot be empty.');
        return;
    }


    dbName = trimmedDbName;
    dbDescr = trimmedDbDescription;


    updateDashboard()
    .then(updateSuccessful => {
        if (updateSuccessful) {
            $('#app-container').show();
            $('.dbSet-container').hide();
        }
    })
}

$('#error-ok-btn').click(function () {
    $('.popupOverlay, .popupContent').removeClass('active');
    $('.error-tip').removeClass('active');
});

function discardDbSetting() {
    if(editPanelFlag){
        $('.panelEditor-container').css('display', 'flex');
        $('.popupOverlay').addClass('active');
        editPanelFlag=false;
    }else{
        $('#app-container').show();
    }
    $('.dbSet-dbName').val("");
    $('.dbSet-dbDescr').val("");
    $('.dbSet-jsonModelData').val("");
    $('.dbSet-container').hide();
    dbName = dbData.name;
    dbDescr = dbData.description;
}

// Refresh handler

function setRefreshItemHandler(){
    $(".refresh-range-item").removeClass("active");
    if(dbRefresh){
        $(`.refresh-range-item:contains('${dbRefresh}')`).addClass("active");
        $('.refresh-container #refresh-picker-btn span').text(dbRefresh);
        startRefreshInterval(dbRefresh)
    }else{
        $('.refresh-container #refresh-picker-btn span').text("");
        $(`.refresh-range-item:contains('Off')`).addClass("active");
    }
}

function refreshRangeItemHandler(evt){
    $.each($(".refresh-range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    let refreshInterval = $(evt.currentTarget).attr('id');
    if(refreshInterval==="0"){
        dbRefresh = "";
        $('.refresh-container #refresh-picker-btn span').html("");
    }else{
        dbRefresh = refreshInterval;
        $('.refresh-container #refresh-picker-btn span').html(refreshInterval);
    }
    startRefreshInterval(refreshInterval)
}

let intervalId;

function startRefreshInterval(refreshInterval) {
    let parsedRefreshInterval = parseInterval(refreshInterval);
    clearInterval(intervalId);
    if (parsedRefreshInterval > 0) {
            intervalId = setInterval(function () {
                refreshDashboardHandler();
            }, parsedRefreshInterval);

    }else{
        pauseRefreshInterval();
    }
}


function pauseRefreshInterval() {
    clearInterval(intervalId);
    return 0;
}

function parseInterval(interval) {
    if(interval==="0"){
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
            throw new Error("Invalid interval unit");
    }
}

// Favorite Handler
function toggleFavorite() {
    $.ajax({
        method: 'put',
        url: 'api/dashboards/favorite/' + dbId,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*',
        },
        crossDomain: true,
    }).then((response) => {
        setFavoriteValue(response.isFavorite);
    });
}

function setFavoriteValue(isFavorite) {
    if(isFavorite) {
        $('#favbutton').addClass('active');
    } else {
        $('#favbutton').removeClass('active');
    }	
}

// Resizing handler
$(window).on('resize', function() {
    setTimeout(resizeCharts, 100);
    var windowWidth = window.innerWidth;

    // If window width is less than a certain threshold, disable resizing and dragging
    if (windowWidth < 978) { // Adjust the threshold as needed
        grid.movable('.grid-stack-item', false);
        grid.resizable('.grid-stack-item', false);
    } else {
        // Enable resizing and dragging
        grid.movable('.grid-stack-item', true);
        grid.resizable('.grid-stack-item', true);
    }
});

function resizeCharts() {
    $('.grid-stack-item-content .panEdit-panel').each(function() {
        var echartsInstanceId = $(this).attr('_echarts_instance_');
        if (echartsInstanceId) {
            var echartsInstance = echarts.getInstanceById(echartsInstanceId);
            if (echartsInstance) {
                echartsInstance.resize();
            }
        }
    });
}
