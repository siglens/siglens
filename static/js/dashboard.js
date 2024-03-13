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

let localPanels = [], dbData, dbName, dbDescr, dbId, panelIndex, flagDBSaved = true, allResultsDisplayed = 0;
let timeRange = "Last 1 Hr";
let dbRefresh ="";
let panelContainer;
let panelContainerWidthGlobal;
let curFocus;

$(document).ready(function () {
    getListIndices();
    var isActive = $('#app-side-nav').hasClass('active');
    if (isActive) {
        $('#new-dashboard').css("transform", "translate(87px)")
        $('#new-dashboard').css("width", "calc(100% - 97px)")
    }
    else {
        $('#new-dashboard').css("transform", "translate(170px)")
        $('#new-dashboard').css("width", "calc(100% - 180px)")
    }

    panelContainer = document.getElementById('panel-container');
    panelContainerWidthGlobal = isActive? panelContainer.offsetWidth-97: panelContainer.offsetWidth-215;

    $('.panelEditor-container').hide();
    $('.dbSet-container').hide();
    if (Cookies.get('theme')) {
        theme = Cookies.get('theme');
        $('body').attr('data-theme', theme);
    }
    $('.theme-btn').on('click', themePickerHandler);
    setupEventHandlers();
    dbId = getDashboardId();

    $("#add-panel-btn").click(() => addPanel());
    $(".all-dashboards").click(function () {
        window.location.href = "../dashboards-home.html";
    })

    displayDashboardName();

    $("#theme-btn").click(() => displayPanels());

    getDashboardData();

    setTimePicker();

    $(`.dbSet-textareaContainer .copy`).tooltip({
        delay: { show: 0, hide: 300 },
        trigger: 'hover'
    });
})
$(document).mouseup(function (e) {
  var popWindows = $("#panel-dropdown-modal");
  let panelHead = $(".panel-header");
  let j1 = !popWindows.is(e.target);
  let j2 = !panelHead.is(e.target);
  let j3 = !$(curFocus + " .dropdown-style").hasClass("hidden");
  if (
    !popWindows.is(e.target) &&
    popWindows.has(e.target).length === 0 &&
    !panelHead.is(e.target) &&
    panelHead.has(e.target).length === 0 &&
    !$(curFocus + " .dropdown-style").hasClass("hidden")
  ) {
    $(curFocus + " .dropdown-btn").toggleClass("active");
    $(curFocus + " .dropdown-style").toggleClass("hidden");
  }
});
window.addEventListener('resize', function (event) {
    if ($('.panelEditor-container').css('display') === 'none'){
        panelContainerWidthGlobal = panelContainer.offsetWidth-97;
        recalculatePanelWidths();
        displayPanels();
        resetPanelLocationsHorizontally();
    }
});
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

function recalculatePanelWidths(){
    localPanels.map(localPanel => {
        localPanel.gridpos.w = localPanel.gridpos.wPercent * panelContainerWidthGlobal;
    })
}

$('#save-db-btn').on("click", updateDashboard);
$('.refresh-btn').on("click", refreshDashboardHandler);
$('.settings-btn').on('click', handleDbSettings);
$('#dbSet-save').on('click', saveDbSetting);
$('#dbSet-discard').on('click', discardDbSetting);
$('.dbSet-goToDB').on('click', discardDbSetting);
$('.panView-goToDB').on("click", goToDashboardFromView)
$('.refresh-range-item').on('click', refreshRangeItemHandler);


function updateDashboard() {
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
                displayDashboardName();
                showToast('Dashboard Updated Successfully');
                return true;
            }
            return res.json();
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
    $('#app-container').show();
    $('.panView-goToDB').css('display', 'block');
    $('#viewPanel-container').show();
    $('#panel-container').hide();
    $('.panelEditor-container').hide();
    $('#add-panel-btn').hide();
    $('#viewPanel-container').css('display', 'flex');
    $('#viewPanel-container').css('height', '100%');
    setTimePicker();
}

function goToDashboardFromView() {
    $('#viewPanel-container').hide();
    $('#panel-container').show();
    $('.panView-goToDB').css('display', 'none');
    $('#add-panel-btn').show();
    $('#viewPanel-container .panel .panel-info-corner').empty();
    updateTimeRangeForPanels();
    displayPanels();
    if(dbRefresh){
		startRefreshInterval(dbRefresh)
	}
}

function handlePanelEdit() {
    $(".panel-edit-li").unbind("click");
    $(".panel-edit-li").on("click", function () {
        panelIndex = $(this).closest(".panel").attr("panel-index");

        if ($('#viewPanel-container').css('display') !== 'none') {
            editPanelInit(-1);
        } else {
            editPanelInit();
        }
        $('.panelEditor-container').show();
        $('#app-container').hide();
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
        $('#cancel-btn-panel, .popupOverlay').on("click", function () {
            $('.popupOverlay, .popupContent').removeClass('active');
        });
    }

    function deletePanel(panelId) {
        flagDBSaved = false;
        const panel = $(`#panel${panelId}`);
        let panelIndex = panel.attr("panel-index");

        localPanels = localPanels.filter(function (el) {
            return el.panelIndex != panelIndex;
        });
        panel.remove();

        resetPanelIndices();
        resetPanelLocationsHorizontally();
        resetPanelLocationsVertically();
        resetPanelContainerHeight();
        displayPanels();
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

function resetPanelLocationsHorizontally() {
    let temp = [];
    for (let i = 0; i < localPanels.length; i++) {
        let x = localPanels[i].gridpos.x;
        temp.push([x, i]);
    }
    temp.sort((a, b) => a[0] - b[0]);
    let indices = [];
    for (let i = 0; i < temp.length; i++)
        indices.push(temp[i][1])

    for (let i = 0; i < indices.length; i++) {
        let hRight = localPanels[indices[i]].gridpos.h;
        let wRight = localPanels[indices[i]].gridpos.wPercent * panelContainerWidthGlobal;
        let xRight = localPanels[indices[i]].gridpos.x;
        let yRight = localPanels[indices[i]].gridpos.y;

        let xmax = 0;
        for (let j = 0; j < i; j++) {
            let hLeft = localPanels[indices[j]].gridpos.h;
            let wLeft = localPanels[indices[j]].gridpos.w;
            let xLeft = localPanels[indices[j]].gridpos.x;
            let yLeft = localPanels[indices[j]].gridpos.y;

            if ((yLeft >= yRight && yLeft <= yRight + hRight) || (yLeft + hLeft >= yRight && yLeft + hLeft <= yRight + hRight) || (yLeft <= yRight && yLeft + hLeft >= yRight + hRight)) {
                xmax = Math.max(xmax, xLeft + wLeft);
            }
        }

        if ((xmax + wRight) < ($('#panel-container')[0].offsetWidth + $('#panel-container')[0].offsetLeft - 20)) {
            localPanels[indices[i]].gridpos.x = xmax + 10;
        }
    }
}

function resetPanelLocationsVertically() {
    let temp = [];

    for (let i = 0; i < localPanels.length; i++) {
        let y = localPanels[i].gridpos.y;
        temp.push([y, i]);
    }
    temp.sort((a, b) => a[0] - b[0]);
    let indices = [];
    for (let i = 0; i < temp.length; i++)
        indices.push(temp[i][1])

    for (let i = 0; i < indices.length; i++) {
        let hDown = localPanels[indices[i]].gridpos.h;
        let wDown = localPanels[indices[i]].gridpos.w;
        let xDown = localPanels[indices[i]].gridpos.x;
        let yDown = localPanels[indices[i]].gridpos.y;

        let ymax = 10;
        for (let j = 0; j < i; j++) {
            let hTop = localPanels[indices[j]].gridpos.h;
            let wTop = localPanels[indices[j]].gridpos.w;
            let xTop = localPanels[indices[j]].gridpos.x;
            let yTop = localPanels[indices[j]].gridpos.y;

            if ((xTop >= xDown && xTop <= xDown + wDown) || (xTop + wTop >= xDown && xTop + wTop <= xDown + wDown) || (xTop <= xDown && xTop + wTop >= xDown + wDown)) {
                ymax = Math.max(ymax, yTop + hTop);
            }
        }

        localPanels[indices[i]].gridpos.y = ymax + 10;
    }
}

function handlePanelDuplicate() {
    $(".panel-dupl-li").unbind("click");
    $(".panel-dupl-li").on("click", function () {
        flagDBSaved = false;
        let duplicatedPanelIndex = $(this).closest(".panel").attr("panel-index");
        addPanel(JSON.parse(JSON.stringify(localPanels[duplicatedPanelIndex])));
        renderDuplicatePanel(duplicatedPanelIndex);
    })
}

function renderDuplicatePanel(duplicatedPanelIndex) {
    let boundaryY = localPanels[duplicatedPanelIndex].gridpos.y + localPanels[duplicatedPanelIndex].gridpos.h;
    for (let i = 0; i < localPanels.length; i++) {
        if (localPanels[i].panelIndex == localPanels.length - 1) continue; // this is the newly created duplicate panel.
        if (localPanels[i].gridpos.y >= boundaryY) // if any panel starts after the ending Y-cordinate of the duplicated panel, then it should be shifted downwards
            localPanels[i].gridpos.y += localPanels[duplicatedPanelIndex].gridpos.h + 20;
    }
    resetPanelLocationsVertically();
    resetPanelLocationsHorizontally();
    resetPanelContainerHeight();
    displayPanelsWithoutRefreshing();
    let localPanel = localPanels[localPanels.length - 1];
    let panelId = localPanels[localPanels.length - 1].panelId;
    // only render the duplicated panel
    $(`#panel${localPanels[localPanels.length - 1].panelId} .panel-header p`).html(localPanels[duplicatedPanelIndex].name + "Copy");

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

function displayDashboardName() {
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
        $(".name-dashboard").text(res.name);
    })
}

async function getDashboardData() {
    await fetch(`/api/dashboards/${dbId}`)
        .then(res => {
            return res.json();
        })
        .then(data => {
            dbData = data;
        })
    dbName = dbData.name;
    dbDescr = dbData.description;
    dbRefresh = dbData.refresh;
    if (dbData.panels != undefined) {
        localPanels = JSON.parse(JSON.stringify(dbData.panels));
    } else localPanels = [];
    if (localPanels != undefined) {
        updateTimeRangeForPanels();
        recalculatePanelWidths();
        resetPanelLocationsHorizontally();
        setRefreshItemHandler();
        refreshDashboardHandler();
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


function displayPanels() {
    allResultsDisplayed = localPanels.length;
    $('#panel-container .panel').remove();
    let panelContainerMinHeight = 0;
    $('body').css('cursor', 'progress');
    localPanels.map((localPanel) => {
        let idpanel = localPanel.panelId;
        let panel = $("<div>").append(panelLayout).addClass("panel").attr("id", `panel${idpanel}`).attr("panel-index", localPanel.panelIndex);
        $("#panel-container").append(panel);
        handleDrag(idpanel);
        handleResize(idpanel);
        $("#panel" + idpanel + " .panel-header").click(function () {
            curFocus = "#panel" + idpanel;
            $("#panel" + idpanel + " .dropdown-btn").toggleClass("active")
            $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
        })
        $("#panel" + idpanel + " .dropdown-btn").click(function (e) {
            e.stopPropagation();
            curFocus = "#panel" + idpanel;
            $("#panel" + idpanel + " .dropdown-btn").toggleClass("active");
            $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
        });
        $(`#panel${idpanel} .panel-header p`).html(localPanel.name);

        if (localPanel.description || (localPanel.queryData && localPanel.queryData.searchText)) {
            handleDescriptionTooltip(idpanel, localPanel.description, localPanel.queryData ? localPanel.queryData.searchText : '');
        } else {
            $(`#panel${idpanel} .panel-info-corner`).hide();
        }

        let panelElement = document.getElementById(`panel${idpanel}`);
        panelElement.style.position = "absolute";
        panelElement.style.height = localPanel.gridpos.h + "px";
        panelElement.style.width = localPanel.gridpos.w + "px";
        panelElement.style.top = localPanel.gridpos.y + "px";
        panelElement.style.left = localPanel.gridpos.x + "px";

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
        } else
            allResultsDisplayed--;
    })
    if(allResultsDisplayed === 0) {
        $('body').css('cursor', 'default');
    }
    handlePanelView();
    handlePanelEdit();
    handlePanelDuplicate();
    resetPanelContainerHeight();
}

function displayPanelView(panelIndex) {
    let localPanel = localPanels[panelIndex];
    let panelId = localPanel.panelId;
    $(`#panel-container #panel${panelId}`).remove();
    $(`#viewPanel-container`).empty();

    let panel = $("<div>").append(panelLayout).addClass("panel").attr("id", `panel${panelId}`).attr("panel-index", localPanel.panelIndex);
    $("#viewPanel-container").append(panel);
    $("#panel" + panelId + " .panel-header").click(function () {
        $("#panel" + panelId + " .dropdown-btn").toggleClass("active")
        $("#panel" + panelId + " .dropdown-style").toggleClass("hidden");
    })
    $("#" + `panel${panelId}` + " .dropdown-btn").click(function (e) {
        e.stopPropagation();
        $("#" + `panel${panelId}` + " .dropdown-btn").toggleClass("active")
        $("#" + `panel${panelId}` + " .dropdown-style").toggleClass("hidden");
    });
    $(`#panel${panelId} .panel-header p`).html(localPanel.name);

    let panelElement = document.getElementById(`panel${panelId}`);
    panelElement.style.position = "absolute";

    panelElement.style.height = "100%";
    panelElement.style.width = "100%";

    handlePanelRemove(localPanel.panelId);
    if (localPanel.description||localPanel.queryData?.searchText) {
        handleDescriptionTooltip(localPanel.panelId,localPanel.description,localPanel.queryData.searchText);
    } else {
        $(`#panel${panelId} .panel-info-corner`).hide();
    }

    if (localPanel.chartType == 'Data Table'| localPanel.chartType == 'loglines') {
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

        if (localPanel.queryRes)
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
        else
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
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

    handlePanelView();
    handlePanelEdit();
}

function displayPanel(panelIndex) {
    let localPanel = localPanels[panelIndex];
    let panelId = localPanel.panelId;
    $(`#panel-container #panel${panelId}`).remove();
    $(`#viewPanel-container`).empty();

    let panel = $("<div>").append(panelLayout).addClass("panel").attr("id", `panel${panelId}`).attr("panel-index", localPanel.panelIndex);
    $("#panel-container").append(panel);
    handleDrag(panelId);
    handleResize(panelId);
    $("#panel" + panelId + " .panel-header").click(function () {
        $("#panel" + panelId + " .dropdown-btn").toggleClass("active")
        $("#panel" + panelId + " .dropdown-style").toggleClass("hidden");
    })
    $("#" + `panel${panelId}` + " .dropdown-btn").click(function (e) {
        e.stopPropagation();
        $("#" + `panel${panelId}` + " .dropdown-btn").toggleClass("active")
        $("#" + `panel${panelId}` + " .dropdown-style").toggleClass("hidden");
    });
    $(`#panel${panelId} .panel-header p`).html(localPanel.name);
    if (localPanel.description||localPanel.queryData.searchText) {
        handleDescriptionTooltip(panelId,localPanel.description,localPanel.queryData.searchText)
    } else {
        $(`#panel${panelId} .panel-info-corner`).hide();
    }


    let panelElement = document.getElementById(`panel${panelId}`);
    panelElement.style.position = "absolute";
    panelElement.style.height = localPanel.gridpos.h + "px";
    panelElement.style.width = (localPanel.gridpos.wPercent * 100) + "%";
    panelElement.style.top = localPanel.gridpos.y + "px";
    panelElement.style.left = localPanel.gridpos.x + "px";
    handlePanelRemove(localPanel.panelId)

    if (localPanel.chartType == 'Data Table'|| localPanel.chartType =='loglines') {
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

        if (localPanel.queryRes)
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex, localPanel.queryRes);
        else
            runPanelAggsQuery(localPanel.queryData, localPanel.panelId, localPanel.chartType, localPanel.dataType, localPanel.panelIndex);
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

    handlePanelView();
    handlePanelEdit();
    handlePanelDuplicate();
    resetPanelContainerHeight();
}

function displayPanelsWithoutRefreshing() {
    localPanels.map((localPanel) => {
        let panelElement = document.getElementById(`panel${localPanel.panelId}`);
        panelElement.style.position = "absolute";
        panelElement.style.height = localPanel.gridpos.h + "px";
        panelElement.style.width = localPanel.gridpos.w + "px";
        panelElement.style.top = localPanel.gridpos.y + "px";
        panelElement.style.left = localPanel.gridpos.x + "px";
    })
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

function handleResize(panelId) {
    $(`#panel${panelId}`).resizable(
        { containment: "parent" }
    );
    $(`#panel${panelId}`).on("resizestop", function (event, ui) {
        flagDBSaved = false;
        panelIndex = $(this).attr("panel-index");
        localPanels[panelIndex].gridpos.w = ui.size.width;
        localPanels[panelIndex].gridpos.wPercent = ui.size.width / panelContainerWidthGlobal;
        localPanels[panelIndex].gridpos.h = ui.size.height;
        displayPanel(panelIndex);
        resetPanelLocationsHorizontally();
        resetPanelLocationsVertically();
        resetPanelContainerHeight();
        displayPanelsWithoutRefreshing();
    })
};

function resizePanelFontSize(panelIndex, panelId) {
    if (panelIndex !== -1) {
        let bigNumText = $(`#panel${panelId} .big-number`);
        let unit = $(`#panel${panelId} .unit`);
        let panelHeight = parseFloat((localPanels[panelIndex].gridpos.h));

        let numFontSize = panelHeight / 2;
        let panelWidth = parseFloat((localPanels[panelIndex].gridpos.w));

        if (numFontSize > 170)
            numFontSize = 170;
        $(bigNumText).css('font-size', `${numFontSize}px`);

        panelWidth = parseFloat((localPanels[panelIndex].gridpos.w));

        if (bigNumText.width() + $(`#panel${panelId} .unit`).width() >= panelWidth) {
            numFontSize -= (bigNumText.width()  + $(`#panel${panelId} .unit`).width() - panelWidth) / 3.5;
        }
        if (numFontSize < 140 && numFontSize > 50){
            $('.unit').css('bottom','18px')
        }

        if (numFontSize < 50)
            numFontSize = 50;
        $(bigNumText).css('font-size', `${numFontSize}px`);
        let unitSize = numFontSize > 10 ? numFontSize - 40 : 12;
        if (unitSize < 25) {
            unitSize = 25;
            $(unit).css('bottom', `10px`);
            $(unit).css('margin-left', `8px`);

        }
        if (unitSize > 85)
            unitSize = 85;
        $(unit).css('font-size', `${unitSize}px`);
    } else {
        $('.big-number-display-container .big-number').css('font-size', `180px`);
    }
}

function handleDrag(panelId) {
    $(`#panel${panelId}`).draggable({
        start: function (event, ui) {
            $(this).removeClass('temp');
        },
        obstacle: ".temp",
        preventCollision: true,
        containment: "parent"
    });

    $(`#panel${panelId}`).on("dragstop", function (event, ui) {
        flagDBSaved = false;
        $(this).addClass('temp')
        panelIndex = $(this).attr("panel-index");

        if ((ui.position.left + $(this).width()) < ($('#panel-container')[0].offsetWidth + $('#panel-container')[0].offsetLeft)) {
            localPanels[panelIndex].gridpos.x = ui.position.left;
            localPanels[panelIndex].gridpos.y = ui.position.top;
            if (checkForVertical(ui.position)) {
                resetPanelLocationsVertically();
                resetPanelLocationsHorizontally();
            } else {
                resetPanelLocationsHorizontally();
                resetPanelLocationsVertically();
            }
        }

        resetPanelContainerHeight();
        displayPanelsWithoutRefreshing();
    })
};

function checkForVertical(uiPos) {
    for (let i = 0; i < localPanels.length; i++) {
        let x = localPanels[i].gridpos.x;
        let y = localPanels[i].gridpos.y;
        let w = localPanels[i].gridpos.w;
        let h = localPanels[i].gridpos.h;

        if (uiPos.left == x && uiPos.top == y) continue;

        if (uiPos.left >= x && uiPos.left <= x + w && uiPos.top >= y && uiPos.top <= y + h) {
            let distRightBound = x + w - uiPos.left;
            let distBottomBound = y + h - uiPos.top;
            return distBottomBound < distRightBound;
        }
    }
}

var panelLayout =
    '<div class="panel-header">' +
    '<p>Panel Title</p>' +
    '<span class="dropdown-btn" id="panel-options-btn"></span>' +
    '<ul class="dropdown-style hidden" id="panel-dropdown-modal">' +
    '<li data-value="view" class="panel-view-li"><span class="view"></span>View</li>' +
    '<li data-value="edit" class="panel-edit-li"><span class="edit"></span>Edit</li>' +
    '<li data-value="duplicate" class="panel-dupl-li"><span class="duplicate"></span>Duplicate</li>' +
    '<li data-value="remove" class="panel-remove-li"><span class="remove"></span>Remove</li>' +
    '</ul>' +
    '</div>' +
    `<div class="panel-body">
    <div class="panEdit-panel"></div>
    </div>
    <div class="panel-info-corner"><i class="fa fa-info" aria-hidden="true" id="panel-desc-info"></i></div>
`;

function checkForAddigInTopRow() {
    let temp = [];

    for (let i = 0; i < localPanels.length; i++) {
        let y = localPanels[i].gridpos.y;
        temp.push([y, i]);
    }
    temp.sort((a, b) => a[0] - b[0]);
    let indices = [];
    for (let i = 0; i < temp.length; i++)
        indices.push(temp[i][1])

    let topmostY = 10000;
    let rightBoundary = 0;
    if (indices.length == 0) topmostY = 0;
    for (let i = 0; i < indices.length; i++) {
        let hPanel = localPanels[indices[i]].gridpos.h;
        let wPanel = localPanels[indices[i]].gridpos.w;
        let xPanel = localPanels[indices[i]].gridpos.x;
        let yPanel = localPanels[indices[i]].gridpos.y;

        if (yPanel <= topmostY) {
            topmostY = yPanel;
            rightBoundary = Math.max(rightBoundary, xPanel + wPanel);
        }
        else break;
    }


    let panelContainerWidth = $('#panel-container').width();
    if (rightBoundary <= panelContainerWidth * 0.49 + 10) return [true, rightBoundary, topmostY];
    else return [false, null, null];
}

function addPanel(panelToDuplicate) {
    flagDBSaved = false;
    panelIndex = localPanels.length;
    let idpanel = uuidv4();
    let panel = $("<div>").append(panelLayout).addClass("panel temp").attr("id", `panel${idpanel}`).attr("panel-index", panelIndex);
    $("#panel-container").append(panel);
    $(`#panel${idpanel} .panel-header p`).html(`panel${panelIndex}`);
    $("#panel" + idpanel + " .panel-header").click(function () {
        $("#panel" + idpanel + " .dropdown-btn").toggleClass("active")
        $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
    })
    $("#panel" + idpanel + " .dropdown-btn").click(function (e) {
        e.stopPropagation();
        $("#panel" + idpanel + " .dropdown-btn").toggleClass("active")
        $("#panel" + idpanel + " .dropdown-style").toggleClass("hidden");
    });
    $(`#panel${idpanel} .panel-info-corner`).hide();
    let marginTop = 0;

    localPanels.map((localPanel) => {
        let val = localPanel.gridpos.y + localPanel.gridpos.h;
        if (val > marginTop) marginTop = val;
    })

    let panelElement = document.getElementById(`panel${idpanel}`);
    let panelHeight = panelToDuplicate ? panelToDuplicate.gridpos.h : panelElement.offsetHeight;
    let panelWidth = panelToDuplicate ? panelToDuplicate.gridpos.w : panelElement.offsetWidth;
    let panelTop = panelToDuplicate ? panelToDuplicate.gridpos.y + panelToDuplicate.gridpos.h + 20 : marginTop + 20;
    let panelLeft = panelToDuplicate ? panelToDuplicate.gridpos.x : panelElement.offsetLeft;
    let panelWidthPercentage = panelWidth / panelContainerWidthGlobal;

    if (panelToDuplicate == undefined) { // means a new panel is being added
        let [shouldAddInTopRow, rightBoundary, topmostY] = checkForAddigInTopRow();
        if (shouldAddInTopRow) {
            panelLeft = rightBoundary == 0 ? rightBoundary : rightBoundary + 20;
            panelTop = topmostY == 0 ? topmostY + 10 : topmostY;
        }
    }

    panelElement.style.position = "absolute"
    panelElement.style.top = panelTop + "px"
    panelElement.style.left = panelLeft + "px"

    if (panelToDuplicate) {
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
    }

    panelToDuplicate
        ?
        localPanels.push(JSON.parse(JSON.stringify(panelToDuplicate)))
        :
        localPanels.push({
            "name": `panel${panelIndex}`,
            "panelIndex": panelIndex,
            "panelId": idpanel,
            "description": "",
            "chartType": "",
            "unit": "",
            "dataType": "",
            "gridpos": {
                "h": panelHeight,
                "w": panelWidth,
                "x": panelLeft,
                "y": panelTop,
                "wPercent": panelWidthPercentage,
            },
            "queryType": "",
        });
    if (!panelToDuplicate) {        
        editPanelInit(panelIndex);
        $('.panelEditor-container').show();
        $('#app-container').hide();
        $('.panelDisplay #panelLogResultsGrid').empty();
        $('.panelDisplay .big-number-display-container').hide();
        $('.panelDisplay #empty-response').hide();
    }
    resetPanelContainerHeight();

    handlePanelView();
    handlePanelEdit();
    handlePanelRemove(idpanel);
    handlePanelDuplicate();
    handleDrag(idpanel);
    handleResize(idpanel);
    $(`#panel${idpanel}`).get(0).scrollIntoView({ behavior: 'smooth' });

}

function resetPanelContainerHeight() {
    let panelContainerMinHeight = 0;
    localPanels.map((localPanel, index) => {
        let val = localPanel.gridpos.y + localPanel.gridpos.h;
        if (val > panelContainerMinHeight) panelContainerMinHeight = val;
    })
    let panelContainer = document.getElementById('panel-container');
    panelContainer.style.minHeight = panelContainerMinHeight + 50 + "px";
}

window.onbeforeunload = function () {
    if (!flagDBSaved) {
        return "Unsaved panel changes will be lost if you leave the page, are you sure?";
    }
    else return;
};


// DASHBOARD SETTINGS PAGE
let editPanelFlag = false;
function handleDbSettings() {
    if ($('.panelEditor-container').css('display') !== 'none') {
        $('.panelEditor-container').hide();
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
        $('.panelEditor-container').show();
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