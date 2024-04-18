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

const newDashboardBtn = $(".new-dashboard-btn");
const existingDashboardBtn = $(".existing-dashboard-btn");
const newDashboard = $(".new-dashboard");
const existingDashboard = $(".existing-dashboard");
let newDashboardFlag = true;
let dashboardID;

$(document).ready(function () {
    existingDashboard.hide();
    $("#create-panel").hide();
    $("#create-db").show();
    newDashboardBtn.on("click", showNewDashboard);
    existingDashboardBtn.on("click", showExistingDashboard);

    $("#add-logs-to-db-btn").on("click", openPopup);
    $("#cancel-dbbtn, .popupOverlay").on("click", closePopup);
    $("#selected-dashboard").on("click", displayExistingDashboards);

});

function showNewDashboard() {
    newDashboardFlag = true;
    newDashboardBtn.addClass("active");
    existingDashboardBtn.removeClass("active");
    newDashboard.show();
    existingDashboard.hide();
    $("#create-panel").hide();
    $("#create-db").show();
}

function showExistingDashboard() {
    newDashboardFlag = false;
    existingDashboardBtn.addClass("active");
    newDashboardBtn.removeClass("active");
    existingDashboard.show();
    newDashboard.hide();
    $("#create-panel").show();
    $("#create-db").hide();
}

function openPopup() {
    $(".popupOverlay, .popupContent").addClass("active");
}

function closePopup() {
    $("#db-name").val("");
    $("#db-description").val("");
    $(".popupOverlay, .popupContent").removeClass("active");
    $(".error-tip").removeClass("active");
    $(".dashboard").removeClass("active");
    $("#selected-dashboard span").html("Choose Dashboard");
    newDashboardBtn.addClass("active");
    existingDashboardBtn.removeClass("active");
    newDashboard.show();
    existingDashboard.hide();
    $("#create-db").show();
    $("#create-panel").hide();
}

//Create panel in a new dashboard
function createPanelToNewDashboard() {
    var inputdbname = $("#db-name").val();
    var inputdbdescription = $("#db-description").val();
    var timeRange = data.startEpoch;
    var refresh = "";

    if (!inputdbname) {
        $(".error-tip").addClass("active");
    } else {
        $("#save-dbbtn").off("click");
        $(document).off("keypress");

        $.ajax({
            method: "post",
            url: "api/dashboards/create",
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                Accept: "*/*",
            },
            data: JSON.stringify(inputdbname),
            dataType: "json",
            crossDomain: true,
        }).then(function (res) {
            $("#db-name").val("");
            $("#db-description").val("");
            $(".error-tip").removeClass("active");
            $(".popupOverlay, .popupContent").removeClass("active");
            let panelCreatedFromLogs = createPanel(0);
            var dashboard = {
                id: Object.keys(res)[0],
                name: Object.values(res)[0],
                details: {
                    name: Object.values(res)[0],
                    description: inputdbdescription,
                    timeRange: timeRange,
                    refresh: refresh,
                    panels: [panelCreatedFromLogs],
                },
            };
            updateDashboard(dashboard);
            var queryString = "?id=" + Object.keys(res)[0];
            window.location.href = "../dashboard.html" + queryString;
        }).catch(function (updateError) {
			if (updateError.status === 409) {
			  $('.error-tip').text('Dashboard name already exists!');
			  $('.error-tip').addClass('active');
			}
		  });
    }
}

$("#create-db").click(function () {
    if (newDashboardFlag) createPanelToNewDashboard();
});

const existingDashboards = [];

// Display list of existing dashboards
function displayExistingDashboards() {
    $.ajax({
        method: "get",
        url: "api/dashboards/listall",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        crossDomain: true,
        dataType: "json",
    }).then(function (res) {
        if (res) {
            let dropdown = $("#dashboard-options");
            // Filtering default dashboards
            let defaultDashboardIds = [
                "10329b95-47a8-48df-8b1d-0a0a01ec6c42",
                "a28f485c-4747-4024-bb6b-d230f101f852",
                "bd74f11e-26c8-4827-bf65-c0b464e1f2a4",
                "53cb3dde-fd78-4253-808c-18e4077ef0f1"
            ];
            let additionalDashboards = Object.keys(res).filter(id => !defaultDashboardIds.includes(id) && !existingDashboards.includes(id));
            if (additionalDashboards.length === 0 && existingDashboards.length === 0) {
                // Add empty list item when there are no additional dashboards
                dropdown.html(`<li class="dashboard"></li>`);
            } else {
            $.each(res, function (id, dashboardName) {
                // exclude default dashboards
                if (!defaultDashboardIds.includes(id) && !existingDashboards.includes(id)) {
                    dropdown.append(`<li class="dashboard" id="${id}">${dashboardName}</li>`);
                    existingDashboards.push(id);
                }
            });
            dropdown.off("click", ".dashboard");
            dropdown.on("click", ".dashboard", selectDashboardHandler);
        }
    }
    });
}

// Select a existing dashboard
function selectDashboardHandler() {
    let selectedOption = $(this).html();
    $(".dashboard").removeClass("active");
    $("#selected-dashboard span").html(selectedOption);
    $(this).addClass("active");
    dashboardID = $(this).attr("id");
    let dashboard;
    
    // Get the selected dashboard details
    function createPanelToExistingDashboard() {
        $.ajax({
            method: "get",
            url: "api/dashboards/" + dashboardID,
            headers: {
                "Content-Type": "application/json; charset=utf-8",
                Accept: "*/*",
            },
            crossDomain: true,
            dataType: "json",
        }).then(function (res) {
            let dashboardDetails = res;
            if (!dashboardDetails.panels) {
                // If there is no existing Panel
                dashboardDetails.panels = [];
            }
            let panelCreatedFromLogs = createPanel(
                dashboardDetails.panels.length,
                dashboardDetails.panels[0]?.queryData?.startEpoch,
            );

            dashboardDetails = handlePanelPosition(
                dashboardDetails,
                panelCreatedFromLogs,
            );
            dashboard = {
                id: dashboardID,
                name: selectedOption,
                details: dashboardDetails,
            };
            updateDashboard(dashboard);
            var queryString = "?id=" + dashboardID;
            window.location.href = "../dashboard.html" + queryString;
        });
    }

    $("#create-panel").click(function () {
        if (!newDashboardFlag) createPanelToExistingDashboard();
    });
}

function handlePanelPosition(existingDashboard, newPanel) {
    const maxY = existingDashboard.panels.reduce((max, panel) => {
        return Math.max(max, panel.gridpos.y + panel.gridpos.h);
    }, 0);
    newPanel.gridpos.y = maxY + 20;
    existingDashboard.panels.push(newPanel);
    return existingDashboard;
}

function updateDashboard(dashboard) {
    $.ajax({
        method: "post",
        url: "api/dashboards/update",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        data: JSON.stringify(dashboard),
        dataType: "json",
        crossDomain: true,
    }).then(function (msg) {
        console.log("done:", msg);
    });
}

function createPanel(panelIndex, startEpoch) {
    let panelId = uuidv4();

    let panel = {
        chartType: "Data Table",
        dataType: "",
        description: "",
        gridpos: {
            h: 250,
            w: 653,
            wPercent: 0.49733434881949734,
            x: 10,
            y: 20,
        },
        logLinesViewType: "Table view",
        name: "New-Panel",
        panelId: panelId,
        panelIndex: panelIndex,
        queryData: {
            endEpoch: data.endEpoch,
            from: 0,
            indexName: data.indexName,
            queryLanguage: data.queryLanguage,
            searchText: data.searchText,
            startEpoch: startEpoch ? startEpoch : data.startEpoch,
            state: data.state,
        },
        queryType: "logs",
        unit: "",
    };
    return panel;
}

$('#alert-from-logs-btn').click(function() {
    var queryParams = {
        "queryLanguage": data.queryLanguage,
        "searchText": data.searchText,
        "startEpoch": data.startEpoch,
        "endEpoch": data.endEpoch,
    };
    var queryString = $.param(queryParams);

    // Open the alert.html in a new tab
    var newTab = window.open("../alert.html" + "?" + queryString, '_blank');
    newTab.focus();
});