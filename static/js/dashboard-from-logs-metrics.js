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

const newDashboardBtn = $('.new-dashboard-btn');
const existingDashboardBtn = $('.existing-dashboard-btn');
const newDashboard = $('.new-dashboard');
const existingDashboard = $('.existing-dashboard');
let newDashboardFlag = true;
let dashboardID;

$(document).ready(function () {
    existingDashboard.hide();
    $('#create-panel').hide();
    $('#create-db').show();
    newDashboardBtn.on('click', showNewDashboard);
    existingDashboardBtn.on('click', showExistingDashboard);

    $('#add-logs-to-db-btn').on('click', openPopup);
    $('#add-metrics-to-db-btn').on('click', openPopup);

    $('#cancel-dbbtn, .popupOverlay').on('click', closePopup);
    $('#selected-dashboard').on('click', function () {
        if (!$('#dashboard-options').hasClass('show')) {
            displayExistingDashboards();
        }
    });

    $('#alert-from-logs-btn').click(function () {
        $('.addrulepopupOverlay, .addrulepopupContent').addClass('active');
    });

    $('#addrule-cancel-btn').click(function () {
        $('#rule-name').tooltip('hide');
        $('#rule-name').val('');
        $('.rule-name-error').removeClass('active').text('');
        $('.addrulepopupOverlay, .addrulepopupContent').removeClass('active');
    });
    $('#addrule-save-btn').click(function () {
        var ruleName = $('#rule-name').val().trim(); // Trim whitespace

        if (!ruleName) {
            $('.rule-name-error').addClass('active').text('Rule name cannot be empty!');
            return;
        }
        $('.rule-name-error').removeClass('active').text(''); // Clear error message if ruleName is not empty

        var encodedRuleName = encodeURIComponent(ruleName);
        const urlParams = new URLSearchParams(window.location.search);
        const filterTab = urlParams.get('filterTab');

        $('#rule-name').tooltip('hide');
        $('#rule-name').val('');
        $('.rule-name-error').removeClass('active').text('');
        $('.addrulepopupOverlay, .addrulepopupContent').removeClass('active');

        var queryParams = {
            queryLanguage: data.queryLanguage,
            searchText: data.searchText,
            startEpoch: data.startEpoch,
            endEpoch: data.endEpoch,
            filterTab: filterTab,
            alertRule_name: encodedRuleName,
        };

        var queryString = $.param(queryParams);
        window.open('../alert.html?' + queryString, '_blank');
    });
    var currentPage = window.location.pathname;
    if (currentPage === '/metrics-explorer.html') {
        //eslint-disable-next-line no-undef
        isMetricsScreen = true;
    }
});

function showNewDashboard() {
    newDashboardFlag = true;
    newDashboardBtn.addClass('active');
    existingDashboardBtn.removeClass('active');
    newDashboard.show();
    existingDashboard.hide();
    $('#create-panel').hide();
    $('#create-db').show();
}

function showExistingDashboard() {
    newDashboardFlag = false;
    existingDashboardBtn.addClass('active');
    newDashboardBtn.removeClass('active');
    existingDashboard.show();
    newDashboard.hide();
    $('#create-panel').show();
    $('#create-db').hide();
}

function openPopup() {
    $('.popupOverlay, #create-db-popup.popupContent').addClass('active');
}

function closePopup() {
    $('#db-name').val('');
    $('#db-description').val('');
    $('.popupOverlay, .popupContent').removeClass('active');
    $('.error-tip').removeClass('active');
    $('.dashboard').removeClass('active');
    $('#selected-dashboard span').html('Choose Dashboard');
    newDashboardBtn.addClass('active');
    existingDashboardBtn.removeClass('active');
    newDashboard.show();
    existingDashboard.hide();
    $('#create-db').show();
    $('#create-panel').hide();
}

//Create panel in a new dashboard
function createPanelToNewDashboard() {
    var inputdbname = $('#db-name').val();
    var inputdbdescription = $('#db-description').val();
    const dashboardData = {
        name: inputdbname,
        description: inputdbdescription,
        parentId: 'root-folder',
    };
    var timeRange;
    //eslint-disable-next-line no-undef
    if (isMetricsScreen) {
        let panelMetricsQueryParams = getMetricsQData();
        if (panelMetricsQueryParams.queriesData?.[0]?.start != undefined) {
            timeRange = panelMetricsQueryParams.queriesData[0].start;
        } else {
            timeRange = filterStartDate;
        }
    } else {
        timeRange = data.startEpoch;
    }
    var refresh = '';

    if (!inputdbname) {
        $('.error-tip').addClass('active');
    } else {
        $('#save-dbbtn').off('click');
        $(document).off('keypress');

        $.ajax({
            method: 'post',
            url: 'api/dashboards/create',
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            data: JSON.stringify(dashboardData),
        })
            .then(function (res) {
                $('#db-name').val('');
                $('#db-description').val('');
                $('.error-tip').removeClass('active');
                $('.popupOverlay, .popupContent').removeClass('active');
                let panelCreatedFromLogs = createPanel(0);
                var dashboard = {
                    id: Object.keys(res)[0],
                    name: Object.values(res)[0],
                    details: {
                        name: Object.values(res)[0],
                        description: inputdbdescription,
                        timeRange: timeRange,
                        refresh: refresh,
                        panels: [
                            {
                                ...panelCreatedFromLogs,
                                style: {
                                    display: panelCreatedFromLogs.style?.display || 'Line chart',
                                    color: panelCreatedFromLogs.style?.color || 'Classic',
                                    lineStyle: panelCreatedFromLogs.style?.lineStyle || 'Solid',
                                    lineStroke: panelCreatedFromLogs.style?.lineStroke || 'Normal',
                                },
                            },
                        ],
                    },
                };
                updateDashboard(dashboard);
                var queryString = '?id=' + Object.keys(res)[0];
                window.open('../dashboard.html' + queryString, '_blank');
            })
            .catch(function (updateError) {
                if (updateError.status === 409) {
                    $('.error-tip').text('Dashboard name already exists!');
                    $('.error-tip').addClass('active');
                }
            });
    }
}

$('#create-db').click(function () {
    if (newDashboardFlag) createPanelToNewDashboard();
});

// Display list of existing dashboards
function displayExistingDashboards() {
    $.ajax({
        method: 'get',
        url: 'api/dashboards/list?type=dashboard',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        crossDomain: true,
        dataType: 'json',
    }).then(function (res) {
        if (res) {
            let dropdown = $('#dashboard-options');
            // Clear existing content first
            dropdown.empty();

            if (res.items.length === 0) {
                dropdown.html(`<li class="dashboard">No Dashboards</li>`);
            } else {
                $.each(res.items, function (id, dashboard) {
                    dropdown.append(`<li class="dashboard" id="${dashboard.id}">${dashboard.fullPath}</li>`);
                });
            }
            dropdown.off('click', '.dashboard');
            dropdown.on('click', '.dashboard', selectDashboardHandler);
        }
    });
}

// Select a existing dashboard
function selectDashboardHandler() {
    let selectedOption = $(this).html();
    $('.dashboard').removeClass('active');
    $('#selected-dashboard span').html(selectedOption);
    $(this).addClass('active');
    dashboardID = $(this).attr('id');
    let dashboard;

    // Get the selected dashboard details
    function createPanelToExistingDashboard() {
        $.ajax({
            method: 'get',
            url: 'api/dashboards/' + dashboardID,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
            dataType: 'json',
        }).then(function (res) {
            let dashboardDetails = res;
            if (!dashboardDetails.panels) {
                // If there is no existing Panel
                dashboardDetails.panels = [];
            }
            let panelCreatedFromLogs = createPanel(dashboardDetails.panels.length, dashboardDetails.panels[0]?.queryData?.startEpoch);

            dashboardDetails = handlePanelPosition(dashboardDetails, panelCreatedFromLogs);
            dashboard = {
                id: dashboardID,
                name: selectedOption,
                details: dashboardDetails,
            };
            updateDashboard(dashboard);
            var queryString = '?id=' + dashboardID;
            window.open('../dashboard.html' + queryString, '_blank');
        });
    }

    $('#create-panel').click(function () {
        if (!newDashboardFlag) createPanelToExistingDashboard();
    });
}

function handlePanelPosition(existingDashboard, newPanel) {
    if (!existingDashboard.panels || existingDashboard.panels.length === 0) {
        // If there are no existing panels
        newPanel.gridpos.x = '0';
        newPanel.gridpos.y = '0';
    } else {
        const maxY = existingDashboard.panels.reduce((max, panel) => {
            return Math.max(max, panel.gridpos.y + panel.gridpos.h);
        }, 0);
        const maxX = existingDashboard.panels.reduce((max, panel) => {
            return Math.max(max, panel.gridpos.x + panel.gridpos.w);
        });
        newPanel.gridpos.y = maxY + 20;
        newPanel.gridpos.x = maxX + 20;
    }
    existingDashboard.panels.push(newPanel);
    return existingDashboard;
}

function updateDashboard(dashboard) {
    $.ajax({
        method: 'post',
        url: 'api/dashboards/update',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify(dashboard),
        dataType: 'json',
        crossDomain: true,
    }).then(function (msg) {
        console.log('done:', msg);
    });
}

function createPanel(panelIndex, startEpoch) {
    let panelId = uuidv4();
    let panel;
    //eslint-disable-next-line no-undef
    if (isMetricsScreen) {
        let panelMetricsQueryParams = getMetricsQData();
        panelMetricsQueryParams.start = startEpoch;
        panel = {
            chartType: 'Line Chart',
            queryType: 'metrics',
            description: '',
            gridpos: {
                h: 2,
                w: 4,
                x: 2,
                y: 0,
            },
            name: `panel${panelIndex}`,
            panelId: panelId,
            panelIndex: panelIndex,
            queryData: panelMetricsQueryParams,
            style: {
                //eslint-disable-next-line no-undef
                display: chartType,
                //eslint-disable-next-line no-undef
                color: selectedTheme,
                //eslint-disable-next-line no-undef
                lineStyle: selectedLineStyle,
                //eslint-disable-next-line no-undef
                lineStroke: selectedStroke,
            },
        };
    } else {
        const queryMode = new URLSearchParams(window.location.search).get('filterTab') === '0' ? 'Builder' : 'Code';
        panel = {
            chartType: 'Data Table',
            dataType: '',
            description: '',
            gridpos: {
                h: 2,
                w: 4,
                x: 2,
                y: 0,
            },
            logLinesViewType: 'Table view',
            name: `panel${panelIndex}`,
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
                queryMode,
            },
            queryType: 'logs',
            unit: '',
        };
    }
    return panel;
}
