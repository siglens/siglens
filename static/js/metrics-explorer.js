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

var queryIndex = 0;
var queries = {};
var lineCharts = {}; // Chart details
var chartDataCollection = {}; // Save label/data for each query
let mergedGraph ;
let chartType = "Line chart";

// Theme
let classic = ["#a3cafd", "#5795e4", "#d7c3fa", "#7462d8", "#f7d048", "#fbf09e"]
let purple = ["#dbcdfa", "#c8b3fb", "#a082fa", "#8862eb", "#764cd8", "#5f36ac", "#27064c"]
let cool =["#cce9be", "#a5d9b6", "#89c4c2", "#6cabc9", "#5491c8", "#4078b1", "#2f5a9f", "#213e7d" ]
let green = ["#d0ebc2", "#c4eab7", "#aed69e", "#87c37d", "#5daa64", "#45884a", "#2e6a34", "#1a431f" ]
let warm = ["#f7e288", "#fadb84", "#f1b65d", "#ec954d", "#f65630" , "#cf3926", "#aa2827", "#761727" ]
let orange = ["#f8ddbd", "#f4d2a9", "#f0b077", "#ec934f", "#e0722f", "#c85621", "#9b4116", "#72300e"]
let gray = ["#c6ccd1", "#adb1b9", "#8d8c96", "#93969e", "#7d7c87", "#656571", "#62636a", "#4c4d57"]
let palette = ["#5596c8", "#9c86cd", "#f9d038", "#66bfa1", "#c160c9", "#dd905a", "#4476c9", "#c5d741", "#9246b7", "#65d1d5", "#7975da", "#659d33", "#cf777e", "#f2ba46", "#59baee", "#cd92d8", "#508260", "#cf5081", "#a65c93", "#b0be4f"]

$(document).ready(function() {
    let stDate = "now-1h";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);

    $('.theme-btn').on('click', themePickerHandler);

    addQueryElement();
});

$('#add-query').on('click', addQueryElement);

$('#add-formula').on('click', addFormulaElement);

// Toggle switch between merged graph and single graphs 
$('#toggle-switch').on('change', function() {
    if ($(this).is(':checked')) {
        $('#metrics-graphs').show();
        $('#merged-graph-container').hide();
    } else {
        $('#metrics-graphs').hide();
        $('#merged-graph-container').show();
    }
});

function addFormulaElement(){
    let formulaElement = $(`
    <div class="formula-box">
        <div style="position: relative;" class="d-flex">
            <div class="formula-arrow">↓</div>
            <input class="formula" placeholder="Formula, eg. 2*a">
            <div class="formula-error-message" style="display: none;">
                <div class="d-flex justify-content-center align-items-center "><i class="fas fa-exclamation"></i></div>
            </div>
        </div>
        <div>
            <div class="remove-query">×</div>
        </div>
    </div>`);

    $('#metrics-formula').append(formulaElement);

    // Remove the formula element
    formulaElement.find('.remove-query').on('click', function() {
        formulaElement.remove();
        $('.metrics-query .remove-query').removeClass('disabled').css('cursor', 'pointer').removeAttr('title');;
    });

    // Validate formula on input change
    let input = formulaElement.find('.formula');
    input.on('input', function() {
        let formula = input.val().trim();
        let errorMessage = formulaElement.find('.formula-error-message');
        if (formula === '') {
            errorMessage.hide();
            input.removeClass('error-border');
            disableQueryRemoval();
            return
        }
        let valid = validateFormula(formula);
        if (valid) {
            errorMessage.hide();
            input.removeClass('error-border');
        } else {
            errorMessage.show();
            input.addClass('error-border');
        }
        // Disable remove button if the query name exists in any formula
        disableQueryRemoval();
    });
}

function validateFormula(formula) {
    let pattern = /^(\w+\s*([-+*/]\s*\w+\s*)*)*$/;
    let matches = formula.match(pattern);
    if (!matches) {
        return false;
    }

    let queryNames = Object.keys(chartDataCollection);
    let parts = formula.split(/[-+*/]/);
    for (let part of parts) {
        if (!queryNames.includes(part.trim())) {
            return false;
        }
    }

    return true;
}

function disableQueryRemoval(){
    // Loop through each query element
    $('.metrics-query').each(function() {
        var queryName = $(this).find('.query-name').text();
        var removeButton = $(this).find('.remove-query');
        var queryNameExistsInFormula = $('.formula').toArray().some(function(formulaInput) {
            return $(formulaInput).val().includes(queryName);
        });

        // If query name exists in any formula, disable the remove button
        if (queryNameExistsInFormula) {
            removeButton.addClass('disabled').css('cursor', 'not-allowed').attr('title', 'Query used in other formulas.');
        } else {
            removeButton.removeClass('disabled').css('cursor', 'pointer').removeAttr('title');
        }
    });
}

function addQueryElement() {
    // Clone the first query element if it exists, otherwise create a new one
    var queryElement;
    if (queryIndex === 0) {
        queryElement = $(`
    <div class="metrics-query">
        <div class="query-box">
            <div class="query-name active">${String.fromCharCode(97 + queryIndex)}</div>
            <input type="text" class="metrics" placeholder="Select a metric">
            <div>from</div>
            <div class="tag-container">
                <input type="text" class="everywhere" placeholder="(everywhere)">
            </div>
            <input class="agg-function" value="avg by">
            <div class="value-container">
                <input class="everything" placeholder="(everything)">
            </div>
        </div>
        <div>
            <div class="alias-box">
                <div class="as-btn">as...</div>
                <div class="alias-filling-box" style="display: none;">
                    <div>as</div>
                    <input type="text" placeholder="alias">
                    <div>×</div>
                </div>
            </div>
            <div class="remove-query">×</div>
        </div>
    </div>`);

    $('#metrics-queries').append(queryElement);
    addVisualizationContainer(String.fromCharCode(97 + queryIndex), convertDataForChart(rawData1));

    } else {
        // Get the last query name
        var lastQueryName = $('#metrics-queries').find('.metrics-query:last .query-name').text();
        // Determine the next query name based on the last query name
        var nextQueryName = String.fromCharCode(lastQueryName.charCodeAt(0) + 1);
        
        queryElement = $('#metrics-queries').find('.metrics-query').last().clone();
        queryElement.find('.query-name').text(nextQueryName);
        queryElement.find('.remove-query').removeClass('disabled').css('cursor', 'pointer').removeAttr('title');

        $('#metrics-queries').append(queryElement);
        addVisualizationContainer(nextQueryName,convertDataForChart(rawData3));
    }

    // Show or hide the query close icon based on the number of queries
    updateCloseIconVisibility();

    // Initialize autocomplete with the details of the previous query if it exists
    initializeAutocomplete(queryElement, queryIndex > 0 ? queries[String.fromCharCode(97 + queryIndex - 1)] : undefined);

    queryIndex++;

    // Remove query element
    queryElement.find('.remove-query').on('click', function() {
        var queryName = queryElement.find('.query-name').text();
        // Check if the query name exists in any of the formula input fields
        var queryNameExistsInFormula = $('.formula').toArray().some(function(formulaInput) {
            return $(formulaInput).val().includes(queryName);
        });

        // If query name exists in any formula, prevent removal of the query element
        if (queryNameExistsInFormula) {
            $(this).addClass('disabled').css('cursor', 'not-allowed').attr('title', 'Query used in other formulas.');
        } else {
            delete queries[queryName];
            queryElement.remove();
            removeVisualizationContainer(queryName);

            // Show or hide the close icon based on the number of queries
            updateCloseIconVisibility();
        }
    });

    // Alias button
    queryElement.find('.as-btn').on('click', function() {
        $(this).hide(); // Hide the "as..." button
        $(this).siblings('.alias-filling-box').show(); // Show alias input box
    });

    // Alias close button
    queryElement.find('.alias-filling-box div').last().on('click', function() {
        $(this).parent().hide();
        $(this).parent().siblings('.as-btn').show();
    });

    // Hide or Show query element and graph on click on query name
    queryElement.find('.query-name').on('click', function() {
        var queryNameElement = $(this);
        var queryName = queryNameElement.text();
        var numberOfGraphVisible = $('#metrics-graphs').children('.metrics-graph').filter(':visible').length;
        var metricsGraph = $('#metrics-graphs').find('.metrics-graph[data-query="' + queryName + '"]');

        if (numberOfGraphVisible > 1 || !metricsGraph.is(':visible')) {
            metricsGraph.toggle();
            queryNameElement.toggleClass('active');
        }
        numberOfGraphVisible = $('#metrics-graphs').children('.metrics-graph').filter(':visible').length;
        if (numberOfGraphVisible === 1) {
            $('.metrics-graph').addClass('full-width');
        } else {
            $('.metrics-graph').removeClass('full-width');
        }
    });
}

function initializeAutocomplete(queryElement, previousQuery = {}) {
    var queryDetails = {
        metrics: '',
        everywhere: [],
        everything: [],
        aggFunction: 'avg by'
    };

    // Use details from the previous query if it exists
    if (!jQuery.isEmptyObject(previousQuery)) {
        queryDetails.metrics = previousQuery.metrics;
        queryDetails.everywhere = previousQuery.everywhere.slice();
        queryDetails.everything = previousQuery.everything.slice();
        queryDetails.aggFunction = previousQuery.aggFunction;
    }

    var availableMetrics = [
        "system.cpu.interrupt",
        "system.disk.used",
        "system.cpu.stolen",
        "system.cpu.num_cores",
        "system.cpu.stolen",
        "system.cpu.idle",
        "system.cpu.guest",
        "system.cpu.system",
    ];

    var availableEverywhere = [
        "device:/dev/disk1s1",
        "device:/dev/disk1s2",
        "device:/dev/disk1s3",
        "device:/dev/disk1s4",
        "device:/dev/disk1s5",
        "device:/dev/disk1s6",
        "device_name:/disk1s1",
        "device_name:/disk1s2",
        "device_name:/disk1s3",
        "device_name:/disk1s4",
        "host:SonamSigScalr.local",
    ];

    var availableEverything = [
        "device",
        "device_name",
        "host",
    ];

    var availableOptions = ["max by", "min by", "avg by", "sum by"];

    // Metrics input
    queryElement.find('.metrics').autocomplete({
        source: availableMetrics,
        minLength: 0,
        select: function(event, ui) {
            queryDetails.metrics = ui.item.value;
            $(this).blur(); 
        }
    }).on('click', function() {
        if ($(this).autocomplete('widget').is(':visible')) {
            $(this).autocomplete('close');
        } else {
            $(this).autocomplete('search', '');
        }
    }).on('click', function() {
        $(this).select();
    }).on('close', function(event) {
        var selectedValue = $(this).val();
        if (selectedValue === '') {
            $(this).val(queryDetails.metrics);
        }
    }).on('keydown', function(event) {
        if (event.keyCode === 27) { // For the Escape key
            var selectedValue = $(this).val();
            if (selectedValue === '') {
                $(this).val(queryDetails.metrics);
            }else if (!availableMetrics.includes(selectedValue)) {
                $(this).val(queryDetails.metrics);
            } else {
                queryDetails.metrics = selectedValue;
            }
            $(this).blur(); 
        }
    }).on('change', function() {
        var selectedValue = $(this).val();
        if (!availableMetrics.includes(selectedValue)) {
            $(this).val(queryDetails.metrics);
        } else {
            queryDetails.metrics = selectedValue;
        }
        $(this).blur(); 
    });
    
    // Everywhere input (tag:value)
    queryElement.find('.everywhere').autocomplete({
        source: function(request, response) {
            var filtered = $.grep(availableEverywhere, function(item) {
                return item.toLowerCase().indexOf(request.term.toLowerCase()) !== -1;
            });
            response(filtered);
        },
        minLength: 0,
        select: function(event, ui) {
            addTag(ui.item.value);
            queryDetails.everywhere.push(ui.item.value);
            var index = availableEverywhere.indexOf(ui.item.value);
            if (index !== -1) {
                availableEverywhere.splice(index, 1);
            }
            $(this).val('');
            updateAutocompleteSource();
            return false;
        },
        open: function(event, ui) {
            var containerPosition = $(".tag-container").offset();

            $(this).autocomplete("widget").css({
                "position": "absolute",
                "top": containerPosition.top + $(".tag-container").outerHeight(),
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
    }).on('input', function() {
        this.style.width = (this.value.length * 8) + 'px'; 
        let typedValue = $(this).val();
        
        // Remove the wildcard option from available options when the input value changes
        if (!typedValue.includes(':')) {
            availableEverywhere = availableEverywhere.filter(function(option) {
                return !option.includes(':*');
            });
        }
        
        // Add the wildcard option if the typed value contains a colon ":"
        if (typedValue.includes(':')) {
            var parts = typedValue.split(':');
            var prefix = parts[0];
            var suffix = parts[1];
            var wildcardOption = prefix + ':' + suffix + '*';
            
            availableEverywhere = availableEverywhere.filter(function(option) {
                return !option.includes('*');
            });
            // Check if the typed value already exists in the available options
            if (!availableEverywhere.includes(typedValue)) {
                availableEverywhere.unshift(wildcardOption);
            }
        }
        updateAutocompleteSource();
    });

    function addTag(value) {
        var tagContainer = queryElement.find('.everywhere');
        var tag = $('<span class="tag">' + value + '<span class="close">×</span></span>');
        tagContainer.before(tag);

        if (queryElement.find('.tag-container').find('.tag').length === 0) {
            tagContainer.attr('placeholder', '(everywhere)');
            tagContainer.css('width', '100%');
        } else {
            tagContainer.removeAttr('placeholder');
            tagContainer.css('width', '5px');
        }
    }
    
    queryElement.on('click', '.tag .close', function() {
        var tagContainer = queryElement.find('.everywhere');

        var tagValue = $(this).parent().contents().filter(function() {
            return this.nodeType === 3;
        }).text().trim();
        var index = queryDetails.everywhere.indexOf(tagValue);
        if (index !== -1) {
            queryDetails.everywhere.splice(index, 1);
        }
        availableEverywhere.push(tagValue);
        queryElement.find('.everywhere').autocomplete('option', 'source', availableEverywhere);

        $(this).parent().remove();

        if (queryElement.find('.tag-container').find('.tag').length === 0) {
            tagContainer.attr('placeholder', '(everywhere)');
            tagContainer.css('width', '100%');
        }
        updateAutocompleteSource(); 
    });

    // Aggregation input 
    queryElement.find('.agg-function').autocomplete({
        source: availableOptions.sort(),
        minLength: 0,
        select: function(event, ui) {
            queryDetails.aggFunction = ui.item.value;
        }
    }).on('click', function() {
        if ($(this).autocomplete('widget').is(':visible')) {
            $(this).autocomplete('close');
        } else {
            $(this).autocomplete('search', '');
        }
    }).on('click', function() {
        $(this).select();
    });

    // Everything input (value)
    queryElement.find('.everything').autocomplete({
        source: function(request, response) {
            var filtered = $.grep(availableEverything, function(item) {
                return item.toLowerCase().indexOf(request.term.toLowerCase()) !== -1;
            });
            response(filtered);
        },
        minLength: 0,
        select: function(event, ui) {
            addValue(ui.item.value);
            queryDetails.everything.push(ui.item.value);
            var index = availableEverything.indexOf(ui.item.value);
            if (index !== -1) {
                availableEverything.splice(index, 1);
            }
            $(this).val('');
            return false;        
        },
        open: function(event, ui) {
            var containerPosition = $(".value-container").offset();

            $(this).autocomplete("widget").css({
                "position": "absolute",
                "top": containerPosition.top + $(".value-container").outerHeight(),
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
        }).on('input', function() {
            this.style.width = (this.value.length * 8) + 'px'; 
        })

    function addValue(value) {
        var valueContainer = queryElement.find('.everything');
        var value = $('<span class="value">' + value + '<span class="close">×</span></span>');
        valueContainer.before(value);

        if (queryElement.find('.value-container').find('.value').length === 0) {
            valueContainer.attr('placeholder', '(everything)');
            valueContainer.css('width', '100%');
        } else {
            valueContainer.removeAttr('placeholder');
            valueContainer.css('width', '5px');
        }
    }

    queryElement.on('click', '.value .close', function() {
        var valueContainer = queryElement.find('.everything');

        var value = $(this).parent().contents().filter(function() {
            return this.nodeType === 3;
        }).text().trim();
        var index = queryDetails.everything.indexOf(value);
        if (index !== -1) {
            queryDetails.everything.splice(index, 1);
        }
        availableEverything.push(value);

        queryElement.find('.everything').autocomplete('option', 'source', availableEverything);

        $(this).parent().remove();

        if (queryElement.find('.value-container').find('.value').length === 0) {
            valueContainer.attr('placeholder', '(everything)');
            valueContainer.css('width', '100%');
        }
    });

    // Wildcard option
    function updateAutocompleteSource() {
        var selectedTags = queryDetails.everywhere.map(function(tag) {
            return tag.split(':')[0];
        });
        var filteredOptions = availableEverywhere.filter(function(option) {
            var optionTag = option.split(':')[0];
            return !selectedTags.includes(optionTag);
        });
        queryElement.find('.everywhere').autocomplete('option', 'source', filteredOptions);
    }

    queries[queryElement.find('.query-name').text()] = queryDetails;
    previousQuery = queryDetails;
}

function updateCloseIconVisibility() {
    var numQueries = $('#metrics-queries').children('.metrics-query').length;
    $('.remove-query').toggle(numQueries > 1);
}

function addVisualizationContainer(queryName, seriesData) {

    var visualizationContainer = $(`
    <div class="metrics-graph" data-query="${queryName}">
        <div>Metrics query - ${queryName}</div>
        <div class="graph-canvas"></div>
    </div>`);

    $('#metrics-graphs').append(visualizationContainer);
    
    var canvas = $('<canvas></canvas>');
    $(`.metrics-graph[data-query="${queryName}"] .graph-canvas`).append(canvas);
    
    var ctx = canvas[0].getContext('2d');
    
    // Extract labels and datasets from seriesData
    var labels = Object.keys(seriesData[0].values);
    var datasets = seriesData.map(function(series, index) {
        return {
            label: series.seriesName,
            data: Object.values(series.values),
            borderColor: classic[index % classic.length],
            backgroundColor : classic[index % classic.length] + 70,
            borderWidth: 2,
            fill: false
        };
    });
    
    var chartData = {
        labels: labels,
        datasets: datasets
    };

    // Save chart data to the global variable
    chartDataCollection[queryName] = chartData;

    var lineChart = new Chart(ctx, {
        type: (chartType === 'Area chart') ? 'line' : (chartType === 'Bar chart') ? 'bar' : 'line',
        data: chartData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    align: 'start'
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: ''
                    },
                    grid: {
                        display: false
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: false,
                    }
                }
            }
        }
    });
    
    // Modify the fill property based on the chart type after chart initialization
    if (chartType === 'Area chart') {
        lineChart.config.data.datasets.forEach(function(dataset) {
            dataset.fill = true;
        });
    } else {
        lineChart.config.data.datasets.forEach(function(dataset) {
            dataset.fill = false;
        });
    }

    lineChart.update();

    lineCharts[queryName] = lineChart;
    updateGraphWidth();
    mergeGraphs(chartType)
}

function removeVisualizationContainer(queryName) {
    var containerToRemove = $('#metrics-graphs').find('.metrics-graph[data-query="' + queryName + '"]');
    containerToRemove.remove();
    delete chartDataCollection[queryName];
    delete lineCharts[queryName];
    updateGraphWidth();
    mergeGraphs(chartType)
}

function updateGraphWidth() {
    var numQueries = $('#metrics-queries').children('.metrics-query').length;
    if (numQueries === 1) {
        $('.metrics-graph').addClass('full-width');
    } else {
        $('.metrics-graph').removeClass('full-width');
    }
}

// Function to show/hide Line Style and Stroke based on Display input
function toggleLineOptions(displayValue) {
    if (displayValue === "Line chart") {
        $("#line-style-div").show();
        $("#stroke-div").show();
    } else {
        $("#line-style-div").hide();
        $("#stroke-div").hide();
    }
}

var displayOptions = ["Line chart", "Bar chart", "Area chart"];
$("#display-input").autocomplete({
    source: displayOptions,
    minLength: 0,
    select: function(event, ui) {
        toggleLineOptions(ui.item.value);
        chartType = ui.item.value;
        toggleChartType(ui.item.value);
        $(this).blur();
    }
}).on('click', function() {
    if ($(this).autocomplete('widget').is(':visible')) {
        $(this).autocomplete('close');
    } else {
        $(this).autocomplete('search', '');
    }
}).on('click', function() {
    $(this).select();
});

function toggleChartType(chartType) {
    // Convert the selected chart type to the corresponding Chart.js chart type
    var chartJsType;
    switch (chartType) {
        case 'Line chart':
            chartJsType = 'line';
            break;
        case 'Bar chart':
            chartJsType = 'bar';
            break;
        case 'Area chart':
            chartJsType = 'line'; // Area chart is essentially a line chart with fill
            break;
        default:
            chartJsType = 'line'; // Default to line chart
    }

    // Loop through each chart data
    for (var queryName in chartDataCollection) {
        if (chartDataCollection.hasOwnProperty(queryName)) {
            var lineChart = lineCharts[queryName];
            
            lineChart.config.type = chartJsType;
            
            if (chartType === 'Area chart') {
                lineChart.config.data.datasets.forEach(function(dataset) {
                    dataset.fill = true;
                });
            } else {
                lineChart.config.data.datasets.forEach(function(dataset) {
                    dataset.fill = false;
                });
            }
            
            lineChart.update();
        }
    }
    
    mergeGraphs(chartType);
}


var colorOptions = ["Classic", "Purple", "Cool", "Green", "Warm", "Orange", "Gray", "D2d0"];
$("#color-input").autocomplete({
   source: colorOptions,
   minLength: 0,
   select: function(event,ui){
        selectedColorTheme = ui.item.value;
        updateChartTheme(selectedColorTheme);
        $(this).blur();
   }
 }).on('click', function() {
    if ($(this).autocomplete('widget').is(':visible')) {
        $(this).autocomplete('close');
    } else {
        $(this).autocomplete('search', '');
    }
}).on('click', function() {
    $(this).select();
});

function updateChartTheme(theme) {
    var colorPalette = {
        "Classic": classic,
        "Purple": purple,
        "Cool": cool,
        "Green": green,
        "Warm": warm,
        "Orange": orange,
        "Gray": gray,
        "D2d0": d2d0
    };

    var selectedPalette = colorPalette[theme] || classic;

    // Loop through each chart data
    for (var queryName in chartDataCollection) {
        if (chartDataCollection.hasOwnProperty(queryName)) {
            var chartData = chartDataCollection[queryName];
            chartData.datasets.forEach(function(dataset, index) {
                dataset.borderColor = selectedPalette[index % selectedPalette.length];
                dataset.backgroundColor = selectedPalette[index % selectedPalette.length] + 70; // opacity
            });

            var lineChart = lineCharts[queryName]; 
            lineChart.update();
        }
    }

    mergedGraph.data.datasets.forEach(function(dataset, index) {
        dataset.borderColor = selectedPalette[index % selectedPalette.length];
        dataset.backgroundColor = selectedPalette[index % selectedPalette.length] + 70;
    });
    mergedGraph.update();
}

var lineStyleOptions = ["Solid", "Dash", "Dotted"];
var strokeOptions = ["Normal", "Thin", "Thick"];

$("#line-style-input").autocomplete({
    source: lineStyleOptions,
    minLength: 0,
    select: function(event, ui) {
        var selectedLineStyle = ui.item.value;
        var selectedStroke = $("#stroke-input").val();
        updateLineCharts(selectedLineStyle, selectedStroke);
        $(this).blur();
    }
}).on('click', function() {
    if ($(this).autocomplete('widget').is(':visible')) {
        $(this).autocomplete('close');
    } else {
        $(this).autocomplete('search', '');
    }
}).on('click', function() {
    $(this).select();
});

$("#stroke-input").autocomplete({
    source: strokeOptions,
    minLength: 0,
    select: function(event, ui) {
        var selectedStroke = ui.item.value;
        var selectedLineStyle = $("#line-style-input").val();
        updateLineCharts(selectedLineStyle, selectedStroke);
        $(this).blur();
    }
}).on('click', function() {
    if ($(this).autocomplete('widget').is(':visible')) {
        $(this).autocomplete('close');
    } else {
        $(this).autocomplete('search', '');
    }
}).on('click', function() {
    $(this).select();
});

// Function to update all line charts based on selected line style and stroke
function updateLineCharts(lineStyle, stroke) {
    // Loop through each chart data
    for (var queryName in chartDataCollection) {
        if (chartDataCollection.hasOwnProperty(queryName)) {
            var chartData = chartDataCollection[queryName];
            // Loop through each dataset in the chart data
            chartData.datasets.forEach(function(dataset) {
                // Update dataset properties
                dataset.borderDash = (lineStyle === "Dash") ? [5, 5] : (lineStyle === "Dotted") ? [1, 3] : [];
                dataset.borderWidth = (stroke === "Thin") ? 1 : (stroke === "Thick") ? 3 : 2; 
            });

            var lineChart = lineCharts[queryName]; 
            lineChart.update();
        }
    }
    mergedGraph.data.datasets.forEach(function(dataset) {
        dataset.borderDash = (lineStyle === "Dash") ? [5, 5] : (lineStyle === "Dotted") ? [1, 3] : [];
        dataset.borderWidth = (stroke === "Thin") ? 1 : (stroke === "Thick") ? 3 : 2; 
    });

    mergedGraph.update();
}

// Merge Graphs in one
function mergeGraphs(chartType) {
    var visualizationContainer = $(`
        <div class="merged-graph-name"></div>
        <div class="merged-graph"></div>`);

    $('#merged-graph-container').empty().append(visualizationContainer);
    
    var mergedCanvas = $('<canvas></canvas>');

    $('.merged-graph').empty().append(mergedCanvas);
    var mergedCtx = mergedCanvas[0].getContext('2d');

    var mergedData = {
        labels: [],
        datasets: []
    };
    var graphNames = [];

    // Loop through chartDataCollection to merge datasets
    for (var queryName in chartDataCollection) {
        if (chartDataCollection.hasOwnProperty(queryName)) {
            // Merge datasets for the current query
            var datasets = chartDataCollection[queryName].datasets;
            graphNames.push(`Metrics query - ${queryName}`); 
            datasets.forEach(function(dataset) {
                mergedData.datasets.push({
                    label: dataset.label,
                    data: dataset.data,
                    borderColor: dataset.borderColor,
                    borderWidth: dataset.borderWidth,
                    fill: (chartType === 'Area chart') ? true : false 
                });
            });

            // Update labels ( same for all graphs)
            mergedData.labels = chartDataCollection[queryName].labels;
        }
    } 
    $('.merged-graph-name').html(graphNames.join(', '));
    var mergedLineChart = new Chart(mergedCtx, {
        type: (chartType === 'Area chart') ? 'line' : (chartType === 'Bar chart') ? 'bar' : 'line',
        data: mergedData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    align: 'start' 
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: ''
                    },
                    grid: {
                        display: false 
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: false,
                    }
                }
            }
        }
    });
    mergedGraph = mergedLineChart;
}

// Converting the response in form to use to create graphs
function convertDataForChart(data) {
    let seriesArray = [];

    // Iterate over each metric in the data
    for (let metric in data.aggStats) {
        if (data.aggStats.hasOwnProperty(metric)) {
            let series = {
                seriesName: metric,
                values: {}
            };

            // Extract timestamp-value pairs for the metric
            for (let timestamp in data.aggStats[metric]) {
                if (data.aggStats[metric].hasOwnProperty(timestamp)) {
                    series.values[timestamp] = data.aggStats[metric][timestamp];
                }
            }

            seriesArray.push(series);
        }
    }

    return seriesArray;
}

// Example usage:
let rawData1 = {
    "aggStats": {
        "metric1-1": {
            "2024-04-26T07:06": 10,
            "2024-04-26T07:07": 20,
            "2024-04-26T07:08": 30,
            "2024-04-26T07:09": 10,
            "2024-04-26T07:10": 40,
            "2024-04-26T07:11": 20,
            "2024-04-26T07:12": 30,
            "2024-04-26T07:13": 28,
            "2024-04-26T07:14": 18,
            "2024-04-26T07:15": 38
        },
        "metric1-2": {
            "2024-04-26T07:06": 29,
            "2024-04-26T07:07": 39,
            "2024-04-26T07:08": 19,
            "2024-04-26T07:09": 49,
            "2024-04-26T07:10": 29,
            "2024-04-26T07:11": 19,
            "2024-04-26T07:12": 39,
            "2024-04-26T07:13": 29,
            "2024-04-26T07:14": 49,
            "2024-04-26T07:15": 19
        }
    }
}

let rawData2 = {
    "aggStats": {
        "metric2-1": {
            "2024-04-26T07:06": 11,
            "2024-04-26T07:07": 12,
            "2024-04-26T07:08": 13,
            "2024-04-26T07:09": 10,
            "2024-04-26T07:10": 4,
            "2024-04-26T07:11": 21,
            "2024-04-26T07:12": 32,
            "2024-04-26T07:13": 2,
            "2024-04-26T07:14": 10,
            "2024-04-26T07:15": 3
        },
        "metric2-2": {
            "2024-04-26T07:06": 21,
            "2024-04-26T07:07": 3,
            "2024-04-26T07:08": 7,
            "2024-04-26T07:09": 8,
            "2024-04-26T07:10": 12,
            "2024-04-26T07:11": 1,
            "2024-04-26T07:12": 32,
            "2024-04-26T07:13": 20,
            "2024-04-26T07:14": 4,
            "2024-04-26T07:15": 19
        }
    }
}

let rawData3= {
    "aggStats": {
        "metric3-1": {
            "2024-04-26T07:06": 110,
            "2024-04-26T07:07": 120,
            "2024-04-26T07:08": 130,
            "2024-04-26T07:09": 100,
            "2024-04-26T07:10": 40,
            "2024-04-26T07:11": 210,
            "2024-04-26T07:12": 320,
            "2024-04-26T07:13": 20,
            "2024-04-26T07:14": 100,
            "2024-04-26T07:15": 30
        }
    }
}