var queryIndex = 0;
var queries = {};
var lineCharts = {};
let mergedGraph ;
$(document).ready(function() {
    addQueryElement();
});

$('#add-query').on('click', addQueryElement);

$('#add-formula').on('click', addFormulaElement);

$('#toggle-switch').on('change', function() {
    if ($(this).is(':checked')) {
        // If the toggle switch is checked, display individual graph containers
        $('#metrics-graphs').show();
        $('#merged-graph-container').hide();
    } else {
        // If the toggle switch is unchecked, hide individual graph containers and display merged graph container
        $('#metrics-graphs').hide();
        $('#merged-graph-container').show();
        // mergeGraphs();
    }
});

// $('#run').on('click', function() {
//     console.log(queries);
// });
function addFormulaElement(){
    let formulaElement = $(`
    <div class="metrics-query">
        <div>
            <div class="formula-arrow">↓</div>
            <input class="formula" placeholder="Formula, eg. 2*a">
        </div>
        <div>
            <div class="remove-query">×</div>
        </div>
    </div>`);

    $('#metrics-formula').append(formulaElement);

    // Add click event handler for the remove button
    formulaElement.find('.remove-query').on('click', function() {
        formulaElement.remove();
    });
}
function addQueryElement() {
    // Clone the first query element if it exists, otherwise create a new one
    var queryElement;
    if (queryIndex === 0) {
        queryElement = $(`
    <div class="metrics-query">
        <div class="query-box">
            <div class="query-name">${String.fromCharCode(97 + queryIndex)}</div>
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
        // Add visualization container for the query
        addVisualizationContainer(String.fromCharCode(97 + queryIndex), convertDataForChart(rawData1));
    } else {
        // Get the last query name
        var lastQueryName = $('#metrics-queries').find('.metrics-query:last .query-name').text();
        // Determine the next query name based on the last query name
        var nextQueryName = String.fromCharCode(lastQueryName.charCodeAt(0) + 1);
        
        queryElement = $('#metrics-queries').find('.metrics-query').last().clone();
        queryElement.find('.query-name').text(nextQueryName);
        // Add visualization container for the query
        $('#metrics-queries').append(queryElement);

        addVisualizationContainer(nextQueryName,convertDataForChart(rawData3));
    }
    

    // Show or hide the close icon based on the number of queries
    updateCloseIconVisibility();
    // Initialize autocomplete with the details of the previous query if it exists
    initializeAutocomplete(queryElement, queryIndex > 0 ? queries[String.fromCharCode(97 + queryIndex - 1)] : undefined);

    queryIndex++;

    // Add click event handler for the remove button
    queryElement.find('.remove-query').on('click', function() {
        var queryName = queryElement.find('.query-name').text();
        // Check if the query name exists in any of the formula input fields
        var queryNameExistsInFormula = $('.formula').toArray().some(function(formulaInput) {
            return $(formulaInput).val().includes(queryName);
        });

        // If query name exists in any formula, prevent removal of the query element
        if (queryNameExistsInFormula) {
            alert("Cannot remove query element because query name is used in a formula.");
        } else {
        delete queries[queryName];
        queryElement.remove();

        // Show or hide the close icon based on the number of queries
        updateCloseIconVisibility();

        // Remove corresponding visualization container
        removeVisualizationContainer(queryName);
    }
    });

    // Add click event handler for the alias button
    queryElement.find('.as-btn').on('click', function() {
        $(this).hide(); // Hide the "as..." button
        $(this).siblings('.alias-filling-box').show(); // Show the alias filling box
    });

    // Add click event handler for the alias close button
    queryElement.find('.alias-filling-box div').last().on('click', function() {
        $(this).parent().hide(); // Hide the alias filling box
        $(this).parent().siblings('.as-btn').show(); // Show the "as..." button
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
    // Close tag event handler
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

    // Close value event handler
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

// Define a global variable to store chart data
var chartDataCollection = {};

function addVisualizationContainer(queryName, seriesData) {
    // Create a new visualization container with a unique identifier
    var visualizationContainer = $(`
    <div class="metrics-graph" data-query="${queryName}">
        <div>Metrics query - ${queryName}</div>
        <div class="graph-canvas"></div>
    </div>`);

    $('#metrics-graphs').append(visualizationContainer);
    
    // Create a canvas element for the line chart
    var canvas = $('<canvas></canvas>');
    $('.graph-canvas').append(canvas);
    
    // Get the context of the canvas element
    var ctx = canvas[0].getContext('2d');
    
    // Extract labels and datasets from seriesData
    var labels = Object.keys(seriesData[0].values);
    var datasets = seriesData.map(function(series, index) {
        return {
            label: series.seriesName,
            data: Object.values(series.values),
            borderColor: getRandomColor(), // Choose a different color for each dataset
            borderWidth: 2,
            fill: false
        };
    });
    
    // Define chart data using extracted labels and datasets
    var chartData = {
        labels: labels,
        datasets: datasets
    };
    console.log(chartType);
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
                    align: 'start' // Align legend to the start (left)
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'X-Axis Label'
                    },
                    grid: {
                        display: false // Hide vertical grid lines
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Y-Axis Label'
                    }
                }
            }
        }
    });
    
    // Modify the fill property based on the chart type after chart initialization
    if (chartType === 'Area chart') {
        lineChart.config.data.datasets.forEach(function(dataset) {
            dataset.fill = true; // Fill area under the line
        });
    } else {
        // For other chart types, ensure fill is false
        lineChart.config.data.datasets.forEach(function(dataset) {
            dataset.fill = false;
        });
    }
    // Update the chart
    lineChart.update();

    lineCharts[queryName] = lineChart;
    updateGraphWidth();
    mergeGraphs(chartType)

    console.log(chartDataCollection);
}




function removeVisualizationContainer(queryName) {
    // Remove the visualization container corresponding to the given queryName
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

 // Options for Display and Color
 var displayOptions = ["Line chart", "Bar chart", "Area chart"];
 var colorOptions = ["Classic", "Cool", "Warm"];

 let chartType = "Line chart";
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
            var lineChart = lineCharts[queryName]; // Assuming you have stored chart instances in lineCharts object
            
            // Update chart type
            lineChart.config.type = chartJsType;
            
            // Update dataset options for area chart
            if (chartType === 'Area chart') {
                lineChart.config.data.datasets.forEach(function(dataset) {
                    dataset.fill = true; // Fill area under the line
                });
            } else {
                // For other chart types, ensure fill is false
                lineChart.config.data.datasets.forEach(function(dataset) {
                    dataset.fill = false;
                });
            }
            
            lineChart.update(); // Update the chart
        }
    }

    // Update merged graph as well
    mergeGraphs(chartType);
}


// Autocomplete for Display input
$("#display-input").autocomplete({
    source: displayOptions,
    minLength: 0,
    select: function(event, ui) {
        toggleLineOptions(ui.item.value);
        chartType = ui.item.value;
        toggleChartType(ui.item.value);
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

 // Autocomplete for Color input
 $("#color-input").autocomplete({
   source: colorOptions,
   minLength: 0
 }).on('click', function() {
    if ($(this).autocomplete('widget').is(':visible')) {
        $(this).autocomplete('close');
    } else {
        $(this).autocomplete('search', '');
    }
}).on('click', function() {
    $(this).select();
});

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

 // Options for Line Style and Stroke
 var lineStyleOptions = ["Solid", "Dash", "Dotted"];
 var strokeOptions = ["Normal", "Thin", "Thick"];

// Autocomplete for Line Style input
$("#line-style-input").autocomplete({
    source: lineStyleOptions,
    minLength: 0,
    select: function(event, ui) {
        var selectedLineStyle = ui.item.value;
        var selectedStroke = $("#stroke-input").val(); // Get the currently selected stroke
        updateLineCharts(selectedLineStyle, selectedStroke);
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

// Autocomplete for Stroke input
$("#stroke-input").autocomplete({
    source: strokeOptions,
    minLength: 0,
    select: function(event, ui) {
        var selectedStroke = ui.item.value;
        var selectedLineStyle = $("#line-style-input").val(); // Get the currently selected line style
        updateLineCharts(selectedLineStyle, selectedStroke);
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
                dataset.borderWidth = (stroke === "Thin") ? 1 : (stroke === "Thick") ? 3 : 2; // Adjust borderWidth as per stroke
            });

            // Update the chart with the modified data
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


function mergeGraphs(chartType) {
    // Create a canvas element for the merged graph
    var mergedCanvas = $('<canvas></canvas>');
    $('#merged-graph-container').empty().append(mergedCanvas);

    // Get the context of the canvas element
    var mergedCtx = mergedCanvas[0].getContext('2d');

    // Merge chart data into a single dataset
    var mergedData = {
        labels: [], // Combine labels from all datasets
        datasets: []
    };

    // Loop through chartDataCollection to merge datasets
    for (var queryName in chartDataCollection) {
        if (chartDataCollection.hasOwnProperty(queryName)) {
            // Merge datasets for the current query
            var datasets = chartDataCollection[queryName].datasets;
            datasets.forEach(function(dataset) {
                mergedData.datasets.push({
                    label: dataset.label, // Use dataset label
                    data: dataset.data,
                    borderColor: dataset.borderColor, // Use dataset border color
                    borderWidth: dataset.borderWidth,
                    fill: (chartType === 'Area chart') ? true : false // Update fill based on chart type
                });
            });

            // Update labels
            mergedData.labels = chartDataCollection[queryName].labels;
        }
    } 

    var mergedLineChart = new Chart(mergedCtx, {
        type: (chartType === 'Area chart') ? 'line' : (chartType === 'Bar chart') ? 'bar' : 'line',
        data: mergedData,
        options: {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'bottom',
                    align: 'start' // Align legend to the start (left)
                }
            },
            scales: {
                x: {
                    display: true,
                    title: {
                        display: true,
                        text: 'X-Axis Label'
                    },
                    grid: {
                        display: false // Hide vertical grid lines
                    }
                },
                y: {
                    display: true,
                    title: {
                        display: true,
                        text: 'Y-Axis Label'
                    }
                }
            }
        }
    });
    mergedGraph = mergedLineChart;
}



// Helper function to generate random color
function getRandomColor() {
    var letters = '0123456789ABCDEF';
    var color = '#';
    for (var i = 0; i < 6; i++) {
        color += letters[Math.floor(Math.random() * 16)];
    }
    return color;
}
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