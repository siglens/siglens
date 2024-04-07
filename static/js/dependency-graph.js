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

"use strict";

const colorArray = [
    "#6347D9",
    "#01BFB3",
    "#E9DC6E",
    "#F2A52B",
    "#4BAE7F",
    "#9178C5",
    "#23A9E2",
    "#8C706B",
    "#22589D",
    "#B33B97",
    "#9FBF46",
    "#BF9A68",
    "#DC756F",
    "#E55D9A",
    "#597C53",
];
let colorMap = {};
let graphData;
let cy;

const lightStyles = {
    edgeColor: "#6F6B7B",
    labelColor: "#262038",
    labelbgColor: "#FFF"
};

const darkStyles = {
    edgeColor: "#DCDBDF",
    labelColor: "#FFF",
    labelbgColor: "#262038"
};

$(document).ready(() => {
  let stDate = "now-1h";
    let endDate = "now";
    datePickerHandler(stDate, endDate, stDate);
    setupDependencyEventHandlers()

    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $(".theme-btn").on("click", themePickerHandler);
    $('.theme-btn').on('click', function() {
      updateGraphStyles(theme);
    });

    $("#error-msg-container, #dependency-info").hide();
    getServiceDependencyData(stDate, endDate);

    $("#dependency-info").tooltip({
      delay: { show: 0, hide: 300 },
      trigger: 'click'
    });
  
    $('#dependency-info').on('click', function (e) {
      $('#dependency-info').tooltip('show');
    });
  
    $(document).mouseup(function (e) {
      if ($(e.target).closest(".tooltip-inner").length === 0) {
        $('#dependency-info').tooltip('hide');
      }
    });
});

function rangeItemHandler(evt) {
  resetCustomDateRange();
  $.each($(".range-item.active"), function () {
      $(this).removeClass('active');
  });
  $(evt.currentTarget).addClass('active');
  const start = $(this).attr('id')
  const end = "now"
  const label = $(this).attr('id')
  datePickerHandler(start, end, label)
  getServiceDependencyData(start, end)
}

function resetDatePickerHandler(evt) {
  evt.stopPropagation();
  resetCustomDateRange();
  $.each($(".range-item.active"), function () {
      $(this).removeClass('active');
  });

}

function showDatePickerHandler(evt) {
  evt.stopPropagation();
  $('#daterangepicker').toggle();
  $(evt.currentTarget).toggleClass('active');
}

function hideDatePickerHandler() {
  $('#daterangepicker').removeClass('active');
}

function setupDependencyEventHandlers(){
    $('#date-picker-btn').on('show.bs.dropdown', showDatePickerHandler);
    $('#date-picker-btn').on('hide.bs.dropdown', hideDatePickerHandler);
    $('#reset-timepicker').on('click', resetDatePickerHandler);

    $('#time-start').on('change', getStartTimeHandler);
    $('#time-end').on('change', getEndTimeHandler);
    $('#customrange-btn').on('click', customRangeHandler);

    $('.range-item').on('click', rangeItemHandler)
}

function getServiceDependencyData(start, end) {
    d3.select("#dependency-graph-container").selectAll("svg").remove();

    $.ajax({
        method: "POST",
        url: "api/traces/dependencies",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
        data: JSON.stringify({
          startEpoch: start || "now-1h",
          endEpoch: end || "now"
        }),
        crossDomain: true,
        success: function (res) {
            if ($.isEmptyObject(res)) {
                $("#dependency-graph-container").hide();
                $("#error-msg-container").show()
            } else {
                $("#dependency-graph-container,#dependency-info").show();
                $("#error-msg-container").hide();
                graphData = res;
                displayDependencyGraph(graphData);
                var lastRunTimestamp =moment(res.timestamp);
                var formattedDate = lastRunTimestamp.format("DD-MMM-YYYY hh:mm:ss");
                $('#last-run-timestamp').text(formattedDate);
            }
        },
        error: function () {
            $("#dependency-graph-container").hide();
            $("#error-msg-container").show();
        },
    });
}

function displayDependencyGraph(data) {
  let nestedKeys = [];
  for (let key in data) {
    if (typeof data[key] === 'object' && key !== '_index' && key !== 'timestamp') {
      nestedKeys.push(...Object.keys(data[key]));
    }
  }

  // Add missing keys to the main object with empty objects as values
  nestedKeys.forEach(key => {
    if (!data.hasOwnProperty(key)) {
      data[key] = {};
    }
  });

  // Extracting nodes from the data
  const nodes = Object.keys(data).filter(key => key !== "_index" && key !== "timestamp").map(node => ({ data: { id: node } }));

  // Extracting links from the data
  const links = [];
  Object.keys(data).forEach(sourceNode => {
    if (sourceNode !== "_index" && sourceNode !== "timestamp") {
      Object.keys(data[sourceNode]).forEach(targetNode => {
        if (sourceNode !== targetNode) {

          if (data[targetNode]) {
            links.push({
              data: {
                id: `${sourceNode}-${targetNode}`,
                source: sourceNode,
                target: targetNode,
                value: data[sourceNode][targetNode]
              }
            });
          }
        }
      });
    }
  });

  cy = cytoscape({
    container: document.getElementById('dependency-graph-canvas'),
    elements: {
      nodes: nodes,
      edges: links
    },

    layout: {
      name: 'dagre', // for hierarchical layout
      rankDir: 'TB',
      nodeSep: 100, 
      edgeSep: 30, 
      rankSep: 50, 
      padding: 20 
    },

    style: []
  });

  // Enable dragging
  cy.nodes().forEach(function(node) {
    node.grabify();
  });

  updateGraphStyles(theme);

  if (cy) {
    cy.fit();
  }
}

function updateGraphStyles(theme) {
  let styles;
  if (theme === "light") {
      styles = lightStyles;
  } else {
      styles = darkStyles;
  }

  cy.style().fromJson([
      {
          selector: 'node',
          style: {
              'label': 'data(id)',
              'color': styles.labelColor,
              'text-valign': 'bottom',
              'text-halign': 'right',
              'font-size': 8,
              'font-weight': 'normal',
              'font-family': '"DINpro", Arial, sans-serif', 
              'text-margin-y': -10,
              'background-color': function(ele) {
                  const nodeId = ele.data('id');
                  if (!colorMap.hasOwnProperty(nodeId)) {
                      const color = colorArray[Object.keys(colorMap).length % colorArray.length];
                      colorMap[nodeId] = color;
                  }
                  return colorMap[nodeId];
              },
              'text-outline-color': styles.labelbgColor,
              'text-outline-width': '1px', 
              'text-max-width': 60,
              'text-wrap': 'wrap',
              'min-zoomed-font-size': 0.4,
              'width': 20,
              'height': 20,
      
          }
      },
      {
          selector: 'edge',
          style: {
              'width': 1,
              'line-color': styles.edgeColor,
              'target-arrow-color': styles.edgeColor,
              'target-arrow-shape': 'triangle',
              'label': 'data(value)',
              'color': styles.labelColor,
              'text-background-color': styles.labelbgColor,
              'text-background-opacity': 0.7,
              'text-background-padding': '1px',
              'curve-style': 'bezier',
              'font-family': '"DINpro", Arial, sans-serif;',
              'font-size': '10px',
              // 'arrow-scale': 0.5 
          }
      }
  ]).update();
}

