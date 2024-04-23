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

"use strict";

const colorArray = [
  "#6347D9", "#01BFB3", "#E9DC6E", "#F2A52B", "#4BAE7F",
  "#9178C5", "#23A9E2", "#8C706B", "#22589D", "#B33B97",
  "#9FBF46", "#BF9A68", "#DC756F", "#E55D9A", "#597C53",
  "#63b598", "#ce7d78", "#ea9e70", "#a48a9e", "#c6e1e8",
  "#648177", "#0d5ac1", "#f205e6", "#14a9ad", "#4ca2f9",
  "#a4e43f", "#d298e2", "#6119d0", "#d2737d", "#c0a43c",
  "#f2510e", "#651be6", "#61da5e", "#cd2f00", "#9348af",
  "#01ac53", "#c5a4fb", "#996635", "#b11573", "#75d89e",
  "#2f3f94", "#2f7b99", "#da967d", "#34891f", "#b0d87b",
  "#ca4751", "#7e50a8", "#c4d647", "#11dec1", "#566ca0",
  "#ffdbe1", "#2f1179", "#916988", "#4b5bdc", "#0cd36d",
  "#cb5bea", "#df514a", "#539397", "#880977", "#f697c1",
  "#e1cf3b", "#5be4f0", "#d00043", "#a4d17a", "#be608b",
  "#96b00c", "#088baf", "#e145ba", "#ee91e3", "#05d371",
  "#802234", "#0971f0", "#8fb413", "#b2b4f0", "#c9a941",
  "#0023b8", "#986b53", "#f50422", "#983f7a", "#ea24a3",
  "#79352c", "#521250", "#c79ed2", "#d6dd92", "#e33e52",
  "#b2be57", "#fa06ec", "#1bb699", "#6b2e5f", "#21538e",
  "#89d534", "#d36647", "#996c48", "#9ab9b7", "#06e052",
  "#e3a481", "#fc458e", "#b2db15", "#aa226d", "#c9a945",
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

    $(".theme-btn").on("click", themePickerHandler);
    $('.theme-btn').on('click', function() {
      updateGraphStyles();
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

  updateGraphStyles();

  if (cy) {
    cy.fit();
  }
}

function updateGraphStyles() {
  let theme = $('html').attr('data-theme');
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
              'font-size': '10px',
              // 'arrow-scale': 0.5 
          }
      }
  ]).update();
}

