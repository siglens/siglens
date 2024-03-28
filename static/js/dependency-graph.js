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
$(document).ready(() => {
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $(".theme-btn").on("click", themePickerHandler);
    $('.theme-btn').on('click', getServiceDependencyData);


    $("#error-msg-container, #dependency-info").hide();
    getServiceDependencyData();

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

function getServiceDependencyData() {
    $.ajax({
        method: "GET",
        url: "api/traces/dependencies",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        dataType: "json",
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
  let edgeColor, labelColor, labelbgColor;
  if ($('body').attr('data-theme') == "light") {
      edgeColor = "#6F6B7B";
      labelColor = "#262038";
      labelbgColor = "#FFF";
    }
    else {
      edgeColor = "#DCDBDF";
      labelColor = "#FFF";
      labelbgColor = "#262038";
  }

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

  const cy = cytoscape({
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

    style: [
      {
        selector: 'node',
        style: {
          'label': 'data(id)',
          'color': labelColor,
          'text-valign': 'bottom',
          'text-halign': 'right',
          'font-weight': 'normal',
          'font-family': 'DINpro', 
          'text-margin-y': -10,
          'background-color': function(ele) {
            const nodeId = ele.data('id');
            if (!colorMap.hasOwnProperty(nodeId)) {
              const color = colorArray[Object.keys(colorMap).length % colorArray.length];
              colorMap[nodeId] = color;
            }
            return colorMap[nodeId];
          },
          'text-outline-color': labelbgColor,
          'text-outline-width': '1px', 
        }
      },
      {
        selector: 'edge',
        style: {
          'width': 1,
          'line-color': edgeColor,
          'target-arrow-color': edgeColor,
          'target-arrow-shape': 'triangle',
          'label': 'data(value)',
          'font-size': '14px',
          'color': labelColor,
          'text-background-color': labelbgColor,
          'text-background-opacity': 0.7,
          'text-background-padding': '1px',
          'curveStyle': 'bezier',
          // 'arrow-scale': 0.5 
        }
      }
    ]
  });

  // Enable dragging
  cy.nodes().forEach(function(node) {
    node.grabify();
  });
}

