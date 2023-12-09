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

let svgWidth;
let svgHeight;

$(document).ready(() => {
    displayNavbar();
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    $(".theme-btn").on("click", themePickerHandler);

    svgWidth = $("#dependency-graph-container").width();
    svgHeight = $("#dependency-graph-container").height();

    $("#error-msg-container").hide();
    getServiceDependencyData();
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
                $("#dependency-graph-container").show();
                $("#error-msg-container").hide();
                createDependencyMatrix(res);
            }
        },
        error: function () {
            $("#dependency-graph-container").hide();
            $("#error-msg-container").show();
        },
    });
}

function createDependencyMatrix(res) {
    const data = {};
    const nodes = [];
    const links = [];

    for (const key in res) {
        if (key !== "_index" && key !== "timestamp") {
            data[key] = res[key];
        }
    }

    Object.keys(data).forEach((parentNode) => {
        if (!nodes.some((node) => node.id === parentNode)) {
            nodes.push({ id: parentNode });
        }
        // Iterate through parent node
        Object.keys(data[parentNode]).forEach((childNode) => {
            if (!nodes.some((node) => node.id === childNode)) {
                nodes.push({ id: childNode });
            }
            // Add link
            links.push({
                source: parentNode,
                target: childNode,
                value: data[parentNode][childNode],
            });
        });
    });

    displayDependencyGraph(nodes, links);
}

function displayDependencyGraph(nodes, links) {
    // Create SVG container
    const svg = d3
        .select("#dependency-graph-container")
        .append("svg")
        .append("g")
        .attr("transform","translate(" + svgWidth / 4 + "," + svgHeight / 4 + ")");

    // Create force simulation
    const simulation = d3
        .forceSimulation(nodes)
        .force("link",d3.forceLink(links).id((d) => d.id).distance(200).strength(0.5))
        .force("charge", d3.forceManyBody().strength(-300))
        .force("center", d3.forceCenter(250, 250))
        .force("radial", d3.forceRadial(250, 250, 250).strength(0.1));

    // Arrow Head
    svg.append("defs")
        .append("marker")
        .attr("id", "arrowhead")
        .attr("viewBox", "-0 -5 10 10")
        .attr("refX", 23)
        .attr("refY", 0)
        .attr("orient", "auto")
        .attr("markerWidth", 12)
        .attr("markerHeight", 12)
        .attr("xoverflow", "visible")
        .append("svg:path")
        .attr("d", "M 0,-5 L 10 ,0 L 0,5");

    // Create links
    const link = svg
        .selectAll(".links")
        .data(links)
        .enter()
        .append("line")
        .attr("class", "links line")
        .attr("marker-end", "url(#arrowhead)");

    // Create nodes
    const node = svg
        .selectAll("circle")
        .data(nodes)
        .enter()
        .append("circle")
        .attr("r", 20) //radius of node
        .attr("fill", (d, i) => colorArray[i % colorArray.length]);

    // Add labels for nodes
    const label = svg
        .selectAll(".label")
        .data(nodes)
        .enter()
        .append("text")
        .text((d) => d.id)
        .attr("class", "label");

    // Add labels for links
    const linkLabel = svg
        .selectAll(".link-label")
        .data(links)
        .enter()
        .append("text")
        .text((d) => d.value)
        .attr("class", "link-label")
        .attr("dy", -10)
        .attr("dx", -10);

    // Update positions on each tick
    simulation.on("tick", () => {
        link.attr("x1", (d) => d.source.x)
            .attr("y1", (d) => d.source.y)
            .attr("x2", (d) => d.target.x)
            .attr("y2", (d) => d.target.y);

        node.attr("cx", (d) => d.x).attr("cy", (d) => d.y);

        label.attr("x", (d) => d.x - 10).attr("y", (d) => d.y - 25);

        linkLabel
            .attr("x", (d) => (d.source.x + d.target.x) / 2)
            .attr("y", (d) => (d.source.y + d.target.y) / 2);
    });

}
let zoom = d3.zoom().on("zoom", displayDependencyGraph);
function initZoom(){
    d3.select("#dependency-graph-container").call(zoom);
}

