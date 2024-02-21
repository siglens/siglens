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

let svgWidth;
let traceId;

$(document).ready(() => {
    $(".theme-btn").on("click", themePickerHandler);
    if (Cookies.get("theme")) {
        theme = Cookies.get("theme");
        $("body").attr("data-theme", theme);
    }
    svgWidth = $("#timeline-container").width();

    traceId = getParameterFromUrl('trace_id');
    getTraceInformation(traceId);
});

function getParameterFromUrl(param) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(param);
}

function getTraceInformation(traceId) {
    console.log("traceId: " + traceId);
    $.ajax({
        method: "POST",
        url: "api/traces/ganttchart",
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        data: JSON.stringify({
            "searchText": `trace_id=${traceId}`,
            "startEpoch": "now-3h",
            "endEpoch": "now"
        }),
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        traceDetails(res);
        displayTimeline(res)
    })
}

$(".back-to-search-traces").on("click", function(){
    window.location.href = "search-traces.html";
});

function traceDetails(res){
    $('#trace-overview').append(
        ` <div class="trace-name">
        <span class="service">${res.service_name}</span>
        <span class="operation">: ${res.operation_name}</span>
        <span class="trace-id">${traceId.substring(0, 7)}</span>
    </div>
    <div class="d-flex trace-details">
        <div>Trace Start: <span>${ convertNanosecondsToDateTime(res.actual_start_time)}</span></div>
        <div>Duration:<span>${ nsToMs(res.duration)}ms</span></div>
    </div>`
    )
}

function nsToMs(ns) {
    return ns / 1e6;
}

function convertNanosecondsToDateTime(timestamp) {
    const milliseconds = timestamp / 1e6;
    const date = new Date(milliseconds);

    const formattedDate = date.toLocaleString('en-US', {
        month: 'long',
        day: 'numeric',
        year: 'numeric',
        hour: 'numeric',
        minute: 'numeric',
        second: 'numeric',
        millisecond: 'numeric',
        timeZoneName: 'short'
    });

    return formattedDate;
}

function displayTimeline(data) {

    const totalHeight = calculateTotalHeight(data);
    const padding = { top: 20, right: 20, bottom: 20, left: 20 };

    const svg = d3
        .select("#timeline-container")
        .append("svg")
        .attr("width", svgWidth + padding.left + padding.right - 50)
        .attr("height", totalHeight + padding.top + padding.bottom)
        .append("g")
        .attr(
            "transform",
            "translate(" + padding.left + "," + padding.top + ")",
        );

    const xScale = d3
        .scaleLinear()
        .domain([nsToMs(data.start_time), nsToMs(data.end_time)])
        .range([400, svgWidth - 100]);

    // Add a time grid
    const timeTicks = xScale.ticks(4); // number of ticks
    svg.selectAll(".time-tick")
        .data(timeTicks)
        .enter()
        .append("line")
        .attr("class", "time-tick")
        .attr("x1", (d) => xScale(d))
        .attr("x2", (d) => xScale(d))
        .attr("y1", 50)
        .attr("y2", 50 + totalHeight);

    // Add time labels
    svg.selectAll(".time-label")
        .data(timeTicks)
        .enter()
        .append("text")
        .attr("class", "time-label")
        .attr("x", (d) => xScale(d))
        .attr("y", 40)
        .attr("text-anchor", "middle")
        .text((d) => `${d}ms`)

    // recursively render the timeline
    let y = 100; 

    function renderTimeline(node, level = 0) {
        if (node.children) {
            node.children.sort((a, b) => a.start_time - b.start_time); // sort by start time
        }
    // Add node labels
    const label = svg
        .append("text")
        .attr("x", 10 * level)
        .attr("y", y + 12)
        .text(`${node.service_name}:${node.operation_name}`)
        .attr("class", "node-label")
        .classed("anomalous-node", node.is_anomalous)
        .classed("normal-node", !node.is_anomalous);


        if (!node.is_anomalous){
        const rect = svg
            .append("rect")
            .attr("x",xScale(nsToMs(node.start_time)))
            .attr("y", y)
            .attr("width", xScale(nsToMs(node.end_time)) - xScale(nsToMs(node.start_time)))
            .attr("height", 20)
            .attr("fill", "#6449D6")
            .on("mouseover", () => {
                rect.style("cursor", "pointer");
                tooltip
                    .style("display", "block")
                    .html(
                        `
                        <strong>SpanId</strong>: ${node.span_id} <br>
                        <strong>Name</strong>: ${node.service_name} : ${node.operation_name}<br>
                        <strong>Start Time</strong>: ${nsToMs(node.start_time)}ms<br>
                        <strong>End Time</strong>: ${nsToMs(node.end_time)}ms<br>
                        <strong>Duration</strong>: ${nsToMs(node.duration)}ms <br>
                        <strong>Tags</strong>: ${Object.entries(node.tags).map(([key, value]) => `<em>${key}</em> <strong>:</strong> <em>${value}</em><br>`).join('')}
                      `,
                    );
            })
            .on("mousemove", (event) => {
                tooltip
                    .style("left", event.pageX + 10 + "px")
                    .style("top", event.pageY - 28 + "px");
            })
            .on("mouseout", () => {
                rect.style("cursor", "default");
                tooltip.style("display", "none");
            });
        }

        // Increment y for the next node
        y += 50;

    if (node.children && node.children.length > 0) {
        node.children.forEach((child) => {
            renderTimeline(child, level + 1);
        });
    }

    }

    const tooltip = d3
        .select("body")
        .append("div")
        .attr("class", "tooltip-gantt");

    renderTimeline(data);
}

function calculateTotalHeight(node) {
    let totalHeight = 0;
    function calculateHeight(node) {
        totalHeight += 50;
        if (node.children !== null) {
            node.children.forEach(calculateHeight);
        }
    }
    calculateHeight(node);
    return totalHeight + 200;
}