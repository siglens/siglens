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

const colorArray = [
    '#6347D9',
    '#01BFB3',
    '#E9DC6E',
    '#F2A52B',
    '#4BAE7F',
    '#9178C5',
    '#23A9E2',
    '#8C706B',
    '#22589D',
    '#B33B97',
    '#9FBF46',
    '#BF9A68',
    '#DC756F',
    '#E55D9A',
    '#597C53',
    '#63b598',
    '#ce7d78',
    '#ea9e70',
    '#a48a9e',
    '#c6e1e8',
    '#648177',
    '#0d5ac1',
    '#f205e6',
    '#14a9ad',
    '#4ca2f9',
    '#a4e43f',
    '#d298e2',
    '#6119d0',
    '#d2737d',
    '#c0a43c',
    '#f2510e',
    '#651be6',
    '#61da5e',
    '#cd2f00',
    '#9348af',
    '#01ac53',
    '#c5a4fb',
    '#996635',
    '#b11573',
    '#75d89e',
    '#2f3f94',
    '#2f7b99',
    '#da967d',
    '#34891f',
    '#b0d87b',
    '#ca4751',
    '#7e50a8',
    '#c4d647',
    '#11dec1',
    '#566ca0',
    '#ffdbe1',
    '#2f1179',
    '#916988',
    '#4b5bdc',
    '#0cd36d',
    '#cb5bea',
    '#df514a',
    '#539397',
    '#880977',
    '#f697c1',
    '#e1cf3b',
    '#5be4f0',
    '#d00043',
    '#a4d17a',
    '#be608b',
    '#96b00c',
    '#088baf',
    '#e145ba',
    '#ee91e3',
    '#05d371',
    '#802234',
    '#0971f0',
    '#8fb413',
    '#b2b4f0',
    '#c9a941',
    '#0023b8',
    '#986b53',
    '#f50422',
    '#983f7a',
    '#ea24a3',
    '#79352c',
    '#521250',
    '#c79ed2',
    '#d6dd92',
    '#e33e52',
    '#b2be57',
    '#fa06ec',
    '#1bb699',
    '#6b2e5f',
    '#21538e',
    '#89d534',
    '#d36647',
    '#996c48',
    '#9ab9b7',
    '#06e052',
    '#e3a481',
    '#fc458e',
    '#b2db15',
    '#aa226d',
    '#c9a945',
];

let svgWidth;
let traceId;
let colorIndex = 0;

$(document).ready(() => {
    $('.theme-btn').on('click', themePickerHandler);

    svgWidth = $('#timeline-container').width();

    traceId = getParameterFromUrl('trace_id');
    getTraceInformation(traceId);
});

function getParameterFromUrl(param) {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get(param);
}

function getTraceInformation(traceId) {
    $.ajax({
        method: 'POST',
        url: 'api/traces/ganttchart',
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            Accept: '*/*',
        },
        data: JSON.stringify({
            searchText: `trace_id=${traceId}`,
            startEpoch: 'now-365d',
            endEpoch: 'now',
        }),
        dataType: 'json',
        crossDomain: true,
    }).then(function (res) {
        traceDetails(res);
        displayTimeline(res);
    });
}

$('.back-to-search-traces').on('click', function () {
    window.history.back();
});

function traceDetails(res) {
    $('#trace-overview').append(
        ` <div class="trace-name">
        <span class="service">${res.service_name}</span>
        <span class="operation">: ${res.operation_name}</span>
        <span class="trace-id">${traceId.substring(0, 7)}</span>
    </div>
    <div class="d-flex trace-details">
        <div>Trace Start: <span>${convertNanosecondsToDateTime(res.actual_start_time)}</span></div>
        <div>Duration:<span>${nsToMs(res.duration)}ms</span></div>
    </div>`
    );
}

function nsToMs(ns) {
    return (ns / 1e6).toFixed(2);
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
        timeZoneName: 'short',
    });

    return formattedDate;
}

function displayTimeline(data) {
    const totalHeight = calculateTotalHeight(data);
    const padding = { top: 0, right: 10, bottom: 10, left: 20 };
    const labelWidth = 400;

    d3.select('#timeline-container').selectAll('*').remove();

    const containerDiv = d3.select('#timeline-container');

    // Create fixed header container
    const headerDiv = containerDiv.append('div').attr('class', 'header-div');
    // Service and Operation header (fixed)
    headerDiv.append('div').style('min-width', `${labelWidth}px`).style('padding-left', `${padding.left}px`).style('padding-top', '10px').style('flex-shrink', '0').append('text').attr('class', 'gantt-chart-heading').text('Service and Operation');

    // Time labels container (scrolls horizontally with chart)
    const timeHeaderDiv = headerDiv.append('div').style('overflow-x', 'hidden').style('flex-grow', '1');

    const timeHeaderSvg = timeHeaderDiv.append('svg').attr('width', svgWidth).attr('height', '45px');

    // Main scrollable container
    const scrollContainer = containerDiv.append('div').style('display', 'flex').style('position', 'relative').style('overflow', 'auto').style('height', 'calc(100% - 45px)');

    // Labels container (Service and Operation)
    const labelsContainer = scrollContainer.append('div').attr('class', 'labels-container').style('min-width', `${labelWidth}px`);

    const labelsSvg = labelsContainer.append('svg').attr('width', labelWidth).attr('height', totalHeight).append('g').attr('transform', `translate(${padding.left},${padding.top})`).attr('class', 'labels-container');

    // Timeline container (scrolls both directions)
    const timelineContainer = scrollContainer.append('div').style('overflow', 'visible').style('flex-grow', '1');

    const timelineSvg = timelineContainer.append('svg').attr('width', svgWidth).attr('height', totalHeight);

    // Setup scales
    const xScale = d3
        .scaleLinear()
        .domain([nsToMs(data.start_time), nsToMs(data.end_time)])
        .range([0, svgWidth - 100]);

    // Add time labels
    const timeTicks = xScale.ticks(4);
    timeHeaderSvg
        .selectAll('.time-label')
        .data(timeTicks)
        .enter()
        .append('text')
        .attr('class', 'time-label')
        .attr('x', (d) => xScale(d) + 14)
        .attr('y', 26)
        .attr('text-anchor', 'middle')
        .text((d) => `${d}ms`);

    // Add time grid
    timelineSvg
        .selectAll('.time-tick')
        .data(timeTicks)
        .enter()
        .append('line')
        .attr('class', 'time-tick')
        .attr('x1', (d) => xScale(d))
        .attr('x2', (d) => xScale(d))
        .attr('y1', 0)
        .attr('y2', totalHeight)
        .attr('stroke', '#eee')
        .attr('stroke-dasharray', '2,2');

    // Sync horizontal scrolling between time header and timeline
    scrollContainer.on('scroll', function () {
        const scrollLeft = this.scrollLeft;
        timeHeaderDiv.node().scrollLeft = scrollLeft;
    });

    let y = 20;
    let firstSpan = null;
    let colorIndex = 0;

    function renderTimeline(node, level = 0) {
        const labelGroup = labelsSvg.append('g').attr('transform', `translate(${10 * level}, ${y})`);

        // Add status circle for error nodes
        if (node.is_anomalous) {
            const errorGroup = labelGroup.append('g').attr('transform', 'translate(-20, 4)').style('cursor', 'pointer');

            // Add red circle
            errorGroup.append('circle').attr('cx', 8).attr('cy', 5).attr('r', 8).attr('fill', '#ef4444');

            // Add exclamation mark
            errorGroup.append('text').attr('x', 8).attr('y', 6).attr('text-anchor', 'middle').attr('fill', 'white').attr('font-size', '12px').attr('font-weight', 'bold').style('dominant-baseline', 'middle').text('!');
            errorGroup.on('click', () => showSpanDetails(node));
        }

        // Add node labels
        labelGroup
            .append('text')
            .attr('x', 0)
            .attr('y', 12)
            .text(`${node.service_name}:${node.operation_name}`)
            .attr('class', 'node-label')
            .classed('anomalous-node', node.is_anomalous)
            .classed('normal-node', !node.is_anomalous)
            .classed('error-node', node.status === 'STATUS_CODE_ERROR')
            .style('cursor', 'pointer')
            .on('click', () => showSpanDetails(node));

        if (!node.is_anomalous) {
            // Store the first non-anomalous span
            if (firstSpan === null) {
                firstSpan = node;
            }

            const rect = timelineSvg
                .append('rect')
                .attr('x', xScale(nsToMs(node.start_time)))
                .attr('y', y)
                .attr('width', xScale(nsToMs(node.end_time)) - xScale(nsToMs(node.start_time)))
                .attr('height', 14)
                .attr('rx', 2)
                .attr('ry', 2)
                .attr('fill', colorArray[colorIndex])
                .on('mouseover', function (event) {
                    d3.select(this).style('cursor', 'pointer');
                    showTooltip(event, node);
                })
                .on('mouseout', hideTooltip)
                .on('click', () => showSpanDetails(node));

            timelineSvg
                .append('text')
                .attr('x', xScale(nsToMs(node.end_time)) + 5)
                .attr('y', y + 12)
                .text(`${nsToMs(node.duration)}ms`)
                .style('font-size', '10px')
                .attr('class', 'normal-node')
                .style('cursor', 'pointer')
                .on('click', () => showSpanDetails(node));
        }

        colorIndex = (colorIndex + 1) % colorArray.length;
        y += 40;

        if (node.children && node.children.length > 0) {
            node.children.forEach((child) => renderTimeline(child, level + 1));
        }
    }

    const tooltip = d3.select('body').append('div').attr('class', 'tooltip-gantt').style('display', 'none');

    function showTooltip(event, node) {
        tooltip
            .style('display', 'block')
            .html(
                `
                <strong>${node.service_name} : ${node.operation_name}</strong><br>
                <strong>SpanId</strong>: ${node.span_id}<br>
                <strong>Start Time</strong>: ${nsToMs(node.start_time)}ms<br>
                <strong>End Time</strong>: ${nsToMs(node.end_time)}ms<br>
                <strong>Duration</strong>: ${nsToMs(node.duration)}ms<br>
                <strong>Tags</strong>: ${Object.entries(node.tags)
                    .map(([key, value]) => `<em>${key}</em> <strong>:</strong> <em>${value}</em><br>`)
                    .join('')}
            `
            )
            .style('left', event.pageX + 10 + 'px')
            .style('top', event.pageY - 28 + 'px');
    }

    function hideTooltip() {
        tooltip.style('display', 'none');
    }

    // Render the timeline
    renderTimeline(data);

    // Show details for the first span by default
    if (firstSpan) {
        showSpanDetails(firstSpan);
    }
}

function showSpanDetails(node) {
    let spanDetailsContainer = d3.select('.span-details-container');
    spanDetailsContainer.style('display', 'block');
    spanDetailsContainer.html(
        `
        <div class="d-flex justify-content-between align-items-center">
            <div class="operation-name"><strong>${node.operation_name}</strong></div>
            <div class="close-btn"></div>
        </div>
        <hr>
        <div class="details-container">
            <div><strong>SpanId</strong>: ${node.span_id} </div>
            <div><strong>Service</strong>: ${node.service_name}</div>
            <div><strong>Start Time</strong>: ${nsToMs(node.start_time)}ms  |  <strong>End Time</strong>: ${nsToMs(node.end_time)}ms</div>
            <div><strong>Duration</strong>: ${nsToMs(node.duration)}ms </div>
            <div><strong>Tags</strong>:</div>
            <table style="border-collapse: collapse; width: 100%; margin-top:6px" >
              ${Object.entries(node.tags)
                  .map(
                      ([key, value]) => `
                <tr>
                  <td ><em>${key}</em></td>
                  <td style="word-break: break-all;"><em>${value}</em></td>
                </tr>`
                  )
                  .join('')}
            </table>
        </div>
        `
    );

    spanDetailsContainer.select('.close-btn').on('click', function () {
        spanDetailsContainer.style('display', 'none');
    });
}

function calculateTotalHeight(node) {
    let totalHeight = 0;
    function calculateHeight(node) {
        totalHeight += 40;
        if (node.children !== null) {
            node.children.forEach(calculateHeight);
        }
    }
    calculateHeight(node);
    return totalHeight + 40;
}

$('.section-button').click(function () {
    $('.section-button').removeClass('active');
    $(this).addClass('active');
});

$('.max-min-btn').click(function () {
    $(this).toggleClass('minimized');
    $('.logs-metrics-container').toggleClass('minimized');
    $('#timeline-container').toggleClass('expanded');
    $('.span-details-container').toggleClass('expanded');
});
