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

    const svg = d3
        .select('#timeline-container')
        .append('svg')
        .attr('width', svgWidth + padding.left + padding.right - 50)
        .attr('height', totalHeight + padding.top + padding.bottom)
        .append('g')
        .attr('transform', 'translate(' + padding.left + ',' + padding.top + ')');

    // Add title
    svg.append('text').attr('x', 0).attr('y', 40).attr('class', 'gantt-chart-heading').text('Service and Operation');

    const xScale = d3
        .scaleLinear()
        .domain([nsToMs(data.start_time), nsToMs(data.end_time)])
        .range([400, svgWidth - 100]);

    // Add a time grid
    const timeTicks = xScale.ticks(4); // number of ticks
    svg.selectAll('.time-tick')
        .data(timeTicks)
        .enter()
        .append('line')
        .attr('class', 'time-tick')
        .attr('x1', (d) => xScale(d))
        .attr('x2', (d) => xScale(d))
        .attr('y1', 50)
        .attr('y2', 100 + totalHeight)
        .attr('stroke-dasharray', '2,2');

    // Add time labels
    svg.selectAll('.time-label')
        .data(timeTicks)
        .enter()
        .append('text')
        .attr('class', 'time-label')
        .attr('x', (d) => xScale(d))
        .attr('y', 40)
        .attr('text-anchor', 'middle')
        .text((d) => `${d}ms`);

    // recursively render the timeline
    let y = 60;
    let firstSpan = null;

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

    function renderTimeline(node, level = 0) {
        if (node.children) {
            node.children.sort((a, b) => a.start_time - b.start_time);
        }
        // Add node labels
        //eslint-disable-next-line no-unused-vars
        const label = svg
            .append('text')
            .attr('x', 10 * level)
            .attr('y', y + 12)
            .text(`${node.service_name}:${node.operation_name}`)
            .attr('class', 'node-label')
            .classed('anomalous-node', node.is_anomalous)
            .classed('normal-node', !node.is_anomalous)
            .classed('error-node', node.status === 'STATUS_CODE_ERROR');

        if (!node.is_anomalous) {
            // Store the first non-anomalous span
            if (firstSpan === null) {
                firstSpan = node;
            }

            const rect = svg
                .append('rect')
                .attr('x', xScale(nsToMs(node.start_time)))
                .attr('y', y)
                .attr('width', xScale(nsToMs(node.end_time)) - xScale(nsToMs(node.start_time)))
                .attr('height', 14)
                .attr('rx', 2)
                .attr('ry', 2)
                .attr('fill', colorArray[colorIndex])
                .on('mouseover', () => {
                    rect.style('cursor', 'pointer');
                    tooltip.style('display', 'block').html(
                        `
                        <strong> ${node.service_name} : ${node.operation_name}</strong><br>
                        <strong>SpanId</strong>: ${node.span_id} <br>
                        <strong>Start Time</strong>: ${nsToMs(node.start_time)}ms<br>
                        <strong>End Time</strong>: ${nsToMs(node.end_time)}ms<br>
                        <strong>Duration</strong>: ${nsToMs(node.duration)}ms <br>
                        <strong>Tags</strong>: ${Object.entries(node.tags)
                            .map(([key, value]) => `<em>${key}</em> <strong>:</strong> <em>${value}</em><br>`)
                            .join('')}
                      `
                    );
                })
                .on('mousemove', (event) => {
                    tooltip.style('left', event.pageX + 10 + 'px').style('top', event.pageY - 28 + 'px');
                })
                .on('mouseout', () => {
                    rect.style('cursor', 'default');
                    tooltip.style('display', 'none');
                })
                .on('click', () => {
                    d3.selectAll('.span-details-box').remove();
                    showSpanDetails(node);
                });

            svg.append('text')
                .attr('x', xScale(nsToMs(node.end_time)) + 5)
                .attr('y', y + 12)
                .text(`${nsToMs(node.duration)}ms`)
                .style('font-size', '10px')
                .attr('class', 'normal-node')
            }

        colorIndex = (colorIndex + 1) % colorArray.length;
        // Increment y for the next node
        y += 40;

        if (node.children && node.children.length > 0) {
            node.children.forEach((child) => {
                renderTimeline(child, level + 1);
            });
        }
    }

    const tooltip = d3.select('body').append('div').attr('class', 'tooltip-gantt');

    renderTimeline(data);

    // Show details for the first span by default
    if (firstSpan) {
        showSpanDetails(firstSpan);
    }
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
