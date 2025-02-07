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
let spanDetailsClosed = false;

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
    spanDetailsClosed = false;
    // Create a map of service names to colors
    const serviceColors = new Map();

    function assignServiceColors(node) {
        // If service doesn't have a color yet, assign one
        if (!serviceColors.has(node.service_name)) {
            serviceColors.set(node.service_name, colorArray[serviceColors.size % colorArray.length]);
        }

        // Assign the service's color to this node
        node.color = serviceColors.get(node.service_name);

        // Process children
        if (node.children && node.children.length > 0) {
            node.children.forEach(assignServiceColors);
        }
    }

    // Add isExpanded property and assign colors
    function prepareData(node) {
        node.isExpanded = true; // Start expanded by default
        if (node.children && node.children.length > 0) {
            node.children.forEach(prepareData);
        }
        return node;
    }

    data = prepareData(data);
    assignServiceColors(data);

    function updateTimeline() {
        const oldContainer = document.querySelector('#timeline-container > div:nth-child(3)');
        const scrollTop = oldContainer ? oldContainer.scrollTop : 0;
        const scrollLeft = oldContainer ? oldContainer.scrollLeft : 0;

        const totalHeight = calculateTotalHeight(data);
        const padding = { top: 0, right: 10, bottom: 10, left: 30 };
        let labelWidth = 400;

        const containerDiv = d3.select('#timeline-container');
        containerDiv.selectAll('*').remove();

        const wrapper = containerDiv.append('div').style('position', 'relative').style('width', '100%').style('display', 'flex').style('flex-direction', 'column').style('min-width', '600px');

        // Create fixed header container
        const headerDiv = wrapper.append('div').attr('class', 'header-div').style('min-width', '600px');

        // Service and Operation header (fixed)
        const labelHeaderDiv = headerDiv.append('div').style('min-width', '200px').style('padding-left', `${padding.left}px`).style('width', `${labelWidth}px`).style('padding-top', '10px').style('flex-shrink', '0').append('text').attr('class', 'gantt-chart-heading').text('Service and Operation');

        // Time labels container
        const timeHeaderDiv = headerDiv.append('div').style('overflow-x', 'hidden').style('flex-grow', '1').style('min-width', '400px');

        const timeHeaderSvg = timeHeaderDiv.append('svg').attr('width', svgWidth).attr('height', '45px').style('min-width', '400px');

        const resizer = containerDiv
            .append('div')
            .attr('class', 'gantt-chart-resizer')
            .style('left', labelWidth + 'px')
            .style('min-left', '200px');

        const scrollContainer = containerDiv.append('div').style('display', 'flex').style('position', 'relative').style('overflow', 'auto').style('height', 'calc(100% - 45px)');
        // Labels container
        const labelsContainer = scrollContainer.append('div').attr('class', 'labels-container').style('min-width', '200px').style('width', `${labelWidth}px`).style('flex-shrink', '0');

        const labelsSvg = labelsContainer.append('svg').attr('width', labelWidth).attr('height', totalHeight).append('g').attr('transform', `translate(${padding.left},${padding.top})`).attr('class', 'labels-container');

        // Timeline container
        const timelineContainer = scrollContainer.append('div').style('flex-grow', '1').style('width', '100%').style('min-width', '400px');

        const timelineSvg = timelineContainer
            .append('svg')
            .style('width', '100%')
            .style('height', totalHeight + 'px')
            .attr('height', totalHeight)
            .attr('preserveAspectRatio', 'none');

        let containerWidth = timelineContainer.node().getBoundingClientRect().width;
        const xScale = d3
            .scaleLinear()
            .domain([nsToMs(data.start_time), nsToMs(data.end_time)])
            .range([0, containerWidth - 100]);

        function updateTimelineElements() {
            // Update time scale
            containerWidth = Math.max(400, timelineContainer.node().getBoundingClientRect().width);
            xScale.range([0, containerWidth - 100]);

            // Update time labels
            const timeTicks = xScale.ticks(4);
            timeHeaderSvg
                .selectAll('.time-label')
                .data(timeTicks)
                .join('text')
                .attr('class', 'time-label')
                .attr('x', (d) => xScale(d) + 16)
                .attr('y', 26)
                .attr('text-anchor', 'middle')
                .text((d) => `${d}ms`);

            // Update grid lines
            timelineSvg
                .selectAll('.time-tick')
                .data(timeTicks)
                .join('line')
                .attr('class', 'time-tick')
                .attr('x1', (d) => xScale(d))
                .attr('x2', (d) => xScale(d))
                .attr('y1', 0)
                .attr('y2', totalHeight)
                .attr('stroke', '#eee')
                .attr('stroke-dasharray', '2,2');

            // Update timeline bars
            timelineSvg
                .selectAll('rect.timeline-bar')
                .attr('x', (d) => xScale(nsToMs(d.start_time)))
                .attr('width', (d) => Math.max(0, xScale(nsToMs(d.end_time)) - xScale(nsToMs(d.start_time))));

            // Update duration labels
            timelineSvg.selectAll('text.duration-label').attr('x', (d) => xScale(nsToMs(d.end_time)) + 5);
        }

        // Setup resize functionality
        let isResizing = false;
        let startX;
        let startWidth;

        resizer.on('mousedown', function (event) {
            isResizing = true;
            startX = event.clientX;
            startWidth = labelWidth;
            d3.select('body').style('cursor', 'col-resize').style('user-select', 'none');
        });

        d3.select('body')
            .on('mousemove', function (event) {
                if (!isResizing) return;

                const dx = event.clientX - startX;
                const newWidth = Math.max(200, Math.min(startWidth + dx, window.innerWidth - 400));

                labelWidth = newWidth;

                labelsContainer.style('width', `${newWidth}px`).style('min-width', newWidth + 'px');
                labelHeaderDiv.style('width', `${newWidth}px`);
                headerDiv.select('div').style('width', `${newWidth}px`);
                resizer.style('left', `${newWidth}px`);
                labelsContainer.select('svg').attr('width', newWidth);
                labelsSvg.attr('width', newWidth);

                updateTimelineElements();
            })
            .on('mouseup', function () {
                if (!isResizing) return;
                isResizing = false;
                d3.select('body').style('cursor', 'default').style('user-select', 'auto');
            });

        // Initial render
        updateTimelineElements();

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

        let y = 0;
        let firstSpan = null;

        function renderTimeline(node, level = 0, isVisible = true) {
            if (!isVisible) return;

            // Draw connecting lines for parent nodes to their children
            if (node.children && node.children.length > 0 && node.isExpanded) {
                const calculateTotalDepth = (node) => {
                    let depth = 1;
                    if (node.children && node.children.length > 0 && node.isExpanded) {
                        node.children.forEach((child) => {
                            depth += calculateTotalDepth(child);
                        });
                    }
                    return depth;
                };
                let totalDepth = 0;
                node.children.forEach((child) => {
                    totalDepth += calculateTotalDepth(child);
                });

                const firstChildY = y + 40;
                const lastChildY = y + 40 + totalDepth * 40;

                labelsSvg
                    .append('line')
                    .attr('x1', 30 * level + 1.5)
                    .attr('y1', firstChildY)
                    .attr('x2', 30 * level + 1.5)
                    .attr('y2', lastChildY)
                    .attr('class', 'connecting-lines')
            }

            const labelBackground = labelsSvg.append('rect').attr('x', -30).attr('y', y).attr('width', '100%').attr('height', 40).attr('fill', 'transparent').attr('class', `hover-area-${node.span_id}`);

            const labelGroup = labelsSvg.append('g').attr('transform', `translate(${30 * level}, ${y})`);

            // Add vertical color strip
            labelGroup.append('rect').attr('x', 0).attr('y', 10).attr('width', 3).attr('height', 20).attr('fill', node.color).attr('rx', 1).attr('ry', 1);

            // Add expand/collapse button if node has children
            if (node.children && node.children.length > 0) {
                const buttonGroup = labelGroup
                    .append('g')
                    .attr('transform', 'translate(-20, 0)')
                    .style('cursor', 'pointer')
                    .on('click', () => {
                        event.stopPropagation();
                        node.isExpanded = !node.isExpanded;
                        updateTimeline();
                    });

                // Create a foreign object to hold the HTML content
                const fo = buttonGroup.append('foreignObject').attr('width', 16).attr('height', 16).attr('y', 12);

                // Add the Font Awesome icon
                const div = fo.append('xhtml:div').style('width', '100%').style('height', '100%').style('display', 'flex').style('align-items', 'center').style('justify-content', 'center');

                const icon = div
                    .append('xhtml:i')
                    .attr('class', node.isExpanded ? 'fa fa-chevron-down' : 'fa fa-chevron-right')
                    .style('color', '#666')
                    .style('font-size', '12px'); // Adjust size as needed
            }

            // Add error indicator
            let errorGroup;
            if (node.is_anomalous) {
                errorGroup = labelGroup.append('g').attr('transform', 'translate(10, 4)');

                errorGroup.append('circle').attr('cx', 6).attr('cy', 16).attr('r', 6).attr('fill', '#ef4444');

                errorGroup
                    .append('text')
                    .attr('x', 6)
                    .attr('y', 17)
                    .attr('text-anchor', 'middle')
                    .attr('fill', 'white')
                    .attr('font-size', '9px')
                    .attr('font-weight', 'bold')
                    .style('dominant-baseline', 'middle')
                    .text('!')
                    .on('click', () => showSpanDetails(node));
            }

            // Add node labels
            labelGroup
                .append('text')
                .attr('x', node.is_anomalous ? 28 : 10)
                .attr('y', 24)
                .attr('class', 'node-label')
                .classed('anomalous-node', node.is_anomalous)
                .classed('normal-node', !node.is_anomalous)
                .classed('error-node', node.status === 'STATUS_CODE_ERROR')
                .style('cursor', 'pointer')
                .on('click', () => showSpanDetails(node))
                .each(function () {
                    const text = d3.select(this);
                    text.append('tspan').attr('class', 'node-label-service').text(node.service_name);
                    text.append('tspan').attr('class', 'node-label-operation').text(node.operation_name).attr('dx', '10');
                });

            const timelineBackground = timelineSvg.append('rect').attr('x', 0).attr('y', y).attr('width', '100%').attr('height', 40).attr('fill', 'transparent').attr('class', `timeline-hover-area-${node.span_id}`);

            let timelineBar, durationLabel;
            if (!node.is_anomalous) {
                if (firstSpan === null) {
                    firstSpan = node;
                }

                //eslint-disable-next-line no-unused-vars
                timelineBar = timelineSvg
                    .append('rect')
                    .attr('class', 'timeline-bar')
                    .attr('x', xScale(nsToMs(node.start_time)))
                    .attr('y', y + 14)
                    .attr('width', xScale(nsToMs(node.end_time)) - xScale(nsToMs(node.start_time)))
                    .attr('height', 14)
                    .attr('rx', 2)
                    .attr('ry', 2)
                    .attr('fill', node.color)
                    .datum(node);

                durationLabel = timelineSvg
                    .append('text')
                    .attr('class', 'duration-label')
                    .datum(node)
                    .attr('x', xScale(nsToMs(node.end_time)) + 5)
                    .attr('y', y + 24)
                    .text(`${nsToMs(node.duration)}ms`)
                    .style('font-size', '10px')
                    .attr('class', 'normal-node duration-label');
            }

            function addHoverEffect() {
                labelBackground.classed('hover-highlight', true);
                timelineBackground.classed('hover-highlight', true);
            }

            function removeHoverEffect() {
                labelBackground.classed('hover-highlight', false);
                timelineBackground.classed('hover-highlight', false);
            }

            const elements = [labelBackground.node(), timelineBackground.node(), labelGroup.node()];

            if (!node.is_anomalous) {
                durationLabel.node();
            }
            if (node.is_anomalous && errorGroup) {
                elements.push(errorGroup.node());
            }

            d3.selectAll(elements)
                .style('cursor', 'pointer')
                .on('mouseover', function (event) {
                    addHoverEffect();
                })
                .on('mouseout', removeHoverEffect)
                .on('click', () => showSpanDetails(node));

            if (timelineBar) {
                timelineBar
                    .style('cursor', 'pointer')
                    .on('mouseover', function (event) {
                        addHoverEffect();
                        showTooltip(event, node);
                    })
                    .on('mouseout', function () {
                        removeHoverEffect();
                        hideTooltip();
                    })
                    .on('click', () => showSpanDetails(node));
            }
            y += 40;

            // Render children only if expanded
            if (node.children && node.children.length > 0 && node.isExpanded) {
                node.children.forEach((child) => renderTimeline(child, level + 1, true));
            }
        }

        // Render the timeline
        renderTimeline(data);

        // Show details for the first span by default
        if (firstSpan && !spanDetailsClosed) {
            showSpanDetails(firstSpan);
            updateTimelineElements();
        }

        requestAnimationFrame(() => {
            const newContainer = document.querySelector('#timeline-container > div:nth-child(3)');
            if (newContainer) {
                newContainer.scrollTop = scrollTop;
                newContainer.scrollLeft = scrollLeft;
            }
        });

        const resizeHandler = () => {
            // Update container width
            containerWidth = timelineContainer.node().getBoundingClientRect().width;

            // Update x scale
            xScale.range([0, containerWidth - 100]);

            // Update time labels
            const timeTicks = xScale.ticks(4);
            timeHeaderSvg
                .selectAll('.time-label')
                .data(timeTicks)
                .join('text')
                .attr('class', 'time-label')
                .attr('x', (d) => xScale(d) + 14)
                .attr('y', 26)
                .attr('text-anchor', 'middle')
                .text((d) => `${d}ms`);

            // Update time grid lines
            timelineSvg
                .selectAll('.time-tick')
                .data(timeTicks)
                .join('line')
                .attr('class', 'time-tick')
                .attr('x1', (d) => xScale(d))
                .attr('x2', (d) => xScale(d))
                .attr('y1', 0)
                .attr('y2', totalHeight)
                .attr('stroke', '#eee')
                .attr('stroke-dasharray', '2,2');

            // Update rectangles
            timelineSvg
                .selectAll('rect.timeline-bar')
                .attr('x', (d) => xScale(nsToMs(d.start_time)))
                .attr('width', (d) => Math.max(0, xScale(nsToMs(d.end_time)) - xScale(nsToMs(d.start_time))));

            // Update duration labels to stay at the end of bars
            timelineSvg.selectAll('text.duration-label').attr('x', (d) => {
                const barEndX = xScale(nsToMs(d.end_time));
                return barEndX + 5; // 5px padding after bar
            });
        };

        const debouncedResize = _.debounce(resizeHandler, 150);
        window.addEventListener('resize', debouncedResize);
    }

    const tooltip = d3.select('body').append('div').attr('class', 'tooltip-gantt').style('display', 'none');

    function hideTooltip() {
        tooltip.style('display', 'none');
    }

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

    // Initial render
    updateTimeline();
}

function showSpanDetails(node) {
    let spanDetailsContainer = d3.select('.span-details-container');
    spanDetailsContainer.style('display', 'block');
    spanDetailsContainer.html(
        `
        <div class="d-flex justify-content-between align-items-center">
            <div class="operation-name"><strong style="margin-right: 10px;">${node.operation_name}</strong><span class="node-label-operation">${node.span_id}</span></div>
            <div class="close-btn"></div>
        </div>
        <hr>
        <div class="details-container">
            <div class="details">
                <div>Service: <strong>${node.service_name}</strong> | Start Time: <strong>${nsToMs(node.start_time)}ms</strong> | Duration: <strong>${nsToMs(node.duration)}ms </strong></div>
            </div>
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

    // Handle close button
    spanDetailsContainer.select('.close-btn').on('click', function () {
        spanDetailsContainer.style('display', 'none');
        spanDetailsClosed = true;
        window.dispatchEvent(new Event('resize'));
    });

    window.dispatchEvent(new Event('resize'));
}

function calculateTotalHeight(node) {
    let totalHeight = 0;
    function calculateHeight(node, isVisible = true) {
        if (!isVisible) return;

        totalHeight += 40;

        // Only process children if node is expanded and has children
        if (node.children && node.children.length > 0 && node.isExpanded) {
            node.children.forEach((child) => calculateHeight(child, true));
        }
    }
    calculateHeight(node);
    return totalHeight;
}
