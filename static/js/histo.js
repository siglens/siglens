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

$(document).ready(function () {
    let logsData = [];

    // Toggle button functionality
    $('#toggle-btn').click(function (event) {
        event.stopPropagation();
        $('#histogram-container').slideToggle(function () {
            if ($('#histogram-container').is(':visible')) {
                $('#initial-response').hide(); // Hide initial response when histogram opens
                renderHistogram($('.selected-interval').text());
            }else{
                $('#initial-response').show();
            }
        });
    });

    // Close when clicking outside
    $(document).click(function (event) {
        if (!$(event.target).closest('#histogram-container, #toggle-btn').length) {
            $('#histogram-container').slideUp();
        }
    });

    // Prevent closing when clicking inside histogram
    $('#histogram-container').click(function (event) {
        event.stopPropagation();
    });

    function formatNumber(num) {
        if (num >= 1000) {
            return (Math.round(num / 500) * 500 / 1000).toFixed(1) + 'k';
        }
        return (Math.round(num / 500) * 500).toString();
    }

    function aggregateDataByInterval(data, interval) {
        const aggregatedData = new Map();

        data.forEach(log => {
            const date = new Date(log.timestamp);
            let key;

            switch(interval.toLowerCase()) {
                case 'millisecond':
                    key = date.getTime();
                    break;
                case 'second':
                    key = new Date(date.setMilliseconds(0)).getTime();
                    break;
                case 'minute':
                    key = new Date(date.setSeconds(0, 0)).getTime();
                    break;
                case 'hour':
                    key = new Date(date.setMinutes(0, 0, 0)).getTime();
                    break;
                case 'day':
                    key = new Date(date.setHours(0, 0, 0, 0)).getTime();
                    break;
                case 'week':
                    const dayOfWeek = date.getDay();
                    const diff = date.getDate() - dayOfWeek;
                    key = new Date(date.setDate(diff)).setHours(0, 0, 0, 0);
                    break;
                default:
                    key = determineAutoInterval(data, date);
            }

            if (!aggregatedData.has(key)) {
                aggregatedData.set(key, {
                    timestamp: new Date(key).toISOString(),
                    count: 0,
                    errors: new Set()
                });
            }

            const entry = aggregatedData.get(key);
            entry.count += log.count;
            entry.errors.add(log.error);
        });

        return Array.from(aggregatedData.values())
            .sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    }

    function determineAutoInterval(data, currentDate) {
        const dates = data.map(log => new Date(log.timestamp));
        const range = Math.max(...dates) - Math.min(...dates);

        if (range <= 1000 * 60 * 60) {
            return new Date(currentDate.setSeconds(0, 0)).getTime();
        } else if (range <= 1000 * 60 * 60 * 24) {
            return new Date(currentDate.setMinutes(0, 0, 0)).getTime();
        } else if (range <= 1000 * 60 * 60 * 24 * 7) {
            return new Date(currentDate.setHours(0, 0, 0, 0)).getTime();
        } else {
            const dayOfWeek = currentDate.getDay();
            const diff = currentDate.getDate() - dayOfWeek;
            return new Date(currentDate.setDate(diff)).setHours(0, 0, 0, 0);
        }
    }

    function updateTimeRange(interval) {
        if (logsData.length > 0) {
            const startDate = new Date(Math.min(...logsData.map(log => new Date(log.timestamp))));
            const endDate = new Date(Math.max(...logsData.map(log => new Date(log.timestamp))));

            const timeRangeText = `${formatDate(startDate)} - ${formatDate(endDate)} (interval: ${interval.toLowerCase()})`;
            $('#time-range').html(`${timeRangeText} <i class="fas fa-exclamation-triangle warning-icon"></i>`);
        }
    }

    function formatDate(date) {
        return moment(date).format('MMM DD, YYYY @ HH:mm:ss.SSS');
    }

    function renderHistogram(interval = 'Second') {
        const aggregatedLogs = aggregateDataByInterval(logsData, interval);

        $('#histogram').empty();
        $('#x-axis').empty();
        $('#y-axis').empty();

        const maxCount = Math.max(...aggregatedLogs.map(log => log.count));
        const yStep = maxCount / 4;
        const yAxisValues = Array.from({length: 5}, (_, i) => Math.round(maxCount - (i * yStep)));

        yAxisValues.forEach((value) => {
            $('#y-axis').append(`<span>${formatNumber(value)}</span>`);
        });

        aggregatedLogs.forEach((log) => {
            const barHeight = (log.count / maxCount) * 100;

            const bar = $('<div>')
                .addClass('bar')
                .css('height', `${barHeight}%`)
                .data({
                    'timestamp': log.timestamp,
                    'count': log.count,
                    'errors': Array.from(log.errors).join(', ')
                });

            bar.hover(function () {
                const timestamp = $(this).data('timestamp');
                const count = $(this).data('count');
                const errors = $(this).data('errors');

                let dateFormat = 'YYYY-MM-DD HH:mm:ss';
                switch(interval.toLowerCase()) {
                    case 'millisecond':
                        dateFormat = 'YYYY-MM-DD HH:mm:ss.SSS';
                        break;
                    case 'second':
                        dateFormat = 'YYYY-MM-DD HH:mm:ss';
                        break;
                    case 'minute':
                        dateFormat = 'YYYY-MM-DD HH:mm';
                        break;
                    case 'hour':
                        dateFormat = 'YYYY-MM-DD HH:00';
                        break;
                    case 'day':
                        dateFormat = 'YYYY-MM-DD';
                        break;
                    case 'week':
                        dateFormat = '[Week of] YYYY-MM-DD';
                        break;
                }

                $(this).attr('title',
                    `Date: ${moment(timestamp).format(dateFormat)}\n` +
                    `Count: ${count}\n` +
                    `Errors: ${errors}`
                );
            });

            $('#histogram').append(bar);

            let xAxisLabel = moment(log.timestamp).format('YYYY-MM-DD');

            $('#x-axis').append(`<span>${xAxisLabel}</span>`);
        });

        updateTimeRange(interval);
    }

    // Dropdown handlers
    $('.interval-btn').click(function(e) {
        e.stopPropagation();
        $('.interval-menu').toggleClass('show');
    });

    $('.dropdown-item').click(function() {
        const selectedValue = $(this).text();
        $('.selected-interval').text(selectedValue);
        $('.interval-menu').removeClass('show');
        renderHistogram(selectedValue);
    });

    // Close dropdowns when clicking outside
    $(document).click(function(e) {
        if (!$(e.target).closest('.dropdown-container').length) {
            $('.dropdown-menu').removeClass('show');
        }
    });


    function fetchLogs() {
        // Simulated data - have to replace with actual API call
        logsData = [
            { timestamp: '2025-02-10T05:30:00.000Z', count: 1200, error: 'Server Error' },
            { timestamp: '2025-02-10T05:30:01.000Z', count: 1800, error: 'Database Error' },
            { timestamp: '2025-02-10T05:30:02.000Z', count: 500, error: 'Memory Leak' },
            { timestamp: '2025-02-10T05:30:03.000Z', count: 900, error: 'Server Down'},
            { timestamp: '2025-02-10T05:30:04.000Z', count: 900, error: 'Network Timeout' },
            { timestamp: '2025-02-10T05:30:05.000Z', count: 600, error: 'File Not Found' },
            { timestamp: '2025-02-10T05:30:06.000Z', count: 1500, error: 'Unauthorized Access' },
            { timestamp: '2025-02-10T05:30:07.000Z', count: 1700, error: 'High CPU Usage' },
            { timestamp: '2025-02-10T05:30:08.000Z', count: 2000, error: 'System Crash' },
            { timestamp: '2025-02-10T05:30:09.000Z', count: 1200, error: 'Server Error' },
            { timestamp: '2025-02-10T05:30:10.000Z', count: 1800, error: 'Database Error' },
            { timestamp: '2025-02-10T05:30:11.000Z', count: 500, error: 'Memory Leak' },
            { timestamp: '2025-02-10T05:30:12.000Z', count: 900, error: 'Network Timeout' },
            { timestamp: '2025-02-11T03:30:00.000Z', count: 600, error: 'File Not Found' },
            { timestamp: '2025-02-11T03:30:03.000Z', count: 1500, error: 'Unauthorized Access' },
            { timestamp: '2025-02-12T07:30:07.000Z', count: 1700, error: 'High CPU Usage' },
            { timestamp: '2025-02-12T07:31:03.000Z', count: 2000, error: 'System Crash' },
            { timestamp: '2025-02-13T05:40:04.000Z', count: 900, error: 'Network Timeout' },
            { timestamp: '2025-02-13T05:10:03.000Z', count: 600, error: 'File Not Found' },
            { timestamp: '2025-02-14T05:50:03.000Z', count: 1500, error: 'Unauthorized Access' },
            { timestamp: '2025-02-14T05:20:03.000Z', count: 1700, error: 'High CPU Usage' },
            { timestamp: '2025-02-15T05:00:03.000Z', count: 2000, error: 'System Crash' },
            { timestamp: '2025-02-15T05:45:03.000Z', count: 1200, error: 'Server Error' },
            { timestamp: '2025-02-16T05:15:03.000Z', count: 1800, error: 'Database Error' },
            { timestamp: '2025-02-16T05:37:03.000Z', count: 500, error: 'Memory Leak' },
            { timestamp: '2025-02-17T05:56:03.000Z', count: 900, error: 'Network Timeout' },
            { timestamp: '2025-02-17T05:23:03.000Z', count: 600, error: 'File Not Found' },
            { timestamp: '2025-02-18T05:44:03.000Z', count: 1500, error: 'Unauthorized Access' },
            { timestamp: '2025-02-18T05:03:03.000Z', count: 1700, error: 'High CPU Usage' },
            { timestamp: '2025-02-19T05:19:03.000Z', count: 2000, error: 'System Crash' },
        ];
    }

    // Initial fetch
    fetchLogs();
});