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

function datePickerHandler(startDate, endDate, label) {
    let displayLabel = 'Last 15 Mins';
    filterStartDate = startDate._i || startDate;
    filterEndDate = endDate._i || endDate;
    displayEnd = moment().valueOf();

    // if (label !== '') {
    //     label = 'now-15m';
    // }

    switch (label) {
        case 'Last 5 Mins':
        case 'now-5m':
            displayStart = moment().subtract(5, 'minutes').valueOf();
            displayLabel = 'Last 5 Mins';
            break;
        case 'Last 15 Mins':
        case 'now-15m':
            displayStart = moment().subtract(15, 'minutes').valueOf();
            displayLabel = 'Last 15 Mins';
            break;
        case 'Last 30 Mins':
        case 'now-30m':
            displayStart = moment().subtract(30, 'minutes').valueOf();
            displayLabel = 'Last 30 Mins';
            break;
        case 'Last 1 Hr':
        case 'now-1h':
            displayStart = moment().subtract(1, 'hours').valueOf();
            displayLabel = 'Last 1 Hr';
            break;
        case 'Last 2 Hrs':
        case 'now-2h':
            displayStart = moment().subtract(2, 'hours').valueOf();
            displayLabel = 'Last 2 Hrs';
            break;
        case 'Last 3 Hrs':
        case 'now-3h':
            displayStart = moment().subtract(3, 'hours').valueOf();
            displayLabel = 'Last 3 Hrs';
            break;
        case 'Last 6 Hrs':
        case 'now-6h':
            displayStart = moment().subtract(6, 'hours').valueOf();
            displayLabel = 'Last 6 Hrs';
            break;
        case 'Last 12 Hrs':
        case 'now-12h':
            displayStart = moment().subtract(12, 'hours').valueOf();
            displayLabel = 'Last 12 Hrs';
            break;
        case 'Last 24 Hrs':
        case 'now-24h':
            displayStart = moment().subtract(24, 'hours').valueOf();
            displayLabel = 'Last 24 Hrs';
            break;
        case 'Last 2 Days':
        case 'now-2d':
            displayStart = moment().subtract(2, 'days').valueOf();
            displayLabel = 'Last 2 Days';
            break;
        case 'Last 7 Days':
        case 'now-7d':
            displayStart = moment().subtract(7, 'days').valueOf();
            displayLabel = 'Last 7 Days';
            break;
        case 'Last 30 Days':
        case 'now-30d':
            displayStart = moment().subtract(30, 'days').valueOf();
            displayLabel = 'Last 30 Days';
            break;
        case 'Last 90 Days':
        case 'now-90d':
            displayStart = moment().subtract(90, 'days').valueOf();
            displayLabel = 'Last 90 Days';
            break;
        case 'now-180d':
            displayStart = moment().subtract(180, 'days').valueOf();
            displayLabel = 'Last 180 Days';
            break;
        case 'now-365d':
            displayStart = moment().subtract(365, 'days').valueOf();
            displayLabel = 'Last 1 Year';
            break;
        case 'custom':
            displayStart = filterStartDate;
            displayEnd = filterEndDate;
            displayLabel = 'Custom';
            break;
    }
    $('.panelEditor-container #date-picker-btn span').html(displayLabel);
    $('#app-container #date-picker-btn span').html(displayLabel);
    $('#alerting-container #date-picker-btn span').html(displayLabel);
    $('#cstats-time-picker #date-picker-btn span').html(displayLabel);
    let currentPage=window.location.href
    if(!(currentPage.includes("cluster-stats.html"))){
        Cookies.set('startEpoch', filterStartDate);
        Cookies.set('endEpoch', filterEndDate);
    }
}