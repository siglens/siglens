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

(function ($) {
    $.fn.timeTicker = function (options) {
        var defaults = {
            spanName: 'Time Picker',
        };
        let setting = $.extend(defaults, options || {});
        this.append(`<button class="btn dropdown-toggle" type="button" id="date-picker-btn" data-toggle="dropdown" aria-haspopup="true"
                            aria-expanded="false" data-bs-toggle="dropdown" title="Pick the time window">
                            <span id="span-time-value">${setting.spanName}</span>
                            <img class="dropdown-arrow orange" src="assets/arrow-btn.svg">
                            <img class="dropdown-arrow blue" src="assets/up-arrow-btn-light-theme.svg">
                        </button>
                        <div class="dropdown-menu daterangepicker dropdown-time-picker" aria-labelledby="index-btn" id="daterangepicker">
                            <p class="dt-header">Search the last</p>
                            <div class="ranges">
                                <div class="inner-range">
                                    <div id="now-5m" class="range-item ">5 Mins</div>
                                    <div id="now-3h" class="range-item">3 Hrs</div>
                                    <div id="now-2d" class="range-item">2 Days</div>
                                </div>
                                <div class="inner-range">
                                    <div id="now-15m" class="range-item">15 Mins</div>
                                    <div id="now-6h" class="range-item">6 Hrs</div>
                                    <div id="now-7d" class="range-item">7 Days</div>
                                </div>
                                <div class="inner-range">
                                    <div id="now-30m" class="range-item">30 Mins</div>
                                    <div id="now-12h" class="range-item">12 Hrs</div>
                                    <div id="now-30d" class="range-item">30 Days</div>
                                </div>
                                <div class="inner-range">
                                    <div id="now-1h" class="range-item">1 Hr</div>
                                    <div id="now-24h" class="range-item">24 Hrs</div>
                                    <div id="now-90d" class="range-item">90 Days</div>
                                </div>
                                <hr>
                                </hr>
                                <div class="dt-header">Custom Search <span id="reset-timepicker" type="reset">Reset</span>
                                </div>
                                <div id="daterange-from"> <span id="dt-from-text"> From</span> <br />
                                    <input type="date" id="date-start" />
                                    <input type="time" id="time-start" value="00:00" />
                                </div>
                                <div id="daterange-to"> <span id="dt-to-text"> To </span> <br />
                                    <input type="date" id="date-end">
                                    <input type="time" id="time-end" value="00:00">
                                </div>
                                <div class="drp-buttons">
                                    <button class="applyBtn btn btn-sm btn-primary" id="customrange-btn" type="button">Apply</button>
                                </div>
                            </div>
                        </div>`);

        $('#date-picker-btn').on('click', showDatePickerHandler);
        $(document).on('click', function (event) {
            if (!$(event.target).closest('#daterangepicker').length) {
                $('#daterangepicker').removeClass('show').hide();
                resetTempValues();
            }
        });
        $('#reset-timepicker').on('click', resetDatePickerHandler);

        $('#date-start').on('change', getStartDateHandler);
        $('#date-end').on('change', getEndDateHandler);
        $('#time-start').on('change', getStartTimeHandler);
        $('#time-end').on('change', getEndTimeHandler);

        $('#customrange-btn').on('click', customRangeHandler);
        $('.range-item').on('click', rangeItemHandler);

        let tempStartDate, tempStartTime, tempEndDate, tempEndTime;
        let appliedStartDate, appliedStartTime, appliedEndDate, appliedEndTime;

        function initializeDatePicker() {
            // Initialize with applied values, current values, or from cookies
            appliedStartDate = tempStartDate = $('#date-start').val() || Cookies.get('customStartDate') || '';
            appliedStartTime = tempStartTime = $('#time-start').val() || Cookies.get('customStartTime') || '';
            appliedEndDate = tempEndDate = $('#date-end').val() || Cookies.get('customEndDate') || '';
            appliedEndTime = tempEndTime = $('#time-end').val() || Cookies.get('customEndTime') || '';

            $('#date-start').val(appliedStartDate).toggleClass('active', !!appliedStartDate);
            $('#date-end').val(appliedEndDate).toggleClass('active', !!appliedEndDate);

            if (appliedStartDate) {
                $('#time-start').val(appliedStartTime).addClass('active');
            } else {
                $('#time-start').val('00:00').removeClass('active');
            }

            if (appliedEndDate) {
                $('#time-end').val(appliedEndTime).addClass('active');
            } else {
                $('#time-end').val('00:00').removeClass('active');
            }
        }

        function resetTempValues() {
            tempStartDate = appliedStartDate;
            tempStartTime = appliedStartTime;
            tempEndDate = appliedEndDate;
            tempEndTime = appliedEndTime;

            // Reset the input values to the applied values
            $('#date-start').val(appliedStartDate);
            $('#date-end').val(appliedEndDate);
            $('#time-start').val(appliedStartTime);
            $('#time-end').val(appliedEndTime);
        }

        function showDatePickerHandler(evt) {
            evt.stopPropagation();
            $('#daterangepicker').toggle();
            $('#daterangepicker').addClass('show');
            $(evt.currentTarget).toggleClass('active');
            initializeDatePicker();
        }

        function hideDatePickerHandler() {
            $('#date-picker-btn').removeClass('active');
            resetTempValues();
        }

        function resetDatePickerHandler(evt) {
            evt.stopPropagation();
            resetCustomDateRange();
            $.each($('.range-item.active'), function () {
                $(this).removeClass('active');
            });
        }

        function getStartDateHandler(_evt) {
            tempStartDate = this.value;
            $(this).addClass('active');
        }

        function getEndDateHandler(_evt) {
            tempEndDate = this.value;
            $(this).addClass('active');
        }

        function getStartTimeHandler() {
            tempStartTime = $(this).val();
            $(this).addClass('active');
        }

        function getEndTimeHandler() {
            tempEndTime = $(this).val();
            $(this).addClass('active');
        }

        function customRangeHandler(evt) {
            evt.stopPropagation();

            if (!tempStartDate || !tempEndDate) {
                if (!tempStartDate) $('#date-start').addClass('error');
                if (!tempEndDate) $('#date-end').addClass('error');
                return;
            } else {
                $.each($('.range-item.active, .db-range-item.active'), function () {
                    $(this).removeClass('active');
                });
            }

            // Apply the temporary values
            appliedStartDate = tempStartDate;
            appliedStartTime = tempStartTime || '00:00';
            appliedEndDate = tempEndDate;
            appliedEndTime = tempEndTime || '00:00';
            $('#time-start', '#time-end').addClass('active');

            // Calculate start and end times
            let startDate = new Date(`${appliedStartDate}T${appliedStartTime}`);
            let endDate = new Date(`${appliedEndDate}T${appliedEndTime}`);

            filterStartDate = startDate.getTime();
            filterEndDate = endDate.getTime();

            Cookies.set('customStartDate', appliedStartDate);
            Cookies.set('customStartTime', appliedStartTime);
            Cookies.set('customEndDate', appliedEndDate);
            Cookies.set('customEndTime', appliedEndTime);

            datePickerHandler(filterStartDate, filterEndDate, 'custom');
            // For dashboards
            const currentUrl = window.location.href;
            if (currentUrl.includes('dashboard.html')) {
                if (currentPanel) {
                    if (currentPanel.queryData) {
                        if (currentPanel.chartType === 'Line Chart' || currentPanel.queryType === 'metrics') {
                            currentPanel.queryData.start = filterStartDate.toString();
                            currentPanel.queryData.end = filterEndDate.toString();
                        } else {
                            currentPanel.queryData.startEpoch = filterStartDate;
                            currentPanel.queryData.endEpoch = filterEndDate;
                        }
                    }
                } else if (!currentPanel) {
                    // if user is on dashboard screen
                    localPanels.forEach((panel) => {
                        delete panel.queryRes;
                        if (panel.queryData) {
                            if (panel.chartType === 'Line Chart' || panel.queryType === 'metrics') {
                                panel.queryData.start = filterStartDate.toString();
                                panel.queryData.end = filterEndDate.toString();
                            } else {
                                panel.queryData.startEpoch = filterStartDate;
                                panel.queryData.endEpoch = filterEndDate;
                            }
                        }
                    });
                    displayPanels();
                }
            }
            $('#daterangepicker').removeClass('show').hide();
        }

        function rangeItemHandler(evt) {
            evt.stopPropagation();

            resetCustomDateRange();
            $.each($('.range-item.active'), function () {
                $(this).removeClass('active');
            });
            $(evt.currentTarget).addClass('active');
            datePickerHandler($(this).attr('id'), 'now', $(this).attr('id'));
            $('#daterangepicker').removeClass('show').hide();
        }

        function resetCustomDateRange() {
            // clear custom selections
            $('#date-start').val('');
            $('#date-end').val('');
            $('#time-start').val('00:00');
            $('#time-end').val('00:00');
            $('#date-start').removeClass('active error');
            $('#date-end').removeClass('active error');
            $('#time-start').removeClass('active');
            $('#time-end').removeClass('active');
            Cookies.remove('customStartDate');
            Cookies.remove('customEndDate');
            Cookies.remove('customStartTime');
            Cookies.remove('customEndTime');
            appliedStartDate = tempStartDate = '';
            appliedEndDate = tempEndDate = '';
            appliedStartTime = tempStartTime = '';
            appliedEndTime = tempEndTime = '';
        }

        return this;
    };
})(jQuery);
