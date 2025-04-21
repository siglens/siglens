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
                            <i class="dropdown-arrow"></i>
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
            $('#date-range-error').remove();

            if (!tempStartDate || !tempEndDate) {
                evt.preventDefault();
                evt.stopPropagation();
                if (!tempStartDate) $('#date-start').addClass('error');
                if (!tempEndDate) $('#date-end').addClass('error');

                $('#daterange-to').after('<div id="date-range-error" class="date-range-error">Please select both start and end dates</div>');

                setTimeout(function () {
                    $('#date-start, #date-end').removeClass('error');
                    $('#date-range-error').fadeOut(300, function () {
                        $(this).remove();
                    });
                }, 2000);
                $(this).trigger('dateRangeInvalid');
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

            if (filterEndDate <= filterStartDate) {
                evt.preventDefault();
                evt.stopPropagation();
                $('#date-start').addClass('error');
                $('#date-end').addClass('error');
                $('.panelEditor-container #date-start').addClass('error');
                $('.panelEditor-container #date-end').addClass('error');
                
                $('#daterange-to').after('<div id="date-range-error" class="date-range-error">End date must be after start date</div>');
        
                setTimeout(function () {
                    $('#date-start, #date-end').removeClass('error');
                    $('.panelEditor-container #date-start, .panelEditor-container #date-end').removeClass('error');
                    $('#date-range-error').fadeOut(300, function() { $(this).remove(); });
                }, 2000);
                $(this).trigger('dateRangeInvalid');
                return;
            }

            Cookies.set('customStartDate', appliedStartDate);
            Cookies.set('customStartTime', appliedStartTime);
            Cookies.set('customEndDate', appliedEndDate);
            Cookies.set('customEndTime', appliedEndTime);

            datePickerHandler(filterStartDate, filterEndDate, 'custom');

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
            $('#date-picker-btn').removeClass('active');
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
