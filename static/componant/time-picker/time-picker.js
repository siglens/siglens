(function ($) {
  $.fn.timeTicker = function (options) {
    var defaults = {
      spanName: "Time Picker"
    };
    let setting = $.extend(defaults, options || {});
    this
      .append(`<button class="btn dropdown-toggle" type="button" id="date-picker-btn" data-toggle="dropdown" aria-haspopup="true"
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
    $("#date-picker-btn").on("click", showDatePickerHandler);
    $("#reset-timepicker").on("click", resetDatePickerHandler);

    $(".panelEditor-container #date-start").on("change", getStartDateHandler);
    $(".panelEditor-container #date-end").on("change", getEndDateHandler);
    $(".panelEditor-container #time-start").on("change", getStartTimeHandler);
    $(".panelEditor-container #time-end").on("change", getEndTimeHandler);
    $(".panelEditor-container #customrange-btn").on(
      "click",
      customRangeHandler
    );
    $(document).mouseup(function (e) {
        var pickerInfo = $("#daterangepicker");
        if (!pickerInfo.is(e.target) && pickerInfo.has(e.target).length === 0) {
          $("#daterangepicker").hide();
          $("#date-picker-btn").removeClass("active");
          $("#daterangepicker").removeClass("show");
        }
    });
    $("#daterangepicker").on("click", timePickerHandler);
    $("#date-start").on("change", getStartDateHandler);
    $("#date-end").on("change", getEndDateHandler);

    $("#time-start").on("change", getStartTimeHandler);
    $("#time-end").on("change", getEndTimeHandler);
    $("#customrange-btn").on("click", customRangeHandler);

    $(".range-item").on("click", rangeItemHandler);
    $(".db-range-item").on("click", dashboardRangeItemHandler);

function timePickerHandler(){
    $("#daterangepicker").addClass("show");
}
function showDatePickerHandler(evt) {
  evt.stopPropagation();
  if(!$("#daterangepicker").hasClass("show")){
    $("#daterangepicker").addClass("show");
     $("#daterangepicker").show();
     $(evt.currentTarget).toggleClass("active");
  }else{
    $("#daterangepicker").hide();
    $("#date-picker-btn").removeClass("active");
    $("#daterangepicker").removeClass("show");
  }
}

function resetDatePickerHandler(evt) {
  evt.stopPropagation();
  resetCustomDateRange();
  $.each($(".range-item.active"), function () {
    $(this).removeClass("active");
  });
}
function getStartDateHandler(evt) {
  let inputDate = new Date(this.value);
  filterStartDate = inputDate.getTime();
  $(this).addClass("active");
  Cookies.set("customStartDate", this.value);
}

function getEndDateHandler(evt) {
  let inputDate = new Date(this.value);
  filterEndDate = inputDate.getTime();
  $(this).addClass("active");
  Cookies.set("customEndDate", this.value);
}

function getStartTimeHandler() {
  let selectedTime = $(this).val();
  let temp =
    (Number(selectedTime.split(":")[0]) * 60 +
      Number(selectedTime.split(":")[1])) *
    60 *
    1000;
  //check if filterStartDate is a number or now-*
  if (!isNaN(filterStartDate)) {
    filterStartDate = filterStartDate + temp;
  } else {
    let start = new Date();
    start.setUTCHours(0, 0, 0, 0);
    filterStartDate = start.getTime() + temp;
  }
  $(this).addClass("active");
  Cookies.set("customStartTime", selectedTime);
}

function getEndTimeHandler() {
  let selectedTime = $(this).val();
  let temp =
    (Number(selectedTime.split(":")[0]) * 60 +
      Number(selectedTime.split(":")[1])) *
    60 *
    1000;
  if (!isNaN(filterEndDate)) {
    filterEndDate = filterEndDate + temp;
  } else {
    let start = new Date();
    start.setUTCHours(0, 0, 0, 0);
    filterEndDate = start.getTime() + temp;
  }
  $(this).addClass("active");
  Cookies.set("customEndTime", selectedTime);
}

function customRangeHandler(evt) {
  $.each($(".range-item.active"), function () {
    $(this).removeClass("active");
  });
  $.each($(".db-range-item.active"), function () {
    $(this).removeClass("active");
  });
  datePickerHandler(filterStartDate, filterEndDate, "custom");

  if (currentPanel) {
    if (currentPanel.queryData) {
      if (
        currentPanel.chartType === "Line Chart" &&
        currentPanel.queryType === "metrics"
      ) {
        currentPanel.queryData.start = filterStartDate.toString();
        currentPanel.queryData.end = filterEndDate.toString();
      } else {
        currentPanel.queryData.startEpoch = filterStartDate;
        currentPanel.queryData.endEpoch = filterEndDate;
      }
    }
  } else if (
    $(`#viewPanel-container`).css("display").toLowerCase() !== "none"
  ) {
    // if user is on view panel screen
    // get panel-index by attribute
    let panelIndex = $(`#viewPanel-container .panel`).attr("panel-index");
    // if panel has some stored query data, reset it
    if (localPanels[panelIndex].queryData) {
      delete localPanels[panelIndex].queryRes;
      if (
        localPanels[panelIndex].chartType === "Line Chart" &&
        localPanels[panelIndex].queryType === "metrics"
      ) {
        localPanels[panelIndex].queryData.start = filterStartDate.toString();
        localPanels[panelIndex].queryData.end = filterEndDate.toString();
      } else {
        localPanels[panelIndex].queryData.startEpoch = filterStartDate;
        localPanels[panelIndex].queryData.endEpoch = filterEndDate;
      }
    }
    displayPanelView(panelIndex);
  } else if (!currentPanel) {
    // if user is on dashboard screen
    localPanels.forEach((panel) => {
      delete panel.queryRes;
      if (panel.queryData) {
        if (panel.chartType === "Line Chart" && panel.queryType === "metrics") {
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

function rangeItemHandler(evt) {
  resetCustomDateRange();
  $.each($(".range-item.active"), function () {
    $(this).removeClass("active");
  });
  $(evt.currentTarget).addClass("active");
  datePickerHandler($(this).attr("id"), "now", $(this).attr("id"));
}

function dashboardRangeItemHandler(evt) {
  resetCustomDateRange();
  $.each($(".db-range-item.active"), function () {
    $(this).removeClass("active");
  });
  $(evt.currentTarget).addClass("active");
  datePickerHandler($(this).attr("id"), "now", $(this).attr("id"));

  // if user is on edit panel screen
  if (currentPanel) {
    if (currentPanel.queryData) {
      if (
        currentPanel.chartType === "Line Chart" &&
        currentPanel.queryType === "metrics"
      ) {
        currentPanel.queryData.start = filterStartDate.toString();
        currentPanel.queryData.end = filterEndDate.toString();
      } else {
        currentPanel.queryData.startEpoch = filterStartDate;
        currentPanel.queryData.endEpoch = filterEndDate;
      }
    }
  } else if (
    $(`#viewPanel-container`).css("display").toLowerCase() !== "none"
  ) {
    // if user is on view panel screen
    // get panel-index by attribute
    let panelIndex = $(`#viewPanel-container .panel`).attr("panel-index");
    // if panel has some stored query data, reset it
    if (localPanels[panelIndex].queryData) {
      delete localPanels[panelIndex].queryRes;
      if (
        localPanels[panelIndex].chartType === "Line Chart" &&
        localPanels[panelIndex].queryType === "metrics"
      ) {
        localPanels[panelIndex].queryData.start = filterStartDate.toString();
        localPanels[panelIndex].queryData.end = filterEndDate.toString();
      } else {
        localPanels[panelIndex].queryData.startEpoch = filterStartDate;
        localPanels[panelIndex].queryData.endEpoch = filterEndDate;
      }
    }
    displayPanelView(panelIndex);
  } else if (!currentPanel) {
    // if user is on dashboard screen
    localPanels.forEach((panel) => {
      delete panel.queryRes;
      if (panel.queryData) {
        if (panel.chartType === "Line Chart" && panel.queryType === "metrics") {
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
function resetCustomDateRange() {
  // clear custom selections
  $("#date-start").val("");
  $("#date-end").val("");
  $("#time-start").val("00:00");
  $("#time-end").val("00:00");
  $("#date-start").removeClass("active");
  $("#date-end").removeClass("active");
  $("#time-start").removeClass("active");
  $("#time-end").removeClass("active");
  Cookies.remove("customStartDate");
  Cookies.remove("customEndDate");
  Cookies.remove("customStartTime");
  Cookies.remove("customEndTime");
  $("#daterangepicker").removeClass("show");
  $("#daterangepicker").removeClass("active");
  $("#daterangepicker").hide();
  $("#date-picker-btn").removeClass("active");
}

    return this;
  };
})(jQuery);
