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

$(document).ready(() => {
  displayNavbar();
  initChart();
  if (Cookies.get("theme")) {
    theme = Cookies.get("theme");
    $("body").attr("data-theme", theme);
  }
  $(".theme-btn").on("click", themePickerHandler);
  $("#service-dropdown").singleBox({
    spanName: "Service",
  });
  $("#operation-dropdown").singleBox({
    spanName: "Operation",
  });
  $("#sort-dropdown").singleBox({
    spanName: "MostRecent",
  });
  $("#service-btn").on("click", getServiceListHandler);
  $("#operation-btn").on("click", getOperationListHandler);
  $("#search-trace-btn").on("click", searchTraceHandler);
  $("#download-trace").on("click", downloadTrace);
  


    $('#date-picker-btn').on('show.bs.dropdown', showDatePickerHandler);
    $('#date-picker-btn').on('hide.bs.dropdown', hideDatePickerHandler);
    $('#reset-timepicker').on('click', resetDatePickerHandler);

    $('.panelEditor-container #date-start').on('change', getStartDateHandler);
    $('.panelEditor-container #date-end').on('change', getEndDateHandler);
    $('.panelEditor-container #time-start').on('change', getStartTimeHandler);
    $('.panelEditor-container #time-end').on('change', getEndTimeHandler);
    $('.panelEditor-container #customrange-btn').on('click', customRangeHandler);

    $('#date-start').on('change', getStartDateHandler);
    $('#date-end').on('change', getEndDateHandler);

    $('#time-start').on('change', getStartTimeHandler);
    $('#time-end').on('change', getEndTimeHandler);
    $('#customrange-btn').on('click', customRangeHandler);

    $('.range-item').on('click', rangeItemHandler)
    $('.db-range-item').on('click', dashboardRangeItemHandler)
});

let currList = [];
let curTraceArray = [], timeList = [], returnRes = [];
function getValuesOfColumn(chooseColumn, spanName) {
  let param = {
    state: "query",
    searchText: "SELECT DISTINCT " + chooseColumn + " FROM `ind-0`",
    startEpoch: filterStartDate,
    endEpoch: filterEndDate,
    indexName: "*",
    queryLanguage: "SQL",
    from: 0,
  };
  $.ajax({
    method: "post",
    url: "api/search",
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      Accept: "*/*",
    },
    crossDomain: true,
    dataType: "json",
    data: JSON.stringify(param),
  }).then((res) => {
    let valuesOfColumn = new Set();
    if (res && res.hits && res.hits.records) {
      for (let i = 0; i < res.hits.records.length; i++) {
        let cur = res.hits.records[i][chooseColumn];
        if (typeof cur == "string") valuesOfColumn.add(cur);
        else valuesOfColumn.add(cur.toString());
      }
    }
    currList = Array.from(valuesOfColumn);
    $(`#${chooseColumn}-dropdown`).singleBox({
      spanName: spanName,
      dataList: currList,
    });
  });
}
function getServiceListHandler(e){
    getValuesOfColumn("service", "Service");
}
function getOperationListHandler(e){
    getValuesOfColumn("name", "Operation");
}
function searchTraceHandler(e){
    let serviceValue = $("#service-span-name").text();
    let operationValue = $("#operation-span-name").text();
    let tagValue = $("#tags-input").val();
    let maxDurationValue = $("#max-duration-input").val();
    let minDurationValue = $("#min-duration-input").val();
    let limitResValue = $("#limit-result-input").val();
    let searchText = "";
    if(serviceValue != "Service") searchText = "service=" + serviceValue + " "; 
    if (operationValue != "Operation") searchText += "name=" + operationValue + " ";
    if (maxDurationValue) searchText += "EndTimeUnixNano=" + maxDurationValue + " ";
    if (minDurationValue) searchText += "StartTimeUnixNano=" + minDurationValue + " ";
    if (tagValue) searchText += tagValue;
    if (searchText == "") searchText = "*";
    else searchText = searchText.trim();
    let queryParams = new URLSearchParams(window.location.search);
     let stDate = queryParams.get("startEpoch") || Cookies.get('startEpoch') || "now-15m";
     let endDate = queryParams.get("endEpoch") || Cookies.get('endEpoch') || "now";
    let params = {
        'searchText': searchText,
         'startEpoch': stDate,
         'endEpoch': endDate,
         'queryLanguage': "Splunk QL",
         'size': 20
    }
    console.log(JSON.stringify(params));
    searchTrace(params);
}
function initChart(){
  let queryParams = new URLSearchParams(window.location.search);
  let stDate =queryParams.get("startEpoch") || Cookies.get("startEpoch") || "now-15m";
  let endDate = queryParams.get("endEpoch") || Cookies.get("endEpoch") || "now";
  let params = {
        'searchText': '*',
         'startEpoch': stDate,
         'endEpoch': endDate,
         'queryLanguage': "Splunk QL",
         'size': 20
    }
    searchTrace(params);
}
function searchTrace(params){
  //fake data
  timeList = [
    "11/29/2023, 5:44:09 PM",
    "11/29/2023, 5:48:41 PM",
    "11/29/2023, 5:39:59 PM",
    "11/29/2023, 5:41:27 PM",
    "11/29/2023, 5:39:00 PM",
  ];
  curTraceArray = [
    [0, 6.804224],
    [1, 8.663552],
    [2, 6.668032],
    [3, 5.275392],
    [4, 4.791552],
  ];
  showScatterPlot();

  //This is useful code. The API is still being modified, so comment out the code that calls the API.
  // $.ajax({
  //   method: "post",
  //   url: "api/traces/search",
  //   headers: {
  //     "Content-Type": "application/json; charset=utf-8",
  //     Accept: "*/*",
  //   },
  //   crossDomain: true,
  //   dataType: "json",
  //   data: JSON.stringify(params),
  // }).then((res) => {
  //   if (res && res.traces) {
    // returnRes = res.traces;
  //   $("#traces-number").text(res.traces.length + "Traces");
  //     for(let i = 0; i < 5; i++){
  //       let json = res.traces[i];
  //       let milliseconds = Number(json.start_time / 1000000);
  //       let dataStr = new Date(milliseconds).toLocaleString();
  //       let duration = Number((json.end_time - json.start_time) / 1000000);
  //       let newArr = [i, duration];
  //       timeList.push(dataStr);
  //       curTraceArray.push(newArr);
  //     }
  //     showScatterPlot();
          // showSpanRes();
  //   }
  // });
}
function showScatterPlot() {
  
  let chartId = document.getElementById("graph-show");
  var chart = echarts.init(chartId);

  let transformedTimeList = timeList.map((time) => {
    // replace AM/PM with a space
    let date = new Date(time.replace(/(AM|PM)/gi, " "));
    // convert to timestamp
    return date.getTime();
  });

  chart.setOption({
    xAxis: {
      type: "time",
      name: "Time",
      data: transformedTimeList,
    },
    yAxis: {
      type: "value",
      name: "Duration",
    },
    series: [
      {
        type: "scatter",
        data: curTraceArray,
      },
    ],
  });
}

function downloadTrace(e){
}

function showSpanRes(){
  for (let i = 0; i < returnRes.length; i++) {
    $("#warn-bottom").append(`<div class="warm-head">
                            <span>Frontend: /dispatch <span class = "span-id" id = "span-id"></span></span>
                            <span class = "duration-time" id  = "duration-time"></span>
                        </div>
                        <div class="warm-content">
                            <div>
                            <div class = "total-span" id = "total-span"></div>
                            <div class = "error-span" id = "error-span"></div>
                            </div>
                            <div>details of message</div>
                            <div class="warm-content-right">
                                <span class = "start-time" id = "start-time"></span>
                                <span class = "how-long-time" id = "how-long-time"></span>
                            </div>
                        </div>`);
    $("#span-id").text(returnRes[i].trace_id);
    $("#total-span").text(returnRes[i].span_count + " Spans");
    $("#error-span").text(returnRes[i].span_errors_count + " Errors");
    $("#duration-time").text(curTraceArray[i][1] + "ms");
    $("#start-time").text(timeList[i]);
    $("#how-long-time").text(calculateTimeToNow(returnRes[i].start_time) + "hours ago");
  }

}

function calculateTimeToNow(startTime){
  const nanosecondsTimestamp = startTime;
  const millisecondsTimestamp = nanosecondsTimestamp / 1000000;
  const now = new Date();
  const timeDifference = now.getTime() - millisecondsTimestamp;
  const hours = Math.floor(timeDifference / 3600000);
  return hours;
}

function showDatePickerHandler(evt) {
  evt.stopPropagation();
  $("#daterangepicker").toggle();
  $(evt.currentTarget).toggleClass("active");
}

function hideDatePickerHandler() {
  $("#date-picker-btn").removeClass("active");
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
  $("#daterangepicker").toggle();
}

function rangeItemHandler(evt) {
  resetCustomDateRange();
  $.each($(".range-item.active"), function () {
    $(this).removeClass("active");
  });
  $(evt.currentTarget).addClass("active");
  datePickerHandler($(this).attr("id"), "now", $(this).attr("id"));
  $('#daterangepicker').toggle();
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
}
function hideColumnHandler(evt) {
  evt.preventDefault();
  evt.stopPropagation();

  availableFieldsSelectHandler(evt);
}

function setQueryLangHandler(e) {
  $(".query-language-option").removeClass("active");
  let currentTab = $("#custom-code-tab").tabs("option", "active");
  if ($(this).attr("id").split("-")[1] != "3" && currentTab == 0) {
    $("#custom-code-tab").tabs("option", "active", 1);
  }
  $("#query-language-btn span").html($(this).html());
  displayQueryLangToolTip($(this).attr("id").split("-")[1]);
  $(this).addClass("active");
}