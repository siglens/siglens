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
  getValuesOfColumn("service", "Service");
  getValuesOfColumn("name", "Operation");
  handleSort();
  handleDownload();
  $("#search-trace-btn").on("click", searchTraceHandler);
  


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
var chart;
let currList = [];
let curSpanTraceArray = [],
  curErrorTraceArray = [],
  timeList = [],
  returnResAdd = [],
  returnResTotal = [];
let pageNumber = 1, traceSize = 0, params = {};
function getValuesOfColumn(chooseColumn, spanName) {
  let param = {
    state: "query",
    searchText: "SELECT DISTINCT " + chooseColumn + " FROM `ind-0`",
    startEpoch: "now-24h",
    endEpoch: filterEndDate,
    indexName: "traces",
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
function handleSort(){
  let currList = ["Most Recent", "Span Number", "Errors Number"];
  $("#sort-dropdown").singleBox({
    spanName: "Most Recent",
    dataList: currList,
    clicked: function (e) {
      if (e.target.innerText == "Most Recent") {
        returnResTotal = returnResTotal.sort(compare("start_time"));
      } else if (e.target.innerText == "Span Number") {
        returnResTotal = returnResTotal.sort(compare("span_count"));
      } else if (e.target.innerText == "Errors Number") {
        returnResTotal = returnResTotal.sort(compare("span_errors_count"));
      }
      reSort();
    },
  });
}
function compare(property) {
  return function (object1, object2) {
    let value1 = object1[property];
    let value2 = object2[property];
    return value2 - value1;
  };
}
function handleDownload(){
  let currList = ["Download as CSV", "Download as JSON"];
  $("#download-dropdown").singleBox({
    fillIn: false,
    spanName: "Download Result",
    dataList: currList,
    clicked: function (e) {
      if (e.target.innerText == "Download as CSV") {
        $("#download-trace").download({
          data: returnResTotal,
          downloadMethod: ".csv",
        });
      } else if (e.target.innerText == "Download as JSON") {
        $("#download-trace").download({
          data: returnResTotal,
          downloadMethod: ".json",
        });
      }
    },
  });
}
function searchTraceHandler(e){
  returnResTotal = [];
  curSpanTraceArray = [];
  curErrorTraceArray = [];
  timeList = [];
  returnResAdd = [];
  pageNumber = 1;
   traceSize = 0;
    params = {};
    $(".warn-box").remove();
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
     pageNumber = 1;
    params = {
      searchText: searchText,
      startEpoch: stDate,
      endEpoch: endDate,
      queryLanguage: "Splunk QL",
      page: pageNumber,
    };
    console.log(JSON.stringify(params));
    searchTrace(params);
}
function initChart(){
  $("#graph-show").removeClass("empty-result-show");
  pageNumber = 1; traceSize = 0;
  returnResAdd = [];
  returnResTotal = [];
  let queryParams = new URLSearchParams(window.location.search);
  let stDate =queryParams.get("startEpoch") || Cookies.get("startEpoch") || "now-15m";
  let endDate = queryParams.get("endEpoch") || Cookies.get("endEpoch") || "now";
  params = {
    searchText: "*",
    startEpoch: "now-24h",
    endEpoch: endDate,
    queryLanguage: "Splunk QL",
    page: pageNumber,
  };
    searchTrace(params);
}
function searchTrace(params){
  $.ajax({
    method: "post",
    url: "api/traces/search",
    headers: {
      "Content-Type": "application/json; charset=utf-8",
      Accept: "*/*",
    },
    crossDomain: true,
    dataType: "json",
    data: JSON.stringify(params),
  }).then((res) => {
    if (res && res.traces && res.traces.length > 0) {
      //concat new traces results
      returnResTotal = returnResTotal.concat(res.traces);
      returnResTotal = returnResTotal.sort(compare("start_time"));
      returnResAdd = res.traces;
      //reset total size
      traceSize = returnResTotal.length;
      $("#traces-number").text(traceSize + " Traces");
      for (let i = 0; i < traceSize; i++) {
        let json = returnResTotal[i];
        let milliseconds = Number(json.start_time / 1000000);
        let dataInfo = new Date(milliseconds);
        let dataStr = dataInfo.toLocaleString().toLowerCase();
        let duration = Number((json.end_time - json.start_time) / 1000000);
        let newArr = [i, duration, json.span_count, json.span_errors_count];
        timeList.push(dataStr);
        if(json.span_errors_count == 0 && i != 1) curSpanTraceArray.push(newArr);
        else curErrorTraceArray.push([i, duration, json.span_count, 10]);
      }
      showScatterPlot();
      reSort();
    }else{
      returnResAdd = [];
      if (returnResTotal.length == 0) {
        $("#traces-number").text("0 Traces");
        let queryText = "Your query returned no data, adjust your query.";
        $("#graph-show").html(queryText);
        $("#graph-show").addClass("empty-result-show");
        chart.dispose();
      }
    }
  });
}
function showScatterPlot() {
  $("#graph-show").removeClass("empty-result-show");
  let chartId = document.getElementById("graph-show");
  if (chart != null && chart != "" && chart != undefined) {
    echarts.dispose(chart);
  }
  // else if ($("#graph-show").hasClass("empty-result-show")) $("#graph-show").removeClass("empty-result-show");
  chart = echarts.init(chartId);
  chart.setOption({
    xAxis: {
      type: "category",
      name: "Time",
      data: timeList,
      scale: true,
      axisLine: {
        show: true,
      },
    },
    yAxis: {
      type: "value",
      name: "Duration",
      scale: true,
      axisLine: {
        show: true,
      },
    },
    tooltip: {
      show: true,
      formatter: function(param){
        var green = param.value[2];
        var red = param.value[3];
        return(
          "<div>" + green + " Spans</div>"
          +
          "<div>" + red + " Errors</div>");
      }
    },
    series: [
      {
        type: "effectScatter",
        showEffectOn: "emphasis",
        rippleEffect: {
          scale: 1,
        },
        data: curSpanTraceArray,
        symbolSize: function (val) {
          return val[2];
        },
        itemStyle: {
          color: "rgba(1, 191, 179, 0.5)",
        },
      },
      {
        type: "effectScatter",
        showEffectOn: "emphasis",
        rippleEffect: {
          scale: 1,
        },
        data: curErrorTraceArray,
        symbolSize: function (val) {
          return val[3];
        },
        itemStyle: {
          color: "rgba(233, 49, 37, 0.5)",
        },
      },
    ],
  });
}
function reSort(){
  let curSize = returnResTotal.length;
  $(".warn-box").remove();
  for (let i = 0; i < returnResTotal.length; i++) {
    $("#warn-bottom").append(`<div class="warn-box"><div class="warn-head">
                            <span  class = "span-id">Frontend: /dispatch <span id = "span-id-${
                              traceSize - curSize + i
                            }"></span></span>
                            <span class = "duration-time" id  = "duration-time-${
                              traceSize - curSize + i
                            }"></span>
                        </div>
                        <div class="warn-content">
                            <div class="spans-box">
                            <div class = "total-span" id = "total-span-${
                              traceSize - curSize + i
                            }"></div>
                            <div class = "error-span" id = "error-span-${
                              traceSize - curSize + i
                            }"></div>
                            </div>
                            <div> </div>
                            <div class="warn-content-right">
                                <span class = "start-time" id = "start-time-${
                                  traceSize - curSize + i
                                }"></span>
                                <span class = "how-long-time" id = "how-long-time-${
                                  traceSize - curSize + i
                                }"></span>
                            </div>
                        </div></div>`);
    let json = returnResTotal[i];
    $(`#span-id-${traceSize - curSize + i}`).text(json.trace_id);
    $(`#total-span-${traceSize - curSize + i}`).text(
      json.span_count + " Spans"
    );
    $(`#error-span-${traceSize - curSize + i}`).text(
      json.span_errors_count + " Errors"
    );
    let duration = Number((json.end_time - json.start_time) / 1000000);
    $(`#duration-time-${traceSize - curSize + i}`).text(
      Math.round(duration * 100) / 100 + "ms"
    );
    let milliseconds = Number(json.start_time / 1000000);
    let dataStr = new Date(milliseconds).toLocaleString();
    let dateText = "";
    let date = dataStr.split(",");
    let dateTime = date[0].split("/");
    //current date
    const currentDate = new Date();
    const currentYear = currentDate.getFullYear();
    const currentMonth = currentDate.getMonth() + 1;
    const currentDay = currentDate.getDate();
    if (
      currentYear === dateTime[2] &&
      currentMonth === dateTime[0] &&
      currentDay === dateTime[1]
    ) {
      dateText = "Today | ";
    } else {
      dateText = date[0] + " | ";
    }
    dateText += date[1].toLowerCase();
    $(`#start-time-${traceSize - curSize + i}`).text(dateText);
    $(`#how-long-time-${traceSize - curSize + i}`).text(
      calculateTimeToNow(json.start_time) + " hours ago"
    );
  }
}
function showSpanRes(){
  let curSize = returnResAdd.length;
  for (let i = 0; i < returnResAdd.length; i++) {
    $("#warn-bottom").append(`<div class="warn-box"><div class="warn-head">
                            <span  class = "span-id">Frontend: /dispatch <span id = "span-id-${traceSize - curSize + i}"></span></span>
                            <span class = "duration-time" id  = "duration-time-${traceSize - curSize + i}"></span>
                        </div>
                        <div class="warn-content">
                            <div class="spans-box">
                            <div class = "total-span" id = "total-span-${traceSize - curSize + i}"></div>
                            <div class = "error-span" id = "error-span-${traceSize - curSize + i}"></div>
                            </div>
                            <div> </div>
                            <div class="warn-content-right">
                                <span class = "start-time" id = "start-time-${traceSize - curSize + i}"></span>
                                <span class = "how-long-time" id = "how-long-time-${traceSize - curSize + i}"></span>
                            </div>
                        </div></div>`);
    let json = returnResAdd[i];
    $(`#span-id-${traceSize - curSize + i}`).text(json.trace_id);
    $(`#total-span-${traceSize - curSize + i}`).text(json.span_count + " Spans");
    $(`#error-span-${traceSize - curSize + i}`).text(json.span_errors_count + " Errors");
    let duration = Number((json.end_time - json.start_time) / 1000000);
    $(`#duration-time-${traceSize - curSize + i}`).text(Math.round(duration * 100) / 100 + "ms");
    let milliseconds = Number(json.start_time / 1000000);
    let dataStr = new Date(milliseconds).toLocaleString();
    let dateText = "";
    let date = dataStr.split(",");
    let dateTime = date[0].split("/");
    //current date
    const currentDate = new Date();
    const currentYear = currentDate.getFullYear();
    const currentMonth = currentDate.getMonth() + 1; 
    const currentDay = currentDate.getDate();
    if (
      currentYear === dateTime[2] &&
      currentMonth === dateTime[0] &&
      currentDay === dateTime[1]
    ) {
      dateText = "Today | "
    }else{
      dateText = date[0] + " | "
    }
    dateText += date[1].toLowerCase();
    $(`#start-time-${traceSize - curSize + i}`).text(dateText);
    $(`#how-long-time-${traceSize - curSize + i}`).text(
      calculateTimeToNow(json.start_time) + " hours ago"
    );
  }
}

$('#warn-bottom').unbind("scroll").on("scroll", function (e) {
    var sum = this.scrollHeight;
    var $obj = $(this);
    if (sum <= $obj.scrollTop() + $obj.height()) {
      params.page = params.page + 1;
      searchTrace(params);
    }
})



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