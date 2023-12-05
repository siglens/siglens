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
  handleTimePicker();
  $("#search-trace-btn").on("click", searchTraceHandler);
});
var chart;
let currList = [];
let curSpanTraceArray = [],
  curErrorTraceArray = [],
  timeList = [],
  returnResTotal = [];
let pageNumber = 1, traceSize = 0, params = {};
let limitation = -1;
function getValuesOfColumn(chooseColumn, spanName) {
  let param = {
    state: "query",
    searchText: "SELECT DISTINCT " + chooseColumn + " FROM `ind-0`",
    startEpoch: "now-3h",
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
    valuesOfColumn.add("All");
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
      defaultValue: "All"
    });
  });
}
function handleTimePicker(){
  Cookies.set("startEpoch", "now-3h");
  Cookies.set("endEpoch", "now");
  $("#lookback").timeTicker({
    spanName: "Last 3 Hrs",
  });
}
function handleSort(){
  let currList = ["Most Recent", "Longest First", "Shortest First", "Most Spans", "Least Spans"];
  $("#sort-dropdown").singleBox({
    spanName: "Most Recent",
    dataList: currList,
    clicked: function (e) {
      if (e.target.innerText == "Most Recent") {
        returnResTotal = returnResTotal.sort(compare("start_time", "most"));
      } else if (e.target.innerText == "Longest First") {
        returnResTotal = returnResTotal.sort(compareDuration("most"));
      } else if (e.target.innerText == "Shortest First") {
        returnResTotal = returnResTotal.sort(compareDuration("least"));
      } else if (e.target.innerText == "Most Spans") {
        returnResTotal = returnResTotal.sort(compare("span_count", "most"));
      } else if (e.target.innerText == "Least Spans") {
        returnResTotal = returnResTotal.sort(compare("span_count", "least"));
      }
      reSort();
    },
  });
}
function compareDuration(method) {
  return function (object1, object2) {
    let value1 = object1["end_time"] - object1["start_time"];
    let value2 = object2["end_time"] - object2["start_time"];
    if (method == "most") return value2 - value1;
    else return value1 - value2;
  };
}
function compare(property, method) {
  return function (object1, object2) {
    let value1 = object1[property];
    let value2 = object2[property];
    if(method == "most") return value2 - value1;
    else return value1 - value2;
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
  e.stopPropagation(); 
  e.preventDefault();
  returnResTotal = [];
  curSpanTraceArray = [];
  curErrorTraceArray = [];
  timeList = [];
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
    if (limitResValue) limitation = limitResValue;
    else limitation = -1;
    let searchText = "";
    if(serviceValue != "All") searchText = "service=" + serviceValue + " "; 
    if (operationValue != "All") searchText += "name=" + operationValue + " ";
    if (maxDurationValue) searchText += "EndTimeUnixNano<=" + maxDurationValue + " ";
    if (minDurationValue) searchText += "StartTimeUnixNano>=" + minDurationValue + " ";
    if (tagValue) searchText += tagValue;
    if (searchText == "") searchText = "*";
    else searchText = searchText.trim();
    let queryParams = new URLSearchParams(window.location.search);
     let stDate = queryParams.get("startEpoch") || Cookies.get('startEpoch') || "now-3h";
     let endDate = queryParams.get("endEpoch") || Cookies.get('endEpoch') || "now";
     pageNumber = 1;
    params = {
      searchText: searchText,
      startEpoch: stDate,
      endEpoch: endDate,
      queryLanguage: "Splunk QL",
      page: pageNumber,
    };
    searchTrace(params);
    handleSort();
    return false;
}
function initChart(){
  $("#graph-show").removeClass("empty-result-show");
  pageNumber = 1; traceSize = 0;
  returnResTotal = [];
  let stDate = "now-3h";
  let endDate = "now";
  params = {
    searchText: "*",
    startEpoch: stDate,
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
        if(limitation < 50 && limitation > 0){
          let newArr = res.traces.sort(compare("start_time", "most"));
          newArr.splice(limitation);
          limitation = 0;
          returnResTotal = returnResTotal.concat(newArr);
        }else{
          returnResTotal = returnResTotal.concat(res.traces);
        }
        //concat new traces results
      returnResTotal = returnResTotal.sort(compare("start_time", "most"));
      //reset total size
      traceSize = returnResTotal.length;
      $("#traces-number").text(traceSize + " Traces");
      timeList = [];
      for (let i = 0; i < traceSize; i++) {
        let json = returnResTotal[i];
        let milliseconds = Number(json.start_time / 1000000);
        let dataInfo = new Date(milliseconds);
        let dataStr = dataInfo.toLocaleString().toLowerCase();
        let duration = Number((json.end_time - json.start_time) / 1000000);
        let newArr = [i, duration, json.span_count, json.span_errors_count];
        timeList.push(dataStr);
        if(json.span_errors_count == 0) curSpanTraceArray.push(newArr);
        else curErrorTraceArray.push(newArr);
      }
      showScatterPlot();
      reSort();
    }else{
      if (returnResTotal.length == 0) {
        if (chart != null && chart != "" && chart != undefined) {
          chart.dispose();
        }
        $("#traces-number").text("0 Traces");
        let queryText = "Your query returned no data, adjust your query.";
        $("#graph-show").html(queryText);
        $("#graph-show").addClass("empty-result-show");
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
      splitLine: { show: false },
    },
    yAxis: {
      type: "value",
      name: "Duration",
      scale: true,
      axisLine: {
        show: true,
      },
      splitLine: { show: false },
    },
    tooltip: {
      show: true,
      formatter: function (param) {
        var green = param.value[2];
        var red = param.value[3];
        return (
          "<div>" + green + " Spans</div>" + "<div>" + red + " Errors</div>"
        );
      },
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
  $(".warn-box").remove();
  for (let i = 0; i < returnResTotal.length; i++) {
    $("#warn-bottom").append(`<div class="warn-box"><div class="warn-head">
                            <span  class = "span-id">Frontend: /dispatch <span id = "span-id-${i}"></span></span>
                            <span class = "duration-time" id  = "duration-time-${i}"></span>
                        </div>
                        <div class="warn-content">
                            <div class="spans-box">
                            <div class = "total-span" id = "total-span-${i}"></div>
                            <div class = "error-span" id = "error-span-${i}"></div>
                            </div>
                            <div> </div>
                            <div class="warn-content-right">
                                <span class = "start-time" id = "start-time-${i}"></span>
                                <span class = "how-long-time" id = "how-long-time-${i}"></span>
                            </div>
                        </div></div>`);
    let json = returnResTotal[i];
    $(`#span-id-${i}`).text(json.trace_id.substring(0, 7));
    $(`#total-span-${i}`).text(
      json.span_count + " Spans"
    );
    $(`#error-span-${i}`).text(
      json.span_errors_count + " Errors"
    );
    let duration = Number((json.end_time - json.start_time) / 1000000);
    $(`#duration-time-${i}`).text(
      Math.round(duration * 100) / 100 + "ms"
    );
    let milliseconds = Number(json.start_time / 1000000);
    let dataStr = new Date(milliseconds).toLocaleString();
    let dateText = "";
    let date = dataStr.split(",");
    let dateTime = date[0].split("/");
    //current date
    const currentDate = new Date();
    const currentYear = currentDate.getFullYear() + "";
    const currentMonth = currentDate.getMonth() + 1 + "";
    const currentDay = currentDate.getDate() + "";
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
    $(`#start-time-${i}`).text(dateText);
    let timePass = calculateTimeToNow(json.start_time);
    let timePassText = "";
    if(timePass == 0) timePassText = "Current Time - Start Time";
    else if (timePass == 1) timePassText = timePass + " hour ago";
    else timePassText = timePass + " hours ago";
    $(`#how-long-time-${i}`).text(timePassText);
  }
}


$('#warn-bottom').unbind("scroll").on("scroll", function (e) {
    var sum = this.scrollHeight;
    var $obj = $(this);
    if (sum <= $obj.scrollTop() + $obj.height() && sum != 240) {
      //users did not set limitation
      if(limitation == -1){
        params.page = params.page + 1;
        searchTrace(params);
      }else if(limitation > 0){
        if(limitation >= 50){
          limitation = limitation - 50;
          params.page = params.page + 1;
          searchTrace(params);
        }else{
          params.page = params.page + 1;
          searchTrace(params);
        }
      }
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
