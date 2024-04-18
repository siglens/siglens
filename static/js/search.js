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

 'use strict';


 function wsURL(path) {
     var protocol = (location.protocol === 'https:') ? 'wss://' : 'ws://';
     var url = protocol + location.host;
     return url + path;
 }
 
 function doCancel(data) {
     socket.send(JSON.stringify(data));
     $('body').css('cursor', 'default');
     $('#run-filter-btn').html(' ');
     $("#run-filter-btn").removeClass("cancel-search");
     $('#run-filter-btn').removeClass('active');
     $("#query-builder-btn").html(" ");
     $("#query-builder-btn").removeClass("cancel-search");
     $("#query-builder-btn").removeClass("active");
     $('#progress-div').html(``);
 }
  function doLiveTailCancel(data) {
    $("body").css("cursor", "default");
    $("#live-tail-btn").html("Live Tail");
    $("#live-tail-btn").removeClass("active");
    $("#progress-div").html(``);
  }
 function resetDataTable(firstQUpdate) {
     if (firstQUpdate) {
         $('#empty-response').hide();
         $("#custom-chart-tab").show();
         let currentTab = $("#custom-chart-tab").tabs("option", "active");
         if (currentTab == 0) {
           $("#logs-view-controls").show();
         } else {
           $("#logs-view-controls").hide();
         }
         $("#agg-result-container").hide();
         $("#data-row-container").hide();
         hideError();
     }
 }
 
 function resetLogsGrid(firstQUpdate){
     if (firstQUpdate){
         resetAvailableFields();
     }
 }
 
 function doSearch(data) {
     startQueryTime = (new Date()).getTime();
     newUri = wsURL("/api/search/ws");
     socket = new WebSocket(newUri);
     let timeToFirstByte = 0;
     let firstQUpdate = true;
     let lastKnownHits = 0;
     socket.onopen = function (e) {
         console.time("socket timing");
         $('body').css('cursor', 'progress');
         $("#run-filter-btn").addClass("cancel-search");
         $('#run-filter-btn').addClass('active');
         $("#query-builder-btn").html("   ");
         $("#query-builder-btn").addClass("cancel-search");
         $("#query-builder-btn").addClass("active");
         socket.send(JSON.stringify(data));
     };
 
     socket.onmessage = function (event) {
         let jsonEvent = JSON.parse(event.data);
         let eventType = jsonEvent.state;
         let totalEventsSearched = jsonEvent.total_events_searched
         let totalTime = (new Date()).getTime() - startQueryTime;
         switch (eventType) {
             case "RUNNING":
                 console.time("RUNNING");
                 console.timeEnd("RUNNING");
                 break;
             case "QUERY_UPDATE":
                 console.time("QUERY_UPDATE");
                 if (timeToFirstByte === 0) {
                     timeToFirstByte = Number(totalTime).toLocaleString();
                 }
                 let totalHits;
 
                 if (jsonEvent && jsonEvent.hits && jsonEvent.hits.totalMatched) {
                     totalHits = jsonEvent.hits.totalMatched
                     totalMatchLogs = totalHits;
                     lastKnownHits = totalHits;
                 } else {
                     // we enter here only because backend sent null hits/totalmatched
                     totalHits = lastKnownHits
                 }
                 resetDataTable(firstQUpdate);
                 processQueryUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, totalHits);
                 console.timeEnd("QUERY_UPDATE");
                 firstQUpdate = false
                 break;
             case "COMPLETE":
                 let eqRel = "eq";
                 if (jsonEvent.totalMatched != null && jsonEvent.totalMatched.relation != null) {
                     eqRel = jsonEvent.totalMatched.relation;
                 }
                 console.time("COMPLETE");
                 canScrollMore = jsonEvent.can_scroll_more;
                 scrollFrom = jsonEvent.total_rrc_count;
                 processCompleteUpdate(jsonEvent, eventType, totalEventsSearched, timeToFirstByte, eqRel);
                 console.timeEnd("COMPLETE");
                 socket.close(1000);
                 break;
             case "TIMEOUT":
                 console.time("TIMEOUT");
                 console.log(`[message] Timeout state received from server: ${jsonEvent}`);
                 processTimeoutUpdate(jsonEvent);
                 console.timeEnd("TIMEOUT");
                 break;
             case "ERROR":
                 console.time("ERROR");
                 console.log(`[message] Error state received from server: ${jsonEvent}`);
                 processErrorUpdate(jsonEvent);
                 console.timeEnd("ERROR");
                 break;
             default:
                 console.log(`[message] Unknown state received from server: `+ JSON.stringify(jsonEvent));
                 if (jsonEvent.message.includes("expected")){
                    jsonEvent.message = "Your query contains syntax error"
                 } else if (jsonEvent.message.includes("not present")){
                    jsonEvent['no_data_err'] = "No data found for the query"
                 }
                 processSearchErrorLog(jsonEvent);
         }
     };
 
     socket.onclose = function (event) {
         if (event.wasClean) {
             console.log(`[close] Connection closed cleanly, code=${event.code} reason=${event.reason}`);
         } else {
             console.log(`Connection close not clean=${event} code=${event.code} reason=${event.reason} `);
         }
         console.timeEnd("socket timing");
     };
 
     socket.addEventListener('error', (event) => {
         console.log('WebSocket error: ', event);
     });
 }
 
  function reconnect() {
    if (lockReconnect) {
      return;
    }
    lockReconnect = true;
    //keep reconnect，set delay to avoid much request, set tt, cancel first, then reset
    clearInterval(tt);
    tt = setInterval(function () {
      if (!liveTailState) {
        lockReconnect = false;
        return;
      }
      data = getLiveTailFilter(false, false, 30);
      createLiveTailSocket(data);
      lockReconnect = false;
  }, refreshInterval);
  }

  function createLiveTailSocket(data) {
    try {
      if (!liveTailState) return;
      startQueryTime = new Date().getTime();
      newUri = wsURL("/api/search/live_tail");
      socket = new WebSocket(newUri);
      doLiveTailSearch(data);
    } catch (e) {
      console.log("live tail connect websocket catch: " + e);
      reconnect();
    }
  }
   function doLiveTailSearch(data) {
     let timeToFirstByte = 0;
     let firstQUpdate = true;
     let lastKnownHits = 0;
     socket.onopen = function (e) {
       //  console.time("socket timing");
       $("body").css("cursor", "progress");
       $("#live-tail-btn").html("Cancel Live Tail");
       $("#live-tail-btn").addClass("active");
       socket.send(JSON.stringify(data));
     };

     socket.onmessage = function (event) {
       let jsonEvent = JSON.parse(event.data);
       let eventType = jsonEvent.state;
       if (
         jsonEvent &&
         jsonEvent.total_events_searched &&
         jsonEvent.total_events_searched != 0
       ) {
         total_liveTail_searched = jsonEvent.total_events_searched;
       }
       let totalEventsSearched = total_liveTail_searched;
       let totalTime = new Date().getTime() - startQueryTime;
       switch (eventType) {
         case "RUNNING":
           console.time("RUNNING");
           console.timeEnd("RUNNING");
           break;
         case "QUERY_UPDATE":
           console.time("QUERY_UPDATE");
           if (timeToFirstByte === 0) {
             timeToFirstByte = Number(totalTime).toLocaleString();
           }
           let totalHits;

           if (jsonEvent && jsonEvent.hits && jsonEvent.hits.totalMatched) {
             totalHits = jsonEvent.hits.totalMatched;
             lastKnownHits = totalHits;
           } else {
             // we enter here only because backend sent null hits/totalmatched
             totalHits = lastKnownHits;
           }
           resetDataTable(firstQUpdate);
           processLiveTailQueryUpdate(
             jsonEvent,
             eventType,
             totalEventsSearched,
             timeToFirstByte,
             totalHits
           );
           //  console.timeEnd("QUERY_UPDATE");
           firstQUpdate = false;
           break;
         case "COMPLETE":
           let eqRel = "eq";
           if (
             jsonEvent.totalMatched != null &&
             jsonEvent.totalMatched.relation != null
           ) {
             eqRel = jsonEvent.totalMatched.relation;
           }
           console.time("COMPLETE");
           console.log(new Date().getTime());
           canScrollMore = jsonEvent.can_scroll_more;
           scrollFrom = jsonEvent.total_rrc_count;
           processLiveTailCompleteUpdate(
             jsonEvent,
             eventType,
             totalEventsSearched,
             timeToFirstByte,
             eqRel
           );
           console.timeEnd("COMPLETE");
           socket.close(1000);
           break;
         case "TIMEOUT":
           console.time("TIMEOUT");
           console.log(
             `[message] Timeout state received from server: ${jsonEvent}`
           );
           processTimeoutUpdate(jsonEvent);
           console.timeEnd("TIMEOUT");
           break;
         case "ERROR":
           console.time("ERROR");
           console.log(
             `[message] Error state received from server: ${jsonEvent}`
           );
           processErrorUpdate(jsonEvent);
           console.timeEnd("ERROR");
           break;
         default:
           console.log(
             `[message] Unknown state received from server: ` +
               JSON.stringify(jsonEvent)
           );
           if (jsonEvent.message.includes("expected")) {
             jsonEvent.message = "Your query contains syntax error";
           } else if (jsonEvent.message.includes("not present")) {
             jsonEvent["no_data_err"] = "No data found for the query";
           }
           processSearchErrorLog(jsonEvent);
       }
     };

     socket.onclose = function (event) {
       if (liveTailState) {
         reconnect();
         console.log("live tail reconnect websocket");
       } else {
         console.log("stop reconnect live tail");
         if (event.wasClean) {
           console.log(
             `[close] Connection closed cleanly, code=${event.code} reason=${event.reason}`
           );
         } else {
           console.log(
             `Connection close not clean=${event} code=${event.code} reason=${event.reason} `
           );
         }
         console.timeEnd("socket timing");
       }
     };

     socket.addEventListener("error", (event) => {
       console.log("WebSocket error: ", event);
     });
   }
 function getInitialSearchFilter(skipPushState, scrollingTrigger) {
     let queryParams = new URLSearchParams(window.location.search);
     let stDate = queryParams.get("startEpoch") || Cookies.get('startEpoch') || "now-15m";
     let endDate = queryParams.get("endEpoch") || Cookies.get('endEpoch') || "now";
     let selIndexName = queryParams.get('indexName');
     let queryLanguage = queryParams.get("queryLanguage") ||$('#query-language-btn span').html();
     queryLanguage = queryLanguage.replace('"', '');
     $("#query-language-btn span").html(queryLanguage);
    $(".query-language-option").removeClass("active");
     if (queryLanguage == "SQL") {
       $("#option-1").addClass("active");
     } else if (queryLanguage == "Log QL") {
       $("#option-2").addClass("active");
     } else if (queryLanguage == "Splunk QL") {
       $("#option-3").addClass("active");
     }
     let filterTab = queryParams.get("filterTab");
     let filterValue = queryParams.get('searchText');
     if(filterTab == "0" || filterTab == null){
      if(filterValue != "*"){
        if(filterValue.indexOf("|") != -1){
          firstBoxSet = new Set(filterValue.split(" | ")[0].split(" "));
          secondBoxSet = new Set(
            filterValue
              .split("stats ")[1]
              .split(" BY")[0]
              .split(/(?=[A-Z])/)
          );
          if (filterValue.includes(" BY ")) {
            thirdBoxSet = new Set(filterValue.split(" BY ")[1].split(","));
          }
        }else{
          firstBoxSet = new Set(filterValue.split(" "));
        }
        if (firstBoxSet && firstBoxSet.size > 0) {
          let tags = document.getElementById("tags");
          while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
          }
          firstBoxSet.forEach((value, i) => {
            let tag = document.createElement("li");
            tag.innerText = value;
            // Add a delete button to the tag
            tag.innerHTML += '<button class="delete-button">x</button>';
            // Append the tag to the tags list
            tags.appendChild(tag);
          });
        }
        if (secondBoxSet && secondBoxSet.size > 0) {
          let tags = document.getElementById("tags-second");
          while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
          }
          secondBoxSet.forEach((value, i) => {
            let tag = document.createElement("li");
            tag.innerText = value;
            // Add a delete button to the tag
            tag.innerHTML += '<button class="delete-button">x</button>';
            // Append the tag to the tags list
            tags.appendChild(tag);
          });
        }
        if (thirdBoxSet && thirdBoxSet.size > 0) {
          let tags = document.getElementById("tags-third");
          while (tags.firstChild) {
            tags.removeChild(tags.firstChild);
          }
          thirdBoxSet.forEach((value, i) => {
            let tag = document.createElement("li");
            tag.innerText = value;
            // Add a delete button to the tag
            tag.innerHTML += '<button class="delete-button">x</button>';
            // Append the tag to the tags list
            
            tags.appendChild(tag);
          });
        }
      }
        $("#query-input").val(filterValue);
     }else{
        $("#custom-code-tab").tabs("option", "active", 1);
        if (filterValue === "*") {
          $("#filter-input").val("").change();
        } else {
          $("#filter-input").val(filterValue).change();
        }
     }
     let sFrom = 0;

     selIndexName.split(',').forEach(function(searchVal){
         $(`.index-dropdown-item[data-index="${searchVal}"]`).toggleClass('active');
     });
 
     selectedSearchIndex = selIndexName.split(",").join(",");
     Cookies.set('IndexList', selIndexName.split(",").join(","));
 
     if (!isNaN(stDate)) {
         stDate = Number(stDate);
         endDate = Number(endDate);
         datePickerHandler(stDate, endDate, "custom");
         loadCustomDateTimeFromEpoch(stDate,endDate);
     } else if (stDate !== "now-15m") {
         datePickerHandler(stDate, endDate, stDate);
     } else {
         datePickerHandler(stDate, endDate, "");
     }

     selectedSearchIndex = selIndexName;
     if (!skipPushState) {
         addQSParm("searchText", filterValue);
         addQSParm("startEpoch", stDate);
         addQSParm("endEpoch", endDate);
         addQSParm("indexName", selIndexName);
         addQSParm("queryLanguage", queryLanguage);
         window.history.pushState({ path: myUrl }, '', myUrl);
     }
 
     if (scrollingTrigger){
         sFrom = scrollFrom;
     }


     return {
         'state': 'query',
         'searchText': filterValue,
         'startEpoch': stDate,
         'endEpoch': endDate,
         'indexName': selIndexName,
         'from' : sFrom,
         'queryLanguage' : queryLanguage,
     };
 }
  function getLiveTailFilter(skipPushState, scrollingTrigger, startTime) {
    let filterValue = $("#filter-input").val().trim() || "*";
    let endDate = "now";
    let date = new Date();
    let stDate = new Date(date.getTime() - startTime * 1000).getTime();
    if (startTime == 1800) stDate = "now-1h";
    let selIndexName = selectedSearchIndex;
    let sFrom = 0;
    let queryLanguage = $("#query-language-btn span").html();

    selIndexName.split(",").forEach(function (searchVal) {
      $(`.index-dropdown-item[data-index="${searchVal}"]`).toggleClass(
        "active"
      );
    });

    selectedSearchIndex = selIndexName.split(",").join(",");
    Cookies.set("IndexList", selIndexName.split(",").join(","));

    addQSParm("searchText", filterValue);
    addQSParm("startEpoch", stDate);
    addQSParm("endEpoch", endDate);
    addQSParm("indexName", selIndexName);
    addQSParm("queryLanguage", queryLanguage);

    window.history.pushState({ path: myUrl }, "", myUrl);

    if (scrollingTrigger) {
      sFrom = scrollFrom;
    }

    return {
      state: wsState,
      searchText: filterValue,
      startEpoch: stDate,
      endEpoch: endDate,
      indexName: selIndexName,
      from: sFrom,
      queryLanguage: queryLanguage,
    };
  }
  let filterTextQB = "";
  /**
   * get real time search text
   * @returns real time search text
   */
  function getQueryBuilderCode() {
   let filterValue = "";
     //concat the first input box
     let index = 0;
     if (firstBoxSet && firstBoxSet.size > 0) {
       firstBoxSet.forEach((value, i) => {
         if (index != firstBoxSet.size - 1) filterValue += value + " ";
         else filterValue += value;
         index++;
       });
     }else{
      filterValue = '*';
     }
     index = 0;
     let bothRight = 0;
     let showError = false;
     //concat the second input box
     if (secondBoxSet && secondBoxSet.size > 0) {
       bothRight++;
       filterValue += " | stats";
       secondBoxSet.forEach((value, i) => {
         if (index != secondBoxSet.size - 1) filterValue += " " + value + ",";
         else filterValue += " " + value;
         index++;
       });
     }
     index = 0;
     if (thirdBoxSet && thirdBoxSet.size > 0) {
      if(bothRight == 0) showError = true;
       //concat the third input box
       filterValue += " BY";
       thirdBoxSet.forEach((value, i) => {
         if (index != thirdBoxSet.size - 1) filterValue += " " + value + ",";
         else filterValue += " " + value;
         index++;
       });
     }
     if (filterValue == "") filterValue = "*";
     $("#query-input").val(filterValue);
     if(thirdBoxSet && thirdBoxSet.size > 0 && (secondBoxSet == null || secondBoxSet.size == 0)) $("#query-builder-btn").addClass("stop-search").prop('disabled', true);
     else $("#query-builder-btn").removeClass("stop-search").prop('disabled', false);
   return showError ? "Searches with a Search Criteria must have an Aggregate Attribute" : filterValue;
  }
 function getSearchFilter(skipPushState, scrollingTrigger) {
   let currentTab = $("#custom-code-tab").tabs("option", "active");
   let endDate = filterEndDate || "now";
   let stDate = filterStartDate || "now-15m";
   let selIndexName = selectedSearchIndex;
   let sFrom = 0;
   let queryLanguage = $("#query-language-btn span").html();
  
   selIndexName.split(",").forEach(function (searchVal) {
     $(`.index-dropdown-item[data-index="${searchVal}"]`).toggleClass("active");
   });

   selectedSearchIndex = selIndexName.split(",").join(",");
   Cookies.set("IndexList", selIndexName.split(",").join(","));

   if (!isNaN(stDate)) {
     datePickerHandler(Number(stDate), Number(endDate), "custom");
   } else if (stDate !== "now-15m") {
     datePickerHandler(stDate, endDate, stDate);
   } else {
     datePickerHandler(stDate, endDate, "");
   }
   let filterValue = "";
   if(currentTab == 0){
    queryLanguage = "Splunk QL";
     //concat the 3 input boxes
     filterValue = getQueryBuilderCode();
     isQueryBuilderSearch = true;
   }else{
    filterValue = $("#filter-input").val().trim() || "*";
    isQueryBuilderSearch = false;
   }
   addQSParm("searchText", filterValue);
   addQSParm("startEpoch", stDate);
   addQSParm("endEpoch", endDate);
   addQSParm("indexName", selIndexName);
   addQSParm("queryLanguage", queryLanguage);

   window.history.pushState({ path: myUrl }, "", myUrl);

   if (scrollingTrigger) {
     sFrom = scrollFrom;
   }

   filterTextQB = filterValue;
   return {
     state: wsState,
     searchText: filterValue,
     startEpoch: stDate,
     endEpoch: endDate,
     indexName: selIndexName,
     from: sFrom,
     queryLanguage: queryLanguage,
   };
 }
 
 function getSearchFilterForSave(qname, qdesc) {
     let filterValue = filterTextQB.trim() || "*";
      let currentTab = $("#custom-code-tab").tabs("option", "active");
     return {
         'queryName': qname,
         'queryDescription': qdesc || "",
         'searchText': filterValue,
         'indexName': selectedSearchIndex,
         'filterTab': currentTab.toString(),
         'queryLanguage': $("#query-language-btn span").html()
     };
 }
  function processLiveTailQueryUpdate(
    res,
    eventType,
    totalEventsSearched,
    timeToFirstByte,
    totalHits
  ) {
    if (
      res.hits &&
      res.hits.records !== null &&
      res.hits.records.length >= 1 &&
      res.qtype === "logs-query"
    ) {
      let columnOrder = _.uniq(
        _.concat(
          // make timestamp the first column
          "timestamp",
          // make logs the second column
          "logs",
          res.allColumns
        )
      );
      allLiveTailColumns = res.allColumns;
      renderAvailableFields(columnOrder);
      renderLogsGrid(columnOrder, res.hits.records);

      if (res && res.hits && res.hits.totalMatched) {
        totalHits = res.hits.totalMatched;
      }
    } else if (logsRowData.length > 0) {
      let columnOrder = _.uniq(
        _.concat(
          // make timestamp the first column
          "timestamp",
          // make logs the second column
          "logs",
          allLiveTailColumns
        )
      );
      renderAvailableFields(columnOrder);
      renderLogsGrid(columnOrder, logsRowData);
      totalHits = logsRowData.length;
    } else if (
      res.measure &&
      (res.qtype === "aggs-query" || res.qtype === "segstats-query")
    ) {
      if (res.groupByCols) {
        columnOrder = _.uniq(_.concat(res.groupByCols));
      }
      let columnOrder = [];
      if (res.measureFunctions) {
        columnOrder = _.uniq(_.concat(columnOrder, res.measureFunctions));
      }

      aggsColumnDefs = [];
      segStatsRowData = [];
      renderMeasuresGrid(columnOrder, res.measure);
    }
    let totalTime = new Date().getTime() - startQueryTime;
    let percentComplete = res.percent_complete;
    renderTotalHits(
      totalHits,
      totalTime,
      percentComplete,
      eventType,
      totalEventsSearched,
      timeToFirstByte,
      "",
      res.qtype
    );
    $("body").css("cursor", "default");
  }
 function processQueryUpdate(res, eventType, totalEventsSearched, timeToFirstByte, totalHits) {
     if (res.hits && res.hits.records!== null && res.hits.records.length >= 1 && res.qtype === "logs-query") {
         let columnOrder = _.uniq(_.concat(
             // make timestamp the first column
             'timestamp',
             // make logs the second column
             'logs',
             res.allColumns));
             
         // for sort function display
         sortByTimestampAtDefault = res.sortByTimestampAtDefault; 

         renderAvailableFields(columnOrder);
         renderLogsGrid(columnOrder, res.hits.records);

        $("#logs-result-container").show();
        $("#agg-result-container").hide();
         
         if (res && res.hits && res.hits.totalMatched) {
             totalHits = res.hits.totalMatched
         }
     } else if (res.measure && (res.qtype === "aggs-query" || res.qtype === "segstats-query")) {
         if (res.groupByCols ) {
             columnOrder = _.uniq(_.concat(
                 res.groupByCols));
         }
         let columnOrder =[]
         if (res.measureFunctions ) {
             columnOrder = _.uniq(_.concat(
                 columnOrder,res.measureFunctions));
         }
 
         aggsColumnDefs=[];
         segStatsRowData=[]; 
         renderMeasuresGrid(columnOrder, res.measure);
 
     }
     let totalTime = (new Date()).getTime() - startQueryTime;
     let percentComplete = res.percent_complete;
     renderTotalHits(totalHits, totalTime, percentComplete, eventType, totalEventsSearched, timeToFirstByte, "", res.qtype);
     $('body').css('cursor', 'default');
 }
 
 function processEmptyQueryResults() {
     $("#logs-result-container").hide();
    $("#custom-chart-tab").hide();
     $("#agg-result-container").hide();
     $("#data-row-container").hide();
     $('#corner-popup').hide();
     $('#empty-response').show();
     $('#logs-view-controls').hide();
     let el = $('#empty-response');
     $('#empty-response').empty();
     el.append('<span>Your query returned no data, adjust your query.</span>')
 }
  function processLiveTailCompleteUpdate(
    res,
    eventType,
    totalEventsSearched,
    timeToFirstByte,
    eqRel
  ) {
    let columnOrder = [];
    let totalHits = res.totalMatched.value + logsRowData.length;
    if (res.totalMatched.value + logsRowData.length > 500) totalHits = 500;
    if (
      logsRowData.length == 0 &&
      res.totalMatched.value === 0 &&
      res.measure === undefined
    ) {
      processEmptyQueryResults();
    }
    if (res.measure) {
      if (res.groupByCols) {
        columnOrder = _.uniq(_.concat(res.groupByCols));
      }
      if (res.measureFunctions) {
        columnOrder = _.uniq(_.concat(columnOrder, res.measureFunctions));
      }
      resetDashboard();
      $("#logs-result-container").hide();
      $("#custom-chart-tab").show();
      $("#agg-result-container").show();
      aggsColumnDefs = [];
      segStatsRowData = [];
      renderMeasuresGrid(columnOrder, res.measure);
      if (
        (res.qtype === "aggs-query" || res.qtype === "segstats-query") &&
        res.bucketCount
      ) {
        totalHits = res.bucketCount;
      }
    }

    let totalTime = new Date().getTime() - startQueryTime;
    let percentComplete = res.percent_complete;
    if (res.total_rrc_count > 0) {
      totalRrcCount += res.total_rrc_count;
    }
    renderTotalHits(
      totalHits,
      totalTime,
      percentComplete,
      eventType,
      totalEventsSearched,
      timeToFirstByte,
      eqRel,
      res.qtype
    );
    $("#run-filter-btn").html(" ");
    $("#run-filter-btn").removeClass("cancel-search");
    $("#run-filter-btn").removeClass("active");
    $("#query-builder-btn").html(" ");
    $("#query-builder-btn").removeClass("cancel-search");
    $("#query-builder-btn").removeClass("active");
    wsState = "query";
    if (canScrollMore === false) {
      scrollFrom = 0;
    }
  }
 function processCompleteUpdate(res, eventType, totalEventsSearched, timeToFirstByte, eqRel) {
     let columnOrder =[]
     let totalHits = res.totalMatched.value;
     if ((res.totalMatched == 0 || res.totalMatched.value === 0) && res.measure ===undefined) {
         processEmptyQueryResults();
     }
     if (res.measureFunctions && res.measureFunctions.length > 0) {
       measureFunctions = res.measureFunctions;
     }
     if (res.measure) {
         measureInfo = res.measure;
         if (res.groupByCols) {
             columnOrder = _.uniq(_.concat(
                 res.groupByCols));
         }
         if (res.measureFunctions) {
             columnOrder = _.uniq(_.concat(
                 columnOrder,res.measureFunctions));
         }
         resetDashboard();
         $("#logs-result-container").hide();
         $("#custom-chart-tab").show();
         $("#agg-result-container").show();
         aggsColumnDefs=[];
         segStatsRowData=[];
         renderMeasuresGrid(columnOrder, res.measure);
         if ((res.qtype ==="aggs-query" || res.qtype === "segstats-query") && res.bucketCount){
             totalHits = res.bucketCount;
         }
     }else{
      measureInfo = [];
     }
    isTimechart = res.isTimechart;
    const currentUrl = window.location.href;
    if (currentUrl.includes("index.html"))
      timeChart();
     let totalTime = (new Date()).getTime() - startQueryTime;
     let percentComplete = res.percent_complete;
     if (res.total_rrc_count > 0){
         totalRrcCount += res.total_rrc_count;
     }
     renderTotalHits(totalHits, totalTime, percentComplete, eventType, totalEventsSearched,
         timeToFirstByte, eqRel, res.qtype);
     $('#run-filter-btn').html(' ');
     $("#run-filter-btn").removeClass("cancel-search");
     $('#run-filter-btn').removeClass('active');
     $("#query-builder-btn").html(" ");
     $("#query-builder-btn").removeClass("cancel-search");
     $("#query-builder-btn").removeClass("active");
     wsState = 'query'
     if (canScrollMore === false){
         scrollFrom = 0;
     }
 }
 
 function processTimeoutUpdate(res) {
     showError(`Query ${res.qid} reached the timeout limit of ${res.timeoutSeconds} seconds`);
 }

 function processErrorUpdate(res) {
     showError(`Message: ${res.message}`);
 }
 
 function processSearchError(res) {
     if (res.can_scroll_more === false){
         showInfo(`You've reached maximum scroll limit (10,000).`);
     } else if (res.message != "") {
         showError(`Message: ${res.message}`);
         resetDashboard();
     }
 }

 function processSearchErrorLog(res){
    if (res.can_scroll_more === false){
        showInfo(`You've reached maximum scroll limit (10,000).`);
    } else if (res.message != "") {
        showErrorResponse(`Message: ${res.message}`,res);
        resetDashboard();
    }
 }

 function showErrorResponse(errorMsg,res){
    $("#logs-result-container").hide();
     $("#agg-result-container").hide();
     $("#data-row-container").hide();
     $('#corner-popup').hide();
     $('#empty-response').show();
     $('#logs-view-controls').hide();
    $("#custom-chart-tab").hide();
     let el = $('#empty-response');
     $('#empty-response').empty();
     if (res && res.no_data_err && res.no_data_err.includes("No data found")){
        el.html(`${res.no_data_err} <br> `+ errorMsg);
    }else{
        el.html(errorMsg);
    }
    $('body').css('cursor', 'default');
    $('#run-filter-btn').html(' ');
    $("#run-filter-btn").removeClass("cancel-search");
    $('#run-filter-btn').removeClass('active');
    $("#query-builder-btn").html(" ");
    $("#query-builder-btn").removeClass("cancel-search");
    $("#query-builder-btn").removeClass("active");
    $('#run-metrics-query-btn').removeClass('active');

    wsState = 'query';
 }


 
 function renderTotalHits(totalHits, elapedTimeMS, percentComplete, eventType, totalEventsSearched, timeToFirstByte, eqRel, qtype) {
     //update chart title
     console.log(`rendering total hits: ${totalHits}. elapedTimeMS: ${elapedTimeMS}`);
     let startDate = displayStart ;
     let endDate = displayEnd;
     // Check if totalHits is undefined and set it to 0
     let totalHitsFormatted = Number(totalHits || 0).toLocaleString();
 
     if (eventType === "QUERY_UPDATE") {
      if (totalHits > 0){
             $('#hits-summary').html(`
             <div><span class="total-hits">${totalHitsFormatted} </span><span>of ${totalEventsSearched} Records Matched</span> </div>
 
             <div class="text-center">${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
             <div class="text-end">Response: ${timeToFirstByte} ms</div>
         `);
         } else{
             $('#hits-summary').html(`<div><span> ${totalEventsSearched} Records Searched</span> </div>
 
             <div class="text-center">${dateFns.format(startDate, timestampDateFmt)} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
             <div class="text-end">Response: ${timeToFirstByte} ms</div>
         `);
         }
         $('#progress-div').html(`
             <progress id="percent-complete" value=${percentComplete} max="100">${percentComplete}</progress>
             <div id="percent-value">${parseInt(percentComplete)}%</div>
         `);
     }
     else if (eventType === "COMPLETE") {
         let operatorSign = '';
         if (eqRel === "gte") {
             operatorSign = '>=';
         }
         if (qtype == "aggs-query" || qtype === "segstats-query") {
           let bucketGrammer = totalHits == 1 ? "bucket was" : "buckets were";
           $("#hits-summary").html(`
             <div><b>Response: ${timeToFirstByte} ms</b></div>
             <div><span class="total-hits"><b>${operatorSign} ${totalHitsFormatted}</b></span><span> ${bucketGrammer} created from <b>${totalEventsSearched}</b> records.</span></div>
             <div>${dateFns.format(
               startDate,
               timestampDateFmt
             )} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
         `);
         } else if (totalHits > 0) {
           $("#hits-summary").html(`
             <div><b>Response: ${timeToFirstByte} ms</b></div>
             <div><span class="total-hits"><b>${operatorSign} ${totalHitsFormatted}</b></span><span> of <b>${totalEventsSearched}</b> Records Matched</span></div>
             <div>${dateFns.format(
               startDate,
               timestampDateFmt
             )} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
         `);
         } else {
           $("#hits-summary").html(`
             <div><b>Response: ${timeToFirstByte} ms</b></div>
             <div><span><b> ${totalEventsSearched} </b>Records Searched</span></div>
             <div>${dateFns.format(
               startDate,
               timestampDateFmt
             )} &mdash; ${dateFns.format(endDate, timestampDateFmt)}</div>
         `);
         }
         $('#progress-div').html(``)
     }
 }

// LiveTail Refresh Duration
let refreshInterval = 10000;

$('.refresh-range-item').on('click', refreshRangeItemHandler);

function refreshRangeItemHandler(evt){
  $.each($(".refresh-range-item.active"), function () {
      $(this).removeClass('active');
  });
  $(evt.currentTarget).addClass('active');
  let interval = $(evt.currentTarget).attr('id');
  $('#refresh-picker-btn span').html(interval);

  refreshInterval = parseInterval(interval); // Parsing interval
  if(liveTailState) 
    reconnect();
}

function parseInterval(interval) {
  const regex = /(\d+)([smhd])/;
  const match = interval.match(regex);
  const value = parseInt(match[1]);
  const unit = match[2];
  
  switch (unit) {
      case 's':
          return value * 1000;
      case 'm':
          return value * 60 * 1000;
      case 'h':
          return value * 60 * 60 * 1000;
      case 'd':
          return value * 24 * 60 * 60 * 1000;
      default:
          throw new Error("Invalid interval unit");
  }
}