"use strict";
$(function () {
  $("#custom-code-tab").tabs();
});
$("#custom-code-tab").tabs({
  activate: function (event, ui) {
    let currentTab = $("#custom-code-tab").tabs("option", "active");
    if (currentTab == 0) {
      $(".query-language-option").removeClass("active");
      $("#query-language-options #option-3").addClass("active");
      $("#query-language-btn span").html("Splunk QL");
      displayQueryLangToolTip("3");
    }else{
      let filterValue = $("#query-input").val();
     if (filterValue != "") $("#filter-input").val(filterValue);
    }
  },
});
//querybuilder and run-filter-btn text: 
//search -> " "
//run -> "  "
//cancel -> "   "
//running -> "    "
$(document).ready(function () {
  $("#add-con").on("click", filterStart);
  $("#filter-box-1").on("click", filterStart);
  $("#add-con-second").on("click", secondFilterStart);
  $("#filter-box-2").on("click", secondFilterStart);
  $("#add-con-third").on("click", ThirdFilterStart);
  $("#filter-box-3").on("click", ThirdFilterStart);
  $("#completed").on("click", filterComplete);
  $("#completed-second").on("click", secondFilterComplete);
  $("#cancel-enter").on("click", cancelInfo);
  $("#cancel-enter-second").on("click", secondCancelInfo);
  $("#cancel-enter-third").on("click", ThirdCancelInfo);

  $("#add-con").show();
  $("#add-con-second").show();
  $("#completed").hide();
  $("#completed-second").hide();
  $("#cancel-enter").hide();
  $("#cancel-enter-second").hide();
  $("#add-filter").hide();
  $("#add-filter-second").hide();
  if (thirdBoxSet.size > 0) $("#aggregations").hide();
  else $("#aggregations").show();
  if (secondBoxSet.size > 0) $("#aggregate-attribute-text").hide();
  else $("#aggregate-attribute-text").show();
  if (firstBoxSet.size > 0) $("#search-filter-text").hide();
  else $("#search-filter-text").show();
  setShowColumnInfoDialog();
});

const tags = document.getElementById("tags");
const tagSecond = document.getElementById("tags-second");
const tagThird = document.getElementById("tags-third");
tags.addEventListener("click", function (event) {
  // If the clicked element has the class 'delete-button'
  if (event.target.classList.contains("delete-button")) {
    // Remove the parent element (the tag)
    let str = event.target.parentNode.textContent;
    firstBoxSet.delete(str.substring(0, str.length - 1));
    event.target.parentNode.remove();
    getSearchText();
    if (firstBoxSet.size > 0) $("#search-filter-text").hide();
    else $("#search-filter-text").show();
    cancelInfo(event);
  }
});
tagSecond.addEventListener("click", function (event) {
  // If the clicked element has the class 'delete-button'
  if (event.target.classList.contains("delete-button")) {
    // Remove the parent element (the tag)
    let str = event.target.parentNode.textContent;
    secondBoxSet.delete(str.substring(0, str.length - 1));
    event.target.parentNode.remove();
    getSearchText();
    if (secondBoxSet.size > 0) $("#aggregate-attribute-text").hide();
    else $("#aggregate-attribute-text").show();
    secondCancelInfo(event);
  }
});
tagThird.addEventListener("click", function (event) {
  // If the clicked element has the class 'delete-button'
  if (event.target.classList.contains("delete-button")) {
    // Remove the parent element (the tag)
    let str = event.target.parentNode.textContent;
    thirdBoxSet.delete(str.substring(0, str.length - 1));
    event.target.parentNode.remove();
    getSearchText();
    if (thirdBoxSet.size > 0) $("#aggregations").hide();
    else $("#aggregations").show();
    ThirdCancelInfo(event);
  }
});
$(document).mouseup(function (e) {
  var firstCon = $("#add-filter");
  var secondCon = $("#add-filter-second");
  var thirdCon = $("#add-filter-third");
  var dropInfo = $(".ui-autocomplete");
  if (
    !firstCon.is(e.target) &&
    firstCon.has(e.target).length === 0 &&
    !dropInfo.is(e.target) &&
    dropInfo.has(e.target).length === 0 &&
    !secondCon.is(e.target) &&
    secondCon.has(e.target).length === 0 &&
    !thirdCon.is(e.target) &&
    thirdCon.has(e.target).length === 0
  ) {
    cancelInfo(e);
    secondCancelInfo(e);
    ThirdCancelInfo(e);
  }
});
var calculations = ["min", "max", "count", "avg", "sum"];
var ColumnsIsNum = ["http_status", "longitude", "latitude", "latency"];
var ifCurIsNum = false;
var availSymbol = [];
let valuesOfColumn = new Set();
let paramFirst;
function filterStart(evt) {
  evt.preventDefault();
  $("#column-first").attr("type", "text");
  $("#add-con").hide();
  $("#cancel-enter").show();
  $("#add-filter").show();
  $("#add-filter").css({ visibility: "visible" });
  $("#filter-box-1").addClass("select-box");
  if (availColNames.length == 0) getColumns();
  $("#column-first")
    .autocomplete({
      source: availColNames,
      minLength: 0,
      maxheight: 100,
      select: function (event, ui) {
        $("#symbol").attr("type", "text");
        //when the column name is not a number, the symbols can only be = && !=
        availSymbol = ["=", "!="];
        valuesOfColumn.clear();
        let curIsNum = false;
        for (let i = 0; i < availColNames.length; i++) {
          if (ui.item.value == ColumnsIsNum[i]) {
            curIsNum = true;
            availSymbol = ["=", "!=", "<=", ">=", ">", "<"];
            break;
          }
        }
        ifCurIsNum = curIsNum;
        $("#symbol").val("");
        $("#value-first").val("");
        //check if complete btn can click
        checkFirstBox(0);
        let chooseColumn = ui.item.value.trim();
        getValuesofColumn(chooseColumn);

        $("#symbol")
          .autocomplete({
            source: availSymbol,
            minLength: 0,
            select: function (event, ui) {
              //check if complete btn can click
              checkFirstBox(1);
              $("#value-first").attr("type", "text");
              $("#completed").show();
              valuesOfColumn.clear();
            },
          })
          .on("focus", function () {
            if (!$(this).val().trim()) $(this).keydown();
          });
      },
    })
    .on("focus", function () {
      if (!$(this).val().trim()) $(this).keydown();
    });
    $("#column-first").focus();
}
function secondFilterStart(evt) {
  evt.preventDefault();
  $("#filter-box-2").addClass("select-box");
  $("#column-second").attr("type", "text");
  $("#add-con-second").hide();
  $("#cancel-enter-second").show();
  $("#add-filter-second").show();
  $("#add-filter-second").css({ visibility: "visible" });
  $("#column-second").focus();
}
function ThirdFilterStart(evt) {
  evt.preventDefault();
  $("#filter-box-3").addClass("select-box");
  $("#column-third").attr("type", "text");
  $("#add-con-third").hide();
  $("#add-filter-third").show();
  $("#add-filter-third").css({ visibility: "visible" });
  $("#column-third")
    .autocomplete({
      source: availColNames,
      minLength: 0,
      maxheight: 100,
      select: function (event, ui) {
        event.preventDefault();
        let tag = document.createElement("li");
        if (ui.item.value !== "") {
          if (thirdBoxSet.has(ui.item.value)) {
            alert("Duplicate filter!");
            return;
          } else thirdBoxSet.add(ui.item.value);
          // Set the text content of the tag to
          // the trimmed value
          tag.innerText = ui.item.value;
          // Add a delete button to the tag
          tag.innerHTML += '<button class="delete-button">x</button>';
          // Append the tag to the tags list
          tagThird.appendChild(tag);
          var dom = $("#tags-third");
          var x = dom[0].scrollWidth;
          dom[0].scrollLeft = x;
          $("#column-third").val("");
          $(this).blur();
          getSearchText();
        }
        if(thirdBoxSet.size > 0) $("#aggregations").hide();
        else $("#aggregations").show();
        $("#column-third").focus();
        return false;
      },
    })
    .on("focus", function () {
      if (!$(this).val().trim()) $(this).keydown();
    });
    $("#column-third").focus();
}
/**
 * check first box
 * @param {*} obj 
 */
function checkContent(obj){
  if($(obj).val() === '' || $(obj).val() === null){
    $("#completed").attr('disabled', true);
  }else{
    $("#completed").attr("disabled", false);
  }
}
function checkFirstBox(curSelect){
  let num = 0;
  if (
    ($("#column-first").val() == null ||
    $("#column-first").val().trim() == "") && curSelect != 0
  ) num++;
  if (($("#symbol").val() == null || $("#symbol").val().trim() == "") && curSelect != 1) num++;
  if (($("#value-first").val() == null || $("#value-first").val().trim() == "") && curSelect != 2) num++;
  if(num != 0){
      $("#completed").attr("disabled", true);
  } else {
      $("#completed").attr("disabled", false);
  }
}
function checkSecondContent(obj) {
  if ($(obj).val() === "" || $(obj).val() === null) {
    $("#completed-second").attr("disabled", true);
  } else {
    $("#completed-second").attr("disabled", false);
  }
}
/**
 * first box complete one filter info
 * @param {*} evt
 */
function filterComplete(evt) {
  evt.preventDefault();
  let val = $("#value-first").val().trim();
  if (
    $("#column-first").val() == null ||
    $("#column-first").val().trim() == "" ||
    $("#symbol").val() == null ||
    $("#symbol").val().trim() == "" ||
    $("#value-first").val() == null ||
    $("#value-first").val().trim() == ""
  ) {
    alert("Please select one of the values below");
    return;
  }
  $("#filter-box-1").removeClass("select-box");
  let tagContent = $("#column-first").val().trim() + $("#symbol").val().trim();
  if (ifCurIsNum) tagContent += val;
  else tagContent += '"' + val + '"';
  $("#column-first").val("");
  $("#symbol").val("");
  $("#value-first").val("");
  $("#column-first").attr("type", "hidden");
  $("#symbol").attr("type", "hidden");
  $("#value-first").attr("type", "hidden");
  $("#completed").hide();
  $("#cancel-enter").hide();
  $("#add-filter").hide();
  $("#add-con").show();
  let tag = document.createElement("li");
  if (tagContent !== "") {
    if (firstBoxSet.has(tagContent)) {
      alert("Duplicate filter!");
      return;
    } else firstBoxSet.add(tagContent);
    // Set the text content of the tag to
    // the trimmed value
    tag.innerText = tagContent;
    // Add a delete button to the tag
    tag.innerHTML += '<button class="delete-button">x</button>';
    // Append the tag to the tags list
    tags.appendChild(tag);
    var dom = $("#tags");
    var x = dom[0].scrollWidth;
    dom[0].scrollLeft = x;
    getSearchText();
    if (firstBoxSet.size > 0) $("#search-filter-text").hide();
    else $("#search-filter-text").show();
  }
}
function secondFilterComplete(evt) {
  evt.preventDefault();
  if (
    $("#column-second").val() == null ||
    $("#column-second").val().trim() == "" ||
    $("#value-second").val() == null ||
    $("#value-second").val().trim() == ""
  ) {
    alert("Please select one of the values below");
    return;
  }
  $("#filter-box-2").removeClass("select-box");
  let tagContent =
    $("#column-second").val().trim() +
    "(" +
    $("#value-second").val().trim() +
    ")";
  $("#column-second").val("");
  $("#value-second").val("");
  $("#column-second").attr("type", "hidden");
  $("#value-second").attr("type", "hidden");
  $("#completed-second").hide();
  $("#cancel-enter-second").hide();
  $("#add-filter-second").hide();
  $("#add-con-second").show();
  let tag = document.createElement("li");
  if (tagContent !== "") {
    if (secondBoxSet.has(tagContent)) {
      alert("Duplicate filter!");
      return;
    } else secondBoxSet.add(tagContent);
    // Set the text content of the tag to
    // the trimmed value
    tag.innerText = tagContent;
    // Add a delete button to the tag
    tag.innerHTML += '<button class="delete-button">x</button>';
    // Append the tag to the tags list
    tagSecond.appendChild(tag);
    var dom = $("#tags-second");
    var x = dom[0].scrollWidth;
    dom[0].scrollLeft = x;
    getSearchText();
    if (secondBoxSet.size > 0) $("#aggregate-attribute-text").hide();
    else $("#aggregate-attribute-text").show();
  }
}
function getSearchText() {
  let filterValue = getQueryBuilderCode();
  if (filterValue != "") $("#query-input").val(filterValue);
  if (filterValue == "Syntax Error") $("#query-builder-btn").addClass("stop-search");
  else $("#query-builder-btn").removeClass("stop-search");
}
function cancelInfo(evt) {
  evt.preventDefault();
  evt.stopPropagation();
  $("#filter-box-1").removeClass("select-box");
  $("#column-first").val("");
  $("#symbol").val("");
  $("#value-first").val("");
  $("#column-first").attr("type", "hidden");
  $("#symbol").attr("type", "hidden");
  $("#value-first").attr("type", "hidden");
  $("#completed").hide();
  $("#add-filter").hide();
  $("#cancel-enter").hide();
  $("#add-con").show();
}
function secondCancelInfo(evt) {
  evt.preventDefault();
  evt.stopPropagation();
  $("#filter-box-2").removeClass("select-box");
  $("#column-second").val("");
  $("#value-second").val("");
  $("#column-second").attr("type", "hidden");
  $("#value-second").attr("type", "hidden");
  $("#completed-second").hide();
  $("#add-filter-second").hide();
  $("#cancel-enter-second").hide();
  $("#add-con-second").show();
}
function ThirdCancelInfo(event) {
  event.preventDefault();
  event.stopPropagation();
  $("#filter-box-3").removeClass("select-box");
  $("#column-third").val("");
  $("#add-filter-third").hide();
  $("#column-third").attr("type", "hidden");
  $("#add-con-third").show();
}
/**
 * first input box
 */

$("#column-second")
  .autocomplete({
    source: calculations,
    minLength: 0,
    maxheight: 100,
    select: function (event, ui) {
      $("#value-second").attr("type", "text");
      $("#completed-second").show();
      $("#value-second").val("");
      let columnInfo = availColNames;
      if (ui.item.value != "count") columnInfo = ColumnsIsNum;
      $("#completed-second").attr("disabled", true);
      $("#value-second")
        .autocomplete({
          source: columnInfo,
          minLength: 0,
          select: function (event, ui) {
            let secVal = $("#column-second").val();
            if (secVal == null || secVal.trim() == "") $("#completed-second").attr("disabled", true);
            else $("#completed-second").attr("disabled", false);
          }
        })
        .on("focus", function () {
          if (!$(this).val().trim()) $(this).keydown();
        });
    },
  })
  .on("focus", function () {
    if (!$(this).val().trim()) $(this).keydown();
  });
/**
 * get values of cur column names from back-end for first input box
 *
 */
function getValuesofColumn(chooseColumn) {
  valuesOfColumn.clear();
  let param = {
    state: "query",
    searchText: "SELECT DISTINCT " + chooseColumn + " FROM `ind-0`",
    startEpoch: filterStartDate,
    endEpoch: filterEndDate,
    indexName: "*",
    queryLanguage: "SQL",
    from: 0,
    size: 1000,
  };
  startQueryTime = new Date().getTime();
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
    if (res && res.hits && res.hits.records) {
      for (let i = 0; i < res.hits.records.length; i++) {
        let cur = res.hits.records[i][chooseColumn];
        if (typeof cur == "string") valuesOfColumn.add(cur);
        else valuesOfColumn.add(cur.toString());
      }
    }
    let arr = Array.from(valuesOfColumn);
    $("#value-first")
      .autocomplete({
        source: arr,
        minLength: 0,
        select: function (event, ui) {
          //check if complete btn can click
          checkFirstBox(2);
          valuesOfColumn.clear();
        },
      })
      .on("focus", function () {
        if (!$(this).val().trim()) $(this).keydown();
      });
  });
}
function setShowColumnInfoDialog(){
  $("#show-record-popup").dialog({
    autoOpen: false,
    resizable: false,
    title: false,
    maxHeight: 307,
    height: 307,
    width: 464,
    modal: true,
    position: {
      my: "center",
      at: "center",
      of: window,
    },
    buttons: {
      Cancel: {
        class: "cancelqButton cancel-record-btn",
        text: "Cancel",
        click: function () {
          $("#show-record-popup").dialog("close");
        },
      },
    }
  });
  $("#show-record-intro-btn").on("click", function () {
    $("#show-record-popup").dialog("open");
    $(".ui-widget-overlay").addClass("opacity-75");
    // return false;
  });
}