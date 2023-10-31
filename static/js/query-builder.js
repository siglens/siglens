"use strict";
$(document).ready(function () {
  $("#add-con").on("click", filterStart);
  $("#add-con-second").on("click", secondFilterStart);
  $("#add-con-third").on("click", ThirdFilterStart);
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
  }
});
tagSecond.addEventListener("click", function (event) {
  // If the clicked element has the class 'delete-button'
  if (event.target.classList.contains("delete-button")) {
    // Remove the parent element (the tag)
    let str = event.target.parentNode.textContent;
    secondBoxSet.delete(str.substring(0, str.length - 1));
    event.target.parentNode.remove();
  }
});
tagThird.addEventListener("click", function (event) {
  // If the clicked element has the class 'delete-button'
  if (event.target.classList.contains("delete-button")) {
    // Remove the parent element (the tag)
    let str = event.target.parentNode.textContent;
    thirdBoxSet.delete(str.substring(0, str.length - 1));
    event.target.parentNode.remove();
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
        for (let i = 0; i < availColNames.length; i++) {
          if (ui.item.value == ColumnsIsNum[i]) {
            availSymbol = ["=", "!=", "<=", ">=", ">", "<"];
            break;
          }
        }
        $("#symbol").val("");
        $("#value-first").val("");
        let chooseColumn = ui.item.value.trim();
        getValuesofColumn(chooseColumn);

        $("#symbol")
          .autocomplete({
            source: availSymbol,
            minLength: 0,
            select: function (event, ui) {
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
}
function secondFilterStart(evt) {
  evt.preventDefault();
  $("#column-second").attr("type", "text");
  $("#add-con-second").hide();
  $("#cancel-enter-second").show();
  $("#add-filter-second").show();
  $("#add-filter-second").css({ visibility: "visible" });
}
function ThirdFilterStart(evt) {
  evt.preventDefault();
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
        }
        return false;
      },
    })
    .on("focus", function () {
      if (!$(this).val().trim()) $(this).keydown();
    });
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
  let tagContent = $("#column-first").val().trim() + $("#symbol").val().trim();
  tagContent += '"' + val + '"';
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
  }
}
function cancelInfo(evt) {
  evt.preventDefault();
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
      $("#value-second")
        .autocomplete({
          source: columnInfo,
          minLength: 0,
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
          valuesOfColumn.clear();
        },
      })
      .on("focus", function () {
        if (!$(this).val().trim()) $(this).keydown();
      });
  });
}
