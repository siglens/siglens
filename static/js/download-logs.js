"use strict";

let dlgridDiv = null;
let dlRowData = [];
let curChoose = "";
let interval = null;
var progressBar = $("#progressbar");
var progressLabel = $(".progress-label");
let confirmDownload = true;
$(function () {
  if (typeof interval != "undefined") {
    doClearInterval();
  } else {
    interval = null;
  }
});

var progressWidth = 0;
function beginProgress(t) {
  progressWidth = 1;
  interval = setInterval("doProgress()", t * 10);
}
function cancelDownload() {
  confirmDownload = false;
  var popBox = document.getElementById("pop-box");
  var popLayer = document.getElementById("pop-layer");
  popLayer.style.display = "none";
  popBox.style.display = "none";
  $("#progressbar").hide();
}
function setProgress(node, width) {
  if (node) {
    progressBar.progressbar({
      value: width,
    });
    progressLabel.text(width + "%");
  }
}
function doProgress() {
  if (progressWidth == 98) {
    doClearInterval();
  }
  setProgress(progressBar, progressWidth);
  progressWidth++;
}
function doClearInterval() {
  clearInterval(interval);
}

function setDownloadLogsDialog() {
  let dialog = null;
  let form = null;
  let qname = $("#qnameDL");
  let description = $("#descriptionR");
  let allFields = $([]).add(qname).add(description);
  let tips = $("#validateTips");

  function updateTips(t) {
    tips.addClass("active");
    $("#validateTips").show();
    tips.text(t).addClass("ui-state-highlight");
  }

  function checkLength(o, n, min, max) {
    if (o.val().length > max || o.val().length < min) {
      o.addClass("ui-state-error");
      updateTips(
        "Length of " + n + " must be between " + min + " and " + max + "."
      );
      return false;
    } else {
      return true;
    }
  }

  function checkRegexp(o, regexp, n) {
    if (!regexp.test(o.val())) {
      o.addClass("ui-state-error");
      updateTips(n);
      return false;
    } else {
      return true;
    }
  }

  function downloadJson(fileName, json) {
    const jsonStr = json instanceof Object ? JSON.stringify(json) : json;
    const url = window.URL || window.webkitURL || window;
    const blob = new Blob([jsonStr]);
    const saveLink = document.createElementNS(
      "http://www.w3.org/1999/xhtml",
      "a"
    );
    saveLink.href = url.createObjectURL(blob);
    saveLink.download = fileName;
    saveLink.click();
  }
  function convertToCSV(json) {
    const items = JSON.parse(json);

    // Get column headers from first item in JSON array
    const headers = Object.keys(items[0]);

    // Build CSV header row
    const csvHeader = headers.join(",");

    // Build CSV body rows
    const csvBody = items
      .map((item) => {
        return headers
          .map((header) => {
            return item[header];
          })
          .join(",");
      })
      .join("\n");

    // Combine header and body into single CSV string
    const csv = `${csvHeader}\n${csvBody}`;

    return csv;
  }
  function downloadCsv(csvData, fileName) {
    const blob = new Blob([csvData], { type: "text/csv" });
    const url = URL.createObjectURL(blob);
    const downloadLink = document.createElement("a");
    downloadLink.href = url;
    downloadLink.download = fileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
  }
  function download() {
    confirmDownload = true;
    let valid = true;
    allFields.removeClass("ui-state-error");
    tips.removeClass("ui-state-highlight");
    tips.text("");
    valid = valid && checkLength(qname, "download name", 3, 16);
    valid =
      valid &&
      checkRegexp(
        qname,
        /^[a-zA-Z0-9_-]+$/i,
        "Download name may consist of a-z, 0-9, dash, underscores."
      );
    let name = $("#qnameDL").val() + curChoose;
    if (valid) {
      dialog.dialog("close");
      $("#progressbar").show();
      //show progress box
      var popBox = document.getElementById("pop-box");
      var popLayer = document.getElementById("pop-layer");
      popLayer.style.width = document.body.scrollWidth + "px";
      popLayer.style.height = document.body.scrollHeight + "px";
      popLayer.style.display = "block";
      popBox.style.display = "block";

      let params = getSearchFilter(false, false);
      params.size = totalMatchLogs;
      $.ajax({
        method: "post",
        url: "api/search",
        headers: {
          "Content-Type": "application/json; charset=utf-8",
          Accept: "*/*",
        },
        crossDomain: true,
        dataType: "json",
        data: JSON.stringify(params),
        beforeSend: function () {
          beginProgress(10);
        },
        success: function (res) {
          //close progress box
          var popBox = document.getElementById("pop-box");
          var popLayer = document.getElementById("pop-layer");
          popLayer.style.display = "none";
          popBox.style.display = "none";
          //set progress finished
          doClearInterval();
          $("#progressbar").hide();
          setProgress(progressBar, 100);
          if (!confirmDownload) return;
          if (res && res.hits && res.hits.records) {
            let json = JSON.stringify(res.hits.records);
            if (curChoose == ".json") downloadJson(name, json);
            else {
              const csvData = convertToCSV(json);
              downloadCsv(csvData, name);
            }
          }
        },
        error: function () {
          doClearInterval();
        },
      });
    }
  }
  dialog = $("#download-info").dialog({
    autoOpen: false,
    resizable: false,
    width: 460,
    modal: true,
    position: {
      my: "center",
      at: "center",
      of: window,
    },
    buttons: {
      Cancel: {
        class: "cancelqButton",
        text: "Cancel",
        click: function () {
          dialog.dialog("close");
        },
      },
      Save: {
        class: "saveqButton",
        text: "Save",
        click: download,
      },
    },
    close: function () {
      form[0].reset();
      allFields.removeClass("ui-state-error");
    },
  });

  form = dialog.find("form").on("submit", function (event) {
    event.preventDefault();
    download();
  });
  $("#cvs-block").on("click", function () {
    curChoose = ".csv";
    $("#validateTips").hide();
    $("#download-info").dialog("open");
    $(".ui-widget-overlay").addClass("opacity-75");
    return false;
  });
  $("#json-block").on("click", function () {
    curChoose = ".json";
    $("#validateTips").hide();
    $("#download-info").dialog("open");
    $(".ui-widget-overlay").addClass("opacity-75");
    return false;
  });
}

// Delete confirmation popup
$(document).ready(function () {
  $("#cancel-loading").on("click", cancelDownload);
});
