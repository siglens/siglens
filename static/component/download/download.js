(function ($) {
  $.fn.download = function (options) {
    var defaults = {
      data: [],
      downloadMethod: ".cvs"
    };
    let setting = $.extend(defaults, options || {});
    this.html(``);
    let curMethod = setting.downloadMethod.replace(".", "");
    this
      .append(`<div id="download-info-${curMethod}" title="Download" class="download-result"><p class="validateTips" id="validateTips"></p>
            <form>
                <fieldset>
                    <input type="text" name="qnameDL" id="qnameDL-${curMethod}" placeholder="Name"
                        class="text ui-widget-content ui-corner-all name-info">
                    <input type="submit" tabindex="-1" style="position:absolute; top:-1000px">
                </fieldset>
            </form></div>
            <div class="pop-download" id="pop-box">
            <div class="pop-text">Downloading...</div>
            <button class="btn cancel-loading" id="cancel-loading">Cancel</button>
        </div>
            `);


let curChoose = setting.downloadMethod;
let interval = null;
var progressBar = $("#progressbar");
var progressLabel = $(".progress-label");
let confirmDownload = true;
if (typeof interval != "undefined") {
  doClearInterval();
} else {
  interval = null;
}
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
$("#cancel-loading").on("click", cancelDownload);
  let dialog = null;
  let form = null;
  let qname = $(`#qnameDL-${curMethod}`);
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
            let col = item[header];
            return typeof col !== "string" ? col : `"${col.replace(/"/g, '""')}"`;
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
          /^[a-zA-Z0-9_.-]+$/i,
          "Download name may consist of a-z, 0-9, period, dash, underscores."
        );
    let enteredName = $(`#qnameDL-${curMethod}`).val();
    let extension = curChoose;
    let name = enteredName;
  
    if (!enteredName.endsWith(extension)) {
      name += extension;
    }
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
        if (
          setting.data.length > 0
        ) {
          let json = JSON.stringify(setting.data);
          if (setting.downloadMethod == ".json") downloadJson(name, json);
          else {
            const csvData = convertToCSV(json);
            downloadCsv(csvData, name);
          }
        } else {
          alert("no data available");
        }
      }
    }
    dialog = $(`#download-info-${curMethod}`).dialog({
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
    $("#validateTips").hide();
    $(`#download-info-${curMethod}`).dialog("open");
    $(".ui-widget-overlay").addClass("opacity-75");
    return this;
  };
})(jQuery);
