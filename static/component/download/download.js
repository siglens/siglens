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
  $.fn.download = function (options) {
    var defaults = {
      data: [],
      downloadMethod: ".csv", // Default to CSV
      supportedFormats: [".json", ".csv", ".xml", ".sql"] // Added ".xml" to supported formats
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
            <button class="btn cancel-loading btn-secondary" id="cancel-loading">Cancel</button>
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
  interval = setInterval(doProgress, t * 10);
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

  // Function to convert JSON data to XML format
  function convertToXML(json) {
    const items = JSON.parse(json);
    let xmlString = '<?xml version="1.0" encoding="UTF-8"?>\n<root>\n';
    items.forEach(item => {
      xmlString += '  <item>\n';
      Object.keys(item).forEach(key => {
        xmlString += `    <${key}>${item[key]}</${key}>\n`;
      });
      xmlString += '  </item>\n';
    });
    xmlString += '</root>';
    return xmlString;
  }

  // Function to download XML data as a file
  function downloadXml(xmlData, fileName) {
    const blob = new Blob([xmlData], { type: "text/xml" });
    const url = URL.createObjectURL(blob);
    const downloadLink = document.createElement("a");
    downloadLink.href = url;
    downloadLink.download = fileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
  }

    // Function to download SQL data as a file
  function downloadSQL(sqlData, fileName) {
    const blob = new Blob([sqlData], { type: "text/sql" }); // Change type to "text/sql"
    const url = URL.createObjectURL(blob);
    const downloadLink = document.createElement("a");
    downloadLink.href = url;
    downloadLink.download = fileName;
    document.body.appendChild(downloadLink);
    downloadLink.click();
    document.body.removeChild(downloadLink);
  }

  // Function to download SQL data as a file
  function convertToSQL(json) {
    const data = JSON.parse(json);
    const tableName = 'SQL_Table'; 
    const columns = Object.keys(data[0]); 
    
    // Generate SQL INSERT statements for each object in the data array
    const sqlStatements = data.map(item => {
      const values = columns.map(col => {
        // Escape single quotes in string values and wrap in quotes
        const value = typeof item[col] === 'string' ? `'${item[col].replace(/'/g, "''")}'` : item[col];
        return value;
      }).join(', '); // Join column values with commas
  
      return `INSERT INTO ${tableName} (${columns.join(', ')}) VALUES (${values});`;
    });
    return sqlStatements.join('\n');
  }

    function download() {
      confirmDownload = true;
      let valid = true;
      allFields.removeClass("ui-state-error");
      tips.removeClass("ui-state-highlight");
      tips.text("");
      valid = valid && checkLength(qname, "download name", 1, 254);
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
          if (setting.downloadMethod == ".json") {
            downloadJson(name, json);
          } else if (setting.downloadMethod == ".csv") {
            const csvData = convertToCSV(json);
            downloadCsv(csvData, name);
          } else if (setting.downloadMethod == ".xml") { // Handle XML format
            const xmlData = convertToXML(json);
            downloadXml(xmlData, name);
          } else if (setting.downloadMethod == ".sql") { // Handle SQL format
            const sqlData = convertToSQL(json);
            downloadSQL(sqlData, name);
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
      title: 'Download Traces',
      position: {
        my: "center",
        at: "center",
        of: window,
      },
      buttons: {
        Cancel: {
          class: "cancelqButton btn btn-secondary",
          text: "Cancel",
          click: function () {
            dialog.dialog("close");
          },
        },
        Save: {
          class: "saveqButton btn btn-primary",
          text: "Save",
          click: download,
        },
      },
      close: function () {
        form[0].reset();
        allFields.removeClass("ui-state-error");
      },
      create: function () {
        $(this).parent().find('.ui-dialog-titlebar').show().addClass('border-bottom p-4');
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
