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


 let sqgridDiv = null;
 let sqRowData = [];
 let delNodeId = ''


 function setSaveQueriesDialog() {
     let dialog = null;
     let form = null;
     let qname = $("#qname");
     let description = $("#description");
     let allFields = $([]).add(qname).add(description);
     let tips = $(".validateTips");
 
     function updateTips(t) {
         tips.addClass("active");
         $(".validateTips").show();
         tips
             .text(t)
             .addClass("ui-state-highlight");
     }
 
     function checkLength(o, n, min, max) {
         if (o.val().length > max || o.val().length < min) {
             o.addClass("ui-state-error");
             updateTips("Length of " + n + " must be between " +
                 min + " and " + max + ".");
             return false;
         } else {
             return true;
         }
     }
 
     function checkRegexp(o, regexp, n) {
         if (!(regexp.test(o.val()))) {
             o.addClass("ui-state-error");
             updateTips(n);
             return false;
         } else {
             return true;
         }
     }
 
     function saveQuery() {
         let valid = true;
         allFields.removeClass("ui-state-error");
         tips.removeClass("ui-state-highlight");
         tips.text('');
         valid = valid && checkLength(qname, "query name", 3, 30);
         valid = valid && checkRegexp(qname, /^[a-zA-Z0-9_-]+$/i, "queryname may consist of a-z, 0-9, dash, underscores.");
 
         if (valid) {
             //post to save api
             let data = getSearchFilterForSave(qname.val(), description.val());
             $.ajax({
                 method: 'post',
                 url: 'api/usersavedqueries/save',
                 headers: {
                     'Content-Type': 'application/json; charset=utf-8',
                     'Accept': '*/*'
                 },
                 crossDomain: true,
                 dataType: 'json',
                 data: JSON.stringify(data)
             })
                 .then(function () {
                     dialog.dialog("close");
                 })
                 .catch(function (err) {
                     if (err.status !== 200) {
                         showError(`Message: ${err.statusText}`);
                     }
                     dialog.dialog("close");
                 })
         }
 
     }
 
     dialog = $('#save-queries').dialog({
         autoOpen: false,
         resizable: false,
         maxHeight: 307,
         height: 307,
         width: 464,
         modal: true,
         position: {
             my: "center",
             at: "center",
             of: window
         },
         buttons: {
             Cancel: {
                 class: 'cancelqButton',
                 text: 'Cancel',
                click : function (){
                     dialog.dialog("close");
                 }
             },
             Save: {
                 class: 'saveqButton',
                 text: 'Save',
                click : saveQuery,
             },
         },
         close: function () {
             form[0].reset();
             allFields.removeClass("ui-state-error");
         }
     });
 
     form = dialog.find("form").on("submit", function (event) {
         event.preventDefault();
         saveQuery();
     });
 
    $('#saveq-btn').on("click",function () {
         $(".validateTips").hide();
         $('#save-queries').dialog('open');
         $('.ui-widget-overlay').addClass('opacity-75');
         return false;
     });
 }
 
 function getSavedQueries() {
     $.ajax({
         method: 'get',
         url: 'api/usersavedqueries/getall',
         headers: {
             'Content-Type': 'application/json; charset=utf-8',
             'Accept': '*/*'
         },
         crossDomain: true,
         dataType: 'json',
     })
         .then(displaySavedQueries);
 }
 
 class linkCellRenderer {
     // init method gets the details of the cell to be renderer
     init(params) {
        this.eGui = document.createElement('span');
        let href = "index.html?searchText=" +
          encodeURIComponent(params.data.searchText) +
          "&indexName=" +
          encodeURIComponent(params.data.indexName) +
          "&filterTab=" +
          encodeURIComponent(params.data.filterTab) +
          "&queryLanguage=" +
          encodeURIComponent(params.data.queryLanguage);
        this.eGui.innerHTML = '<a class="query-link" href=' + href +
          '" title="' +
          params.data.description +
          '"style="display:block;">' +
          params.data.qname +
          "</a>";
     }
 
     getGui() {
         return this.eGui;
     }
     refresh(params) {
         return false;
     }
 }
 
 class btnCellRenderer {
     // init method gets the details of the cell to be renderer
     init(params) {
         this.eGui = document.createElement('div');
         this.eGui.innerHTML = `<input type="button" class="btn-simple" id="delbutton"  />`;
 
         // get references to the elements we want
         this.eButton = this.eGui.querySelector('.btn-simple');
         this.eventListener = () => {
          $('.popupOverlay, .popupContent').addClass('active');
          $('#delete-btn').data('params', params);        
         }
         this.eButton.addEventListener('click', this.eventListener); 
     }
 
     getGui() {
         return this.eGui;
     }
 
     // gets called when the cell is removed from the grid
     destroy() {
         // do cleanup, remove event listener from button
         if (this.eButton) {
             // check that the button element exists as destroy() can be called before getGui()
             this.eButton.removeEventListener('click', this.eventListener);
         }
     }
 
     refresh(params) {
         return false;
     }
 }

 // Delete confirmation popup
$(document).ready(function () {
  $('#cancel-btn, .popupOverlay, #delete-btn').click(function () {
    $('.popupOverlay, .popupContent').removeClass('active');
  });

  // delete function
  $('#delete-btn').click(function () {
    let params= $('#delete-btn').data('params');
    $.ajax({
      method: 'get',
      url: 'api/usersavedqueries/deleteone/' + params.data.qname,
      headers: {
        'Content-Type': 'application/json; charset=utf-8',
        Accept: '*/*',
      },
      crossDomain: true,
    }).then(function () {
      let deletedRowID = params.data.rowId;
      sqgridOptions.api.applyTransaction({
        remove: [{ rowId: deletedRowID }],
      });
    });
  });

  $(document).keypress(function(event){
    if(event.keyCode == '13'){
      $('.popupOverlay, .popupContent').removeClass('active');
    }
  })

  $('#sq-filter-input').keyup(function(event){
    if (event.keyCode == '13'){
        searchSavedQueryHandler(event);
    }else{
        displayOriginalSavedQueries();
    }
    })
});

 let queriesColumnDefs = [
   {
     field: "rowId",
     hide: true,
   },
   {
     field: "qname",
     headerName: "Query Name",
     cellRenderer: linkCellRenderer,
     resizable: true,
   },
   {
     field: "qdescription",
     headerName: "Description",
     resizable: true,
   },
   {
     field: "queryLanguage",
     headerName: "QueryLanguage",
     resizable: true,
   },
   {
     field: "filterTab",
     headerName: "FilterTab",
     hide: true,
   },
   {
     field: "qdelete",
     headerName: "Delete",
     cellRenderer: btnCellRenderer,
     resizable: false,
   },
 ];
 
 
 // let the grid know which columns and what data to use
 const sqgridOptions = {
     columnDefs: queriesColumnDefs,
     rowData: sqRowData,
     animateRows: true,
     headerHeight:32,
     defaultColDef: {
         initialWidth: 200,
         sortable: true,
     },
     enableCellTextSelection: true,
     suppressScrollOnNewData: true,
     suppressAnimationFrame: true,
     getRowId: (params) => params.data.rowId,
     onGridReady(params) {
         this.gridApi = params.api; // To access the grids API
     },
     localeText: {
        noRowsToShow: 'No Saved Query Found'
    }
    //  domLayout: 'autoHeight',
 };
 
 function displaySavedQueries(res,flag) {
     // loop through res and add data to savedQueries
    if (flag === -1){
        //for search
        let sqFilteredRowData = [];
        if (sqgridDiv === null) {
            sqgridDiv = document.querySelector('#queries-grid');
            new agGrid.Grid(sqgridDiv, sqgridOptions);
        }
        sqgridOptions.api.setColumnDefs(queriesColumnDefs);
        let idx = 0;
        let newRow = new Map()
        $.each(res, function (key, value) {
            newRow.set("rowId", idx)
            newRow.set("qdescription", res[key].description)
            newRow.set("searchText", value.searchText)
            newRow.set("indexName", value.indexName)
            newRow.set("qname", key)
            newRow.set("queryLanguage", value.queryLanguage)
            newRow.set("filterTab", value.filterTab)
            sqFilteredRowData = _.concat(sqFilteredRowData, Object.fromEntries(newRow));
            idx = idx + 1
    })
    sqgridOptions.api.setRowData(sqFilteredRowData);
        sqgridOptions.api.sizeColumnsToFit();
    }else{
        if (sqgridDiv === null) {
            sqgridDiv = document.querySelector('#queries-grid');
            new agGrid.Grid(sqgridDiv, sqgridOptions);
        }
        sqgridOptions.api.setColumnDefs(queriesColumnDefs);
        let idx = 0;
        let newRow = new Map()
        $.each(res, function (key, value) {
            newRow.set("rowId", idx)
            newRow.set("qdescription", res[key].description)
            newRow.set("searchText", value.searchText)
            newRow.set("indexName", value.indexName)
            newRow.set("qname", key)
            newRow.set("queryLanguage", value.queryLanguage)
            newRow.set("filterTab", value.filterTab)
            sqRowData = _.concat(sqRowData, Object.fromEntries(newRow));
            idx = idx + 1
    })
    sqgridOptions.api.setRowData(sqRowData);
    sqgridOptions.api.sizeColumnsToFit();
}}

function displayOriginalSavedQueries(){
    let searchText = $('#sq-filter-input').val()
    if (searchText.length === 0){
        if (sqgridDiv === null) {
            sqgridDiv = document.querySelector('#queries-grid');
            new agGrid.Grid(sqgridDiv, sqgridOptions);
        }
    $("#queries-grid-container").show()
    $('#empty-qsearch-response').hide()
    sqgridOptions.api.setColumnDefs(queriesColumnDefs);
    sqgridOptions.api.setRowData(sqRowData);
    sqgridOptions.api.sizeColumnsToFit();
    }
}

function getSearchedQuery() {
    let searchText = $('#sq-filter-input').val();
    $.ajax({
        method: 'get',
        url: 'api/usersavedqueries/' + searchText,
        headers: {
            'Content-Type': 'application/json; charset=utf-8',
            'Accept': '*/*'
        },
        crossDomain: true,
        dataType: 'json',
    })
    .then((res)=>{
        $("#queries-grid-container").show()
        displaySavedQueries(res, -1);
    })
    .catch(function (err) {

        if (err.status !== 200) {
            showError(`Message: ${err.statusText}`);
        }
        $("#queries-grid-container").hide();
        let el = $('#empty-qsearch-response');
        el.empty();
        el.append('<span>Query not found.</span>')
        el.show();
    })

}