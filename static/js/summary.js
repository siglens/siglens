'use strict';
$(document).ready(() => {
  addSummaryElement();
});

async function addSummaryElement() {
  // Clone the first query element if it exists, otherwise create a new one
  var queryElement;
  queryElement = $(`
  <div class="summary_layout_grid_area input-container">
      <label class="tag"> Metric
      </label>
      <input type="text" class="summary_form_input_text metrics" placeholder="Search metrics">
      <label class="tag"> Tag
      </label>
      <input type="text" class="summary_form_input_text" placeholder="Filter by Tag Value">
      <div class="alias-box">
          <div class="alias-filling-box" style="display: none;">
              <input type="text" placeholder="alias">
              <div>×</div>
          </div>
      </div>
      <div class="remove-query">×</div>
  </div>`);

  $('#summary-element-id').append(queryElement);
  const metricNamesData = await getMetricNames();
  metricNamesData.metricNames.sort();
  queryElement.find('.metrics').val(metricNamesData.metricNames[0]); 

  // display metric data in table as well
  displayMetricsTable(metricNamesData.metricNames);

  // Initialize autocomplete with the details of the previous query if it exists
  initializeAutocomplete(queryElement, queryIndex > 0 ? queries[queryIndex - 1] : undefined);

  // Alias close button
  queryElement.find('.alias-filling-box div').last().on('click', function () {
      $(this).parent().hide();
      $(this).parent().siblings('.as-btn').show();
  });

  // Initialize autocomplete for metrics input
  initializeAutocomplete(queryElement.find('.metrics'));
}

let gridDiv = null;
let metricsData = [];
const columnDefs = [
  { headerName: "Metric Name", field: "metricName" },
];

const gridOptions = {
  rowData: metricsData,
  headerHeight: 32,
  rowHeight: 42,
  defaultColDef: {
    cellClass: 'align-center-grid',
    resizable: true,
    sortable: true,
    animateRows: true,
    readOnlyEdit: true,
    autoHeight: true,
    icons: {
      sortAscending: '<i class="fa fa-sort-alpha-down"/>',
      sortDescending: '<i class="fa fa-sort-alpha-up"/>',
    },
  },
  columnDefs: columnDefs,
};

function displayMetricsTable(res) {
  debugger;
  if (gridDiv === null) {
    gridDiv = document.querySelector('#ag-grid');
    new agGrid.Grid(gridDiv, gridOptions);
  }
  gridOptions.api.setColumnDefs(columnDefs);
  let newRow = new Map()
  metricsData = []
  $.each(res, function (index, value) {
    newRow.set("rowId", index);
    newRow.set("metricName", value);
    metricsData = _.concat(metricsData, Object.fromEntries(newRow));
  })
  gridOptions.api.setRowData(metricsData);
  gridOptions.api.sizeColumnsToFit();
  gridOptions.columnApi.applyColumnState({
    state: [{ colId: 'error', sort: 'desc' }],
    defaultState: { sort: null },
  });
}
