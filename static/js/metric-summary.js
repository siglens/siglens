let stDate = Cookies.get('startEpoch') || "now-3h";
let endDate = Cookies.get('endEpoch') || "now";
$(document).ready(() => {
    $('.inner-range #' + stDate).addClass('active');
    datePickerHandler(stDate, endDate, stDate);
    $('.range-item').on('click', isMetricsDatePickerHandler);
    let data = getTimeRange();
    getMetricNames();
});


function isMetricsDatePickerHandler(evt) {
    evt.preventDefault();
    $.each($(".range-item.active"), function () {
        $(this).removeClass('active');
    });
    $(evt.currentTarget).addClass('active');
    datePickerHandler($(this).attr('id'), "now", $(this).attr('id'))
    getMetricNames();
    $('#daterangepicker').hide();
}
function getTimeRange() {
    return {
        'startEpoch': filterStartDate || "now-1h",
        'endEpoch': filterEndDate || "now",
    };
}
function getMetricNames() {
    const data = getTimeRange();
    const pl={
        start:data.startEpoch,
        end:data.endEpoch,
    };
    $.ajax({
        method: "post",
        url: "metrics-explorer/api/v1/metric_names",
        headers: {
            "Content-Type": "application/json; charset=utf-8",
            Accept: "*/*",
        },
        crossDomain: true,
        dataType: "json",
        data: JSON.stringify(pl),
    }).then(function (res){
        displaydata(res);
    })
}
function displaydata(res) {
    if (!res || !res.metricNames || !Array.isArray(res.metricNames)) {
        console.error('Invalid response format:', res);
        return;
    }

    const metricNames = res.metricNames;

    if (gridDiv === null) {
        gridDiv = document.querySelector('#ag-grid');
        new agGrid.Grid(gridDiv, gridOptions);
    }

    const metricRows = metricNames.map((metric, index) => {
        return { metricName: metric };
    });

    gridOptions.api.setColumnDefs([{ headerName: "Metric Name", field: "metricName" }]);
    gridOptions.api.setRowData(metricRows);
    gridOptions.api.sizeColumnsToFit();
}

