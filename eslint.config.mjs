import globals from 'globals';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import js from '@eslint/js';
import { FlatCompat } from '@eslint/eslintrc';
const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

const compat = new FlatCompat({
  baseDirectory: __dirname,
  recommendedConfig: js.configs.recommended,
  allConfig: js.configs.all
});

export default [
  ...compat.extends('eslint:recommended', 'prettier'),
  {
    ignores: ['static/js/common.js','static/js/cluster-stats.js','static/js/application-version.js','static/js/all-slos.js','static/js/test-data.js','static/js/settings.js','static/js/navbar.js'],
  },
  {   
    languageOptions: {
      globals: {
        ...globals.node,
        ...globals.browser,
				...globals.jquery,
        "jQuery": true,
        "indexValues" : true,
        "themePickerHandler" : true,
        "datePickerHandler" : true,
        "setupEventHandlers" : true,
        "getListIndices" : true,
        "initializeIndexAutocomplete" : true,
        "setIndexDisplayValue" : true,
        "selectedSearchIndex" : true,
        "addQueryElement" : true,
        "isQueryBuilderSearch" : true,
        "getQueryBuilderCode" : true,
        "filterStartDate" : true,
        "filterEndDate" : true,
        "addQueryElementOnAlertEdit" : true,
        "addAlertsFormulaElement" : true,
        "showToast" : true,
        "loadBarOptions" : true,
        "echarts" : true,
        "_" : true,
        "parsePromQL" : true,
        "functionsArray" : true,
        "getFunctions" : true,
        "fetchLogsPanelData" : true,
        "metricsQueryParams" : true,
        "codeToBuilderParsing" : true,
        "wsState" : true,
        "d3" : true,
        "Cookies" : true,
        "data" : true,
        "Chart" : true,
        "firstBoxSet" : true,
        "secondBoxSet" : true,
        "thirdBoxSet" : true,
        "dateFns": true,
        "timestampDateFmt" : true,
        "renderAvailableFields": true,
        "availColNames": true,
        "string2Hex": true,
        "updatedSelFieldList": true,
        "selectedFieldsList": true,
        "resetAvailableFields": true,
        "removeToast": true,
        "showDeleteIndexToast": true,
        "shouldCloseAllDetails": true,
        "moment": true,
        "showInfo": true,
        "gridDiv": true,
        "eGridDiv": true,
        "currentPanel": true,
        "localPanels": true,
        "renderPanelAggsGrid": true,
        "renderPanelLogsGrid": true,
        "allResultsDisplayed": true,
        "panelLogsRowData": true,
        "panelGridDiv": true,
        "renderBarChart": true,
        "renderLineChart": true,
        "addSelectedIndex": true,
        "displayBigNumber": true,
        "panelChart": true,
        "defaultDashboardIds": true,
        "uuidv4": true,
        "ReadOnlyCellEditor": true,
        "cellEditorParams": true,
        "canScrollMore": true,
        "getQueryParamsData": true,
        "runPanelLogsQuery": true,
        "defaultColumnCount": true,
        "myCellRenderer": true,
        "displayTextWidth": true,
        "findColumnIndex": true,
        "theme": true,
        "runPanelAggsQuery": true,
        "runMetricsQuery": true,
        "displayPanelView": true,
        "setTimePicker": true,
        "updateTimeRangeForPanel": true,
        "editPanelInit": true,
        "flagDBSaved": true,
        "GridStack": true,
        "resetPanelTimeRanges": true,
        "displayStart": true,
        "displayEnd": true,
        "resetCustomDateRange": true,
        "getStartTimeHandler": true,
        "getEndTimeHandler": true,
        "customRangeHandler": true,
        "cytoscape" : true,
        "getSearchFilter": true,
        "dbName": true,
        "queryStr": true,
        "pauseRefreshInterval": true,
        "updateDashboard": true,
        "displayPanels": true,
        "updateTimeRangeForPanels": true,
        "startRefreshInterval": true,
        "isDefaultDashboard": true,
        "dbRefresh": true,
        "viewPanelInit": true,
        "panelIndex": true,
        "resetDashboard": true,
        "runQueryBtnHandler": true,
        "logsColumnDefs": true, //check this
        "gridOptions": true,
        "getInitialSearchFilter": true,
        "doSearch": true,
        "displayQueryLangToolTip": true,
        "alertChart": true,
        "logsRowData": true,
        "hideError": true,
        "liveTailState": true,
        "initialSearchData": true,
        "total_liveTail_searched": true,
        "getLiveTailFilter": true,
        "createLiveTailSocket": true,
        "updateColumns": true,
        "panelGridOptions": true,
        "socket": true,
        "allLiveTailColumns": true,
        "loadCustomDateTimeFromEpoch": true,
        "setSaveQueriesDialog": true,
        "availableEverywhere": true,
        "availableEverything": true,
        "startQueryTime": true,
        "timeChart": true,
        "getSavedQueries": true,
        "showError": true,
        "getSearchFilterForSave": true,
        "showSendTestDataUpdateToast": true,
        "newUri": true,
        "addQSParm": true,
        "renderMeasuresGrid": true,
        "measureFunctions": true,
        "measureInfo": true,
        "logOptionSingleHandler": true,
        "logOptionMultiHandler": true,
        "logOptionTableHandler": true,
        "myUrl": true,
        "sortByTimestampAtDefault": true,
        "scrollFrom": true,
        "lockReconnect": true,
        "tt": true,
        "handleTabAndTooltip": true,
        "aggsColumnDefs": true,
        "segStatsRowData": true,
        "isTimechart": true,
        "totalRrcCount": true,
        "aggGridOptions": true,
        "getStartDateHandler": true,
        "getEndDateHandler": true,
        "initializeFilterInputEvents": true,
        "createQueryString": true,
        "updateTimeRangeForAllPanels": true,
        "queries": true,
        "formulas": true,
        "getUrlParameter": true,
        "addQueryElementForAlertAndPanel": true,
        "fetchTimeSeriesData": true,
        "chartDataCollection": true,
        "generateUniqueId": true,
        "addMetricsFormulaElement": true,
        "convertDataForChart": true,
        "addVisualizationContainer": true,

      },
    },
    rules: {
      strict: [ "error", "function" ],
      'no-undef': 2,
      'no-unused-vars': [2, 
        {
          'vars': 'local',
          'args': 'after-used',
          "argsIgnorePattern": "^_", 
          "varsIgnorePattern": "^_",
        }]
    }
  }
];