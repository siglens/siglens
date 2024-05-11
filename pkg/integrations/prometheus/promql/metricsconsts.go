package promql

const MIN_IN_MS = 60_000
const HOUR_IN_MS = 3600_000
const DAY_IN_MS = 86400_000
const TEN_YEARS_IN_SECS = 315_360_000

const metricFunctions = `[
	{
		"fn": "abs",
		"name": "Absolute",
		"desc": "Returns the input vector with all datapoint values converted to their absolute value.",
		"eg": "abs(avg (system.disk.used{*}))",
		"isTimeRangeFunc": false
	}, 
	{
		"fn": "ceil",
		"name": "Ceil",
		"desc": "Rounds the datapoint values of all elements in v up to the nearest integer.",
		"eg": "ceil(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "floor",
		"name": "Floor",
		"desc": "Rounds the datapoint values of all elements in v down to the nearest integer.",
		"eg": "floor(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "round",
		"name": "Round",
		"desc": "Rounds the datapoint values of all elements in v to the nearest integer.",
		"eg": "round(avg (system.disk.used)), round(avg (system.disk.used, 1/2))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "ln",
		"name": "Natural logarithm",
		"desc": "Calculates the natural logarithm for all elements in v.",
		"eg": "ln(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "log2",
		"name": "Binary logarithm",
		"desc": "Calculates the binary logarithm for all elements in v.",
		"eg": "log2(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "log10",
		"name": "Decimal logarithm",
		"desc": "Calculates the decimal logarithm for all elements in v.",
		"eg": "log10(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "rate", 
		"name": "Rate", 
		"desc": "Calculates the per-second average rate of increase of the time series in the range vector.", 
		"eg": "rate(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "irate", 
		"name": "Instant Rate", 
		"desc": "Calculates the per-second instant rate of increase of the time series in the range vector", 
		"eg": "irate(http_requests_total[5m])",
		"isTimeRangeFunc": true
	},
	{
		"fn": "increase", 
		"name": "Increase", 
		"desc": "Calculates the increase in the time series in the range vector",
		"eg": "increase(http_requests_total[5m])",
		"isTimeRangeFunc": true
	},
	{
		"fn": "delta", 
		"name": "Delta", 
		"desc": "Calculates the difference between the first and last value of each time series element in a range vector v",
		"eg": "delta(cpu_temp_celsius[2h])",
		"isTimeRangeFunc": true
	},
	{
		"fn": "idelta", 
		"name": "Instant Delta", 
		"desc": "Calculates the difference between the last two samples in the range vector v",
		"eg": "idelta(cpu_temp_celsius[2h])",
		"isTimeRangeFunc": true
	},
	{
		"fn": "deriv", 
		"name": "Derivative", 
		"desc": "Calculates the per-second derivative of the time series in a range vector v, using simple linear regression", 
		"eg": "deriv(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	}
]`

const PromQLBuildInfo = `{
    "status": "success",
    "data": {
        "version": "2.23.1",
        "revision": "cb7cbad5f9a2823a622aaa668833ca04f50a0ea7",
        "branch": "master",
        "buildUser": "julius@desktop",
        "buildDate": "20060102-15:04:05",
        "goVersion": "go1.22.2"
    }
}`
