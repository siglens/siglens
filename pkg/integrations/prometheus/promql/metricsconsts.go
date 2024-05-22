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
		"fn": "sqrt",
		"name": "Square root",
		"desc": "calculates the square root of all elements in v.",
		"eg": "sqrt(avg (system.disk.used{*}))",
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
		"fn": "exp",
		"name": "Exponential",
		"desc": "Calculates the exponential function for all elements in v.",
		"eg": "exp(avg (system.disk.used))",
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
		"fn": "sgn",
		"name": "Sign",
		"desc": "Returns a vector with all sample values converted to their sign, defined as this: 1 if v is positive, -1 if v is negative and 0 if v is equal to zero.",
		"eg": "sgn(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "deg",
		"name": "Degree",
		"desc": "Converts radians to degrees for all elements in v.",
		"eg": "deg(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "rad",
		"name": "Radian",
		"desc": "converts degrees to radians for all elements in v.",
		"eg": "rad(avg (system.disk.used))",
		"isTimeRangeFunc": false
	},
	{
		"fn": "clamp",
		"name": "Clamp",
		"desc": "Clamps the sample values of all elements in v to have a lower limit of min and an upper limit of max.",
		"eg": "clamp(avg (system.disk.used), 0, 99)",
		"isTimeRangeFunc": false
	},
	{
		"fn": "clamp_max",
		"name": "Clamp Max",
		"desc": "Clamps the sample values of all elements in v to have an upper limit of max.",
		"eg": "clamp_max(avg (system.disk.used), 99)",
		"isTimeRangeFunc": false
	},
	{
		"fn": "clamp_min",
		"name": "Clamp Min",
		"desc": "Clamps the sample values of all elements in v to have a lower limit of min.",
		"eg": "clamp_min(avg (system.disk.used), 0)",
		"isTimeRangeFunc": false
	},
	{
		"fn": "timestamp",
		"name": "Timestamp",
		"desc": "Returns the timestamp of each of the samples of the given vector as the number of seconds since January 1, 1970 UTC.",
		"eg": "timestamp(avg (system.disk.used))",
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
	},
	{
		"fn": "changes", 
		"name": "Changes", 
		"desc": "Returns the number of times its value has changed within the provided time range as an instant vector.", 
		"eg": "changes(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "resets", 
		"name": "Resets", 
		"desc": "returns the number of counter resets within the provided time range as an instant vector.", 
		"eg": "resets(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "avg_over_time", 
		"name": "Average Over Time", 
		"desc": "The average value of all points in the specified interval.", 
		"eg": "avg_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "min_over_time", 
		"name": "Minimum Over Time", 
		"desc": "The minimum value of all points in the specified interval.", 
		"eg": "min_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "max_over_time", 
		"name": "Maximum Over Time", 
		"desc": "The maximum value of all points in the specified interval.", 
		"eg": "max_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "sum_over_time", 
		"name": "Sum Over Time", 
		"desc": "The sum of all values in the specified interval.", 
		"eg": "sum_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "count_over_time", 
		"name": "Count Over Time", 
		"desc": "The count of all values in the specified interval.", 
		"eg": "count_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
        "fn": "hour",
        "name": "Hour",
        "desc": "Extracts the hour of a timestamp, represented as the number of hours since midnight.",
        "eg": "hour(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
        "fn": "minute",
        "name": "Minute",
        "desc": "Extracts the minute of a timestamp, represented as the number of minutes since the last hour.",
        "eg": "minute(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
        "fn": "month",
        "name": "Month",
        "desc": "Extracts the month of a timestamp, represented as a number from 1 (January) to 12 (December).",
        "eg": "month(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },    
	{
        "fn": "year",
        "name": "Year",
        "desc": "Extracts the year of a timestamp.",
        "eg": "year(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
        "fn": "day_of_month",
        "name": "Day of Month",
        "desc": "Extracts the day of the month from a timestamp, represented as a number from 1 to 31.",
        "eg": "day_of_month(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
        "fn": "day_of_week",
        "name": "Day of Week",
        "desc": "Extracts the day of the week from a timestamp, represented as a number from 1 (Monday) to 7 (Sunday).",
        "eg": "day_of_week(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
        "fn": "day_of_year",
        "name": "Day of Year",
        "desc": "Extracts the day of the year from a timestamp, represented as a number from 1 to 366.",
        "eg": "day_of_year(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
        "fn": "days_in_month",
        "name": "Days in Month",
        "desc": "Returns the number of days in the month of a timestamp.",
        "eg": "days_in_month(timestamp(system.disk.used))",
        "isTimeRangeFunc": false
    },
    {
    "fn": "quantile_over_time", 
		"name": "Quantile Over Time", 
		"desc": "The φ-quantile (0 ≤ φ ≤ 1) of the values in the specified interval.", 
		"eg": "quantile_over_time(0.6, avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "stddev_over_time", 
		"name": "Standard deviation Over Time", 
		"desc": "The population standard deviation of the values in the specified interval.", 
		"eg": "stddev_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "stdvar_over_time", 
		"name": "Standard variance Over Time", 
		"desc": "The population standard variance of the values in the specified interval.", 
		"eg": "stdvar_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "last_over_time", 
		"name": "Last Over Time", 
		"desc": " the most recent point value in the specified interval.", 
		"eg": "last_over_time(avg (system.disk.used[5m]))",
		"isTimeRangeFunc": true
	},
	{
		"fn": "present_over_time", 
		"name": "Present Over Time", 
		"desc": "The value 1 for any series in the specified interval.", 
		"eg": "present_over_time(avg (system.disk.used[5m]))",
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
