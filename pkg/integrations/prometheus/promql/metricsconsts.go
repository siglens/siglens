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
		"eg": "abs(avg (system.disk.used{*}))"
	}, 
	{
		"fn": "ceil",
		"name": "Ceil",
		"desc": "Rounds the datapoint values of all elements in v up to the nearest integer.",
		"eg": "ceil(avg (system.disk.used))"
	},
	{
		"fn": "floor",
		"name": "Floor",
		"desc": "Rounds the datapoint values of all elements in v down to the nearest integer.",
		"eg": "floor(avg (system.disk.used))"
	},
	{
		"fn": "round",
		"name": "Round",
		"desc": "Rounds the datapoint values of all elements in v to the nearest integer.",
		"eg": "round(avg (system.disk.used)), round(avg (system.disk.used, 1/2))"
	},
	{
		"fn": "ln",
		"name": "Natural logarithm",
		"desc": "Calculates the natural logarithm for all elements in v.",
		"eg": "ln(avg (system.disk.used))"
	},
	{
		"fn": "log2",
		"name": "Binary logarithm",
		"desc": "Calculates the binary logarithm for all elements in v.",
		"eg": "log2(avg (system.disk.used))"
	},
	{
		"fn": "log10",
		"name": "Decimal logarithm",
		"desc": "Calculates the decimal logarithm for all elements in v.",
		"eg": "log10(avg (system.disk.used))"
	}
]`
