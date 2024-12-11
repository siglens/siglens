package tests

import (
	"testing"

	"github.com/siglens/siglens/pkg/ast/pipesearch"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/colusage"
	"github.com/siglens/siglens/pkg/segment/structs"
	"github.com/stretchr/testify/assert"
)

type stringSet = map[string]struct{}

func set(items ...string) stringSet {
	s := make(stringSet)
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

func parseSPL(t *testing.T, query string) (*structs.ASTNode, *structs.QueryAggregators) {
	astNode, aggs, _, err := pipesearch.ParseQuery(query, 0, "Splunk QL")
	assert.NoError(t, err)
	return astNode, aggs
}

func assertCols(t *testing.T, query string, expectedFilterCols stringSet, expectedQueryCols stringSet) {
	astNode, aggs := parseSPL(t, query)
	actualFilterCols, actualQueryCols := colusage.GetFilterAndQueryColumns(astNode, aggs)
	assert.Equal(t, expectedFilterCols, actualFilterCols)
	assert.Equal(t, expectedQueryCols, actualQueryCols)
}

func Test_ColUsage(t *testing.T) {
	config.SetNewQueryPipelineEnabled(true)

	// Basic tests, and eval
	assertCols(t, "*", set(), set())
	assertCols(t, "city=Boston", set("city"), set("city"))
	assertCols(t, "city=Boston OR latency>1000 AND (age<100 OR NOT app=web)", set("city", "latency", "age", "app"), set("city", "latency", "age", "app"))
	assertCols(t, "* | sort weekday", set(), set("weekday"))
	assertCols(t, "* | eval x=weekday", set(), set("weekday"))
	assertCols(t, "* | eval x=latency | eval y=x+1", set(), set("latency"))
	assertCols(t, "* | eval x=latency | eval y=x+1 | eval z=y + age", set(), set("latency", "age"))
	assertCols(t, "city=Boston | eval x=latency | eval y=x+1", set("city"), set("city", "latency"))
	assertCols(t, "* | eval x=latency | sort x, age", set(), set("latency", "age"))

	// Bin
	assertCols(t, "* | bin span=1h timestamp", set(), set("timestamp"))
	assertCols(t, "* | eval x=latency | bin span=1h x", set(), set("latency"))
	assertCols(t, "* | bin span=1h latency as binned | eval y=binned", set(), set("latency"))

	// Dedup
	assertCols(t, "* | dedup city", set(), set("city"))
	assertCols(t, "* | dedup city latency", set(), set("city", "latency"))
	assertCols(t, "* | eval x=hostname | dedup city latency sortby -age +x", set(), set("hostname", "city", "latency", "age"))

	// Fields
	assertCols(t, "* | fields city", set(), set())
	assertCols(t, "city=Boston | fields city, age, latency", set("city"), set("city"))

	// Fillnull
	assertCols(t, "* | fillnull value=unknown latency", set(), set("latency"))
	assertCols(t, "* | fillnull value=unknown latency host", set(), set("latency", "host"))

	// Gentimes
	assertCols(t, "| gentimes start=01/01/2000 end=01/01/2001", set(), set())
	assertCols(t, "| gentimes start=01/01/2000 end=01/01/2001 | eval x=starttime", set(), set())

	// Inputlookup
	assertCols(t, "| inputlookup lookup.csv", set(), set())
	assertCols(t, "| inputlookup lookup.csv | sort host", set(), set()) // "host" has to be in the lookup file
	// "host" might not be in the lookup file.
	assertCols(t, "city=Boston | inputlookup append=true lookup.csv | sort host", set("city"), set("city", "host"))

	// Head
	assertCols(t, "* | head 10", set(), set())
	assertCols(t, "* | head latency>1000", set(), set("latency"))
	assertCols(t, "* | head weekday=\"Friday\"", set(), set("weekday"))
	assertCols(t, "* | head weekday=dayOfWeek", set(), set("weekday", "dayOfWeek"))
	assertCols(t, "* | head weekday=\"Friday\" OR (latency>1000 AND weekday=\"Monday\")", set(), set("weekday", "latency"))

	// Makemv
	assertCols(t, "* | makemv host", set(), set("host"))
	assertCols(t, "* | eval x=host | makemv x", set(), set("host"))

	// Mvexpand
	assertCols(t, "* | mvexpand host", set(), set("host"))
	assertCols(t, "* | eval x=host | mvexpand x", set(), set("host"))

	// Regex
	// The SPL parser puts regex in the search filter when it's the first command.
	assertCols(t, "* | regex host=\".*\"", set("host"), set("host"))
	assertCols(t, "* | eval x=host | regex x=\".*\"", set(), set("host"))

	// Rename
	assertCols(t, "* | rename host as hostname", set(), set("host"))
	assertCols(t, "* | eval x=host | rename x as hostname | eval y=hostname", set(), set("host"))

	// Rex
	assertCols(t, "* | rex field=host \"(?<name>.*)\"", set(), set("host"))
	assertCols(t, "* | rex field=host \"(?<name>.*)\" | eval x=name", set(), set("host"))

	// Stats
	assertCols(t, "* | stats count by host", set(), set("host"))
	assertCols(t, "* | stats count by host, region", set(), set("host", "region"))
	assertCols(t, "* | stats avg(latency) by host, region", set(), set("latency", "host", "region"))
	assertCols(t, "* | stats avg(latency), dc(weekday) by host", set(), set("latency", "weekday", "host"))
	assertCols(t, "* | stats avg(latency), dc(weekday)", set(), set("latency", "weekday"))

	// Streamstats
	assertCols(t, "* | streamstats count by host", set(), set("host"))
	assertCols(t, "* | streamstats count by host, region", set(), set("host", "region"))
	assertCols(t, "* | streamstats avg(latency) by host, region", set(), set("latency", "host", "region"))
	assertCols(t, "* | streamstats avg(latency), dc(weekday) by host", set(), set("latency", "weekday", "host"))
	assertCols(t, "* | streamstats avg(latency), dc(weekday)", set(), set("latency", "weekday"))
	// The Avg column must be read from the ingested data.
	assertCols(t, "* | streamstats reset_before=(Avg>100 OR platform=\"web\") avg(latency) by host", set(), set("Avg", "platform", "latency", "host"))
	assertCols(t, "* | streamstats reset_before=(Avg>100 OR platform=\"web\") avg(latency) as Avg by host", set(), set("Avg", "platform", "latency", "host"))
	assertCols(t, "* | streamstats reset_after=(Avg>100 OR platform=\"web\") avg(latency) by host", set(), set("Avg", "platform", "latency", "host"))
	// Now Avg is a computed column.
	assertCols(t, "* | streamstats reset_after=(Avg>100 OR platform=\"web\") avg(latency) as Avg by host", set(), set("platform", "latency", "host"))
	assertCols(t, "* | streamstats reset_after=(Avg>100 OR platform=\"web\") avg(latency) as Avg", set(), set("platform", "latency"))

	// Tail
	assertCols(t, "* | tail 10", set(), set())
	assertCols(t, "city=Boston | tail 10", set("city"), set("city"))

	// Timechart
	assertCols(t, "* | timechart count", set(), set("timestamp"))
	assertCols(t, "* | timechart avg(latency), sum(latitude) by http_status", set(), set("timestamp", "latency", "latitude", "http_status"))
	assertCols(t, "* | timechart span=30m avg(lat_http) as avg_lat_http by http_method limit=10", set(), set("timestamp", "lat_http", "http_method"))

	// Top/Rare
	assertCols(t, "* | top 10 host", set(), set("host"))
	assertCols(t, "* | top 10 host, age by city", set(), set("host", "age", "city"))
	assertCols(t, "* | rare 10 host", set(), set("host"))
	assertCols(t, "* | rare 10 host, age by city", set(), set("host", "age", "city"))

	// Transaction
	assertCols(t, "* | transaction host ip", set(), set("host", "ip"))
	assertCols(t, "* | transaction startswith=eval(city=\"Boston\") endswith=eval(latency>1000)", set(), set("city", "latency"))

	// Where
	assertCols(t, "* | where latency>1000", set(), set("latency"))
	assertCols(t, "* | where city=residence", set(), set("city", "residence"))
	assertCols(t, "* | where city=\"residence\"", set(), set("city"))
	assertCols(t, "* | where (city=home OR city=work) AND latency>1000", set(), set("city", "home", "work", "latency"))
}
