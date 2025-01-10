package tests

import (
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/segment/query/processor"
	"github.com/stretchr/testify/assert"
)

func Test_SearchSorter(t *testing.T) {
	runningConfig := config.GetTestConfig(t.TempDir())
	runningConfig.SSInstanceName = "test"
	config.SetConfig(runningConfig)
	config.SetNewQueryPipelineEnabled(true)
	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)

	_, aggs := parseSPL(t, `* | eval n = "weekday" | sort weekday`)
	sortExpr := processor.MutateForSearchSorter(aggs)
	assert.NotNil(t, sortExpr)

	_, aggs = parseSPL(t, `* | eval n = 1 | eval weekday = n | sort weekday`)
	sortExpr = processor.MutateForSearchSorter(aggs)
	assert.Nil(t, sortExpr)

	_, aggs = parseSPL(t, `* | head 20 | eval new_weekday = weekday | fields new_weekday, weekday | sort weekday`)
	sortExpr = processor.MutateForSearchSorter(aggs)
	assert.NotNil(t, sortExpr)

	_, aggs = parseSPL(t, `* | head 20 | eval weekday = old_weekday | fields old_weekday, weekday | where city="Boston" | sort weekday`)
	sortExpr = processor.MutateForSearchSorter(aggs)
	assert.Nil(t, sortExpr)
}
