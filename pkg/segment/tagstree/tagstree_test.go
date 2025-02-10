package tagstree

import (
	"fmt"
	"sync"
	"testing"

	"github.com/buger/jsonparser"
	"github.com/siglens/siglens/pkg/config"
	treereader "github.com/siglens/siglens/pkg/segment/reader/metrics/tagstree"
	treewriter "github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/stretchr/testify/assert"
)

func initTestConfig(t *testing.T) {
	runningConfig := config.GetTestConfig(t.TempDir())
	config.SetConfig(runningConfig)

	err := config.InitDerivedConfig("test")
	assert.NoError(t, err)
}

func Test_SimpleReadWrite(t *testing.T) {
	initTestConfig(t)

	// Create writer
	treeHolder, err := treewriter.InitTagsTreeHolder("test_mid")
	assert.NoError(t, err)

	tagsHolder := treewriter.GetTagsHolder()
	tagKey := "host"
	tagsHolder.Insert(tagKey, []byte("server1"), jsonparser.String)
	tagsHolder.Insert(tagKey, []byte("server2"), jsonparser.String)
	tsid, err := tagsHolder.GetTSID([]byte("metric1"))
	assert.NoError(t, err)

	err = treeHolder.AddTagsForTSID([]byte("metric1"), tagsHolder, tsid)
	assert.NoError(t, err)

	// Flush to disk
	err = treeHolder.EncodeTagsTreeHolder()
	assert.NoError(t, err)

	// Read from disk
	baseDir := treewriter.GetFinalTagsTreeDir("test_mid", 0)
	reader, err := treereader.InitAllTagsTreeReader(baseDir)
	assert.NoError(t, err)
	defer reader.CloseAllTagTreeReaders()

	tagPairs, err := reader.GetAllTagPairs()
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tagPairs))
	values, ok := tagPairs[tagKey]
	assert.True(t, ok)
	assert.Equal(t, 2, len(values))

	_, ok = values["server1"]
	assert.True(t, ok)
	_, ok = values["server2"]
	assert.True(t, ok)
}

func Test_ConcurrentReadWrite(t *testing.T) {
	initTestConfig(t)

	// Create writer
	treeHolder, err := treewriter.InitTagsTreeHolder("test_mid")
	assert.NoError(t, err)

	tagsHolder := treewriter.GetTagsHolder()
	tagKey := "host"
	tagsHolder.Insert(tagKey, []byte("server1"), jsonparser.String)

	firstFlushChan := make(chan struct{})
	waitGroup := sync.WaitGroup{}
	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		for i := 0; i < 100; i++ {
			metric := []byte(fmt.Sprintf("metric%d", i))
			tsid, err := tagsHolder.GetTSID(metric)
			assert.NoError(t, err)

			err = treeHolder.AddTagsForTSID(metric, tagsHolder, tsid)
			assert.NoError(t, err)

			// Flush to disk
			err = treeHolder.EncodeTagsTreeHolder()
			assert.NoError(t, err)
			if i == 0 {
				firstFlushChan <- struct{}{}
			}
		}
	}()

	baseDir := treewriter.GetFinalTagsTreeDir("test_mid", 0)
	uniqueNumMetrics := make(map[int]struct{})

	waitGroup.Add(1)
	go func() {
		defer waitGroup.Done()

		// Wait for first flush
		<-firstFlushChan

		numMetrics := 0
		for i := 0; i < 100; i++ {
			// Read from disk
			reader, err := treereader.InitAllTagsTreeReader(baseDir)
			assert.NoError(t, err)

			tagPairs, err := reader.GetAllTagPairs()
			assert.NoError(t, err)
			assert.Len(t, tagPairs, 1)
			values, ok := tagPairs[tagKey]
			assert.True(t, ok)
			assert.Len(t, values, 1)

			metrics, err := reader.GetHashedMetricNames()
			assert.NoError(t, err)
			assert.True(t, len(metrics) >= numMetrics)
			numMetrics = len(metrics)

			uniqueNumMetrics[len(metrics)] = struct{}{}

			reader.CloseAllTagTreeReaders()
		}
	}()
	waitGroup.Wait()

	if len(uniqueNumMetrics) < 5 { // Somewhat arbitrary number
		// The purpose of this unit test is to test concurrent read/write, and
		// we expect a lot of interleaving of those operations. If we got here,
		// the test doesn't sufficiently test concurrency, so fail the test. We
		// may need to increase the number of iterations.
		t.Errorf("Insufficient testing; only got %d unique number of metrics", len(uniqueNumMetrics))
		t.FailNow()
	}

	reader, err := treereader.InitAllTagsTreeReader(baseDir)
	assert.NoError(t, err)
	defer reader.CloseAllTagTreeReaders()

	metrics, err := reader.GetHashedMetricNames()
	assert.NoError(t, err)
	assert.Len(t, metrics, 100)
}
