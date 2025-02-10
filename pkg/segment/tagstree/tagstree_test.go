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
	go func() {
		waitGroup.Add(1)
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

	// Wait for first flush
	<-firstFlushChan

	// Read from disk
	baseDir := treewriter.GetFinalTagsTreeDir("test_mid", 0)

	go func() {
		waitGroup.Add(1)
		defer waitGroup.Done()

		numMetrics := 0
		for i := 0; i < 100; i++ {
			reader, err := treereader.InitAllTagsTreeReader(baseDir)
			assert.NoError(t, err)

			tagPairs, err := reader.GetAllTagPairs()
			assert.NoError(t, err)
			assert.Equal(t, 1, len(tagPairs))
			values, ok := tagPairs[tagKey]
			assert.True(t, ok)
			assert.Equal(t, len(values), 1)

			metrics, err := reader.GetHashedMetricNames()
			assert.NoError(t, err)
			assert.True(t, len(metrics) >= numMetrics)
			numMetrics = len(metrics)

			reader.CloseAllTagTreeReaders()
		}
	}()
	waitGroup.Wait()

	reader, err := treereader.InitAllTagsTreeReader(baseDir)
	assert.NoError(t, err)
	defer reader.CloseAllTagTreeReaders()

	metrics, err := reader.GetHashedMetricNames()
	assert.NoError(t, err)
	assert.Equal(t, 100, len(metrics))
}
