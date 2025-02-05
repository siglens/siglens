package tagstree

import (
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

	tagPairs := reader.GetAllTagPairs()
	assert.Equal(t, 1, len(tagPairs))
	values, ok := tagPairs[tagKey]
	assert.True(t, ok)
	assert.Equal(t, 2, len(values))

	_, ok = values["server1"]
	assert.True(t, ok)
	_, ok = values["server2"]
	assert.True(t, ok)
}
