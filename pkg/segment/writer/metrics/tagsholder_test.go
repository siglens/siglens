package metrics

import (
	"fmt"
	"sync"
	"testing"

	jp "github.com/buger/jsonparser"
	"github.com/stretchr/testify/assert"
)

func TestTagsHolderConcurrency(t *testing.T) {
	var wg sync.WaitGroup
	numGoroutines := 100
	numInsertsPerGoroutine := 1000

	insertTags := func(holder *TagsHolder, wg *sync.WaitGroup) {
		defer wg.Done()
		for i := 0; i < numInsertsPerGoroutine; i++ {
			key := "key" + fmt.Sprint(i)
			value := []byte("value" + fmt.Sprint(i))
			vType := jp.String
			holder.Insert(key, value, vType)
		}

		holder.finish()

		assert.Equal(t, numInsertsPerGoroutine, holder.idx, "The number of inserted tags should match the expected count")
	}

	wg.Add(numGoroutines)
	for i := 0; i < numGoroutines; i++ {
		holder := GetTagsHolder()
		go insertTags(holder, &wg)
	}

	wg.Wait()
}
