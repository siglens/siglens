package fileutils

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_GetAllFilesWithSameNameInDirectory(t *testing.T) {
	temp := t.TempDir()
	_, err := os.Create(temp + "/abc.txt")
	assert.Nil(t, err)
	err = os.Mkdir(temp+"/dir1", 0755)
	assert.Nil(t, err)
	err = os.Mkdir(temp+"/dir2", 0755)
	assert.Nil(t, err)
	err = os.Mkdir(temp+"/dir1/dir3", 0755)
	assert.Nil(t, err)
	err = os.Mkdir(temp+"/dir2/dir4", 0755)
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir1/abc.txt")
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir2/def.txt")
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir1/dir3/abc.txt")
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir2/dir4/abc.txt")
	assert.Nil(t, err)

	// Directory Structure
	// - temp
	//   - abc.txt
	// 	 - dir1
	// 	   - abc.txt
	// 	   - dir3
	// 	     - abc.txt
	// 	 - dir2
	// 	   - def.txt
	// 	   - dir4
	// 	     - abc.txt

	retVal := GetAllFilesWithSameNameInDirectory(temp, "abc.txt")
	expectedPaths := []string{
		temp + "/abc.txt",
		temp + "/dir1/abc.txt",
		temp + "/dir1/dir3/abc.txt",
		temp + "/dir2/dir4/abc.txt",
	}
	assert.Equal(t, 4, len(retVal))
	assert.ElementsMatch(t, expectedPaths, retVal)
}
