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

func Test_GetAllFilesWithSpecificExtensions(t *testing.T) {
	temp := t.TempDir()
	_, err := os.Create(temp + "/abc.xyz")
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
	_, err = os.Create(temp + "/dir1/abc.mno")
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir2/def.xyz")
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir1/dir3/abc.abc")
	assert.Nil(t, err)
	err = os.Mkdir(temp+"/dir1/dir3/abc", 0755)
	assert.Nil(t, err)
	_, err = os.Create(temp + "/dir2/dir4/abc.ghi")
	assert.Nil(t, err)

	// Directory Structure
	// - temp
	//   - abc.xyz
	// 	 - dir1
	// 	   - abc.txt
	//     - abc.mno
	// 	   - dir3
	// 	     - abc.abc
	//       - abc
	// 	 - dir2
	// 	   - def.xyz
	// 	   - dir4
	// 	     - abc.ghi

	extensions := map[string]struct{}{
		".xyz": {},
		".abc": {},
		".ghi": {},
	}

	retVal := GetAllFilesWithSpecificExtensions(temp, extensions)
	expectedPaths := []string{
		temp + "/abc.xyz",
		temp + "/dir1/dir3/abc.abc",
		temp + "/dir2/def.xyz",
		temp + "/dir2/dir4/abc.ghi",
	}
	assert.Equal(t, 4, len(retVal))
	assert.ElementsMatch(t, expectedPaths, retVal)
}
