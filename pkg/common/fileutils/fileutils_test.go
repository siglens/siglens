package fileutils

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
)

func forceCreate(t *testing.T, path string) {
	dir := filepath.Dir(path)
	err := os.MkdirAll(dir, 0755)
	assert.Nil(t, err)
	// If it is a file, create it
	if filepath.Ext(path) != "" {
		_, err = os.Create(path)
		assert.Nil(t, err)
	}
}

func Test_GetAllFilesWithSameNameInDirectory(t *testing.T) {
	temp := t.TempDir()
	forceCreate(t, temp+"/abc.txt")
	forceCreate(t, temp+"/dir1/abc.txt")
	forceCreate(t, temp+"/dir2/def.txt")
	forceCreate(t, temp+"/dir1/dir3/abc.txt")
	forceCreate(t, temp+"/dir2/dir4/abc.txt")

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
	forceCreate(t, temp+"/abc.xyz")
	forceCreate(t, temp+"/dir1/abc.txt")
	forceCreate(t, temp+"/dir1/abc.mno")
	forceCreate(t, temp+"/dir2/def.xyz")
	forceCreate(t, temp+"/dir1/dir3/abc.abc")
	forceCreate(t, temp+"/dir1/dir3/abc")
	forceCreate(t, temp+"/dir2/dir4/abc.ghi")

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
