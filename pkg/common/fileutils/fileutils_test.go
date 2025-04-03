package fileutils

import (
	"os"
	"path/filepath"
	"strings"
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

func Test_GetAllFilesInDirectory(t *testing.T) {
	dir := t.TempDir()
	paths := []string{
		filepath.Join(dir, "abc.txt"),
		filepath.Join(dir, "def.txt"),
		filepath.Join(dir, "dir1/abc.txt"),
		filepath.Join(dir, "dir2/def.txt"),
		filepath.Join(dir, "dir1/dir3/abc.txt"),
	}

	for _, path := range paths {
		forceCreate(t, path)
	}

	allFiles := GetAllFilesInDirectory(dir)
	assert.ElementsMatch(t, paths, allFiles)
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

func Test_GetDirSize_empty(t *testing.T) {
	tmpDir := t.TempDir()
	size, err := GetDirSize(tmpDir)
	assert.NoError(t, err)
	assert.Equal(t, int64(0), int64(size))
}

func Test_GetDirSize_nonExistent(t *testing.T) {
	size, err := GetDirSize("non-existent")
	assert.Error(t, err)
	assert.Equal(t, int64(0), int64(size))
}

func Test_GetDirSize(t *testing.T) {
	tmpDir := t.TempDir()
	file1Size := 100
	file2Size := 200
	file3Size := 300
	expectedSize := int64(file1Size + file2Size + file3Size)

	rootFile := filepath.Join(tmpDir, "root.txt")
	assert.NoError(t, os.WriteFile(rootFile, []byte(strings.Repeat("a", file1Size)), 0644))

	nestedDir := filepath.Join(tmpDir, "nested")
	assert.NoError(t, os.Mkdir(nestedDir, 0755))

	file1 := filepath.Join(nestedDir, "file1.txt")
	file2 := filepath.Join(nestedDir, "file2.txt")
	assert.NoError(t, os.WriteFile(file1, []byte(strings.Repeat("b", file2Size)), 0644))
	assert.NoError(t, os.WriteFile(file2, []byte(strings.Repeat("c", file3Size)), 0644))

	size, err := GetDirSize(tmpDir)
	assert.NoError(t, err)
	assert.Equal(t, expectedSize, int64(size))
}
