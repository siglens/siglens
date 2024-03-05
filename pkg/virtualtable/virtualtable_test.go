/*
Copyright 2023.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package virtualtable

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func Test_AddGetVTables(t *testing.T) {

	config.InitializeDefaultConfig()
	_ = InitVTable()

	VTableBaseDir = "vtabbase/"
	VTableMappingsDir = "vtabbase/mappings/"
	VTableTemplatesDir = "vtabbase/templates/"
	VTableAliasesDir = "vtabbase/aliases/"
	// special test code only to override the default paths and have idempotent tests
	_ = CreateVirtTableBaseDirs(VTableBaseDir, VTableMappingsDir, VTableTemplatesDir, VTableAliasesDir)

	mapdata := ""
	idx1 := "idx-blah1"
	_ = AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	idx1 = "idx-blah2"
	_ = AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	idx1 = "idx-blah3"
	_ = AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	idx1 = "idx-blah4"
	_ = AddVirtualTableAndMapping(&idx1, &mapdata, 0)

	result, _ := GetVirtualTableNames(0)

	expected := map[string]bool{
		"idx-blah1": true,
		"idx-blah2": true,
		"idx-blah3": true,
		"idx-blah4": true}
	//	t.Logf("result=%v", result)

	assert.EqualValues(t, expected, result, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", expected, result))

	// special test code only to override the default paths and have idempotent tests
	os.RemoveAll(VTableBaseDir)
	os.RemoveAll("data/")

}

func Test_AddAliases(t *testing.T) {

	_ = InitVTable()
	// special test code only to override the default paths and have idempotent tests

	VTableBaseDir = "vtabbase/"
	VTableMappingsDir = "vtabbase/mappings/"
	VTableTemplatesDir = "vtabbase/templates/"
	VTableAliasesDir = "vtabbase/aliases/"
	// special test code only to override the default paths and have idempotent tests
	_ = CreateVirtTableBaseDirs(VTableBaseDir, VTableMappingsDir, VTableTemplatesDir, VTableAliasesDir)

	expected := map[string]bool{}
	expected["myalias1"] = true
	expected["myalias2"] = true
	expected["myalias3"] = true

	idxname := "myidx1"

	_ = AddAliases(idxname, []string{"myalias1"}, 0)
	_ = AddAliases(idxname, []string{"myalias2"}, 0)
	_ = AddAliases(idxname, []string{"myalias3"}, 0)

	actual, _ := GetAliases(idxname, 0)

	assert.EqualValues(t, expected, actual, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", expected, actual))

	// special test code only to override the default paths and have idempotent tests
	os.RemoveAll(VTableBaseDir)
	//os.RemoveAll(vTableBaseDir)
}

func Test_GetIndexNameFromAlias(t *testing.T) {

	_ = InitVTable()
	os.RemoveAll(VTableBaseDir)

	// special test code only to ove\rride the default paths and have idempotent tests

	VTableBaseDir = "vtabbase/"
	VTableMappingsDir = "vtabbase/mappings/"
	VTableTemplatesDir = "vtabbase/templates/"
	VTableAliasesDir = "vtabbase/aliases/"
	// special test code only to override the default paths and have idempotent tests
	_ = CreateVirtTableBaseDirs(VTableBaseDir, VTableMappingsDir, VTableTemplatesDir, VTableAliasesDir)

	idxname := "myidx1"

	_ = AddAliases(idxname, []string{"myalias1"}, 0)
	_ = AddAliases(idxname, []string{"myalias2"}, 0)
	_ = AddAliases(idxname, []string{"myalias3"}, 0)

	actual, err := GetIndexNameFromAlias("myalias2", 0)

	assert.Nil(t, err)

	t.Logf("GetIndexNameFromAlias: idxName=%v", actual)
	assert.EqualValues(t, idxname, actual)

	// special test code only to override the default paths and have idempotent tests
	os.RemoveAll(VTableBaseDir)
}

func Test_AddRemoveAlias(t *testing.T) {

	_ = InitVTable()
	os.RemoveAll(VTableBaseDir)

	// special test code only to ove\rride the default paths and have idempotent tests

	VTableBaseDir = "vtabbase/"
	VTableMappingsDir = "vtabbase/mappings/"
	VTableTemplatesDir = "vtabbase/templates/"
	VTableAliasesDir = "vtabbase/aliases/"
	// special test code only to override the default paths and have idempotent tests
	_ = CreateVirtTableBaseDirs(VTableBaseDir, VTableMappingsDir, VTableTemplatesDir, VTableAliasesDir)

	idxname := "myidx1"

	_ = AddAliases(idxname, []string{"myalias1"}, 0)

	actual, err := GetIndexNameFromAlias("myalias1", 0)

	//	t.Logf("GetIndexNameFromAlias: idxName=%v", actual)

	assert.Nil(t, err)
	assert.EqualValues(t, idxname, actual)

	_ = RemoveAliases(idxname, []string{"myalias1"}, 0)
	assert.Nil(t, err)

	actual, err = GetIndexNameFromAlias(idxname, 0)
	assert.NotNil(t, err)
	assert.Equal(t, "", actual)

	// special test code only to override the default paths and have idempotent tests
	os.RemoveAll(VTableBaseDir)

}

func Test_DeleteVirtualTable(t *testing.T) {

	indexName := "valtix2"

	_ = InitVTable()

	var check string
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "ingestnodes/" + config.GetHostID() + "/vtabledata")
	fname := sb.String()
	_ = os.MkdirAll(fname, 0764)
	_, errC := os.Create(fname + VIRTUAL_TAB_FILENAME)
	assert.Nil(t, errC)
	check += "valtix1" + "\n" + "valtix3" + "\n"
	check += indexName
	t.Logf("VirtualTableNames before deletion = %s", check)
	err := os.WriteFile(fname+VIRTUAL_TAB_FILENAME+VIRTUAL_TAB_FILE_EXT, []byte(check), 0644)
	assert.Nil(t, err)
	t.Logf("VirtualTableName for deletion = %s", indexName)

	err = DeleteVirtualTable(&indexName, 0)
	assert.Nil(t, err)
	fd, _ := os.OpenFile(fname+VIRTUAL_TAB_FILENAME+VIRTUAL_TAB_FILE_EXT, os.O_APPEND|os.O_RDONLY, 0644)
	scanner := bufio.NewScanner(fd)

	var flag bool
	flag = true
	for scanner.Scan() {
		t.Logf("VirtualTableNames left after deletion = %s ", string(scanner.Bytes()))
		flag = strings.Contains(string(scanner.Bytes()), indexName)
		assert.Equal(t, flag, false)
	}
	assert.Equal(t, flag, false)
	os.RemoveAll(fname)
	os.RemoveAll(config.GetRunningConfig().DataPath)

}

func Test_ExpandAndReturnIndexNames(t *testing.T) {

	indexPattern := "idx-blah1"
	indicesEntries := ExpandAndReturnIndexNames(indexPattern, 0, false)
	indicesExpected := "idx-blah1"
	assert.Equal(t, indicesExpected, indicesEntries[0])

	indexPattern = "traces"
	indicesEntries = ExpandAndReturnIndexNames(indexPattern, 0, false)
	indicesExpected = "traces"
	assert.Equal(t, indicesExpected, indicesEntries[0])

	indexPattern = "service-dependency"
	indicesEntries = ExpandAndReturnIndexNames(indexPattern, 0, false)
	indicesExpected = "service-dependency"
	assert.Equal(t, indicesExpected, indicesEntries[0])

	indexPattern = "red-traces"
	indicesEntries = ExpandAndReturnIndexNames(indexPattern, 0, false)
	indicesExpected = "red-traces"
	assert.Equal(t, indicesExpected, indicesEntries[0])

	// special test code only to override the default paths and have idempotent tests
	os.RemoveAll(config.GetRunningConfig().DataPath)
	os.RemoveAll(VTableBaseDir)
}
