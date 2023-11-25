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

package reader

import (
	"fmt"
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	esutils "github.com/siglens/siglens/pkg/es/utils"
	vtable "github.com/siglens/siglens/pkg/virtualtable"
	"github.com/stretchr/testify/assert"
)

func Test_ExpandAndReturnIndexNames(t *testing.T) {
	config.InitializeDefaultConfig()
	_ = vtable.InitVTable()

	vTableBaseDir := "vtabbase/"
	vTableMappingsDir := "vtabbase/mappings/"
	vTableTemplatesDir := "vtabbase/templates/"
	vTableAliasesDir := "vtabbase/aliases/"
	// special test code only to override the default paths and have idempotent tests
	_ = vtable.CreateVirtTableBaseDirs(vTableBaseDir, vTableMappingsDir, vTableTemplatesDir, vTableAliasesDir)

	mapdata := ""
	idx1 := "idx-blah1"
	_ = vtable.AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	idx1 = "idx-blah2"
	_ = vtable.AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	subStringIndex := "blah3-idx"
	_ = vtable.AddVirtualTableAndMapping(&subStringIndex, &mapdata, 0)
	idx1 = "traces"
	_ = vtable.AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	idx1 = "red-traces"
	_ = vtable.AddVirtualTableAndMapping(&idx1, &mapdata, 0)
	idx1 = "service-dependency"
	_ = vtable.AddVirtualTableAndMapping(&idx1, &mapdata, 0)

	allVirtualTableNames, _ := vtable.GetVirtualTableNames(0)
	expected := map[string]bool{
		"idx-blah1":          true,
		"idx-blah2":          true,
		"blah3-idx":          true,
		"traces":             true,
		"red-traces":         true,
		"service-dependency": true}

	assert.EqualValues(t, expected, allVirtualTableNames, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", expected, allVirtualTableNames))

	indexPattern := "idx*"

	indicesEntries, aliasesEntries, err := ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected := []esutils.ResolveIndexEntry{
		esutils.ResolveIndexEntry{
			Name:       "idx-blah1",
			Attributes: []string{"open"},
			Aliases:    []string{}},
		esutils.ResolveIndexEntry{
			Name:       "idx-blah2",
			Attributes: []string{"open"},
			Aliases:    []string{}}}

	aliasesExpected := []esutils.ResolveAliasEntry{}

	for i := range indicesExpected {
		assert.Contains(t, indicesEntries, indicesExpected[i])
	}
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "a*"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{}
	aliasesExpected = []esutils.ResolveAliasEntry{}

	assert.EqualValues(t, indicesExpected, indicesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", indicesExpected, indicesEntries))
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "idx-blah1"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{
		esutils.ResolveIndexEntry{
			Name:       "idx-blah1",
			Attributes: []string{"open"},
			Aliases:    []string{}}}

	aliasesExpected = []esutils.ResolveAliasEntry{}

	assert.EqualValues(t, indicesExpected, indicesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", indicesExpected, indicesEntries))
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "*idx*"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{
		{
			Name:       "idx-blah1",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
		{
			Name:       "idx-blah2",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
		{
			Name:       "blah3-idx",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
	}
	for i := range indicesExpected {
		assert.Contains(t, indicesEntries, indicesExpected[i])
	}
	assert.Len(t, indicesEntries, len(indicesExpected))
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "bl*"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{
		{
			Name:       "blah3-idx",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
	}
	assert.EqualValues(t, indicesExpected, indicesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", indicesExpected, indicesEntries))
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "*"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{
		{
			Name:       "idx-blah1",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
		{
			Name:       "idx-blah2",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
		{
			Name:       "blah3-idx",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
	}
	assert.EqualValues(t, len(indicesExpected), len(indicesEntries), fmt.Sprintf("Comparison failed, expected=%v, actual=%v", indicesExpected, indicesEntries))
	assert.EqualValues(t, len(aliasesExpected), len(aliasesEntries), fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "red-traces"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{
		{
			Name:       "red-traces",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
	}
	assert.EqualValues(t, indicesExpected, indicesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", indicesExpected, indicesEntries))
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	indexPattern = "service-dependency"

	indicesEntries, aliasesEntries, err = ExpandAndReturnIndexNames(indexPattern, allVirtualTableNames, 0)
	assert.Nil(t, err)

	indicesExpected = []esutils.ResolveIndexEntry{
		{
			Name:       "service-dependency",
			Attributes: []string{"open"},
			Aliases:    []string{},
		},
	}
	assert.EqualValues(t, indicesExpected, indicesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", indicesExpected, indicesEntries))
	assert.EqualValues(t, aliasesExpected, aliasesEntries, fmt.Sprintf("Comparison failed, expected=%v, actual=%v", aliasesExpected, aliasesEntries))

	// special test code only to override the default paths and have idempotent tests
	os.RemoveAll(config.GetRunningConfig().DataPath)
	os.RemoveAll(vTableBaseDir)
}
