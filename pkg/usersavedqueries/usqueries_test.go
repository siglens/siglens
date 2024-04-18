// Copyright (c) 2021-2024 SigScalr, Inc.
//
// This file is part of SigLens Observability Solution
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package usersavedqueries

import (
	"os"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func Test_WriteAndGetUsq(t *testing.T) {

	config.InitializeDefaultConfig()

	_ = InitUsq()

	expected := make(map[string]interface{})

	expected["searchText"] = "abcsearch"
	expected["indexName"] = "myidx2"
	expected["description"] = "blah blah"

	usname := "myname2"
	err := writeUsq(usname, expected, 0)
	assert.Nil(t, err)

	found, actual, err := getUsqOne(usname, 0)
	assert.Nil(t, err)
	assert.True(t, found)

	assert.EqualValues(t, expected, actual[usname], "Comparison failed, expected=%v, actual=%v", expected, actual)

	_ = writeUsq("usname1", expected, 0)
	_ = writeUsq("usname2", expected, 0)
	_ = writeUsq("usname3", expected, 0)
	_ = writeUsq("usname4", expected, 0)
	_ = writeUsq("usname5", expected, 0)
	_ = writeUsq("usname6", expected, 0)
	_ = writeUsq("usname7", expected, 0)

	found, allActuals, err := getUsqAll(0)
	assert.Nil(t, err)
	assert.True(t, found)

	names := []string{"usname1", "usname2", "usname3", "usname4", "usname5", "usname6", "usname7"}

	for _, n1 := range names {
		adata, found := allActuals[n1]
		assert.True(t, found, "did not find usname", n1)
		assert.EqualValues(t, expected, adata, "Comparison failed, expected=%v, actual=%v", expected, adata)
	}

	_, err = deleteUsq("usname7", 0)
	assert.Nil(t, err)

	_, allActualsaafterdel, err := getUsqAll(0)
	assert.Nil(t, err)
	_, ok := allActualsaafterdel["usname7"]

	assert.False(t, ok, "usname7 was supposed to be deleted, res=", allActualsaafterdel)

	os.RemoveAll("data/")
}

func Test_WriteAndSaveMultipleOrgsUsq(t *testing.T) {
	config.InitializeDefaultConfig()
	_ = InitUsq()

	expected := make(map[string]interface{})

	expected["searchText"] = "abcsearch"
	expected["indexName"] = "myidx2"
	expected["description"] = "blah blah"

	usname := "mynameorg1_1"
	err := writeUsq(usname, expected, 0)
	assert.Nil(t, err)

	_ = writeUsq("mynameorg1_2", expected, 0)
	_ = writeUsq("mynameorg1_3", expected, 0)
	_ = writeUsq("mynameorg1_4", expected, 0)

	usname = "mynameorg2_1"
	err = writeUsq(usname, expected, 1)
	assert.Nil(t, err)

	_ = writeUsq("mynameorg2_2", expected, 1)
	_ = writeUsq("mynameorg2_3", expected, 1)
	_ = writeUsq("mynameorg2_4", expected, 1)

	// test two files are created
	path := usqBaseFilename + ".bin"
	_, err = os.ReadFile(path)
	assert.Nil(t, err)

	path = usqBaseFilename + "-1.bin"
	_, err = os.ReadFile(path)
	assert.Nil(t, err)

	found, allActuals, err := getUsqAll(0)
	assert.Nil(t, err)
	assert.True(t, found)

	names := []string{"mynameorg1_1", "mynameorg1_2", "mynameorg1_3", "mynameorg1_4"}
	for _, n1 := range names {
		adata, found := allActuals[n1]
		assert.True(t, found, "did not find usname", n1)
		assert.EqualValues(t, expected, adata, "Comparison failed, expected=%v, actual=%v", expected, adata)
	}

	found, allActuals, err = getUsqAll(1)
	assert.Nil(t, err)
	assert.True(t, found)

	names = []string{"mynameorg2_1", "mynameorg2_2", "mynameorg2_3", "mynameorg2_4"}
	for _, n1 := range names {
		adata, found := allActuals[n1]
		assert.True(t, found, "did not find usname", n1)
		assert.EqualValues(t, expected, adata, "Comparison failed, expected=%v, actual=%v", expected, adata)
	}

	_, err = deleteUsq("mynameorg2_2", 1)
	assert.Nil(t, err)

	_, allActualsaafterdel, err := getUsqAll(1)
	assert.Nil(t, err)
	_, ok := allActualsaafterdel["mynameorg2_2"]

	assert.False(t, ok, "mynameorg2_2 was supposed to be deleted, res=", allActualsaafterdel)

	os.RemoveAll("data/")
}
