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

package virtualtable

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

var (
	VTableBaseDir      string
	VTableMappingsDir  string
	VTableTemplatesDir string
	VTableAliasesDir   string
)

const (
	VIRTUAL_TAB_FILENAME = "/virtualtablenames"
	VIRTUAL_TAB_FILE_EXT = ".txt"
)

var vTableBaseFileName string

var (
	globalTableAccessLock   sync.RWMutex = sync.RWMutex{}
	vTableRawFileAccessLock sync.RWMutex = sync.RWMutex{}
)

var aliasToIndexNames map[int64]map[string]map[string]bool = make(map[int64]map[string]map[string]bool)

// holds all the tables for orgid -> tname -> bool
var allVirtualTables map[int64]map[string]bool

var excludedInternalIndices = [...]string{"traces", "red-traces", "service-dependency"}

func InitVTable(fnMyIds func() []int64) error {
	allVirtualTables = make(map[int64]map[string]bool)
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "ingestnodes/" + config.GetHostID() + "/vtabledata")
	VTableBaseDir = sb.String()
	VTableMappingsDir = VTableBaseDir + "/mappings/"
	VTableTemplatesDir = VTableBaseDir + "/templates/"
	VTableAliasesDir = VTableBaseDir + "/aliases/"

	var sb1 strings.Builder
	sb1.WriteString(VTableBaseDir)
	sb1.WriteString(VIRTUAL_TAB_FILENAME)

	vTableBaseFileName = sb1.String()

	err := CreateVirtTableBaseDirs(VTableBaseDir, VTableMappingsDir, VTableTemplatesDir, VTableAliasesDir)
	if err != nil {
		return err
	}
	err = initializeAliasToIndexMap()
	if err != nil {
		log.Errorf("InitVTable: Failed to initialize alias to index map, err=%v", err)
		return err
	}

	refreshInMemoryTableForAllIds(fnMyIds)
	go refreshInMemoryTable(fnMyIds)
	return nil
}

func getVirtualTableFileName(orgid int64) string {
	var vTableFileName string
	if orgid == 0 {
		vTableFileName = vTableBaseFileName + VIRTUAL_TAB_FILE_EXT
	} else {
		vTableFileName = vTableBaseFileName + "-" + strconv.FormatInt(orgid, 10) + VIRTUAL_TAB_FILE_EXT
	}
	return vTableFileName
}

func refreshInMemoryTableForAllIds(fnMyIds func() []int64) {
	myids := fnMyIds()

	globalTableAccessLock.Lock()
	defer globalTableAccessLock.Unlock()

	wg := sync.WaitGroup{}

	for _, myid := range myids {
		vTableMap := make(map[string]bool)
		allVirtualTables[myid] = vTableMap

		wg.Add(1)
		go func(myid int64, vTableMap map[string]bool) {
			defer wg.Done()

			err := getVirtualTableNamesInternal(myid, vTableMap)
			if err != nil {
				log.Errorf("refreshInMemoryTableForAllIds: Failed to get virtual table names! err=%v", err)
			}
		}(myid, vTableMap)
	}

	wg.Wait()
}

func refreshInMemoryTable(fnMyIds func() []int64) {
	for {
		refreshInMemoryTableForAllIds(fnMyIds)
		time.Sleep(1 * time.Minute)
	}
}

func GetFilePathForRemoteNode(node string, orgid int64) string {
	var vFile strings.Builder
	vFile.WriteString(config.GetDataPath() + "ingestnodes/" + node + "/vtabledata")
	vFile.WriteString(VIRTUAL_TAB_FILENAME)
	if orgid != 0 {
		vFile.WriteString("-" + strconv.FormatInt(orgid, 10) + VIRTUAL_TAB_FILE_EXT)
	} else {
		vFile.WriteString(VIRTUAL_TAB_FILE_EXT)
	}
	vfname := vFile.String()
	return vfname
}

func CreateVirtTableBaseDirs(vTableBaseDir string, vTableMappingsDir string,
	vTableTemplatesDir string, vTableAliasesDir string,
) error {
	err := os.MkdirAll(vTableBaseDir, 0764)
	if err != nil {
		log.Errorf("createVirtTableBaseDir: failed to create vTableBaseDir=%v, err=%v", vTableBaseDir, err)
		return err
	}

	err = os.MkdirAll(vTableMappingsDir, 0764)
	if err != nil {
		log.Errorf("createVirtTableBaseDir: failed to create vTableMappingsDir=%v, err=%v", vTableMappingsDir, err)
		return err
	}

	err = os.MkdirAll(vTableTemplatesDir, 0764)
	if err != nil {
		log.Errorf("createVirtTableBaseDir: failed to create vTableTemplatesDir=%v, err=%v", vTableTemplatesDir, err)
		return err
	}

	err = os.MkdirAll(vTableAliasesDir, 0764)
	if err != nil {
		log.Errorf("createVirtTableBaseDir: failed to create vTableAliasesDir=%v, err=%v", vTableAliasesDir, err)
		return err
	}

	return nil
}

// addVirtualTableHelper adds the given virtual table to the global virtual table map and writes it to the file
// It returns true if a virtual table was added to the file, false otherwise
func addVirtualTableHelper(vTableMap map[string]struct{}, orgid int64) (bool, error) {
	vTablesToAppend := make(map[string]struct{})

	globalTableAccessLock.Lock()
	orgVTableMap, exists := allVirtualTables[orgid]
	if !exists {
		orgVTableMap = make(map[string]bool)
		allVirtualTables[orgid] = orgVTableMap
	}

	for tname := range vTableMap {
		if _, exists := orgVTableMap[tname]; !exists {
			vTablesToAppend[tname] = struct{}{}
			orgVTableMap[tname] = true
		}
	}
	globalTableAccessLock.Unlock()

	if len(vTablesToAppend) == 0 {
		return false, nil
	}

	vTableFileName := getVirtualTableFileName(orgid)
	fd, err := os.OpenFile(vTableFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("AddVirtualTable: Failed to open virtual table file=%v, err=%v", vTableFileName, err)
		return false, err
	}
	defer fd.Close()

	for tname := range vTablesToAppend {
		if _, err := fd.WriteString(tname); err != nil {
			log.Errorf("AddVirtualTable: Failed to write virtual tablename=%v, in file=%v, err=%v", tname, vTableFileName, err)

			return false, err
		}
		if _, err := fd.WriteString("\n"); err != nil {
			log.Errorf("AddVirtualTable: Failed to write \n to virtual tablename=%v, in file=%v, err=%v", tname, vTableFileName, err)
			return false, err
		}
	}

	if err = fd.Sync(); err != nil {
		log.Errorf("AddVirtualTable: Failed to sync virtual table file=%v, err=%v", vTableFileName, err)
		return false, err
	}

	return true, nil
}

func AddVirtualTable(tname *string, orgid int64) error {
	vTableMap := make(map[string]struct{})
	vTableMap[*tname] = struct{}{}

	vTableRawFileAccessLock.Lock()
	vTableFileUpdated, err := addVirtualTableHelper(vTableMap, orgid)
	vTableRawFileAccessLock.Unlock()
	if err != nil {
		log.Errorf("AddVirtualTable: Error in adding virtual table=%v to the file!. Err: %v", tname, err)
		return err
	}

	if vTableFileUpdated {
		go func() {
			err := blob.UploadIngestNodeDir()
			if err != nil {
				log.Errorf("AddVirtualTable: Failed to upload vtable data to blob store, err=%v", err)
			}
		}()
	}

	return nil
}

func BulkAddVirtualTableNames(myIdToVTableMap map[int64]map[string]struct{}) error {
	vTableRawFileAccessLock.Lock()
	defer vTableRawFileAccessLock.Unlock()

	wg := sync.WaitGroup{}

	for myid, vTableNamesMap := range myIdToVTableMap {
		wg.Add(1)
		go func(id int64, vTableMap map[string]struct{}) {
			defer wg.Done()

			_, err := addVirtualTableHelper(vTableMap, id)
			if err != nil {
				log.Errorf("BulkAddVirtualTableNames: Error in adding virtual table names for myid=%d, err:%v", id, err)
			}
		}(myid, vTableNamesMap)
	}

	wg.Wait()

	return nil
}

func IsVirtualTablePresent(tname *string, orgid int64) bool {
	vtables, err := GetVirtualTableNames(orgid)
	if err != nil {
		log.Errorf("Could not get virtual tables for orgid %v. Err: %v", orgid, err)
		return false
	}
	for vtable := range vtables {
		if vtable == *tname {
			return true
		}
	}
	return false
}

func AddVirtualTableAndMapping(tname *string, mapping *string, orgid int64) error {
	// todo for dupe entries, write a goroutine that wakes up once per day (random time) and reads the
	// central place of virtualtablenames.txt and de-dupes the table names by creating a lock

	err := AddVirtualTable(tname, orgid)
	if err != nil {
		return err
	}

	return AddMapping(tname, mapping, orgid)
}

func AddMapping(tname *string, mapping *string, orgid int64) error {
	var sb1 strings.Builder
	sb1.WriteString(VTableMappingsDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatInt(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(*tname)
	sb1.WriteString(".json")

	fname := sb1.String()

	fd, err := os.OpenFile(fname, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("AddMapping: Failed to open mappings file tablename=%v, in file=%v, err=%v", *tname, fname, err)
		return err
	}
	if _, err := fd.WriteString(*mapping); err != nil {
		log.Errorf("AddMapping: Failed to write mappings file tablename=%v, in file=%v, err=%v", *tname, fname, err)
		return err
	}
	if err = fd.Sync(); err != nil {
		log.Errorf("AddMapping: Failed to sync mappings file tablename=%v, in file=%v, err=%v", *tname, fname, err)
		return err
	}
	fd.Close()
	return nil
}

func GetVirtualTableNames(orgid int64) (map[string]bool, error) {
	vTableMap := make(map[string]bool)
	err := getVirtualTableNamesInternal(orgid, vTableMap)
	if err != nil {
		return nil, utils.TeeErrorf("GetVirtualTableNames: Error in getting virtual table names for orgid=%v, err=%v", orgid, err)
	}
	return vTableMap, nil
}

func getVirtualTableNamesInternal(orgid int64, vTableMap map[string]bool) error {
	vTableFileName := getVirtualTableFileName(orgid)
	err := LoadVirtualTableNamesFromFile(vTableFileName, vTableMap)
	if err != nil {
		return fmt.Errorf("getVirtualTableNamesInternal: Error in loading virtual table names for orgid=%v, err=%v", orgid, err)
	}
	return nil
}

func LoadVirtualTableNamesFromFile(fileName string, vTableMap map[string]bool) error {
	if vTableMap == nil {
		return fmt.Errorf("GetVirtualTableNamesHelper: vTableMap is nil")
	}
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		log.Errorf("GetVirtualTableNames: Failed to open file=%v, err=%v", fileName, err)
		return err
	}

	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		vTableMap[string(rawbytes)] = true
	}
	if err := scanner.Err(); err != nil {
		return fmt.Errorf("getVirtualTableNamesHelper: Error scanning file %v, err: %v", fileName, err)
	}
	return nil
}

func AddAliases(indexName string, aliases []string, orgid int64) error {
	if indexName == "" {
		log.Errorf("AddAliases: indexName is null. len(indexName)=%v", len(indexName))
		return errors.New("indexName is null")
	}

	alLen := len(aliases)
	if alLen == 0 {
		log.Errorf("AddAliases: len of aliases is 0. len(aliases)=%v", alLen)
		return errors.New("len of aliases is 0")
	}

	currentAliases, err := GetAliases(indexName, orgid)
	if err != nil {
		log.Errorf("AddAliases: For indexName=%v, GetAliases returned err=%v", indexName, err)
		return err
	}

	log.Infof("AddAliases: indexname=%v, existing aliases=[%v], newaliases=[%v]", indexName, currentAliases, aliases)

	for i := 0; i < alLen; i++ {
		currentAliases[aliases[i]] = true
	}

	err = writeAliasFile(&indexName, currentAliases, orgid)
	if err != nil {
		return err
	}

	for key := range currentAliases {
		putAliasToIndexInMem(key, indexName, orgid)
	}

	return nil
}

func GetAllAliasesAsMapArray(orgid int64) (map[string][]string, error) {
	retVal := make(map[string][]string)

	if _, ok := aliasToIndexNames[orgid]; ok {
		for alias, indexNames := range aliasToIndexNames[orgid] {
			allIdxNames := []string{}
			for idxName := range indexNames {
				allIdxNames = append(allIdxNames, idxName)
			}
			retVal[alias] = allIdxNames
		}
	}

	return retVal, nil
}

func GetAliasesAsArray(indexName string, orgid int64) ([]string, error) {
	retVal := []string{}

	aliasNames, err := GetAliases(indexName, orgid)
	if err != nil {
		return retVal, err
	}

	for aliasName := range aliasNames {
		retVal = append(retVal, aliasName)
	}
	return retVal, nil
}

func GetAliases(indexName string, orgid int64) (map[string]bool, error) {
	var sb1 strings.Builder
	sb1.WriteString(VTableAliasesDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatInt(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(indexName)
	sb1.WriteString(".json")

	filename := sb1.String()

	retval := map[string]bool{}
	rdata, err := os.ReadFile(filename)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return retval, nil
		}
		log.Errorf("GetAliases: Failed to readfile filename=%v, err=%v", filename, err)
		return retval, err
	}
	if len(strings.TrimSpace(string(rdata))) == 0 {
		log.Errorf("GetAliases: No data to parse in file %v. FileData Size=%v", filename, len(strings.TrimSpace(string(rdata))))
		return retval, fmt.Errorf("GetAliases: No data to parse in file %v", filename)
	}

	err = json.Unmarshal(rdata, &retval)
	if err != nil {
		log.Errorf("GetAliases: Failed to unmarshall data in filename=%v, err=%v", filename, err)
		return retval, err
	}

	return retval, nil
}

func writeAliasFile(indexName *string, allnames map[string]bool, orgid int64) error {
	var sb1 strings.Builder
	sb1.WriteString(VTableAliasesDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatInt(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(*indexName)
	sb1.WriteString(".json")

	filename := sb1.String()

	jdata, err := json.Marshal(&allnames)
	if err != nil {
		log.Errorf("writeAliasFile: Failed to marshall allnamesmap=%v, err=%v", allnames, err)
		return err
	}

	err = os.WriteFile(filename, jdata, 0644)
	if err != nil {
		log.Errorf("writeAliasFile: Failed write to the file=%v, err=%v", filename, err)
		return err
	}

	return nil
}

func initializeAliasToIndexMap() error {
	dirs, err := os.ReadDir(VTableAliasesDir)
	if err != nil {
		log.Errorf("initializeAliasToIndexMap: Failed to read directory, vTableAliasesDir=%v, err=%v", VTableAliasesDir, err)
		return err
	}

	for _, dir := range dirs {
		if dir.IsDir() {
			orgid := dir.Name()
			orgIdNumber, _ := strconv.ParseInt(orgid, 10, 64)
			files, err := os.ReadDir(VTableAliasesDir + dir.Name())
			if err != nil {
				log.Errorf("initializeAliasToIndexMap: Failed to read directory=%v, for org =%v, err=%v", VTableAliasesDir+dir.Name(), orgid, err)
				return err
			}
			for _, f := range files {
				var sb strings.Builder
				sb.WriteString(VTableAliasesDir)
				fname := f.Name()

				if strings.HasSuffix(fname, ".json") {
					indexName := strings.TrimSuffix(fname, ".json")
					aliasNames, err := GetAliases(indexName, orgIdNumber)
					if err != nil {
						log.Errorf("initializeAliasToIndexMap: For indexName=%v, Failed to getAllAliasInIndexFile fname=%v, err=%v", indexName, fname, err)
						return err
					}

					for aliasName := range aliasNames {
						putAliasToIndexInMem(aliasName, indexName, orgIdNumber)
					}
				}
			}
		}
	}
	return nil
}

func putAliasToIndexInMem(aliasName string, indexName string, orgid int64) {
	if aliasName == "" {
		log.Errorf("putAliasToIndexInMem: aliasName is empty. len(aliasName)=%v", len(aliasName))
		return
	}

	if indexName == "" {
		log.Errorf("putAliasToIndexInMem: indexName is empty. len(indexName)=%v", len(indexName))
		return
	}

	if _, ok := aliasToIndexNames[orgid]; !ok {
		aliasToIndexNames[orgid] = make(map[string]map[string]bool)
	}
	if _, pres := aliasToIndexNames[orgid][aliasName]; !pres {
		aliasToIndexNames[orgid][aliasName] = make(map[string]bool)
	}

	aliasToIndexNames[orgid][aliasName][indexName] = true
}

func FlushAliasMapToFile() error {
	log.Warnf("FlushAliasMapToFile: Flushing alias map to file on exit")
	for orgid := range aliasToIndexNames {
		for alias, indexNames := range aliasToIndexNames[orgid] {
			err := writeAliasFile(&alias, indexNames, orgid)
			if err != nil {
				log.Errorf("FlushAliasMapToFile: Failed to save alias map! alias=%v, Error= %+v", alias, err)
			}
		}
	}
	return nil
}

func GetIndexNameFromAlias(aliasName string, orgid int64) (string, error) {
	if aliasName == "" {
		log.Errorf("getIndexNameFromAlias: aliasName is empty")
		return "", errors.New("getIndexNameFromAlias: aliasName is empty")
	}

	if _, pres := aliasToIndexNames[orgid][aliasName]; pres {
		for key := range aliasToIndexNames[orgid][aliasName] {
			return key, nil
		}
	}

	return "", errors.New("not found")
}

func IsAlias(nameToCheck string, orgid int64) (bool, string) {
	if valMap, ok := aliasToIndexNames[orgid][nameToCheck]; ok {
		for indexName := range valMap {
			return true, indexName
		}
	}

	return false, ""
}

func RemoveAliases(indexName string, aliases []string, orgid int64) error {
	if indexName == "" {
		log.Errorf("RemoveAliases: indexName is null.len(indexName)=%v", len(indexName))
		return errors.New("indexName is null")
	}

	alLen := len(aliases)
	if alLen == 0 {
		log.Errorf("RemoveAliases: len of aliases was 0. len(aliases)=%v", alLen)
		return errors.New("len of aliases was 0")
	}

	currentAliases, err := GetAliases(indexName, orgid)
	if err != nil {
		log.Errorf("RemoveAliases: For indexName=%v, GetAliases returned err=%v", currentAliases, err)
		return err
	}

	log.Infof("RemoveAliases: idxname=%v, existing aliases=[%v], aliasesToRemove=[%v]", indexName, currentAliases, aliases)

	for i := 0; i < alLen; i++ {
		delete(currentAliases, aliases[i])
		delete(aliasToIndexNames[orgid][aliases[i]], indexName)
	}

	if len(currentAliases) == 0 {
		return removeAliasFile(&indexName, orgid)
	}

	err = writeAliasFile(&indexName, currentAliases, orgid)
	if err != nil {
		return err
	}
	return nil
}

func removeAliasFile(indexName *string, orgid int64) error {
	var sb1 strings.Builder
	sb1.WriteString(VTableAliasesDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatInt(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(*indexName)
	sb1.WriteString(".json")

	filename := sb1.String()

	err := os.Remove(filename)
	if err != nil {
		log.Errorf("removeAliasFile: Failed to remove filename=%v, err=%v", filename, err)
		return err
	}

	return nil
}

func ExpandAndReturnAllIndexNames(indexNameIn string, orgid int64, isElastic bool) []string {
	return ExpandAndReturnIndexNames(indexNameIn, orgid, isElastic, nil)
}

// returns all indexNames that the input corresponding to after expanding "*" && aliases
// if isElastic is false, indices containing .kibana will not be matched
func ExpandAndReturnIndexNames(indexNameIn string, orgid int64, isElastic bool, ctx *fasthttp.RequestCtx) []string {
	finalResultsMap := make(map[string]bool)

	// we don't support  <remoteCluster:indexPattern>, so we will just convert this to indexPattern
	if idx := strings.Index(indexNameIn, ":"); idx != -1 {
		log.Infof("ExpandAndReturnIndexNames: converting *:<> to *")
		indexNameIn = indexNameIn[idx+1:]
	}

	if indexNameIn == "*" {
		indexNames, err := GetVirtualTableNames(orgid)
		if err != nil {
			log.Infof("ExpandAndReturnIndexNames: Error reading virtual table names for orgid %v: %v", orgid, err)
			return []string{}
		}
		for indexName := range indexNames {
			if isIndexExcluded(indexName) {
				continue
			}
			if !isElastic && strings.Contains(indexName, ".kibana") {
				continue
			}

			finalResultsMap[indexName] = true
		}
	} else {
		indexNames := strings.Split(indexNameIn, ",")
		for _, indexName := range indexNames {
			if strings.Contains(indexName, "*") {
				if isIndexExcluded(indexName) {
					continue
				}
				regexStr := "^" + strings.ReplaceAll(indexName, "*", `.*`) + "$"
				indexRegExp, err := regexp.Compile(regexStr)
				if err != nil {
					log.Infof("ExpandAndReturnIndexNames: Error compiling regexStr=%v, Error=%v", regexStr, err)
					return []string{}
				}
				// check all aliases for matches
				// TODO: what to do for alias when orgid != 0
				for alias, indexMap := range aliasToIndexNames[orgid] {
					if indexRegExp.Match([]byte(alias)) {
						for index := range indexMap {
							finalResultsMap[index] = true
						}
					}
				}
				// check all indexName matches
				indexNamesFromFile, _ := GetVirtualTableNames(orgid)
				for indexNameFromFile := range indexNamesFromFile {
					if indexRegExp.Match([]byte(indexNameFromFile)) {
						finalResultsMap[indexNameFromFile] = true
					}
				}
			} else {
				// check if the indexnameIn is an alias if no wildcard
				// TODO: what to do for alias when orgid != 0
				if indexMap, pres := aliasToIndexNames[orgid][indexName]; pres {
					for index := range indexMap {
						finalResultsMap[index] = true
					}
				} else {
					finalResultsMap[indexName] = true
				}
			}
		}
	}

	// check if indices found
	indexCount := len(finalResultsMap)

	// if there are no entries in the results map, return the index as is
	if indexCount == 0 {
		if isIndexExcluded(indexNameIn) {
			return []string{}
		}
		results := []string{indexNameIn}
		return filterOutUnauthorized(results, ctx)
	} else {
		results := make([]string, indexCount)
		i := 0
		for indexName := range finalResultsMap {
			results[i] = indexName
			i++
		}
		finalResults := filterOutUnauthorized(results[:i], ctx)
		slices.Sort(finalResults)
		return finalResults
	}
}

func filterOutUnauthorized(indexes []string, ctx *fasthttp.RequestCtx) []string {
	if hook := hooks.GlobalHooks.FilteroutUnauthorizedIndexes; ctx != nil && hook != nil {
		return hook(indexes, ctx)
	}
	return indexes
}

func isIndexExcluded(indexName string) bool {
	for _, value := range excludedInternalIndices {
		if strings.ReplaceAll(indexName, "*", "") == value {
			return true
		}
	}
	return false
}

func DeleteVirtualTable(tname *string, orgid int64) error {
	vTableRawFileAccessLock.Lock()
	defer vTableRawFileAccessLock.Unlock()
	vTableFileName := getVirtualTableFileName(orgid)
	vtableFd, err := os.OpenFile(vTableFileName, os.O_APPEND|os.O_RDONLY, 0644)
	if err != nil {
		log.Errorf("DeleteVirtualTable : Error opening file: %v, err: %v", vTableFileName, err)
		return err
	}
	defer func() {
		err = vtableFd.Close()
		if err != nil {
			log.Errorf("DeleteVirtualTable: Failed to close file name=%v, err:%v", vTableFileName, err)
		}
	}()
	scanner := bufio.NewScanner(vtableFd)
	var store string
	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		val := string(rawbytes)
		if val == *tname {
			continue
		}
		store += val
		store += "\n"
	}
	if err := scanner.Err(); err != nil {
		return utils.TeeErrorf("DeleteVirtualTable : Error while scanning file: %v, err: %v", vTableFileName, err)
	}
	errW := os.WriteFile(vTableFileName, []byte(store), 0644)
	if errW != nil {
		log.Errorf("DeleteVirtualTable : Error writing to vtableFilename=%v, Error=%v", vTableFileName, errW)
		return errW
	}
	return nil
}
