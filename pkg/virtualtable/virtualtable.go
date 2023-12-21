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
	"encoding/json"
	"errors"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
)

var vTableBaseDir string
var vTableMappingsDir string
var vTableTemplatesDir string
var vTableAliasesDir string

const VIRTUAL_TAB_FILENAME = "/virtualtablenames"
const VIRTUAL_TAB_FILE_EXT = ".txt"

var vTableBaseFileName string

var globalTableAccessLock sync.RWMutex = sync.RWMutex{}
var vTableRawFileAccessLock sync.RWMutex = sync.RWMutex{}

var aliasToIndexNames map[uint64]map[string]map[string]bool = make(map[uint64]map[string]map[string]bool)

// holds all the tables for orgid -> tname -> bool
var allVirtualTables map[uint64]map[string]bool

var excludedInternalIndices = [...]string{"traces", "red-traces", "service-dependency"}

func InitVTable() error {
	allVirtualTables = make(map[uint64]map[string]bool)
	var sb strings.Builder
	sb.WriteString(config.GetDataPath() + "ingestnodes/" + config.GetHostID() + "/vtabledata")
	vTableBaseDir = sb.String()
	vTableMappingsDir = vTableBaseDir + "/mappings/"
	vTableTemplatesDir = vTableBaseDir + "/templates/"
	vTableAliasesDir = vTableBaseDir + "/aliases/"

	var sb1 strings.Builder
	sb1.WriteString(vTableBaseDir)
	sb1.WriteString(VIRTUAL_TAB_FILENAME)

	vTableBaseFileName = sb1.String()

	err := CreateVirtTableBaseDirs(vTableBaseDir, vTableMappingsDir, vTableTemplatesDir, vTableAliasesDir)
	if err != nil {
		return err
	}
	err = initializeAliasToIndexMap()
	if err != nil {
		log.Errorf("InitVTable: Failed to initialize alias to index map, err=%v", err)
		return err
	}

	go refreshInMemoryTable()
	return nil
}

func getVirtualTableFileName(orgid uint64) string {
	var vTableFileName string
	if orgid == 0 {
		vTableFileName = vTableBaseFileName + VIRTUAL_TAB_FILE_EXT
	} else {
		vTableFileName = vTableBaseFileName + "-" + strconv.FormatUint(orgid, 10) + VIRTUAL_TAB_FILE_EXT
	}
	return vTableFileName
}

func refreshInMemoryTable() {
	for {
		allReadTables, err := GetVirtualTableNames(0)
		if err != nil {
			log.Errorf("refreshInMemoryTable: Failed to get virtual table names! err=%v", err)
		} else {
			globalTableAccessLock.Lock()
			allVirtualTables[uint64(0)] = allReadTables
			globalTableAccessLock.Unlock()
		}
		time.Sleep(1 * time.Minute)
	}
}

func GetFilePathForRemoteNode(node string, orgid uint64) string {
	var vFile strings.Builder
	vFile.WriteString(config.GetDataPath() + "ingestnodes/" + node + "/vtabledata")
	vFile.WriteString(VIRTUAL_TAB_FILENAME)
	if orgid != 0 {
		vFile.WriteString("-" + strconv.FormatUint(orgid, 10) + VIRTUAL_TAB_FILE_EXT)
	} else {
		vFile.WriteString(VIRTUAL_TAB_FILE_EXT)
	}
	vfname := vFile.String()
	return vfname
}

func CreateVirtTableBaseDirs(vTableBaseDir string, vTableMappingsDir string,
	vTableTemplatesDir string, vTableAliasesDir string) error {

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

func addVirtualTableHelper(tname *string, orgid uint64) error {
	var tableExists bool
	globalTableAccessLock.RLock()
	_, tableExists = allVirtualTables[orgid][*tname]
	globalTableAccessLock.RUnlock()
	if tableExists {
		return nil
	}

	globalTableAccessLock.Lock()
	if _, orgExists := allVirtualTables[orgid]; !orgExists {
		allVirtualTables[orgid] = make(map[string]bool)
	}
	allVirtualTables[orgid][*tname] = true
	globalTableAccessLock.Unlock()

	vTableFileName := getVirtualTableFileName(orgid)
	fd, err := os.OpenFile(vTableFileName, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		log.Errorf("AddVirtualTable: Failed to open virtual tablename=%v, in file=%v, err=%v", *tname, vTableFileName, err)
		return err
	}
	defer fd.Close()
	if _, err := fd.WriteString(*tname); err != nil {
		log.Errorf("AddVirtualTable: Failed to write virtual tablename=%v, in file=%v, err=%v", *tname, vTableFileName, err)

		return err
	}
	if _, err := fd.WriteString("\n"); err != nil {
		log.Errorf("AddVirtualTable: Failed to write \n to virtual tablename=%v, in file=%v, err=%v", *tname, vTableFileName, err)
		return err
	}
	if err = fd.Sync(); err != nil {
		log.Errorf("AddVirtualTable: Failed to sync virtual tablename=%v, in file=%v, err=%v", *tname, vTableFileName, err)
		return err
	}
	return nil
}

func AddVirtualTable(tname *string, orgid uint64) error {

	vTableRawFileAccessLock.Lock()
	err := addVirtualTableHelper(tname, orgid)
	vTableRawFileAccessLock.Unlock()
	if err != nil {
		log.Errorf("AddVirtualTable: Error in adding virtual table to the file!. Err: %v", err)
		return err
	}
	return nil
}

func IsVirtualTablePresent(tname *string, orgid uint64) bool {
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

func AddVirtualTableAndMapping(tname *string, mapping *string, orgid uint64) error {

	//todo for dupe entries, write a goroutine that wakes up once per day (random time) and reads the
	// central place of virtualtablenames.txt and de-dupes the table names by creating a lock

	err := AddVirtualTable(tname, orgid)
	if err != nil {
		return err
	}

	return AddMapping(tname, mapping, orgid)

}

func AddMapping(tname *string, mapping *string, orgid uint64) error {
	var sb1 strings.Builder
	sb1.WriteString(vTableMappingsDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatUint(orgid, 10))
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

func GetVirtualTableNames(orgid uint64) (map[string]bool, error) {
	vTableFileName := getVirtualTableFileName(orgid)
	return getVirtualTableNamesHelper(vTableFileName)
}

func getVirtualTableNamesHelper(fileName string) (map[string]bool, error) {
	var result = make(map[string]bool)
	fd, err := os.OpenFile(fileName, os.O_RDONLY, 0644)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return result, nil
		}
		log.Errorf("GetVirtualTableNames: Failed to open file=%v, err=%v", fileName, err)
		return nil, err
	}

	defer fd.Close()

	scanner := bufio.NewScanner(fd)
	for scanner.Scan() {
		rawbytes := scanner.Bytes()
		result[string(rawbytes)] = true
	}
	return result, nil
}

func AddAliases(indexName string, aliases []string, orgid uint64) error {

	if indexName == "" {
		log.Errorf("AddAliases: indexName was null")
		return errors.New("indexName was null")
	}

	alLen := len(aliases)
	if alLen == 0 {
		log.Errorf("AddAliases: len of aliases was 0")
		return errors.New("len of aliases was 0")
	}

	currentAliases, err := GetAliases(indexName, orgid)
	if err != nil {
		log.Errorf("AddAliases: GetAliases returned err=%v", err)
		return err
	}

	log.Infof("AddAliases: idxname=%v, existing aliases=[%v], newaliases=[%v]", indexName, currentAliases, aliases)

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

func GetAllAliasesAsMapArray(orgid uint64) (map[string][]string, error) {
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

func GetAliasesAsArray(indexName string, orgid uint64) ([]string, error) {

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

func GetAliases(indexName string, orgid uint64) (map[string]bool, error) {

	var sb1 strings.Builder
	sb1.WriteString(vTableAliasesDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatUint(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(indexName)
	sb1.WriteString(".json")

	fullname := sb1.String()

	retval := map[string]bool{}
	rdata, err := os.ReadFile(fullname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return retval, nil
		}
		log.Errorf("GetAliases: Failed to readfile fullname=%v, err=%v", fullname, err)
		return retval, err
	}

	err = json.Unmarshal(rdata, &retval)
	if err != nil {
		log.Errorf("GetAliases: Failed to unmarshall fullname=%v, err=%v", fullname, err)
		return retval, err
	}

	return retval, nil

}

func writeAliasFile(indexName *string, allnames map[string]bool, orgid uint64) error {

	var sb1 strings.Builder
	sb1.WriteString(vTableAliasesDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatUint(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(*indexName)
	sb1.WriteString(".json")

	fullname := sb1.String()

	jdata, err := json.Marshal(&allnames)
	if err != nil {
		log.Errorf("writeAliasFile: Failed to marshall fullname=%v, err=%v", fullname, err)
		return err
	}

	err = os.WriteFile(fullname, jdata, 0644)
	if err != nil {
		log.Errorf("writeAliasFile: Failed to writefile fullname=%v, err=%v", fullname, err)
		return err
	}

	return nil
}

func initializeAliasToIndexMap() error {
	dirs, err := os.ReadDir(vTableAliasesDir)
	if err != nil {
		log.Errorf("initializeAliasToIndexMap: Failed to readdir vTableAliasesDir=%v, err=%v", vTableAliasesDir, err)
		return err
	}

	for _, dir := range dirs {
		if dir.IsDir() {
			orgid := dir.Name()
			orgIdNumber, _ := strconv.ParseUint(orgid, 10, 64)
			files, err := os.ReadDir(vTableAliasesDir + dir.Name())
			if err != nil {
				log.Errorf("initializeAliasToIndexMap: Failed to readdir for org =%v, err=%v", orgid, err)
				return err
			}
			for _, f := range files {
				var sb strings.Builder
				sb.WriteString(vTableAliasesDir)
				fname := f.Name()

				if strings.HasSuffix(fname, ".json") {
					indexName := strings.TrimSuffix(fname, ".json")
					aliasNames, err := GetAliases(indexName, orgIdNumber)
					if err != nil {
						log.Errorf("initializeAliasToIndexMap: Failed to getAllAliasInIndexFile fname=%v, err=%v", fname, err)
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

func putAliasToIndexInMem(aliasName string, indexName string, orgid uint64) {

	if aliasName == "" {
		log.Errorf("putAliasToIndexInMem: aliasName was empty")
		return
	}

	if indexName == "" {
		log.Errorf("putAliasToIndexInMem: indexName was empty")
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
	log.Warnf("Flushing alias map to file on exit")
	for orgid := range aliasToIndexNames {
		for alias, indexNames := range aliasToIndexNames[orgid] {
			err := writeAliasFile(&alias, indexNames, orgid)
			if err != nil {
				log.Errorf("Failed to save alias map! %+v", err)
			}
		}
	}
	return nil
}

func GetIndexNameFromAlias(aliasName string, orgid uint64) (string, error) {
	if aliasName == "" {
		log.Errorf("getIndexNameFromAlias: aliasName was empty")
		return "", errors.New("getIndexNameFromAlias: aliasName was empty")
	}

	//	log.Infof("GetIndexNameFromAlias: aliasName=%v, aliasToIndexNames=%v", aliasName, aliasToIndexNames)

	if _, pres := aliasToIndexNames[orgid][aliasName]; pres {
		for key := range aliasToIndexNames[orgid][aliasName] {
			return key, nil
		}
	}

	return "", errors.New("not found")
}

func IsAlias(nameToCheck string, orgid uint64) (bool, string) {

	if valMap, ok := aliasToIndexNames[orgid][nameToCheck]; ok {
		for indexName := range valMap {
			return true, indexName
		}
	}

	return false, ""
}

func RemoveAliases(indexName string, aliases []string, orgid uint64) error {

	if indexName == "" {
		log.Errorf("RemoveAliases: indexName was null")
		return errors.New("indexName was null")
	}

	alLen := len(aliases)
	if alLen == 0 {
		log.Errorf("RemoveAliases: len of aliases was 0")
		return errors.New("len of aliases was 0")
	}

	currentAliases, err := GetAliases(indexName, orgid)
	if err != nil {
		log.Errorf("RemoveAliases: GetAliases returned err=%v", err)
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

func removeAliasFile(indexName *string, orgid uint64) error {
	var sb1 strings.Builder
	sb1.WriteString(vTableAliasesDir)
	if orgid != 0 {
		sb1.WriteString(strconv.FormatUint(orgid, 10))
		sb1.WriteString("/")
	}
	sb1.WriteString(*indexName)
	sb1.WriteString(".json")

	fullname := sb1.String()

	err := os.Remove(fullname)
	if err != nil {
		log.Errorf("removeAliasFile: Failed to remove fullname=%v, err=%v", fullname, err)
		return err
	}

	return nil

}

// returns all indexNames that the input corresponding to after expanding "*" && aliases
// if isElastic is false, indices containing .kibana will not be matched
func ExpandAndReturnIndexNames(indexNameIn string, orgid uint64, isElastic bool) []string {

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
					log.Infof("ExpandAndReturnIndexNames: Error compiling match: %v", err)
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
		finalResults := []string{indexNameIn}
		return finalResults
	} else {
		finalResults := make([]string, indexCount)
		i := 0
		for indexName := range finalResultsMap {
			finalResults[i] = indexName
			i++
		}
		finalResults = finalResults[:i]
		sort.Strings(finalResults)
		return finalResults
	}

}

func isIndexExcluded(indexName string) bool {
	for _, value := range excludedInternalIndices {
		if strings.ReplaceAll(indexName, "*", "") == value {
			return true
		}
	}
	return false
}

func DeleteVirtualTable(tname *string, orgid uint64) error {

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
	errW := os.WriteFile(vTableFileName, []byte(store), 0644)
	if errW != nil {
		log.Errorf("DeleteVirtualTable : Error writing to vtableFd: %v", errW)
		return errW
	}
	return nil
}
