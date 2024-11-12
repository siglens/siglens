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

package utils

import (
	"bufio"
	"encoding/xml"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/brianvoe/gofakeit/v6"
	jsoniter "github.com/json-iterator/go"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fastrand"
)

type ConfigType uint8

const (
	Default ConfigType = iota
	FunctionalTest
	PerformanceTest
)

var json = jsoniter.ConfigFastest

var allFixedColumns = []string{}

type Generator interface {
	Init(fName ...string) error
	GetLogLine() ([]byte, error)
	GetRawLog() (map[string]interface{}, error)
}

// file reader loads chunks from the file. Each request will get a sequential entry from the chunk.
// When the read index is close to the next chunk, pre-load next chunks
// Chunks should loop over the file multiple times if necessary
type FileReader struct {
	file    string
	lineNum int //number of lines read. lets us know where to start from

	editLock *sync.Mutex

	logLines [][]byte
	currIdx  int

	nextLogLines      [][]byte
	isChunkPrefetched bool
	asyncPrefetch     bool // is the chunk currenly being prefetched?
}

// Repeats the same log line each time
type StaticGenerator struct {
	logLine []byte
	ts      bool
}
type K8sGenerator struct {
	baseBody  map[string]interface{}
	tNowEpoch uint64
	ts        bool
	faker     *gofakeit.Faker
	seed      int64
}

type DynamicUserGenerator struct {
	baseBody     map[string]interface{}
	tNowEpoch    uint64
	ts           bool
	faker        *gofakeit.Faker
	seed         int64
	accFakerSeed int64
	accountFaker *gofakeit.Faker
	DataConfig   *GeneratorDataConfig
}

type GeneratorDataConfig struct {
	ConfigType      ConfigType
	MaxColumns      int                   // Mximumn Number of columns per record.
	VariableColumns bool                  // Flag to indicate variable columns per record
	MinColumns      int                   // Minimum number of columns per record
	functionalTest  *FunctionalTestConfig // It exclusively used for functional test
	perfTestConfig  *PerfTestConfig       // It exclusively used for performance test
	EndTimestamp    time.Time
	MaxColSuffix    []int
}

type FunctionalTestConfig struct {
	FixedColumns       int // Fixed columns for testing
	MaxVariableColumns int // Max columns,
	EndTimestamp       uint64
	variableFaker      *gofakeit.Faker
	fixedFaker         *gofakeit.Faker
	jsonFaker          *gofakeit.Faker
	xmlFaker           *gofakeit.Faker
	variableColNames   []string
}

type PerfTestConfig struct {
	LogChan        chan Log
	LogToSend      Log
	ShouldSendData bool
}

type Log struct {
	Data            map[string]interface{}
	AllFixedColumns []string
	Timestamp       time.Time
}

func (dataGenConfig *GeneratorDataConfig) SendLog() {
	if dataGenConfig.ConfigType != PerformanceTest {
		return
	}
	if dataGenConfig.perfTestConfig == nil {
		log.Errorf("SendLog: Perf test config is nil")
		return
	}
	if dataGenConfig.perfTestConfig.LogChan == nil {
		log.Errorf("SendLog: Log channel is nil")
		return
	}

	dataGenConfig.perfTestConfig.LogToSend.Timestamp = time.Now()

	select {
	case dataGenConfig.perfTestConfig.LogChan <- dataGenConfig.perfTestConfig.LogToSend:
		dataGenConfig.perfTestConfig.ShouldSendData = true
	default:
		// skip if the channel is full
	}
}

func GetVariableColumnNames(maxVariableColumns int) []string {
	variableColNames := make([]string, 0)
	for i := 0; i < maxVariableColumns; i++ {
		variableColNames = append(variableColNames, fmt.Sprintf("variable_col_%d", i))
	}
	return variableColNames
}

func GetVariablesColsNamesFromConfig(dataGenConfig *GeneratorDataConfig) []string {
	if dataGenConfig.functionalTest != nil {
		return dataGenConfig.functionalTest.variableColNames
	}
	return []string{}
}

func InitFunctionalTestGeneratorDataConfig(fixedColumns, maxVariableColumns int) *GeneratorDataConfig {
	return &GeneratorDataConfig{
		ConfigType: FunctionalTest,
		functionalTest: &FunctionalTestConfig{
			FixedColumns:       fixedColumns,
			MaxVariableColumns: maxVariableColumns,
			variableColNames:   GetVariableColumnNames(maxVariableColumns),
		},
	}
}

func InitPerformanceTestGeneratorDataConfig(fixedColumns, maxVariableColumns int, logChan chan Log) *GeneratorDataConfig {
	return &GeneratorDataConfig{
		ConfigType: PerformanceTest,
		functionalTest: &FunctionalTestConfig{
			FixedColumns:       fixedColumns,
			MaxVariableColumns: maxVariableColumns,
			variableColNames:   GetVariableColumnNames(maxVariableColumns),
		},
		perfTestConfig: &PerfTestConfig{
			LogChan: logChan,
		},
	}
}

func InitGeneratorDataConfig(maxColumns int, variableColumns bool, minColumns int, uniqColumns int) *GeneratorDataConfig {
	if minColumns > maxColumns {
		minColumns = maxColumns
	}

	if minColumns == 0 {
		minColumns = maxColumns
	}

	if minColumns == maxColumns {
		variableColumns = false
	}

	genConfig := &GeneratorDataConfig{
		MaxColumns:      maxColumns,
		VariableColumns: variableColumns,
		MinColumns:      minColumns,
	}

	if uniqColumns > 0 {
		MaxColSuffix := make([]int, maxColumns)
		extra := uniqColumns % maxColumns
		each := uniqColumns / maxColumns
		for i := 0; i < maxColumns; i++ {
			MaxColSuffix[i] = each
			if extra > 0 {
				MaxColSuffix[i]++
				extra--
			}
		}
		genConfig.MaxColSuffix = MaxColSuffix
	}

	return genConfig
}

func InitDynamicUserGenerator(ts bool, seed int64, accfakerSeed int64, dataConfig *GeneratorDataConfig) *DynamicUserGenerator {
	return &DynamicUserGenerator{
		ts:           ts,
		seed:         seed,
		accFakerSeed: accfakerSeed,
		DataConfig:   dataConfig,
	}
}

func InitFunctionalUserGenerator(ts bool, seed int64, accFakerSeed int64, dataConfig *GeneratorDataConfig, processIndex int) (*DynamicUserGenerator, error) {
	fixedfakerSeed := 1000 + processIndex*20
	jsonFakerSeed := 10 + processIndex
	xmlFakerSeed := 20 + processIndex
	varFakerSeed := 30 + processIndex

	fixFaker := gofakeit.NewUnlocked(int64(fixedfakerSeed)) // Cols to test should use fixedFaker only
	jsonFaker := gofakeit.NewUnlocked(int64(jsonFakerSeed))
	xmlFaker := gofakeit.NewUnlocked(int64(xmlFakerSeed))
	varFaker := gofakeit.NewUnlocked(int64(varFakerSeed))

	if dataConfig.functionalTest == nil {
		return nil, fmt.Errorf("Functional test config is nil")
	}

	functionalTest := &FunctionalTestConfig{
		FixedColumns:       dataConfig.functionalTest.FixedColumns,
		MaxVariableColumns: dataConfig.functionalTest.MaxVariableColumns,
		variableColNames:   dataConfig.functionalTest.variableColNames,
		fixedFaker:         fixFaker,
		jsonFaker:          jsonFaker,
		xmlFaker:           xmlFaker,
		variableFaker:      varFaker,
	}

	endTimestamp := dataConfig.EndTimestamp
	if processIndex > 0 {
		endTimestamp = dataConfig.EndTimestamp.Add(3 * time.Duration(-processIndex) * time.Hour)
	}
	functionalTest.EndTimestamp = uint64(endTimestamp.UnixMilli())

	return &DynamicUserGenerator{
		ts:           ts,
		seed:         seed,
		accFakerSeed: accFakerSeed,
		DataConfig: &GeneratorDataConfig{
			ConfigType:     FunctionalTest,
			functionalTest: functionalTest,
		},
	}, nil
}

func InitPerfTestGenerator(ts bool, seed int64, accFakerSeed int64, dataConfig *GeneratorDataConfig, processIndex int) (*DynamicUserGenerator, error) {
	if dataConfig.perfTestConfig == nil {
		return nil, fmt.Errorf("Perf test config is nil")
	}
	gen, err := InitFunctionalUserGenerator(ts, seed, accFakerSeed, dataConfig, processIndex)
	if err != nil {
		return nil, fmt.Errorf("InitPerfTestGenerator: Failed to initialize functional test generator: %v", err)
	}
	gen.DataConfig.ConfigType = PerformanceTest
	gen.DataConfig.perfTestConfig = &PerfTestConfig{
		LogChan: dataConfig.perfTestConfig.LogChan,
	}
	return gen, nil
}

func InitK8sGenerator(ts bool, seed int64) *K8sGenerator {

	return &K8sGenerator{
		ts:   ts,
		seed: seed,
	}
}

func InitStaticGenerator(ts bool) *StaticGenerator {
	return &StaticGenerator{
		ts: ts,
	}
}

func InitFileReader() *FileReader {
	return &FileReader{}
}

var logMessages = []string{
	"%s for DRA plugin '%q' failed. Plugin returned an empty list for supported versions",
	"'%s' for DRA plugin %q failed. None of the versions specified %q are supported. err='%v'",
	"Unable to write event '%v' (retry limit exceeded!)",
	"Unable to start event watcher: '%v' (will not retry!)",
	"Could not construct reference to: '%v' due to: '%v'. Will not report event: '%v' '%v' '%v'",
}

// replacePlaceholders replaces formatting placeholders in a string with random values.
// The placeholders are identified using the % character followed by a type specifier:
//   - %s: Replaced with a random word.
//   - %q: Replaced with a random buzzword.
//   - %v: Replaced with a random number between 1 and 100.
//
// Placeholders within single or double quotes are also supported.
// For example, "Error: %v is not %s" could become "Error: 42 is not foo".
func replacePlaceholders(template string) string {

	// The regex (placeholderRegex) captures placeholders in a string.
	// It matches placeholders within single/double quotes or unquoted,
	// identified by a '%' followed by non-whitespace characters.

	placeholderRegex := regexp.MustCompile(`(%[^\s%])`)
	indices := placeholderRegex.FindStringIndex(template)
	for len(indices) > 0 {
		start := indices[0]
		end := indices[1]
		placeholderType := template[start:end]
		placeholderType = strings.Replace(placeholderType, "%", "", 1)
		var replacement string
		switch placeholderType {
		case "s":
			replacement = gofakeit.Word()

		case "q":
			replacement = gofakeit.BuzzWord()

		case "v":
			replacement = fmt.Sprintf("%d", gofakeit.Number(1, 100))

		default:
			replacement = "UNKNOWN"
			log.Infof("Unknown placeholder type: %s", placeholderType)
		}

		template = string(template[:start]) + replacement + string(template[end:])
		indices = placeholderRegex.FindStringIndex(template)
	}
	return template
}

func getColumnName(name string, colIndex int) string {
	if colIndex == 0 {
		return name
	}
	return fmt.Sprintf("%s_c%d", name, colIndex)
}

func randomizeBody(f *gofakeit.Faker, m map[string]interface{}, addts bool, accountFaker *gofakeit.Faker) {
	getStaticUserColumnValue(f, m, accountFaker)

	if addts {
		m["timestamp"] = uint64(time.Now().UnixMilli())
	}
}

func randomizeBody_dynamic(f *gofakeit.Faker, m map[string]interface{}, addts bool, config *GeneratorDataConfig) {
	for col := range m {
		delete(m, col)
	}

	dynamicUserColumnsLen := len(dynamicUserColumnNames)
	dynamicUserColIndex := 0

	numColumns := 0

	colSuffix := 1

	var skipIndexes map[int]struct{}

	if config != nil {
		numColumns = config.MaxColumns
		if config.VariableColumns {
			rng := rand.New(rand.NewSource(time.Now().UnixNano()))
			columnsToBeInserted := rng.Intn(config.MaxColumns-config.MinColumns+1) + config.MinColumns
			skipColumnsCount := config.MaxColumns - columnsToBeInserted
			skipIndexes = make(map[int]struct{}, skipColumnsCount)
			for skipColumnsCount > 0 {
				index := rng.Intn(config.MaxColumns)
				if _, ok := skipIndexes[index]; !ok {
					skipIndexes[index] = struct{}{}
					skipColumnsCount--
				}
			}
		}
	}

	colCount := 0

	loopCount := numColumns

	if numColumns == 0 {
		loopCount = len(dynamicUserColumnNames)
		colSuffix = 0
	}

	p := f.Person()

	for colCount < loopCount {

		if dynamicUserColIndex >= dynamicUserColumnsLen {
			dynamicUserColIndex = 0
			colSuffix++
			p = f.Person()
		}

		cname := dynamicUserColumnNames[dynamicUserColIndex]
		insertColName := getColumnName(cname, colSuffix)

		if skipIndexes != nil {
			if _, ok := skipIndexes[colCount]; ok {
				colCount++
				dynamicUserColIndex++
				delete(m, insertColName)
				continue
			}
		}

		if len(config.MaxColSuffix) > 0 {
			insertColName = fmt.Sprintf("%v_%v", insertColName, rand.Intn(config.MaxColSuffix[colCount]))
		}

		m[insertColName] = getDynamicUserColumnValue(f, cname, p)
		colCount++
		dynamicUserColIndex++
	}

	if addts {
		m["timestamp"] = uint64(time.Now().UnixMilli())
	}
}

type Data struct {
	Person      *Person                    `json:"person" xml:"person"`
	CreditCards []*gofakeit.CreditCardInfo `json:"credit_cards" xml:"credit_cards"`
	IPAddress   string                     `json:"ip_address" xml:"ip_address"`
	Numbers     []int                      `json:"numbers" xml:"numbers"`
}

type Person struct {
	Name    *Name                 `json:"name" xml:"name"`
	Address *Address              `json:"address" xml:"address"`
	Contact *gofakeit.ContactInfo `json:"contact" xml:"contact"`
	Hobbies []string              `json:"hobbies" xml:"hobbies"`
	Gender  string                `json:"gender" xml:"gender"`
}

type Name struct {
	FirstName string `json:"first_name" xml:"first_name"`
	LastName  string `json:"last_name" xml:"last_name"`
}

type Address struct {
	City    string `json:"city" xml:"city"`
	State   string `json:"state" xml:"state"`
	Zip     string `json:"zip" xml:"zip"`
	Country string `json:"country" xml:"country"`
}

func getData(f *gofakeit.Faker) Data {
	data := Data{}
	data.Person = &Person{}
	data.Person.Name = &Name{
		FirstName: f.FirstName(),
		LastName:  f.LastName(),
	}
	if f.Bool() {
		data.Person.Contact = f.Contact()
	}
	if f.Bool() {
		addr := &Address{}
		addr.City = f.City()
		addr.State = f.State()
		if f.Bool() {
			addr.Zip = f.Zip()
		}
		if f.Bool() {
			addr.Country = f.Country()
		}
		data.Person.Address = addr
	}

	num := f.Number(0, 6)
	hobbies := []string{}
	for i := 0; i < num; i++ {
		hobbies = append(hobbies, f.Hobby())
	}
	data.Person.Hobbies = hobbies
	data.Person.Gender = f.Gender()

	// credit cards
	num = f.Number(0, 3)
	credit_cards := make([]*gofakeit.CreditCardInfo, num)
	for i := 0; i < num; i++ {
		credit_cards[i] = f.CreditCard()
	}
	if num != 0 {
		data.CreditCards = credit_cards
	}

	data.IPAddress = f.IPv4Address()

	num = f.Number(0, 4)
	numbers := make([]int, num)
	for i := 0; i < num; i++ {
		numbers[i] = f.Number(1, 9999999999)
	}
	data.Numbers = numbers

	return data
}

func randomizeBody_functionalTest(f *gofakeit.Faker, m map[string]interface{}, addts bool, config *FunctionalTestConfig, accountFaker *gofakeit.Faker) {
	// Fixed faker is reserved for default columns that will be used for testing.
	m["bool_col"] = config.fixedFaker.Bool()
	lang := config.fixedFaker.Language()
	if config.fixedFaker.Bool() {
		lang = "_" + lang
	}
	m["language"] = lang

	lenDynamicUserCols := len(dynamicUserColumnNames)
	getStaticUserColumnValue(config.fixedFaker, m, accountFaker)
	fixedCols := 4 + lenDynamicUserCols

	data := getData(config.jsonFaker)
	jsonData, err := json.Marshal(data)
	if err == nil {
		m["json_data"] = string(jsonData)
	} else {
		log.Errorf("Failed to generate JSON data: %+v", err)
	}

	XMLdata := getData(config.xmlFaker)
	xmlData, err := xml.Marshal(XMLdata)
	if err == nil {
		m["xml_data"] = string(xmlData)
	} else {
		log.Errorf("Failed to generate XML data: %+v", err)
	}

	suffix := 0
	curP := f.Person()
	for i := 0; fixedCols < config.FixedColumns; i++ {
		if i%lenDynamicUserCols == 0 {
			curP = f.Person()
			suffix++
		}
		cname := dynamicUserColumnNames[i%lenDynamicUserCols]
		colName := getColumnName(cname, suffix)
		m[colName] = getDynamicUserColumnValue(f, cname, curP)
		fixedCols++
	}

	if len(allFixedColumns) == 0 {
		for col := range m {
			allFixedColumns = append(allFixedColumns, col)
		}
	}

	variableP := config.variableFaker.Person()
	for i, col := range config.variableColNames {
		if i%lenDynamicUserCols == 0 {
			variableP = config.variableFaker.Person()
		}
		m[col] = getDynamicUserColumnValue(config.variableFaker, dynamicUserColumnNames[i%lenDynamicUserCols], variableP)
	}

	deleteCols := config.variableFaker.Number(0, config.MaxVariableColumns-1)
	deletedCols := map[int]struct{}{}
	for deleteCols > 0 {
		index := config.variableFaker.Number(0, config.MaxVariableColumns-1)
		if _, ok := deletedCols[index]; !ok {
			delete(m, config.variableColNames[index])
			deletedCols[index] = struct{}{}
			deleteCols--
		}
	}

	if addts {
		m["timestamp"] = config.EndTimestamp
		config.EndTimestamp -= 1
	}
}

func randomizeBody_perfTest(f *gofakeit.Faker, m map[string]interface{}, addts bool, config *GeneratorDataConfig, accountFaker *gofakeit.Faker) {
	randomizeBody_functionalTest(f, m, addts, config.functionalTest, accountFaker)

	if !config.perfTestConfig.ShouldSendData {
		return
	}

	data := make(map[string]interface{})

	for _, col := range config.functionalTest.variableColNames {
		if val, ok := m[col]; ok {
			data[col] = val
		}
	}

	for _, col := range dynamicUserColumnNames {
		if val, ok := m[col]; ok {
			data[col] = val
		}
	}

	logToSend := Log{
		Data:            data,
		AllFixedColumns: allFixedColumns,
	}

	config.perfTestConfig.ShouldSendData = false
	config.perfTestConfig.LogToSend = logToSend
}

func (r *DynamicUserGenerator) generateRandomBody() {
	if r.DataConfig != nil {
		if r.DataConfig.ConfigType == PerformanceTest {
			randomizeBody_perfTest(r.faker, r.baseBody, r.ts, r.DataConfig, r.accountFaker)
		} else if r.DataConfig.ConfigType == FunctionalTest {
			randomizeBody_functionalTest(r.faker, r.baseBody, r.ts, r.DataConfig.functionalTest, r.accountFaker)
		} else {
			randomizeBody_dynamic(r.faker, r.baseBody, r.ts, r.DataConfig)
		}
	} else {
		randomizeBody(r.faker, r.baseBody, r.ts, r.accountFaker)
	}
}

func (r *K8sGenerator) createK8sBody() {
	randomTemplate := logMessages[gofakeit.Number(0, len(logMessages)-1)]
	logEntry := replacePlaceholders(randomTemplate)
	r.baseBody["batch"] = fmt.Sprintf("batch-%d", r.faker.Number(1, 1000))
	r.baseBody["DomainName"] = r.faker.DomainName()
	r.baseBody["Region"] = r.faker.TimeZoneRegion()
	r.baseBody["Az"] = r.faker.Country()
	r.baseBody["hostname"] = r.faker.IPv4Address()
	r.baseBody["httpStatus"] = r.faker.HTTPStatusCodeSimple()
	r.baseBody["UserAgent"] = r.faker.UserAgent()
	r.baseBody["Url"] = r.faker.URL()
	r.baseBody["latency"] = r.faker.Number(0, 100)
	r.baseBody["IPv4Address"] = r.faker.IPv4Address()
	r.baseBody["Port"] = r.faker.Number(0, 65535)
	r.baseBody["msg"] = logEntry
}

func (r *K8sGenerator) Init(fName ...string) error {
	gofakeit.Seed(r.seed)
	r.faker = gofakeit.NewUnlocked(r.seed)
	rand.Seed(r.seed)
	r.baseBody = make(map[string]interface{})
	r.createK8sBody()
	body, err := json.Marshal(r.baseBody)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of a random log line is %+v bytes", stringSize)
	r.tNowEpoch = uint64(time.Now().UnixMilli()) - 80*24*3600*1000
	return nil
}

func (r *DynamicUserGenerator) Init(fName ...string) error {
	gofakeit.Seed(r.seed)
	r.faker = gofakeit.NewUnlocked(r.seed)
	r.accountFaker = gofakeit.NewUnlocked(r.accFakerSeed)
	rand.Seed(r.seed)
	r.baseBody = make(map[string]interface{})
	r.generateRandomBody()
	body, err := json.Marshal(r.baseBody)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of a random log line is %+v bytes", stringSize)
	r.tNowEpoch = uint64(time.Now().UnixMilli()) - 80*24*3600*1000
	if r.DataConfig != nil && r.DataConfig.perfTestConfig != nil {
		r.DataConfig.perfTestConfig.ShouldSendData = true
	}

	return nil
}

func (r *K8sGenerator) GetLogLine() ([]byte, error) {
	r.createK8sBody()
	return json.Marshal(r.baseBody)
}

func (r *DynamicUserGenerator) GetLogLine() ([]byte, error) {
	r.generateRandomBody()
	return json.Marshal(r.baseBody)
}

func (r *DynamicUserGenerator) GetRawLog() (map[string]interface{}, error) {
	r.generateRandomBody()
	return r.baseBody, nil
}

func (r *K8sGenerator) GetRawLog() (map[string]interface{}, error) {
	r.createK8sBody()
	return r.baseBody, nil
}

func (r *StaticGenerator) Init(fName ...string) error {
	m := make(map[string]interface{})
	f := gofakeit.NewUnlocked(int64(fastrand.Uint32n(1_000)))
	randomizeBody(f, m, r.ts, gofakeit.NewUnlocked(10000))
	body, err := json.Marshal(m)
	if err != nil {
		return err
	}
	stringSize := len(body) + int(unsafe.Sizeof(body))
	log.Infof("Size of event log line is %+v bytes", stringSize)
	r.logLine = body
	return nil
}

func (sr *StaticGenerator) GetLogLine() ([]byte, error) {
	return sr.logLine, nil
}

func (sr *StaticGenerator) GetRawLog() (map[string]interface{}, error) {
	final := make(map[string]interface{})
	err := json.Unmarshal(sr.logLine, &final)
	if err != nil {
		return nil, err
	}
	return final, nil
}

var chunkSize int = 10000

func (fr *FileReader) Init(fName ...string) error {
	fr.file = fName[0]
	fr.lineNum = 0
	fr.logLines = make([][]byte, 0)
	fr.nextLogLines = make([][]byte, 0)
	fr.isChunkPrefetched = false
	fr.asyncPrefetch = false
	fr.editLock = &sync.Mutex{}
	if _, err := os.Stat(fName[0]); errors.Is(err, os.ErrNotExist) {
		return err
	}
	err := fr.swapChunks()
	if err != nil {
		return err
	}
	return nil
}

func (fr *FileReader) GetLogLine() ([]byte, error) {
	fr.editLock.Lock()
	defer fr.editLock.Unlock()
	if fr.currIdx >= len(fr.logLines) {
		err := fr.prefetchChunk(false)
		if err != nil {
			return []byte{}, err
		}
		err = fr.swapChunks()
		if err != nil {
			return []byte{}, err
		}
	}
	retVal := fr.logLines[fr.currIdx]
	fr.currIdx++
	if fr.currIdx > len(fr.logLines)/2 {
		go func() { _ = fr.prefetchChunk(false) }()
	}
	return retVal, nil
}

func (fr *FileReader) GetRawLog() (map[string]interface{}, error) {
	rawLog, err := fr.GetLogLine()
	if err != nil {
		return nil, err
	}
	final := make(map[string]interface{})
	err = json.Unmarshal(rawLog, &final)
	if err != nil {
		return nil, err
	}
	return final, nil
}

func (fr *FileReader) swapChunks() error {
	err := fr.prefetchChunk(false)
	if err != nil {
		return err
	}
	for fr.asyncPrefetch {
		time.Sleep(100 * time.Millisecond)
	}
	fr.logLines, fr.nextLogLines = fr.nextLogLines, fr.logLines
	fr.nextLogLines = make([][]byte, 0)
	fr.currIdx = 0
	fr.isChunkPrefetched = false
	return nil
}

// function will be called multiple times & will check if the next slice is already pre loaded
func (fr *FileReader) prefetchChunk(override bool) error {
	if fr.isChunkPrefetched {
		return nil
	}
	if fr.asyncPrefetch || override {
		return nil
	}
	fr.asyncPrefetch = true
	defer func() { fr.asyncPrefetch = false }()
	fd, err := os.Open(fr.file)
	if err != nil {
		log.Errorf("Failed to open file %s: %+v", fr.file, err)
		return err
	}
	defer fd.Close()
	_, err = fd.Seek(0, 0)
	if err != nil {
		return err
	}
	fileScanner := bufio.NewScanner(fd)
	tmpMap := make(map[string]interface{})
	lNum := 0
	for fileScanner.Scan() {
		if lNum < fr.lineNum {
			lNum++
			continue
		}
		err := json.Unmarshal(fileScanner.Bytes(), &tmpMap)
		if err != nil {
			log.Errorf("Failed to unmarshal log entry %+v: lineNum %+v %+v", tmpMap, fr.lineNum, err)
			return err
		}
		logs, err := json.Marshal(tmpMap)
		if err != nil {
			log.Errorf("Failed to marshal log entry %+v: %+v", tmpMap, err)
			return err
		}
		fr.nextLogLines = append(fr.nextLogLines, logs)
		if len(fr.nextLogLines) > chunkSize {
			fr.isChunkPrefetched = true
			break
		}
		lNum++
	}

	if err := fileScanner.Err(); err != nil {
		log.Errorf("error in file scanner %+v", err)
		return err
	}
	fr.lineNum = lNum
	if len(fr.nextLogLines) <= chunkSize {
		// this will only happen if we reached the end of the file before filling the chunk
		fr.lineNum = 0
		return fr.prefetchChunk(true)
	}
	return nil
}
