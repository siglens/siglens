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

package scroll

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/siglens/siglens/pkg/config"
	segutils "github.com/siglens/siglens/pkg/segment/utils"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type Scroll struct {
	Scroll_id string
	Results   *utils.HttpServerESResponseOuter
	Size      uint64
	TimeOut   uint64
	Expiry    string
	Offset    uint64
	Valid     bool
}

var allScrollRecords = map[string]*Scroll{}
var allScrollRecordsLock sync.RWMutex

func init() {
	go checkStaleScrollContext()
}

func checkStaleScrollContext() {

	//TODO On init load AllScrollRecords from file

	for {
		time.Sleep(1 * time.Minute)
		allScrollRecordsLock.Lock()
		for scroll_id, scrollRecord := range allScrollRecords {
			if scrollRecord == nil {
				continue
			}
			if scrollRecord.Valid && isScrollExpired(scrollRecord.TimeOut) {
				scrollRecord.Valid = false
				err := scrollRecord.FlushScrollContextToFile()
				if err != nil {
					log.Errorf("checkStaleScrollContext: failed to flush scroll context %v, err=%v", scroll_id, err)
					continue
				}
				log.Infof("Scroll Context Expired %v", scroll_id)
				//delete result file with scroll_id
				removeScrollResultFile(scroll_id)
			}
		}
		allScrollRecordsLock.Unlock()
	}
}

func removeScrollResultFile(scroll_id string) {
	filename := getScrollResultsFilename(getBaseScrollDir(), scroll_id)
	e := os.Remove(filename)
	if e != nil {
		log.Errorf("Error in removeScrollResultFile %v", e)
	}
}

func isScrollExpired(TimeOut uint64) bool {
	return TimeOut < segutils.GetCurrentTimeMillis()
}

func getBaseScrollDir() string {

	var sb strings.Builder
	sb.WriteString(config.GetRunningConfig().DataPath)
	sb.WriteString(config.GetHostID())
	sb.WriteString("/scroll/")
	basedir := sb.String()
	return basedir
}

func getScrollFilename(baseDir string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		return ""
	}
	sb.WriteString(baseDir)
	sb.WriteString("scroll.csv")
	return sb.String()
}

func getScrollResultsFilename(baseDir string, scroll_id string) string {
	var sb strings.Builder

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		return ""
	}
	sb.WriteString(baseDir)
	sb.WriteString(scroll_id + ".csv")
	return sb.String()
}

/*
Loads scroll from file
*/
func loadScrollContextFromFile(scroll_id string) (*Scroll, error) {
	scrollRecord := &Scroll{}
	filename := getScrollResultsFilename(getBaseScrollDir(), scroll_id)
	fd, err := os.OpenFile(filename, os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	defer fd.Close()
	err = json.NewDecoder(fd).Decode(scrollRecord)
	return scrollRecord, err
}

func (scroll *Scroll) FlushScrollContextToFile() error {

	filename := getScrollFilename(getBaseScrollDir())
	fd, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return err
	}
	defer fd.Close()
	w := csv.NewWriter(fd)
	var record []string
	var records [][]string

	record = append(record, scroll.Scroll_id, scroll.Expiry, fmt.Sprint(scroll.Offset), fmt.Sprint(scroll.Size), fmt.Sprint(scroll.TimeOut), fmt.Sprint(scroll.Valid))
	records = append(records, record)

	err = w.WriteAll(records)
	if err != nil {
		log.Errorf("flushScrollContextToFile: write failed, filename=%v, err=%v", filename, err)
		return err
	}
	w.Flush()
	return nil
}

func (scrollRecord *Scroll) WriteScrollResultToFile() error {
	filename1 := getScrollResultsFilename(getBaseScrollDir(), scrollRecord.Scroll_id)
	fd1, err := os.OpenFile(filename1, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return err
	}
	defer fd1.Close()
	jdata1, err := json.MarshalIndent(scrollRecord, "", " ")
	if err != nil {
		log.Errorf("WriteScrollResultToFile error: %v", err)
		return err
	}
	_, err = io.Copy(fd1, bytes.NewReader(jdata1))
	if err != nil {
		log.Errorf("WriteScrollResultToFile: io copy failed, err=%v", err)
		return err
	}
	return nil

}

func ForcedFlushToScrollFile() {
	log.Infof("Flushing scroll context to file")
	allScrollRecordsLock.Lock()
	for _, scrollRecord := range allScrollRecords {
		err := scrollRecord.FlushScrollContextToFile()
		if err != nil {
			log.Errorf("Forcedflushtoscrollfile: flush failed, scrollRecord=%v, err=%v", scrollRecord, err)
		}
	}
	allScrollRecordsLock.Unlock()
}

func GetScrollRecord(scroll_id string, timeOut string, sizeLimit uint64) *Scroll {
	allScrollRecordsLock.Lock()
	var scrollRecord *Scroll
	_, present := allScrollRecords[scroll_id]
	if !present {
		scroll_id = uuid.New().String()
		scrollRecord = &Scroll{Scroll_id: scroll_id, Expiry: timeOut, Offset: 0, Size: sizeLimit, Valid: true}
		allScrollRecords[scroll_id] = scrollRecord
	} else if present && allScrollRecords[scroll_id].Valid {
		//read scrollRecord from file
		scrollRecord, _ = loadScrollContextFromFile(scroll_id)
	}
	allScrollRecordsLock.Unlock()
	return scrollRecord
}

func GetScrollTimeOut(scrollTimeout string, qid uint64) (uint64, error) {
	var validTimeUnitRegex = regexp.MustCompile(`^([0-9])+(.*)$`)
	scrollTime := validTimeUnitRegex.FindStringSubmatch(scrollTimeout)
	scrollExpiry := utils.GetCurrentTimeInMs()
	if len(scrollTime) >= 3 {
		if scrollTimeValue, err := strconv.ParseUint(scrollTime[1], 10, 64); err == nil {
			switch scrollTime[2] {
			case "d":
				log.Errorf("qid=%d, InvalidTimeUnit for scroll %v", qid, scrollTime)
				return 0, errors.New("InvalidTimeUnit for scroll")
			case "h":
				scrollExpiry = scrollExpiry + scrollTimeValue*60*60*1000
			case "m":
				scrollExpiry = scrollExpiry + scrollTimeValue*60*1000
			case "s":
				scrollExpiry = scrollExpiry + scrollTimeValue*1000
			case "ms":
				scrollExpiry = scrollExpiry + scrollTimeValue
			case "micros":
				log.Errorf("qid=%d, InvalidTimeUnit for scroll %v", qid, scrollTime)
				return 0, errors.New("InvalidTimeUnit for scroll")
			case "nanos":
				log.Errorf("qid=%d, InvalidTimeUnit for scroll %v", qid, scrollTime)
				return 0, errors.New("InvalidTimeUnit for scroll")
			default:
				log.Errorf("qid=%d, InvalidTimeUnit for scroll %v ", qid, scrollTime)
				return 0, errors.New("InvalidTimeUnit for scroll")
			}
		} else {
			log.Errorf("qid=%d, InvalidTimeUnit for scroll %v ", qid, scrollTime)
			return 0, errors.New("InvalidTimeUnit for scroll")
		}
	} else {
		log.Errorf("qid=%d, InvalidTimeUnit for scroll %v ", qid, scrollTime)
		return 0, errors.New("InvalidTimeUnit for scroll")
	}

	return scrollExpiry, nil

}

/*
Returns if the scroll id is valid.

False if does not exist or is not valid
*/
func IsScrollIdValid(scrollId string) bool {
	allScrollRecordsLock.RLock()
	scroll, ok := allScrollRecords[scrollId]
	allScrollRecordsLock.RUnlock()
	if !ok {
		return false
	}
	return scroll.Valid
}

/*
Sets scroll based on scroll id

Interally protects against concurrent scroll operations
*/
func SetScrollRecord(scrollId string, scroll *Scroll) {
	allScrollRecordsLock.Lock()
	allScrollRecords[scrollId] = scroll
	allScrollRecordsLock.Unlock()
}

/*
Returns the total hits of scroll.

Returns 0 if scroll does not exist
*/
func GetScrollTotalHits(scrollId string) uint64 {
	allScrollRecordsLock.RLock()
	scroll, ok := allScrollRecords[scrollId]
	allScrollRecordsLock.RUnlock()
	if !ok {
		return 0
	}
	return scroll.Results.Hits.GetHits()
}
