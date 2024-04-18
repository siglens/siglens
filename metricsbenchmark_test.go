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

package bench

import (
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/buger/jsonparser"
	localstorage "github.com/siglens/siglens/pkg/blob/local"
	"github.com/siglens/siglens/pkg/config"
	otsdbquery "github.com/siglens/siglens/pkg/integrations/otsdb/query"
	otsdbwriter "github.com/siglens/siglens/pkg/integrations/otsdb/writer"
	"github.com/siglens/siglens/pkg/segment"
	"github.com/siglens/siglens/pkg/segment/memory/limit"
	"github.com/siglens/siglens/pkg/segment/query"
	"github.com/siglens/siglens/pkg/segment/writer"
	serverutils "github.com/siglens/siglens/pkg/server/utils"
	"github.com/valyala/fastrand"

	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/segment/writer/metrics/meta"
	log "github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

var rawJson = []byte(`[{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"white","fuel_type":"LPG","group":"group 1","model":"Slk350"},"timestamp":1677604613,"value":595},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"green","fuel_type":"LPG","group":"group 1","model":"Xlr"},"timestamp":1677604613,"value":316},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"black","fuel_type":"LPG","group":"group 0","model":"4runner 4wd"},"timestamp":1677604613,"value":316},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"gray","fuel_type":"LPG","group":"group 0","model":"Forenza"},"timestamp":1677604613,"value":316},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"purple","fuel_type":"Gasoline","group":"group 0","model":"E320 Cdi"},"timestamp":1677604613,"value":15},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"blue","fuel_type":"Diesel","group":"group 0","model":"Gs 300/gs 430"},"timestamp":1677604613,"value":15},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"white","fuel_type":"Ethanol","group":"group 0","model":"L-147/148 Murcielago"},"timestamp":1677604613,"value":806},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"olive","fuel_type":"CNG","group":"group 0","model":"Colorado Cab Chassis Inc 2wd"},"timestamp":1677604613,"value":549},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"silver","fuel_type":"LPG","group":"group 0","model":"Ranger Pickup 2wd"},"timestamp":1677604613,"value":549},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"navy","fuel_type":"CNG","group":"group 0","model":"V70 R Awd"},"timestamp":1677604613,"value":986},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"fuchsia","fuel_type":"LPG","group":"group 1","model":" 325xi"},"timestamp":1677604613,"value":146},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"purple","fuel_type":"Electric","group":"group 0","model":"Uplander Fwd"},"timestamp":1677604613,"value":409},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"olive","fuel_type":"Electric","group":"group 0","model":"Ridgeline 4wd"},"timestamp":1677604613,"value":409},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"purple","fuel_type":"Electric","group":"group 0","model":" 530xi Sport Wagon"},"timestamp":1677604613,"value":409},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"purple","fuel_type":"CNG","group":"group 0","model":"Gti"},"timestamp":1677604613,"value":409},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"silver","fuel_type":"LPG","group":"group 0","model":"Forenza Wagon"},"timestamp":1677604613,"value":409},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"aqua","fuel_type":"Electric","group":"group 1","model":"S60 R Awd"},"timestamp":1677604613,"value":795},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"fuchsia","fuel_type":"LPG","group":"group 0","model":"A4 Cabriolet"},"timestamp":1677604613,"value":637},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"green","fuel_type":"Diesel","group":"group 1","model":"Odyssey 2wd"},"timestamp":1677604613,"value":637},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"aqua","fuel_type":"Electric","group":"group 0","model":"Zephyr"},"timestamp":1677604613,"value":721},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"green","fuel_type":"Gasoline","group":"group 0","model":"Accent"},"timestamp":1677604613,"value":834},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"purple","fuel_type":"CNG","group":"group 1","model":" X5"},"timestamp":1677604613,"value":834},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"green","fuel_type":"Ethanol","group":"group 0","model":"S4"},"timestamp":1677604613,"value":442},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"yellow","fuel_type":"LPG","group":"group 0","model":" 750li"},"timestamp":1677604613,"value":442},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"aqua","fuel_type":"Electric","group":"group 0","model":"Ranger Pickup 4wd"},"timestamp":1677604613,"value":5},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"maroon","fuel_type":"Gasoline","group":"group 0","model":"Sonata"},"timestamp":1677604613,"value":273},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"yellow","fuel_type":"Ethanol","group":"group 1","model":"V12 Vanquish S"},"timestamp":1677604613,"value":273},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"fuchsia","fuel_type":"LPG","group":"group 0","model":"Accord Hybrid"},"timestamp":1677604613,"value":493},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"aqua","fuel_type":"Electric","group":"group 1","model":"Passat"},"timestamp":1677604613,"value":493},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"lime","fuel_type":"Ethanol","group":"group 0","model":"Cts"},"timestamp":1677604613,"value":493},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"olive","fuel_type":"CNG","group":"group 1","model":"S40 Fwd"},"timestamp":1677604613,"value":432},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"lime","fuel_type":"LPG","group":"group 1","model":"Rendezvous Fwd"},"timestamp":1677604613,"value":432},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"gray","fuel_type":"LPG","group":"group 0","model":"Tt Coupe"},"timestamp":1677604613,"value":113},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"fuchsia","fuel_type":"LPG","group":"group 0","model":" Mini Cooper Convertible"},"timestamp":1677604613,"value":113},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"maroon","fuel_type":"CNG","group":"group 1","model":"V50 Awd"},"timestamp":1677604613,"value":113},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"aqua","fuel_type":"Gasoline","group":"group 1","model":"Dakota Pickup 4wd"},"timestamp":1677604613,"value":169},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"purple","fuel_type":"Electric","group":"group 1","model":"Camry Solara"},"timestamp":1677604613,"value":169},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"purple","fuel_type":"Methanol","group":"group 1","model":"S4"},"timestamp":1677604613,"value":609},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"navy","fuel_type":"Electric","group":"group 1","model":"Five Hundred Awd"},"timestamp":1677604613,"value":609},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"aqua","fuel_type":"Electric","group":"group 1","model":"A8 L"},"timestamp":1677604613,"value":722},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"lime","fuel_type":"LPG","group":"group 1","model":"Thunderbird"},"timestamp":1677604613,"value":495},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"olive","fuel_type":"Diesel","group":"group 1","model":"Grand Cherokee 2wd"},"timestamp":1677604613,"value":495},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"purple","fuel_type":"Methanol","group":"group 1","model":"Pathfinder 2wd"},"timestamp":1677604613,"value":184},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"maroon","fuel_type":"Diesel","group":"group 1","model":"Escalade 2wd"},"timestamp":1677604613,"value":543},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"olive","fuel_type":"CNG","group":"group 1","model":"Thunderbird"},"timestamp":1677604613,"value":543},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"purple","fuel_type":"Ethanol","group":"group 1","model":"Monte Carlo"},"timestamp":1677604613,"value":633},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"yellow","fuel_type":"CNG","group":"group 0","model":"Yaris"},"timestamp":1677604613,"value":633},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"olive","fuel_type":"Methanol","group":"group 1","model":"Rx 330 2wd"},"timestamp":1677604613,"value":574},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"aqua","fuel_type":"Electric","group":"group 0","model":"Gs 300 4wd"},"timestamp":1677604613,"value":569},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"fuchsia","fuel_type":"Gasoline","group":"group 1","model":"F150 Pickup 2wd"},"timestamp":1677604613,"value":903},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"navy","fuel_type":"Diesel","group":"group 1","model":" 325xi"},"timestamp":1677604613,"value":903},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"maroon","fuel_type":"LPG","group":"group 0","model":"Aveo"},"timestamp":1677604613,"value":903},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"purple","fuel_type":"Methanol","group":"group 0","model":"Mdx 4wd"},"timestamp":1677604613,"value":395},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"purple","fuel_type":"Methanol","group":"group 0","model":"C8 Spyder"},"timestamp":1677604613,"value":703},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"aqua","fuel_type":"CNG","group":"group 0","model":"S4 Cabriolet"},"timestamp":1677604613,"value":703},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"maroon","fuel_type":"Gasoline","group":"group 1","model":" Z4 3.0i"},"timestamp":1677604613,"value":528},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"teal","fuel_type":"CNG","group":"group 1","model":"Cr-v 4wd"},"timestamp":1677604613,"value":528},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"maroon","fuel_type":"CNG","group":"group 0","model":"F150 Pickup 2wd"},"timestamp":1677604613,"value":626},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"teal","fuel_type":"Methanol","group":"group 0","model":"Sts Awd"},"timestamp":1677604613,"value":626},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"aqua","fuel_type":"Ethanol","group":"group 0","model":"Wrangler/tj 4wd"},"timestamp":1677604613,"value":626},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"purple","fuel_type":"Electric","group":"group 0","model":"Xc 70 Awd"},"timestamp":1677604613,"value":626},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"aqua","fuel_type":"Gasoline","group":"group 1","model":"Tucson 4wd"},"timestamp":1677604613,"value":94},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"blue","fuel_type":"Diesel","group":"group 1","model":"Colorado Crew Cab 4wd"},"timestamp":1677604613,"value":94},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"navy","fuel_type":"Electric","group":"group 1","model":"Tl"},"timestamp":1677604613,"value":94},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"silver","fuel_type":"Diesel","group":"group 1","model":"G35"},"timestamp":1677604613,"value":421},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"silver","fuel_type":"Electric","group":"group 1","model":"C8 Spyder"},"timestamp":1677604613,"value":208},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"fuchsia","fuel_type":"Diesel","group":"group 1","model":"V70 R Awd"},"timestamp":1677604613,"value":208},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"white","fuel_type":"CNG","group":"group 0","model":" M3 Convertible"},"timestamp":1677604613,"value":189},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"blue","fuel_type":"Electric","group":"group 0","model":"Clk55 Amg (cabriolet)"},"timestamp":1677604613,"value":189},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"silver","fuel_type":"LPG","group":"group 1","model":"A4 Avant Quattro"},"timestamp":1677604613,"value":624},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"navy","fuel_type":"Diesel","group":"group 1","model":"Titan 4wd"},"timestamp":1677604613,"value":962},{"metric":"test.metric.0","tags":{"car_type":"Passenger car compact","color":"yellow","fuel_type":"Electric","group":"group 0","model":"H3 4wd"},"timestamp":1677604613,"value":962},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"aqua","fuel_type":"Ethanol","group":"group 0","model":"Pathfinder 2wd"},"timestamp":1677604613,"value":962},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"olive","fuel_type":"LPG","group":"group 1","model":"Trailblazer Ext 4wd"},"timestamp":1677604613,"value":962},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"purple","fuel_type":"Diesel","group":"group 0","model":"Solstice"},"timestamp":1677604613,"value":217},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"gray","fuel_type":"Methanol","group":"group 1","model":" Mini Cooper S Convertible"},"timestamp":1677604613,"value":140},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"white","fuel_type":"LPG","group":"group 0","model":"Sl65 Amg"},"timestamp":1677604613,"value":140},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"fuchsia","fuel_type":"Methanol","group":"group 0","model":"Ferrari 612 Scaglietti"},"timestamp":1677604613,"value":974},{"metric":"test.metric.0","tags":{"car_type":"Pickup truck","color":"gray","fuel_type":"LPG","group":"group 1","model":"Coupe Cambiocorsa/gt/g-sport"},"timestamp":1677604613,"value":884},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"fuchsia","fuel_type":"Diesel","group":"group 1","model":"Five Hundred Awd"},"timestamp":1677604613,"value":884},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"fuchsia","fuel_type":"Ethanol","group":"group 1","model":"Elise/exige"},"timestamp":1677604613,"value":374},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"aqua","fuel_type":"Ethanol","group":"group 0","model":"Milan"},"timestamp":1677604613,"value":437},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"blue","fuel_type":"Electric","group":"group 1","model":"Vdp Lwb"},"timestamp":1677604613,"value":931},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"lime","fuel_type":"Methanol","group":"group 0","model":"Crown Victoria Police"},"timestamp":1677604613,"value":931},{"metric":"test.metric.0","tags":{"car_type":"Passenger car mini","color":"aqua","fuel_type":"Electric","group":"group 0","model":"Outlander 2wd"},"timestamp":1677604613,"value":547},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"maroon","fuel_type":"Gasoline","group":"group 0","model":"E150 Econoline 2wd"},"timestamp":1677604613,"value":547},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"lime","fuel_type":"Ethanol","group":"group 1","model":"Ls 430"},"timestamp":1677604613,"value":547},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"lime","fuel_type":"Ethanol","group":"group 1","model":"C15 Silverado Hybrid 2wd"},"timestamp":1677604613,"value":161},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"gray","fuel_type":"Electric","group":"group 1","model":"Sportage 4wd"},"timestamp":1677604613,"value":146},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"white","fuel_type":"Methanol","group":"group 1","model":"Frontier V6-4wd"},"timestamp":1677604613,"value":146},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"green","fuel_type":"Diesel","group":"group 1","model":"S60 Fwd"},"timestamp":1677604613,"value":146},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"purple","fuel_type":"Electric","group":"group 1","model":"C350"},"timestamp":1677604613,"value":146},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"silver","fuel_type":"Diesel","group":"group 0","model":"Mustang"},"timestamp":1677604613,"value":146},{"metric":"test.metric.0","tags":{"car_type":"Passenger car medium","color":"lime","fuel_type":"Gasoline","group":"group 0","model":" 750li"},"timestamp":1677604613,"value":860},{"metric":"test.metric.0","tags":{"car_type":"Sport utility vehicle","color":"black","fuel_type":"CNG","group":"group 1","model":"Maybach 57s"},"timestamp":1677604613,"value":505},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"maroon","fuel_type":"Gasoline","group":"group 0","model":"Escalade 2wd"},"timestamp":1677604613,"value":505},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"navy","fuel_type":"Ethanol","group":"group 0","model":"F150 Pickup 2wd"},"timestamp":1677604613,"value":693},{"metric":"test.metric.0","tags":{"car_type":"Van","color":"olive","fuel_type":"Electric","group":"group 1","model":"Pathfinder 2wd"},"timestamp":1677604613,"value":49},{"metric":"test.metric.0","tags":{"car_type":"Passenger car heavy","color":"silver","fuel_type":"Electric","group":"group 0","model":"S60 R Awd"},"timestamp":1677604613,"value":49},{"metric":"test.metric.0","tags":{"car_type":"Passenger car light","color":"green","fuel_type":"Methanol","group":"group 0","model":"Scion Tc"},"timestamp":1677604613,"value":49}]`)

func Benchmark_InsertJson(b *testing.B) {
	config.InitializeTestingConfig()
	writer.InitWriterNode()

	sTime := time.Now()
	totalSuccess := uint64(0)
	for i := 0; i < 10_000; i++ {
		success, fail, err := otsdbwriter.HandlePutMetrics(rawJson, uint64(0))
		assert.NoError(b, err)
		assert.Equal(b, success, uint64(100))
		assert.Equal(b, fail, uint64(0))
		atomic.AddUint64(&totalSuccess, success)
		if i%1_000 == 0 {
			log.Infof("Ingested %+v metrics in %+v", totalSuccess, time.Since(sTime))
		}
	}
	log.Infof("Ingested %+v metrics in %+v", totalSuccess, time.Since(sTime))

	/*
	   cd pkg/otsdb/writer
	   go test -run=Bench -bench=Benchmark_InsertJson  -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_InsertJson -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out

	*/
	err := os.RemoveAll(config.GetDataPath())
	assert.NoError(b, err)
}

type TagsTreeBenchmarkObject struct {
	metricName []byte
	tagsHolder metrics.TagsHolder
	tsid       uint64
}

func Benchmark_TagsTree(b *testing.B) {
	numMetrics := uint32(1)
	numTags := uint32(10)

	config.InitializeTestingConfig()
	writer.InitWriterNode()
	metrics.InitTestingConfig()
	metrics.InitMetricsSegStore()
	tTime := int64(0)
	entryCount := 1_000_000
	fakeData := make([]TagsTreeBenchmarkObject, entryCount)
	for i := 0; i < entryCount; i++ {
		mName := fmt.Sprintf("metric-%d", fastrand.Uint32n(numMetrics))
		numRandomTags := fastrand.Uint32n(numTags)
		tagsholder := metrics.GetTagsHolder()
		for j := 0; j < int(numRandomTags); j++ {
			randomTag := fmt.Sprintf("tag-%d", fastrand.Uint32n(numRandomTags))
			randomTagValue := fmt.Sprintf("random-string-%d", fastrand.Uint32n(10_000))
			randomTagValueType := 1
			tagsholder.Insert(randomTag, []byte(randomTagValue), jsonparser.ValueType(randomTagValueType))
		}
		tsid, _ := tagsholder.GetTSID([]byte(mName))
		fakeData[i] = TagsTreeBenchmarkObject{
			metricName: []byte(mName),
			tagsHolder: *tagsholder,
			tsid:       tsid,
		}
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < entryCount; i++ {
		sTime := time.Now()
		err := metrics.GetTagsTreeHolder(0, "0").AddTagsForTSID(fakeData[i].metricName, &fakeData[i].tagsHolder, fakeData[i].tsid)
		if err != nil {
			log.Errorf("Error adding tags for tsid!")
		}
		tTime += time.Since(sTime).Milliseconds()
	}
	log.Infof("Average tags tree ingest finished in time %.2fms", float64(tTime)/float64(entryCount))
}

func Benchmark_MetricsEndToEnd(b *testing.B) {

	/*
	   go test -run=Bench -bench=Benchmark_MetricsEndToEnd -cpuprofile cpuprofile.out -o rawsearch_cpu
	   go tool pprof ./rawsearch_cpu cpuprofile.out

	   (for mem profile)
	   go test -run=Bench -bench=Benchmark_MetricsEndToEnd -benchmem -memprofile memprofile.out -o rawsearch_mem
	   go tool pprof ./rawsearch_mem memprofile.out
	*/

	config.InitializeDefaultConfig()
	_ = localstorage.InitLocalStorage()
	limit.InitMemoryLimiter()
	metrics.InitTestingConfig()
	err := meta.InitMetricsMeta()
	if err != nil {
		b.Fatalf("failed to initialize metrics meta")
	}

	baseDir := "/Users/ssubramanian/Desktop/SigLens/siglens/data/ingestnodes/Sris-MBP.attlocal.net/"
	config.SetSmrBaseDirForTestOnly(baseDir)
	err = query.InitQueryNode(getMyIds, serverutils.ExtractKibanaRequests)
	if err != nil {
		b.Fatalf("failed to initialize metrics meta")
	}
	startTime := "1678060800"
	endTime := "1678147140"
	m := "max:1h-sum:cpu.usage_guest_nice{region=*}"
	mQRequest, err := otsdbquery.ParseRequest(startTime, endTime, m, 0)
	assert.NoError(b, err)
	segment.LogMetricsQuery("metrics query parser", mQRequest, 1)
	count := 25
	b.ResetTimer()
	b.ReportAllocs()
	tTime := int64(0)
	for i := 0; i < count; i++ {
		sTime := time.Now()
		mQResponse := segment.ExecuteMetricsQuery(&mQRequest.MetricsQuery, &mQRequest.TimeRange, uint64(i))
		if mQResponse == nil {
			b.Fatal("Benchmark_MetricsEndToEnd: Failed to get metrics query response")
		}
		mResult, err := mQResponse.GetOTSDBResults(&mQRequest.MetricsQuery)
		assert.NoError(b, err)
		log.Errorf("Query %d has %d series in %+v", i, len(mResult), time.Since(sTime))
		tTime += time.Since(sTime).Milliseconds()
	}
	log.Errorf("After %d iterations avg latency %.2fms", count, float64(tTime)/float64(count))
}
