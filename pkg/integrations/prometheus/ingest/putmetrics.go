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

package writer

import (
	"encoding/json"
	"fmt"
	"math"

	wal "github.com/siglens/siglens/pkg/segment/writer/wal"

	"github.com/buger/jsonparser"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/prompb"
	"github.com/siglens/siglens/pkg/grpc"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/segment/writer/metrics"
	"github.com/siglens/siglens/pkg/usageStats"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type PrometheusPutResp struct {
	Failed  uint64   `json:"failed"`
	Success uint64   `json:"success"`
	Errors  []string `json:"errors,omitempty"`
}

const (
	NAME = "__name__"
)

type WalData struct {
	TsidLookup map[uint64]int
	AllSeries  []*metrics.TimeSeries
}

func getTimeSeries(tsid uint64, wd *WalData) (*metrics.TimeSeries, bool) {
	var ts *metrics.TimeSeries
	idx, ok := wd.TsidLookup[tsid]
	if !ok {
		return ts, false
	}
	ts = wd.AllSeries[idx]
	return ts, true
}

func insertTimeSeries(tsid uint64, ts *metrics.TimeSeries, wd *WalData) {
	_, ok := wd.TsidLookup[tsid]
	if !ok {
		wd.TsidLookup[tsid] = len(wd.AllSeries)
		wd.AllSeries = append(wd.AllSeries, ts)
	}
}

func flushWal(wd *WalData) {
	var Tsids []uint64
	Compressed := make(map[uint64][]byte)
	for tsid, index := range wd.TsidLookup {
		Tsids = append(Tsids, tsid)
		err := wd.AllSeries[index].CFinishFn()
		if err != nil {
			log.Errorf("flushWal: Could not mark the finish of raw encoding time series, err:%v", err)
		}
		Compressed[tsid] = wd.AllSeries[index].RawEncoding.Bytes()
	}

	walManager, _ := wal.CreateWal("0", "0", "0", 0)
	block := wal.TimeSeriesBlock{
		Tsids:      Tsids,
		Compressed: Compressed,
	}
	walManager.FlushWal(block)
}

func decodeWriteRequest(compressed []byte) (*prompb.WriteRequest, error) {
	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		log.Errorf("decodeWriteRequest: Error decompressing request body. Compressed length: %v, data: %v, err=%v", len(compressed), compressed, err)
		return nil, err
	}
	var req prompb.WriteRequest
	if err := proto.Unmarshal(reqBuf, &req); err != nil {
		log.Errorf("decodeWriteRequest: Error unmarshalling request body %v, err=%v", reqBuf, err)
		return nil, err
	}
	return &req, nil
}

func PutMetrics(ctx *fasthttp.RequestCtx, myid int64) {
	if hook := hooks.GlobalHooks.OverrideIngestRequestHook; hook != nil {
		alreadyHandled := hook(ctx, myid, grpc.INGEST_FUNC_PROMETHEUS_METRICS, false)
		if alreadyHandled {
			return
		}
	}

	var processedCount uint64
	var failedCount uint64
	var err error
	version := string(ctx.Request.Header.Peek("X-Prometheus-Remote-Write-Version"))
	if version != "0.1.0" {
		log.Errorf("PutMetrics: Unsupported remote write protocol version %v, expected 0.1.0", version)
		writePrometheusResponse(ctx, processedCount, failedCount, "unsupported remote write protocol", fasthttp.StatusBadRequest)
		return
	}
	cType := string(ctx.Request.Header.ContentType())
	if cType != "application/x-protobuf" {
		log.Errorf("PutMetrics: unknown content type: %s, expected application/x-protobuf", cType)
		writePrometheusResponse(ctx, processedCount, failedCount, "unknown content type", fasthttp.StatusBadRequest)
		return
	}
	encoding := string(ctx.Request.Header.ContentEncoding())
	if encoding != "snappy" {
		log.Errorf("PutMetrics: unknown content encoding %s, expected snappy", encoding)
		writePrometheusResponse(ctx, processedCount, failedCount, "unknown content encoding", fasthttp.StatusBadRequest)
		return
	}

	compressed := ctx.PostBody()
	processedCount, failedCount, err = HandlePutMetrics(compressed, myid)
	if err != nil {
		log.Errorf("PutMetrics: failed to handle put metrics for compressed data: %v. err=%+v", compressed, err)
		writePrometheusResponse(ctx, processedCount, failedCount, err.Error(), fasthttp.StatusBadRequest)
		return
	}
	writePrometheusResponse(ctx, processedCount, failedCount, "", fasthttp.StatusOK)
}

func HandlePutMetrics(compressed []byte, myid int64) (uint64, uint64, error) {
	var successCount uint64 = 0
	var failedCount uint64 = 0

	req, err := decodeWriteRequest(compressed)
	if err != nil {
		err = fmt.Errorf("HandlePutMetrics: failed to decode request %v, err=%v", compressed, err)
		log.Errorf(err.Error())
		return successCount, failedCount, err
	}

	var walData = &WalData{
		TsidLookup: make(map[uint64]int),
		AllSeries:  []*metrics.TimeSeries{},
	}

	for _, ts := range req.Timeseries {
		tagHolder := metrics.GetTagsHolder()
		var mName []byte
		for _, l := range ts.Labels {
			if l.Name == NAME {
				mName = []byte(l.Value)
				continue
			}
			tagHolder.Insert(l.Name, []byte(l.Value), jsonparser.String)
		}

		for _, s := range ts.Samples {

			if isBadValue(float64(s.Value)) {
				failedCount++
				continue
			}

			ts1 := parseTimestamp(s.Timestamp)

			tsid, err := tagHolder.GetTSID(mName)
			AddWalSeriesEntry(tsid, walData, s.Value, ts1)

			err = metrics.EncodeDatapoint(mName, tagHolder, s.Value, ts1, uint64(len(compressed)), myid, tsid)
			if err != nil {
				log.Errorf("HandlePutMetrics: failed to encode data for metric=%s, orgid=%v, err=%v", mName, myid, err)
				failedCount++
				continue
			}
			successCount++

		}
	}

	flushWal(walData)
	bytesReceived := uint64(len(compressed))
	usageStats.UpdateMetricsStats(bytesReceived, successCount, myid)
	return successCount, failedCount, nil
}

func AddWalSeriesEntry(tsid uint64, walData *WalData, dpVal float64, timeStamp uint32) {

	var ts *metrics.TimeSeries
	var exists bool
	var err error
	ts, exists = getTimeSeries(tsid, walData)

	if !exists {
		ts, _, err = metrics.InitTimeSeries(tsid, dpVal, timeStamp)
		if err != nil {
			log.Errorf("AddWalSeriesEntry: failed to create time series for tsid=%v, dp=%v, timestamp=%v, err=%v",
				tsid, dpVal, timeStamp, err)
		}
		insertTimeSeries(tsid, ts, walData)
	} else {
		_, err := ts.Compressor.Compress(timeStamp, dpVal)
		if err != nil {
			log.Errorf("AddWalSeriesEntry: failed to compress dpTS=%v, dpVal=%v, num entries=%v, err=%v", timeStamp, dpVal, ts.NEntries, err)
		}
		ts.NEntries++
		ts.LastKnownTS = timeStamp
	}
}

func parseTimestamp(timestamp int64) uint32 {
	var ts uint32
	if utils.IsTimeInNano(uint64(timestamp)) {
		ts = uint32(timestamp / 1_000_000_000)
	} else if utils.IsTimeInMilli(uint64(timestamp)) {
		ts = uint32(timestamp / 1000)
	} else {
		ts = uint32(timestamp)
	}
	return ts
}

func isBadValue(v float64) bool {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return true
	}

	return false
}

func writePrometheusResponse(ctx *fasthttp.RequestCtx, processedCount uint64, failedCount uint64, err string, code int) {
	resp := PrometheusPutResp{Success: processedCount, Failed: failedCount}
	if err != "" {
		resp.Errors = []string{err}
	}

	ctx.SetStatusCode(code)
	ctx.SetContentType(utils.ContentJson)
	jval, mErr := json.Marshal(resp)
	if mErr != nil {
		log.Errorf("writePrometheusResponse: failed to marshal %v to json, err=%v", resp, mErr)
		return
	}
	_, mErr = ctx.Write(jval)

	if mErr != nil {
		log.Errorf("writePrometheusResponse: failed to write jval=%v to http context, err=%v", jval, mErr)
		return
	}
}
