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

package systemconfig

import (
	"encoding/json"
	"fmt"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/valyala/fasthttp"

	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"

	"math"
	"time"
)

type SystemInfo struct {
	OS     string     `json:"os"`
	VCPU   int        `json:"v_cpu"`
	Memory MemoryInfo `json:"memory"`
	Disk   DiskInfo   `json:"disk"`
	Uptime int        `json:"uptime"`
}

type MemoryInfo struct {
	Total       uint64  `json:"total"`
	Free        uint64  `json:"free"`
	UsedPercent float64 `json:"used_percent"`
}

type DiskInfo struct {
	Total       uint64  `json:"total"`
	Free        uint64  `json:"free"`
	UsedPercent float64 `json:"used_percent"`
}

func GetSystemInfo(ctx *fasthttp.RequestCtx) {
	cpuInfo, err := cpu.Info()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("GetSystemInfo: Failed to retrieve CPU info: %v", err)
		return
	}

	var totalCores int
	for _, info := range cpuInfo {
		totalCores += int(info.Cores)
	}

	memInfo, err := mem.VirtualMemory()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("GetSystemInfo: Failed to retrieve memory info: %v", err)
		return
	}

	diskInfo, err := disk.Usage("/")
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("GetSystemInfo: Failed to retrieve disk info: %v", err)
		return
	}

	hostInfo, err := host.Info()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("GetSystemInfo: Failed to retrieve host info: %v", err)
		return
	}

	uptime := math.Round(time.Since(utils.GetServerStartTime()).Minutes())

	systemInfo := SystemInfo{
		OS:   hostInfo.OS,
		VCPU: totalCores,
		Memory: MemoryInfo{
			Total:       memInfo.Total,
			Free:        memInfo.Free,
			UsedPercent: memInfo.UsedPercent,
		},
		Disk: DiskInfo{
			Total:       diskInfo.Total,
			Free:        diskInfo.Free,
			UsedPercent: diskInfo.UsedPercent,
		},
		Uptime: int(uptime),
	}

	response, err := json.Marshal(systemInfo)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		fmt.Fprintf(ctx, "Failed to marshal system info: %v", err)
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetBody(response)
}
