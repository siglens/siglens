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
	"path/filepath"
	"syscall"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/valyala/fasthttp"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
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

type InodeStats struct {
	TotalInodes uint64 `json:"totalInodes"`
	UsedInodes  uint64 `json:"usedInodes"`
	FreeInodes  uint64 `json:"freeInodes"`
}

func getMemoryInfo() (*MemoryInfo, error) {
	memoryInUse, err := config.GetContainerMemoryUsage()
	if err != nil {
		memInfo, err := mem.VirtualMemory()
		if err != nil {
			return nil, err
		}

		return &MemoryInfo{
			Total:       memInfo.Total,
			Free:        memInfo.Free,
			UsedPercent: memInfo.UsedPercent,
		}, nil
	}

	totalMemory := config.GetMemoryMax()
	freeMemory := totalMemory - memoryInUse

	return &MemoryInfo{
		Total:       totalMemory,
		Free:        freeMemory,
		UsedPercent: float64(memoryInUse) / float64(totalMemory) * 100,
	}, nil
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

	memInfo, err := getMemoryInfo()
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("GetSystemInfo: Failed to retrieve memory info: %v", err)
		return
	}

	diskInfo, err := disk.Usage(config.GetDataPath())
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

func GetInodeStats(ctx *fasthttp.RequestCtx) {
	dataPath := config.GetDataPath()
	var stat syscall.Statfs_t

	err := syscall.Statfs(filepath.Clean(dataPath), &stat)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		log.Errorf("GetInodeStats: Failed to retrieve inode stats: %v", err)
		return
	}

	total := stat.Files
	free := stat.Ffree
	used := total - free

	inodeStats := InodeStats{
		TotalInodes: total,
		UsedInodes:  used,
		FreeInodes:  free,
	}

	response, err := json.Marshal(inodeStats)
	if err != nil {
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		fmt.Fprintf(ctx, "Failed to marshal inode stats: %v", err)
		return
	}

	ctx.SetContentType("application/json")
	ctx.SetBody(response)
}

func ProcessVersionInfo(ctx *fasthttp.RequestCtx) {
	if hook := hooks.GlobalHooks.ProcessVersionInfoHook; hook != nil {
		hook(ctx)
	} else {
		responseBody := make(map[string]interface{})
		ctx.SetStatusCode(fasthttp.StatusOK)
		responseBody["version"] = config.SigLensVersion
		utils.WriteJsonResponse(ctx, responseBody)
	}
}
