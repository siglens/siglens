package systemconfig

import (
	"encoding/json"
	"fmt"

	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/host"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/valyala/fasthttp"

	log "github.com/sirupsen/logrus"
)

type SystemInfo struct {
	OS     string     `json:"os"`
	VCPU   int        `json:"v_cpu"`
	Memory MemoryInfo `json:"memory"`
	Disk   DiskInfo   `json:"disk"`
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
