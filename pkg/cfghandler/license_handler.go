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

package cfghandler

import (
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/valyala/fasthttp"
)

type LicenseInfo struct {
	LicenseType  string `json:"license_type"`
	LicensedTo   string `json:"licensed_to"`
	Organization string `json:"organization"`
	Version      string `json:"version"`
	MaxUsers     string `json:"max_users"`
	ExpiryDate   string `json:"expiry_date"`
}

func GetLicenseDetails(ctx *fasthttp.RequestCtx) {
	extractorPath := "/root/hyperion/cmd/create-license/license_extractor"

	cmd := exec.Command(extractorPath)
	cmd.Dir = "/root/siglens"

	out, err := cmd.CombinedOutput()
	if err != nil {
		ctx.Error(fmt.Sprintf("Failed to read license: %v", err), fasthttp.StatusInternalServerError)
		return
	}

	licenseInfo := ParseLicenseFile(out)

	if licenseInfo.LicenseType == "" {
		licenseInfo.LicenseType = "Enterprise"
	}

	ctx.Response.Header.SetContentType("application/json")
	if err := json.NewEncoder(ctx).Encode(licenseInfo); err != nil {
		ctx.Error("Failed to encode response", fasthttp.StatusInternalServerError)
		return
	}
}

func ParseLicenseFile(out []byte) LicenseInfo {
	result := LicenseInfo{
		LicenseType: "Enterprise",
	}

	lines := strings.Split(string(out), "\n")

	for _, line := range lines {
		if len(strings.TrimSpace(line)) == 0 {
			continue
		}

		if strings.Contains(line, " msg=\"") {
			msgStart := strings.Index(line, " msg=\"")
			if msgStart > 0 {
				msgContent := strings.TrimSuffix(line[msgStart+6:], "\"")

				switch {
				case strings.HasPrefix(msgContent, "end date:"):
					dateStr := strings.TrimSpace(strings.TrimPrefix(msgContent, "end date:"))
					if t, err := time.Parse("2006/01/02", dateStr); err == nil {
						result.ExpiryDate = t.Format("2006-01-02")
					}
				case strings.HasPrefix(msgContent, "company:"):
					result.LicensedTo = strings.TrimSpace(strings.TrimPrefix(msgContent, "company:"))
				case strings.HasPrefix(msgContent, "org:"):
					result.Organization = strings.TrimSpace(strings.TrimPrefix(msgContent, "org:"))
				case strings.HasPrefix(msgContent, "version:"):
					result.Version = strings.TrimSpace(strings.TrimPrefix(msgContent, "version:"))
				case strings.HasPrefix(msgContent, "allowedVolumeGB:"):
					volumeStr := strings.TrimSpace(strings.TrimPrefix(msgContent, "allowedVolumeGB:"))
					result.MaxUsers = fmt.Sprintf("%s GB", volumeStr)
				}
			}
		}
	}

	return result
}
