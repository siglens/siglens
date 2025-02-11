// Copyright (c) 2021-2024 SigScalr, Inc.
//
// # This file is part of SigLens Observability Solution
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
	"os"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type QueryTimeoutConfig struct {
	TimeoutSecs int `json:"timeoutSecs"`
}

func GetQueryTimeout(ctx *fasthttp.RequestCtx) {
	// Read the value from the runmod config file, but if that doesn't exist,
	// read the value from the config file.
	var timeoutSecs int
	runModConfig, err := config.ReadRunModConfig(config.RunModFilePath)
	if err != nil {
		timeoutSecs = config.GetQueryTimeoutSecs()
	} else {
		timeoutSecs = runModConfig.QueryTimeoutSecs
		if timeoutSecs == 0 {
			timeoutSecs = config.GetQueryTimeoutSecs()
		}
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	_, err = ctx.WriteString(`{"timeoutSecs":` + fmt.Sprintf("%v", timeoutSecs) + `}`)
	if err != nil {
		log.Errorf("GetQueryTimeout: Error writing response: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
}

func SaveQueryTimeoutToRunMod(filepath string, timeoutSecs int) error {
	configData, err := config.ReadRunModConfig(filepath)
	if err != nil {
		log.Errorf("SavePQSConfigToRunMod: Using defaults as couldn't read config: %v", err)
		configData = config.GetDefaultRunModConfig()
	}

	configData.QueryTimeoutSecs = timeoutSecs

	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0666)
	if err != nil {
		log.Errorf("SaveQueryTimeoutToRunMod: Failed to open file %s: %v", filepath, err)
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(configData); err != nil {
		log.Errorf("SaveQueryTimeoutToRunMod: Failed to encode JSON data to file %s: %v", filepath, err)
		return err
	}

	config.SetQueryTimeoutSecs(timeoutSecs)
	return nil
}

func UpdateQueryTimeout(ctx *fasthttp.RequestCtx) {
	var cfg QueryTimeoutConfig
	err := json.Unmarshal(ctx.PostBody(), &cfg)
	if err != nil {
		log.Errorf("UpdateQueryTimeoutUpdate: Error parsing request body: %v. RequestBody=%v", err, string(ctx.PostBody()))
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}

	if cfg.TimeoutSecs < config.MIN_QUERY_TIMEOUT_SECONDS || cfg.TimeoutSecs > config.MAX_QUERY_TIMEOUT_SECONDS {
		log.Errorf("UpdateQueryTimeoutUpdate: Invalid timeout value %d. Must be between %d and %d seconds",
			cfg.TimeoutSecs, config.MIN_QUERY_TIMEOUT_SECONDS, config.MAX_QUERY_TIMEOUT_SECONDS)
		ctx.Error("Timeout must be between 1 and 30 minutes", fasthttp.StatusBadRequest)
		return
	}

	if err := SaveQueryTimeoutToRunMod(config.RunModFilePath, cfg.TimeoutSecs); err != nil {
		log.Errorf("UpdateQueryTimeoutUpdate: Error saving timeout to RunMod: %v. RunModFilePath=%v",
			err, config.RunModFilePath)
		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	_, err = ctx.WriteString(`{"status":"success"}`)
	if err != nil {
		log.Errorf("UpdateQueryTimeoutUpdate: Error writing response: %v", err)
		return
	}
}
