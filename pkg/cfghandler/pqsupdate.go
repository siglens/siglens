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
	"os"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type PqsConfig struct {
	PQSEnabled bool `json:"pqsEnabled"`
}

func GetPqsEnabled(ctx *fasthttp.RequestCtx) {
	// Read the value from the runmod config file, but if that doesn't exist,
	// read the value from the config file.
	var pqsEnabled bool
	runModConfig, err := config.ReadRunModConfig(config.RunModFilePath)
	if err != nil {
		log.Infof("GetPqsEnabled:Error reading runmod config: %v", err)
		pqsEnabled = config.IsPQSEnabled()
	} else {
		pqsEnabled = runModConfig.PQSEnabled
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	_, err = ctx.WriteString(`{"pqsEnabled":` + fmt.Sprintf("%v", pqsEnabled) + `}`)
	if err != nil {
		log.Errorf("GetPqsEnabled:Error writing response: %v", err)
		ctx.SetStatusCode(fasthttp.StatusInternalServerError)
		return
	}
}

func PostPqsUpdate(ctx *fasthttp.RequestCtx) {
	var cfg PqsConfig
	err := json.Unmarshal(ctx.PostBody(), &cfg)
	if err != nil {
		log.Errorf("PostPqsUpdate:Error parsing request body: %v", err)
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}
	if err := SavePQSConfigToRunMod(config.RunModFilePath, cfg.PQSEnabled); err != nil {
		log.Errorf("PostPqsUpdate:Error saving pqsEnabled: %v", err)

		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	_, err = ctx.WriteString(`{"status":"success"}`)
	if err != nil {
		log.Errorf("PostPqsUpdate:Error writing response: %v", err)
		return

	}
}
func SavePQSConfigToRunMod(filepath string, pqsEnabled bool) error {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Errorf("SavePQSConfigToRunMod:Failed to open or create the file %s: %v", filepath, err)
		return err
	}
	defer file.Close()
	configData := map[string]bool{"PQSEnabled": pqsEnabled}
	encoder := json.NewEncoder(file)

	err = encoder.Encode(configData)
	if err != nil {
		log.Errorf("SavePQSConfigToRunMod:Failed to encode JSON data to file %s: %v", filepath, err)
		return err
	}

	if !pqsEnabled {
		querytracker.ClearPqs()
	}

	return nil
}
