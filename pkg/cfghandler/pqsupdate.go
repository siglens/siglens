package cfghandler

import (
	"encoding/json"
	"os"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/querytracker"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

type PqsConfig struct {
	PQSEnabled string `json:"pqsEnabled"`
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
func SavePQSConfigToRunMod(filepath, pqsEnabled string) error {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Errorf("SavePQSConfigToRunMod:Failed to open or create the file %s: %v", filepath, err)
		return err
	}
	defer file.Close()
	configData := map[string]string{"PQSEnabled": pqsEnabled}
	encoder := json.NewEncoder(file)

	err = encoder.Encode(configData)
	if err != nil {
		log.Errorf("SavePQSConfigToRunMod:Failed to encode JSON data to file %s: %v", filepath, err)
		return err
	}

	if strings.ToLower(pqsEnabled) == "disabled" {
		querytracker.ClearPqs()
	}

	return nil
}