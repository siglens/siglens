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
	var config PqsConfig
	err := json.Unmarshal(ctx.PostBody(), &config)
	if err != nil {
		log.Errorf("Error parsing request body: %v", err)
		ctx.Error("Bad Request", fasthttp.StatusBadRequest)
		return
	}
	if err := SavePQSConfigToRunMod(config.PQSEnabled); err != nil {
		log.Errorf("Error saving pqsEnabled: %v", err)

		ctx.Error("Internal Server Error", fasthttp.StatusInternalServerError)
		return
	}

	ctx.SetStatusCode(fasthttp.StatusOK)
	ctx.SetContentType("application/json")
	_, err = ctx.WriteString(`{"status":"success"}`)
	if err != nil {
		log.Errorf("Error writing response: %v", err)
		return

	}
}
func SavePQSConfigToRunMod(pqsEnabled string) error {
	file, err := os.OpenFile(config.RunModFilePath, os.O_CREATE|os.O_WRONLY, 0666)
	if err != nil {
		log.Errorf("Failed to open or create the file %s: %v", config.RunModFilePath, err)
		return err
	}
	defer file.Close()
	configData := map[string]string{"PQSEnabled": pqsEnabled}
	encoder := json.NewEncoder(file)

	err = encoder.Encode(configData)
	if err != nil {
		log.Errorf("Failed to encode JSON data to file %s: %v", config.RunModFilePath, err)
		return err
	}

	if strings.ToLower(pqsEnabled) == "disabled" {
		querytracker.ClearAllPQSData()
		err = config.ClearPqsFiles()
		if err != nil {
			log.Errorf("Failed to clear PQS data and files: %v", err)
			return err
		}
	}

	return nil
}
