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

package lookups

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/siglens/siglens/pkg/config"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const (
	allowedExtCSV   = ".csv"
	allowedExtCSVGZ = ".csv.gz"
)

func UploadLookupFile(ctx *fasthttp.RequestCtx) {
	fileName := string(ctx.FormValue("name"))
	if fileName == "" {
		log.Error("UploadLookupFile: File name is required")
		ctx.Error("File name is required", fasthttp.StatusBadRequest)
		return
	}

	fileHeader, err := ctx.FormFile("file")
	if err != nil {
		log.Errorf("UploadLookupFile: Error retrieving the file: %v", err)
		ctx.Error("Error retrieving the file", fasthttp.StatusBadRequest)
		return
	}

	// Check for .csv and .csv.gz extensions
	lowerFileName := strings.ToLower(fileHeader.Filename)
	isCSV := strings.HasSuffix(lowerFileName, allowedExtCSV)
	isCSVGZ := strings.HasSuffix(lowerFileName, allowedExtCSVGZ)

	if !isCSV && !isCSVGZ {
		log.Errorf("UploadLookupFile: Invalid file type: %s", filepath.Ext(fileHeader.Filename))
		ctx.Error(fmt.Sprintf("Invalid file type. Only %s and %s files are allowed", allowedExtCSV, allowedExtCSVGZ), fasthttp.StatusBadRequest)
		return
	}

	if !strings.HasSuffix(strings.ToLower(fileName), allowedExtCSV) &&
		!strings.HasSuffix(strings.ToLower(fileName), allowedExtCSVGZ) {
		if isCSVGZ {
			fileName += allowedExtCSVGZ
		} else {
			fileName += allowedExtCSV
		}
	}

	fullLookupsDir := config.GetLookupPath()
	if err := os.MkdirAll(fullLookupsDir, os.ModePerm); err != nil {
		log.Errorf("UploadLookupFile: Error creating lookups directory: %v", err)
		ctx.Error("Error creating lookups directory", fasthttp.StatusInternalServerError)
		return
	}

	dstPath := filepath.Join(fullLookupsDir, fileName)
	if _, err := os.Stat(dstPath); !os.IsNotExist(err) {
		log.Errorf("UploadLookupFile: File already exists: %s", fileName)
		ctx.Error("A file with the same name already exists", fasthttp.StatusConflict)
		return
	}

	dst, err := os.Create(dstPath)
	if err != nil {
		log.Errorf("UploadLookupFile: Error creating the destination file: %v", err)
		ctx.Error("Error creating the destination file", fasthttp.StatusInternalServerError)
		return
	}
	defer dst.Close()

	file, err := fileHeader.Open()
	if err != nil {
		log.Errorf("UploadLookupFile: Unable to open the uploaded file: %v", err)
		ctx.Error("Unable to open the uploaded file", fasthttp.StatusInternalServerError)
		return
	}
	defer file.Close()

	if _, err := io.Copy(dst, file); err != nil {
		log.Errorf("UploadLookupFile: Error saving the file: %v", err)
		ctx.Error("Error saving the file", fasthttp.StatusInternalServerError)
		return
	}

	log.Infof("UploadLookupFile: File uploaded successfully: %s", fileName)
	ctx.SetStatusCode(fasthttp.StatusOK)
	fmt.Fprintf(ctx, "File uploaded successfully: %s", fileName)
}

func GetAllLookupFiles(ctx *fasthttp.RequestCtx) {
	fullLookupsDir := config.GetLookupPath()

	if err := os.MkdirAll(fullLookupsDir, os.ModePerm); err != nil {
		log.Errorf("GetAllLookupFiles: Error creating lookups directory: %v", err)
		ctx.Error("Error accessing lookups directory", fasthttp.StatusInternalServerError)
		return
	}

	files, err := os.ReadDir(fullLookupsDir)
	if err != nil {
		log.Errorf("GetAllLookupFiles: Error reading lookups directory %s: %v", fullLookupsDir, err)
		ctx.Error("Error reading lookups directory", fasthttp.StatusInternalServerError)
		return
	}

	fileNames := []string{}
	for _, file := range files {
		if file.Type().IsRegular() {
			fileNames = append(fileNames, file.Name())
		}
	}

	jsonResponse, err := json.Marshal(fileNames)
	if err != nil {
		log.Errorf("GetAllLookupFiles: Error marshalling file names: %v", err)
		ctx.Error("Error marshalling file names", fasthttp.StatusInternalServerError)
		return
	}

	log.Infof("GetAllLookupFiles: Successfully retrieved %d files", len(fileNames))
	ctx.SetContentType("application/json")
	ctx.SetStatusCode(fasthttp.StatusOK)
	if _, err := ctx.Write(jsonResponse); err != nil {
		log.Errorf("GetAllLookupFiles: Error writing response: %v", err)
		ctx.Error("Error writing response", fasthttp.StatusInternalServerError)
		return
	}
}
