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

package dashboards

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/valyala/fasthttp"

	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type CreateDashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	ParentID    string `json:"parentId"` // Folder ID where dashboard should be created
}

func isDefaultDashboard(id string) bool {
	defaultStructure, err := readDefaultFolderStructure()
	if err != nil {
		return false
	}

	item, exists := defaultStructure.Items[id]
	return exists && item.Type == "dashboard"
}

func InitDashboards() error {
	// Create base directories
	baseDir := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards"

	err := os.MkdirAll(baseDir, 0764)
	if err != nil {
		log.Errorf("InitDashboard: failed to create basedir=%v, err=%v", baseDir, err)
		return err
	}

	err = os.MkdirAll(baseDir+"/details", 0764)
	if err != nil {
		log.Errorf("InitDashboard: failed to create basedir=%v, err=%v", baseDir, err)
		return err
	}

	// Check if folder structure exists
	folderFile := getFolderStructureFilePath()
	if _, err := os.Stat(folderFile); err != nil {
		if os.IsNotExist(err) {
			// If folder structure doesn't exist, migrate from allids.json
			if err := migrateToFolderStructure(0); err != nil {
				log.Errorf("Failed to migrate to folder structure: %v", err)
				return err
			}
		} else {
			log.Errorf("Error checking folder structure: %v", err)
			return err
		}
	}

	return nil
}

func createDashboard(req *CreateDashboardRequest, orgid uint64) (map[string]string, error) {
	if req.Name == "" {
		return nil, errors.New("dashboard name cannot be empty")
	}

	// If no parent ID specified, use root folder
	if req.ParentID == "" {
		req.ParentID = rootFolderID
	}

	// Read current folder structure
	structure, err := readFolderStructure()
	if err != nil {
		return nil, fmt.Errorf("failed to read folder structure: %v", err)
	}

	// Validate parent folder exists
	parent, exists := structure.Items[req.ParentID]
	if !exists {
		return nil, errors.New("parent folder not found")
	}
	if parent.Type != "folder" {
		return nil, errors.New("parent must be a folder")
	}

	// Check if dashboard name already exists in the same folder
	for _, itemID := range structure.Order[req.ParentID] {
		if item, exists := structure.Items[itemID]; exists {
			if item.Type == "dashboard" && item.Name == req.Name {
				return nil, errors.New("dashboard name already exists in this folder")
			}
		}
	}

	// Generate new dashboard ID
	newId := uuid.New().String()

	// Add dashboard to folder structure
	structure.Items[newId] = FolderItem{
		Name:      req.Name,
		Type:      "dashboard",
		ParentID:  req.ParentID,
		CreatedAt: time.Now().UnixMilli(),
		IsDefault: false,
	}
	structure.Order[req.ParentID] = append(structure.Order[req.ParentID], newId)

	// Write updated folder structure
	if err := writeFolderStructure(structure); err != nil {
		return nil, fmt.Errorf("failed to update folder structure: %v", err)
	}

	// Create dashboard details file
	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + newId + ".json"

	// Get folder path for metadata
	folderPath := ""
	if req.ParentID != rootFolderID {
		currentID := req.ParentID
		folderNames := []string{}

		// Build folder path
		for currentID != "" && currentID != rootFolderID {
			if item, exists := structure.Items[currentID]; exists {
				folderNames = append([]string{item.Name}, folderNames...)
				currentID = item.ParentID
			} else {
				break
			}
		}
		if len(folderNames) > 0 {
			folderPath = strings.Join(folderNames, "/")
		}
	}

	breadcrumbs := generateBreadcrumbs(req.ParentID, structure)

	// Enhanced details with folder information
	details := map[string]interface{}{
		"name":        req.Name,
		"description": req.Description,
		"createdAt":   time.Now().UnixMilli(),
		"isFavorite":  false, // Default value when creating
		"folder": map[string]interface{}{
			"id":          req.ParentID,
			"name":        structure.Items[req.ParentID].Name,
			"path":        folderPath,
			"breadcrumbs": breadcrumbs,
		},
	}

	detailsData, err := json.MarshalIndent(details, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dashboard details: %v", err)
	}

	if err := os.WriteFile(dashboardDetailsFname, detailsData, 0644); err != nil {
		return nil, fmt.Errorf("failed to write dashboard details: %v", err)
	}

	// Upload to blob storage if needed
	if err := blob.UploadQueryNodeDir(); err != nil {
		log.Errorf("createDashboard: Failed to upload query nodes dir, err=%v", err)
		return nil, err
	}

	// Return the newly created dashboard info
	retval := make(map[string]string)
	retval[newId] = req.Name
	return retval, nil
}

func toggleFavorite(id string) (bool, error) {
	// Load the dashboard JSON file
	var dashboardDetailsFname string
	if isDefaultDashboard(id) {
		dashboardDetailsFname = "defaultDBs/details/" + id + ".json"
	} else {
		dashboardDetailsFname = config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	}
	dashboardJson, err := os.ReadFile(dashboardDetailsFname)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to read file=%v, err=%v", dashboardDetailsFname, err)
		return false, err
	}

	// Unmarshal JSON file into a map
	var dashboard map[string]interface{}
	err = json.Unmarshal(dashboardJson, &dashboard)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to unmarshal json, dashboardDetailsFname: %v, dashdata: %v, err: %v",
			dashboardDetailsFname, dashboard, err)
		return false, err
	}

	// Toggle the "isFavorite" field
	isFavorite, ok := dashboard["isFavorite"].(bool)
	if !ok {
		// If the "isFavorite" field does not exist or is not a bool, treat the dashboard as not favorited
		isFavorite = false
	}
	dashboard["isFavorite"] = !isFavorite

	// Marshal the updated dashboard back into JSON
	updatedDashboardJson, err := json.Marshal(dashboard)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to marshal json, err=%v", err)
		return false, err
	}

	// Save the updated dashboard back to the JSON file
	err = os.WriteFile(dashboardDetailsFname, updatedDashboardJson, 0644)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to write file=%v, err=%v", dashboardDetailsFname, err)
		return false, err
	}

	return !isFavorite, nil
}
func getDashboard(id string) (map[string]interface{}, error) {
	var dashboardDetailsFname string
	if isDefaultDashboard(id) {
		dashboardDetailsFname = "defaultDBs/details/" + id + ".json"
	} else {
		dashboardDetailsFname = config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	}
	rdata, err := os.ReadFile(dashboardDetailsFname)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		log.Errorf("getDashboard: Failed to read dashboard file fname=%v, err=%v", dashboardDetailsFname, err)
		return nil, err
	}

	var detailDashboardInfo map[string]interface{} = make(map[string]interface{})

	err = json.Unmarshal(rdata, &detailDashboardInfo)
	if err != nil {
		log.Errorf("getDashboard: Failed to unmarshall dashboard file fname: %v, rdata: %v,  err: %v",
			dashboardDetailsFname, rdata, err)
		return nil, err
	}

	return detailDashboardInfo, nil
}

func updateDashboard(id string, dName string, dashboardDetails map[string]interface{}, orgid uint64) error {

	if isDefaultDashboard(id) {
		return errors.New("cannot update default dashboard")
	}

	// Read folder structure
	structure, err := readFolderStructure()
	if err != nil {
		return fmt.Errorf("failed to read folder structure: %v", err)
	}

	// Check if dashboard exists in folder structure
	item, exists := structure.Items[id]
	if !exists {
		return errors.New("dashboard not found")
	}

	currentParentID := item.ParentID
	var newParentID string

	// Check if folder is being changed
	if folder, ok := dashboardDetails["folder"].(map[string]interface{}); ok {
		if newFolderID, ok := folder["id"].(string); ok {
			newParentID = newFolderID
		}
	}

	// If folder is changing
	if newParentID != "" && newParentID != currentParentID {
		// Validate new parent exists and is a folder
		newParent, exists := structure.Items[newParentID]
		if !exists {
			return errors.New("new parent folder not found")
		}
		if newParent.Type != "folder" {
			return errors.New("new parent must be a folder")
		}

		// Check for name conflicts in new folder
		for _, siblingID := range structure.Order[newParentID] {
			if sibling, exists := structure.Items[siblingID]; exists {
				if sibling.Type == "dashboard" && sibling.Name == dName && siblingID != id {
					return errors.New("dashboard name already exists in target folder")
				}
			}
		}

		// Remove from old parent's order
		newOrder := make([]string, 0)
		for _, itemID := range structure.Order[currentParentID] {
			if itemID != id {
				newOrder = append(newOrder, itemID)
			}
		}
		structure.Order[currentParentID] = newOrder

		// Add to new parent's order
		structure.Order[newParentID] = append(structure.Order[newParentID], id)

		// Update parent ID in items
		item.ParentID = newParentID
		structure.Items[id] = item
	} else {
		// If not changing folders, still check for name conflicts in current folder
		if item.Name != dName {
			for _, siblingID := range structure.Order[currentParentID] {
				if sibling, exists := structure.Items[siblingID]; exists {
					if sibling.Type == "dashboard" && sibling.Name == dName && siblingID != id {
						return errors.New("dashboard name already exists in this folder")
					}
				}
			}
		}
	}

	// Update name in folder structure if changed
	if item.Name != dName {
		item.Name = dName
		structure.Items[id] = item
	}

	// Write updated folder structure
	if err := writeFolderStructure(structure); err != nil {
		return fmt.Errorf("failed to update folder structure: %v", err)
	}

	// Get folder path for metadata
	folderPath := ""
	if item.ParentID != rootFolderID {
		currentID := item.ParentID
		folderNames := []string{}

		for currentID != "" && currentID != rootFolderID {
			if folderItem, exists := structure.Items[currentID]; exists {
				folderNames = append([]string{folderItem.Name}, folderNames...)
				currentID = folderItem.ParentID
			} else {
				break
			}
		}
		if len(folderNames) > 0 {
			folderPath = strings.Join(folderNames, "/")
		}
	}

	// Update folder information in dashboard details
	dashboardDetails["folder"] = map[string]interface{}{
		"id":   item.ParentID,
		"name": structure.Items[item.ParentID].Name,
		"path": folderPath,
	}

	// Write dashboard details file
	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	detailsData, err := json.MarshalIndent(dashboardDetails, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal dashboard details: %v", err)
	}

	if err := os.WriteFile(dashboardDetailsFname, detailsData, 0644); err != nil {
		return fmt.Errorf("failed to write dashboard details: %v", err)
	}

	// Upload to blob storage
	if err := blob.UploadQueryNodeDir(); err != nil {
		return fmt.Errorf("failed to upload query nodes dir: %v", err)
	}

	return nil
}

func deleteDashboard(id string, orgid uint64) error {

	if isDefaultDashboard(id) {
		return errors.New("cannot delete default dashboard")
	}

	// Read folder structure
	structure, err := readFolderStructure()
	if err != nil {
		return fmt.Errorf("failed to read folder structure: %v", err)
	}

	// Check if dashboard exists in structure
	item, exists := structure.Items[id]
	if !exists {
		return errors.New("dashboard not found")
	}

	if item.Type != "dashboard" {
		return errors.New("specified ID is not a dashboard")
	}

	// Remove from parent folder's order
	parentID := item.ParentID
	if parentOrder, exists := structure.Order[parentID]; exists {
		newOrder := make([]string, 0)
		for _, itemID := range parentOrder {
			if itemID != id {
				newOrder = append(newOrder, itemID)
			}
		}
		structure.Order[parentID] = newOrder
	}

	// Remove from items
	delete(structure.Items, id)

	// Write updated folder structure
	if err := writeFolderStructure(structure); err != nil {
		return fmt.Errorf("failed to update folder structure: %v", err)
	}

	// Delete dashboard details file
	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	if err := os.Remove(dashboardDetailsFname); err != nil && !os.IsNotExist(err) {
		log.Errorf("deleteDashboard: Error deleting dashboard file %s: %v", dashboardDetailsFname, err)
		return fmt.Errorf("failed to delete dashboard file: %v", err)
	}

	// Update the query node dir
	if err := blob.UploadQueryNodeDir(); err != nil {
		return fmt.Errorf("failed to upload query nodes dir: %v", err)
	}

	return nil
}

// method to set conflict message and 409 status code
func setConflictMsg(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse
	ctx.SetStatusCode(fasthttp.StatusConflict)
	httpResp.Message = "Conflict: Dashboard name already exists"
	httpResp.StatusCode = fasthttp.StatusConflict
	utils.WriteResponse(ctx, httpResp)
}

func ProcessCreateDashboardRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	var req CreateDashboardRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		log.Errorf("ProcessCreateDashboardRequest: could not unmarshal body: %v, err=%v", ctx.PostBody(), err)
		utils.SetBadMsg(ctx, "Invalid request body")
		return
	}

	dashboardInfo, err := createDashboard(&req, myid)
	if err != nil {
		if err.Error() == "dashboard name already exists in this folder" {
			setConflictMsg(ctx)
			return
		}
		log.Errorf("ProcessCreateDashboardRequest: could not create dashboard: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}

	utils.WriteJsonResponse(ctx, dashboardInfo)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessFavoriteRequest(ctx *fasthttp.RequestCtx) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	if dId == "" {
		log.Errorf("ProcessFavoriteRequest: received empty dashboard id")
		utils.SetBadMsg(ctx, "")
		return
	}

	isFavorite, err := toggleFavorite(dId)
	if err != nil {
		log.Errorf("ProcessFavoriteRequest: could not toggle favorite status for Dashboard=%v, err=%v", dId, err)
		utils.SetBadMsg(ctx, "")
		return
	}

	response := map[string]bool{"isFavorite": isFavorite}
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func parseUpdateDashboardRequest(readJSON map[string]interface{}) (string, string, map[string]interface{}, error) {
	// Get dashboard ID
	dId, ok := readJSON["id"].(string)
	if !ok {
		return "", "", nil, errors.New("id field is missing or not a string")
	}

	// Get details object
	details, ok := readJSON["details"].(map[string]interface{})
	if !ok {
		return "", "", nil, errors.New("details field is missing or not an object")
	}

	// Get name from details
	dName, ok := details["name"].(string)
	if !ok {
		return "", "", nil, errors.New("name field is missing or not a string in details")
	}

	return dId, dName, details, nil
}

func ProcessUpdateDashboardRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	rawJSON := ctx.PostBody()
	if rawJSON == nil {
		log.Errorf("ProcessUpdateDashboardRequest: received empty request body")
		utils.SetBadMsg(ctx, "")
		return
	}

	readJSON := make(map[string]interface{})
	if err := json.Unmarshal(rawJSON, &readJSON); err != nil {
		log.Errorf("ProcessUpdateDashboardRequest: could not unmarshal body: %v, err=%v", rawJSON, err)
		utils.SetBadMsg(ctx, "Invalid request body")
		return
	}

	dId, dName, dashboardDetails, err := parseUpdateDashboardRequest(readJSON)
	if err != nil {
		log.Errorf("ProcessUpdateDashboardRequest: parseUpdateDashboardRequest failed, readJSON: %v, err: %v", readJSON, err)
		utils.SetBadMsg(ctx, "Invalid request format")
		return
	}

	// Panel validation if needed
	if panels, ok := dashboardDetails["panels"].([]interface{}); ok {
		if pflag, ok := dashboardDetails["panelFlag"]; ok {
			if pflag == "false" && len(panels) > 10 {
				utils.SetBadMsg(ctx, "Too many panels for free tier")
				return
			}
		}
	}

	// Update the dashboard
	err = updateDashboard(dId, dName, dashboardDetails, myid)
	if err != nil {
		switch err.Error() {
		case "dashboard name already exists in this folder":
			setConflictMsg(ctx)
		case "new parent folder not found":
			utils.SetBadMsg(ctx, "Target folder not found")
		default:
			log.Errorf("ProcessUpdateDashboardRequest: could not update Dashboard, dId: %v, myid: %v, err: %v", dId, myid, err)
			utils.SetBadMsg(ctx, "")
		}
		return
	}

	// Get updated dashboard details for response
	updatedDashboard, err := getDashboard(dId)
	if err != nil {
		log.Errorf("ProcessUpdateDashboardRequest: failed to get updated dashboard: %v", err)
		// Still return success since update was successful
		utils.WriteJsonResponse(ctx, map[string]string{"message": "Dashboard updated successfully"})
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	// Return complete updated dashboard
	response := map[string]interface{}{
		"message":   "Dashboard updated successfully",
		"dashboard": updatedDashboard,
	}

	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetDashboardRequest(ctx *fasthttp.RequestCtx) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	dashboardDetails, err := getDashboard(dId)
	if err != nil {
		log.Errorf("ProcessGetDashboardRequest: could not get Dashboard, id: %v, err: %v", dId, err)
		utils.SetBadMsg(ctx, "")
		return
	}
	utils.WriteJsonResponse(ctx, dashboardDetails)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessDeleteDashboardRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	if dId == "" {
		utils.SetBadMsg(ctx, "Dashboard ID is required")
		return
	}

	err := deleteDashboard(dId, myid)
	if err != nil {
		log.Errorf("ProcessDeleteDashboardRequest: Failed to delete dashboard, id: %v, err=%v", dId, err)
		if err.Error() == "dashboard not found" {
			ctx.SetStatusCode(fasthttp.StatusNotFound)
			utils.WriteJsonResponse(ctx, map[string]interface{}{
				"message": "Dashboard not found",
				"status":  fasthttp.StatusNotFound,
			})
			return
		}
		utils.SetBadMsg(ctx, "")
		return
	}

	utils.WriteJsonResponse(ctx, map[string]interface{}{
		"message": "Dashboard deleted successfully",
		"status":  fasthttp.StatusOK,
	})
	ctx.SetStatusCode(fasthttp.StatusOK)
}
