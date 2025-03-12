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
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/valyala/fasthttp"

	"github.com/siglens/siglens/pkg/audit"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/hooks"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
)

type CreateDashboardRequest struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	ParentID    string `json:"parentId"`
}

const ErrDashboardNameExists = "dashboard name already exists in this folder"

func isDefaultDashboard(id string) bool {
	defaultStructure, err := readDefaultFolderStructure()
	if err != nil {
		return false
	}

	item, exists := defaultStructure.Items[id]
	return exists && item.Type == "dashboard"
}

func getDashboardDetailsPath(id string) string {
	if isDefaultDashboard(id) {
		return fmt.Sprintf("defaultDBs/details/%s.json", id)
	}
	return fmt.Sprintf("%squerynodes/%s/dashboards/details/%s.json", config.GetDataPath(), config.GetHostID(), id)
}

func InitDashboards(myid int64) error {
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
	folderFile := getFolderStructureFilePath(myid)
	if _, err := os.Stat(folderFile); err != nil {
		if os.IsNotExist(err) {
			// If folder structure doesn't exist, migrate from allids.json
			if err := migrateToFolderStructure(myid); err != nil {
				log.Warnf("Migration failed: %v, creating new folder structure", err)
				// Create basic structure even if migration fails
				if err := InitFolderStructure(myid); err != nil {
					return fmt.Errorf("InitDashboard: failed to create folder structure: %v", err)
				}
			}
		} else {
			log.Errorf("InitDashboards: Error checking folder structure: %v", err)
			return err
		}
	}

	return nil
}

func createDashboard(req *CreateDashboardRequest, myid int64) (map[string]string, error) {
	if req.Name == "" {
		return nil, errors.New("dashboard name cannot be empty")
	}

	// If no parent ID specified, use root folder
	if req.ParentID == "" {
		req.ParentID = rootFolderID
	}

	structure, err := readFolderStructure(myid)
	if err != nil {
		return nil, fmt.Errorf("createDashboard: failed to read folder structure: %v", err)
	}

	parent, exists := structure.Items[req.ParentID]
	if !exists {
		return nil, errors.New("parent folder not found")
	}
	if parent.Type != ItemTypeFolder {
		return nil, errors.New("parent must be a folder")
	}

	// Check if dashboard name already exists in the same folder
	for _, itemID := range structure.Order[req.ParentID] {
		if item, exists := structure.Items[itemID]; exists {
			if item.Type == "dashboard" && item.Name == req.Name {
				return nil, errors.New(ErrDashboardNameExists)
			}
		}
	}

	newId := uuid.New().String()

	// Add dashboard to folder structure
	structure.Items[newId] = StoredFolderItem{
		Name:        req.Name,
		Type:        "dashboard",
		ParentID:    req.ParentID,
		IsDefault:   false,
		CreatedAtMs: time.Now().UnixMilli(),
	}
	structure.Order[req.ParentID] = append(structure.Order[req.ParentID], newId)

	if err := writeFolderStructure(structure, myid); err != nil {
		return nil, fmt.Errorf("createDashboard: failed to update folder structure: %v", err)
	}

	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + newId + ".json"

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

	details := map[string]interface{}{
		"name":        req.Name,
		"description": req.Description,
		"createdAtMs": time.Now().UnixMilli(),
		"isFavorite":  false,
		"folder": map[string]interface{}{
			"id":          req.ParentID,
			"name":        structure.Items[req.ParentID].Name,
			"path":        folderPath,
			"breadcrumbs": breadcrumbs,
		},
	}

	detailsData, err := json.MarshalIndent(details, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("createDashboard: failed to marshal dashboard details: %v", err)
	}

	if err := os.WriteFile(dashboardDetailsFname, detailsData, 0644); err != nil {
		return nil, fmt.Errorf("createDashboard: failed to write dashboard details: %v", err)
	}

	if err := blob.UploadQueryNodeDir(); err != nil {
		log.Errorf("createDashboard: Failed to upload query nodes dir, err=%v", err)
		return nil, err
	}

	retval := make(map[string]string)
	retval[newId] = req.Name
	return retval, nil
}

func toggleFavorite(id string, myid int64) (bool, error) {
	// Load the dashboard JSON file
	dashboardDetailsFname := getDashboardDetailsPath(id)

	dashboardJson, err := os.ReadFile(dashboardDetailsFname)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to read file=%v, err=%v", dashboardDetailsFname, err)
		return false, err
	}

	var dashboard map[string]interface{}
	err = json.Unmarshal(dashboardJson, &dashboard)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to unmarshal json, dashboardDetailsFname: %v, dashdata: %v, err: %v",
			dashboardDetailsFname, dashboard, err)
		return false, err
	}

	isFavorite, ok := dashboard["isFavorite"].(bool)
	if !ok {
		isFavorite = false
	}
	dashboard["isFavorite"] = !isFavorite

	updatedDashboardJson, err := json.Marshal(dashboard)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to marshal json, err=%v", err)
		return false, err
	}

	err = os.WriteFile(dashboardDetailsFname, updatedDashboardJson, 0644)
	if err != nil {
		log.Errorf("toggleFavorite: Failed to write file=%v, err=%v", dashboardDetailsFname, err)
		return false, err
	}

	return !isFavorite, nil
}

func getDashboard(id string, myid int64) (map[string]interface{}, error) {

	dashboardDetailsFname := getDashboardDetailsPath(id)

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

func updateDashboard(id string, dName string, dashboardDetails map[string]interface{}, myid int64) error {

	if isDefaultDashboard(id) {
		return errors.New("updateDashboard: cannot update default dashboard")
	}

	structure, err := readFolderStructure(myid)
	if err != nil {
		return fmt.Errorf("updateDashboard: failed to read folder structure: %v", err)
	}

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
		if newParent.Type != ItemTypeFolder {
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
						return errors.New(ErrDashboardNameExists)
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

	if err := writeFolderStructure(structure, myid); err != nil {
		return fmt.Errorf("updateDashboard: failed to update folder structure: %v", err)
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

	dashboardDetails["folder"] = map[string]interface{}{
		"id":   item.ParentID,
		"name": structure.Items[item.ParentID].Name,
		"path": folderPath,
	}

	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	detailsData, err := json.MarshalIndent(dashboardDetails, "", "  ")
	if err != nil {
		return fmt.Errorf("updateDashboard: failed to marshal dashboard details: %v", err)
	}

	if err := os.WriteFile(dashboardDetailsFname, detailsData, 0644); err != nil {
		return fmt.Errorf("updateDashboard: failed to write dashboard details: %v", err)
	}

	if err := blob.UploadQueryNodeDir(); err != nil {
		return fmt.Errorf("updateDashboard: failed to upload query nodes dir: %v", err)
	}

	return nil
}

func deleteDashboard(id string, myid int64) error {

	if isDefaultDashboard(id) {
		return errors.New("deleteDashboard: cannot delete default dashboard")
	}

	structure, err := readFolderStructure(myid)
	if err != nil {
		return fmt.Errorf("deleteDashboard: failed to read folder structure: %v", err)
	}

	item, exists := structure.Items[id]
	if !exists {
		return errors.New("deleteDashboard: dashboard not found")
	}

	if item.Type != "dashboard" {
		return errors.New("deleteDashboard: specified ID is not a dashboard")
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

	if err := writeFolderStructure(structure, myid); err != nil {
		return fmt.Errorf("deleteDashboard: failed to update folder structure: %v", err)
	}

	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
	if err := os.Remove(dashboardDetailsFname); err != nil && !os.IsNotExist(err) {
		log.Errorf("deleteDashboard: Error deleting dashboard file %s: %v", dashboardDetailsFname, err)
		return fmt.Errorf("deleteDashboard: failed to delete dashboard file: %v", err)
	}

	if err := blob.UploadQueryNodeDir(); err != nil {
		return fmt.Errorf("deleteDashboard: failed to upload query nodes dir: %v", err)
	}

	return nil
}

func setConflictMsg(ctx *fasthttp.RequestCtx) {
	var httpResp utils.HttpServerResponse
	ctx.SetStatusCode(fasthttp.StatusConflict)
	httpResp.Message = "Conflict: Dashboard name already exists"
	httpResp.StatusCode = fasthttp.StatusConflict
	utils.WriteResponse(ctx, httpResp)
}

func parseUpdateDashboardRequest(readJSON map[string]interface{}) (string, string, map[string]interface{}, error) {
	// Get dashboard ID
	dId, ok := readJSON["id"].(string)
	if !ok {
		return "", "", nil, errors.New("parseUpdateDashboardRequest: id field is missing or not a string")
	}

	// Get details object
	details, ok := readJSON["details"].(map[string]interface{})
	if !ok {
		return "", "", nil, errors.New("parseUpdateDashboardRequest: details field is missing or not an object")
	}

	// Get name from details
	dName, ok := details["name"].(string)
	if !ok {
		return "", "", nil, errors.New("parseUpdateDashboardRequest: name field is missing or not a string in details")
	}

	return dId, dName, details, nil
}

func ProcessCreateDashboardRequest(ctx *fasthttp.RequestCtx, myid int64) {
	var req CreateDashboardRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		log.Errorf("ProcessCreateDashboardRequest: could not unmarshal body: %v, err=%v", ctx.PostBody(), err)
		utils.SetBadMsg(ctx, "Invalid request body")
		return
	}

	dashboardInfo, err := createDashboard(&req, myid)
	if err != nil {
		if err.Error() == ErrDashboardNameExists {
			setConflictMsg(ctx)
			return
		}
		log.Errorf("ProcessCreateDashboardRequest: could not create dashboard: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}
	// audit log
	username := "No-user" //TODO : Add logged in user when user auth is implemented
	var orgId int64
	if hook := hooks.GlobalHooks.MiddlewareExtractOrgIdHook; hook != nil {
		orgId, err = hook(ctx)
		if err != nil {
			log.Errorf("pipeSearchWebsocketHandler: failed to extract orgId from context. Err=%+v", err)
			utils.SetBadMsg(ctx, "")
			return
		}
	}
	epochTimestampSec := time.Now().Unix()
	actionString := "Created dashboard"
	extraMsg := fmt.Sprintf("Dashboard Name: %s", req.Name)

	audit.CreateAuditEvent(username, actionString, extraMsg, epochTimestampSec, strconv.FormatInt(orgId, 10))

	utils.WriteJsonResponse(ctx, dashboardInfo)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessGetDashboardRequest(ctx *fasthttp.RequestCtx, myid int64) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	dashboardDetails, err := getDashboard(dId, myid)
	if err != nil {
		log.Errorf("ProcessGetDashboardRequest: could not get Dashboard, id: %v, err: %v", dId, err)
		utils.SetBadMsg(ctx, "")
		return
	}
	utils.WriteJsonResponse(ctx, dashboardDetails)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessFavoriteRequest(ctx *fasthttp.RequestCtx, myid int64) {
	dId := utils.ExtractParamAsString(ctx.UserValue("dashboard-id"))
	if dId == "" {
		log.Errorf("ProcessFavoriteRequest: received empty dashboard id")
		utils.SetBadMsg(ctx, "")
		return
	}

	isFavorite, err := toggleFavorite(dId, myid)
	if err != nil {
		log.Errorf("ProcessFavoriteRequest: could not toggle favorite status for Dashboard=%v, err=%v", dId, err)
		utils.SetBadMsg(ctx, "")
		return
	}

	response := map[string]bool{"isFavorite": isFavorite}
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessUpdateDashboardRequest(ctx *fasthttp.RequestCtx, myid int64) {
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
	if err := updateDashboard(dId, dName, dashboardDetails, myid); err != nil {
		log.Errorf("ProcessUpdateDashboardRequest: failed to update dashboard %s: %v", dId, err)
		if err.Error() == ErrDashboardNameExists {
			setConflictMsg(ctx)
			return
		}
		utils.SetBadMsg(ctx, err.Error())
		return
	}

	// Get updated dashboard details for response
	updatedDashboard, err := getDashboard(dId, myid)
	if err != nil {
		log.Errorf("ProcessUpdateDashboardRequest: failed to get updated dashboard: %v", err)
		utils.WriteJsonResponse(ctx, map[string]string{"message": "Dashboard updated successfully"})
		ctx.SetStatusCode(fasthttp.StatusOK)
		return
	}

	response := map[string]interface{}{
		"message":   "Dashboard updated successfully",
		"dashboard": updatedDashboard,
	}

	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessDeleteDashboardRequest(ctx *fasthttp.RequestCtx, myid int64) {
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

func ProcessDeleteDashboardsByOrgId(myid int64) error {
	structure, err := readFolderStructure(myid)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("ProcessDeleteOrgData: Failed to read folder structure: %v", err)
	}

	for id := range structure.Items {
		dashboardPath := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + id + ".json"
		if err := os.Remove(dashboardPath); err != nil && !os.IsNotExist(err) {
			log.Warnf("ProcessDeleteOrgData: Failed to delete dashboard file %s: %v", dashboardPath, err)
		}

	}

	folderStructurePath := getFolderStructureFilePath(myid)
	if err := os.Remove(folderStructurePath); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("ProcessDeleteOrgData: Failed to delete folder structure file: %v", err)
	}

	if err := blob.UploadQueryNodeDir(); err != nil {
		return fmt.Errorf("ProcessDeleteOrgData: failed to upload query nodes dir: %v", err)
	}

	return nil
}
