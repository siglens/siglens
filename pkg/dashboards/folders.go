package dashboards

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/google/uuid"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

// FolderItem represents a single item in the folder structure
type FolderItem struct {
	Name      string `json:"name"`
	Type      string `json:"type"` // "folder" or "dashboard"
	ParentID  string `json:"parentId"`
	IsDefault bool   `json:"isDefault"` // True for default items
}

// FolderStructure represents the complete folder hierarchy
type FolderStructure struct {
	Items map[string]FolderItem `json:"items"` // uuid -> item
	Order map[string][]string   `json:"order"` // parentId -> ordered child IDs
}

var (
	folderStructureLock sync.RWMutex
	rootFolderID        = "root-folder" // Fixed ID for root folder
)

// getFolderStructureFilePath returns the path to folder_structure.json
func getFolderStructureFilePath() string {
	return config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/folder_structure.json"
}

func getDefaultFolderStructureFilePath() string {
	return "defaultDBs/folder_structure.json"
}

// initFolderStructure creates initial folder structure with root if it doesn't exist
func InitFolderStructure() error {
	folderStructureLock.Lock()
	defer folderStructureLock.Unlock()

	filePath := getFolderStructureFilePath()

	// Check if file exists
	if _, err := os.Stat(filePath); err == nil {
		return nil // File exists, no need to initialize
	}

	// Create initial structure with root folder
	structure := FolderStructure{
		Items: map[string]FolderItem{
			rootFolderID: {
				Name:     "Root",
				Type:     "folder",
				ParentID: "",
			},
		},
		Order: map[string][]string{
			rootFolderID: {},
		},
	}

	// Marshal and write to file
	data, err := json.MarshalIndent(structure, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal initial folder structure: %v", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write initial folder structure: %v", err)
	}

	return nil
}

func readCombinedFolderStructure() (*FolderStructure, error) {
	// Read user folder structure
	userStructure, err := readFolderStructure()
	if err != nil {
		return nil, fmt.Errorf("failed to read user folder structure: %v", err)
	}

	// Read default folder structure
	defaultData, err := os.ReadFile(getDefaultFolderStructureFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			// If no default structure exists, just return user structure
			return userStructure, nil
		}
		return nil, fmt.Errorf("failed to read default folder structure: %v", err)
	}

	var defaultStructure FolderStructure
	if err := json.Unmarshal(defaultData, &defaultStructure); err != nil {
		return nil, fmt.Errorf("failed to unmarshal default folder structure: %v", err)
	}

	// Merge default items into user structure
	for id, item := range defaultStructure.Items {
		if _, exists := userStructure.Items[id]; !exists {
			item.IsDefault = true // Mark as default item
			userStructure.Items[id] = item
		}
	}

	// Merge order lists
	for parentID, defaultChildren := range defaultStructure.Order {
		if _, exists := userStructure.Order[parentID]; !exists {
			userStructure.Order[parentID] = defaultChildren
		} else {
			// For existing parents, append default children if they're not already there
			existingChildren := make(map[string]bool)
			for _, childID := range userStructure.Order[parentID] {
				existingChildren[childID] = true
			}

			for _, childID := range defaultChildren {
				if !existingChildren[childID] {
					userStructure.Order[parentID] = append(userStructure.Order[parentID], childID)
				}
			}
		}
	}

	return userStructure, nil
}

func readDefaultFolderStructure() (*FolderStructure, error) {
	data, err := os.ReadFile(getDefaultFolderStructureFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to read default folder structure: %v", err)
	}

	var structure FolderStructure
	if err := json.Unmarshal(data, &structure); err != nil {
		return nil, fmt.Errorf("failed to unmarshal default folder structure: %v", err)
	}

	return &structure, nil
}

// readFolderStructure reads the current folder structure from file
func readFolderStructure() (*FolderStructure, error) {
	folderStructureLock.RLock()
	defer folderStructureLock.RUnlock()

	data, err := os.ReadFile(getFolderStructureFilePath())
	if err != nil {
		return nil, fmt.Errorf("failed to read folder structure: %v", err)
	}

	var structure FolderStructure
	if err := json.Unmarshal(data, &structure); err != nil {
		return nil, fmt.Errorf("failed to unmarshal folder structure: %v", err)
	}

	return &structure, nil
}

// writeFolderStructure writes the folder structure to file
func writeFolderStructure(structure *FolderStructure) error {
	folderStructureLock.Lock()
	defer folderStructureLock.Unlock()

	data, err := json.MarshalIndent(structure, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal folder structure: %v", err)
	}

	if err := os.WriteFile(getFolderStructureFilePath(), data, 0644); err != nil {
		return fmt.Errorf("failed to write folder structure: %v", err)
	}

	return nil
}

// CreateFolderRequest represents the request body for creating a folder
type CreateFolderRequest struct {
	Name     string `json:"name"`
	ParentID string `json:"parentId"`
}

// createFolder creates a new folder
func createFolder(req *CreateFolderRequest, orgID uint64) (string, error) {
	if req.Name == "" {
		return "", errors.New("folder name cannot be empty")
	}

	// Use root if no parent specified
	if req.ParentID == "" {
		req.ParentID = rootFolderID
	}

	structure, err := readFolderStructure()
	if err != nil {
		return "", err
	}

	// Validate parent exists and is a folder
	parent, exists := structure.Items[req.ParentID]
	if !exists {
		return "", errors.New("Parent folder not found")
	}
	if parent.Type != "folder" {
		return "", errors.New("Parent must be a folder")
	}

	// Check for duplicate names in the same parent
	for _, childID := range structure.Order[req.ParentID] {
		if child, exists := structure.Items[childID]; exists && child.Name == req.Name {
			return "", errors.New("Folder with same name already exists in this location!")
		}
	}

	// Generate new folder ID
	folderID := uuid.New().String()

	// Add new folder to structure
	structure.Items[folderID] = FolderItem{
		Name:     req.Name,
		Type:     "folder",
		ParentID: req.ParentID,
	}

	// Initialize order array for new folder
	structure.Order[folderID] = []string{}

	// Add to parent's order
	structure.Order[req.ParentID] = append(structure.Order[req.ParentID], folderID)

	// Write updated structure
	if err := writeFolderStructure(structure); err != nil {
		return "", err
	}

	return folderID, nil
}

// ProcessCreateFolderRequest handles the HTTP request to create a folder
func ProcessCreateFolderRequest(ctx *fasthttp.RequestCtx, orgID uint64) {
	var req CreateFolderRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		log.Errorf("ProcessCreateFolderRequest: failed to unmarshal request: %v", err)
		utils.SetBadMsg(ctx, "Invalid request body")
		return
	}

	folderID, err := createFolder(&req, orgID)
	if err != nil {
		log.Errorf("ProcessCreateFolderRequest: failed to create folder: %v", err)
		utils.SetBadMsg(ctx, err.Error())
		return
	}

	response := map[string]string{
		"id":      folderID,
		"message": "Folder created successfully",
	}

	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// FolderContentResponse represents the response structure
type FolderContentResponse struct {
	Folder      FolderInfo       `json:"folder"`
	Items       []FolderItemInfo `json:"items"`
	Breadcrumbs []Breadcrumb     `json:"breadcrumbs"`
}

type FolderInfo struct {
	ID   string `json:"id"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type FolderItemInfo struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	ChildCount int    `json:"childCount,omitempty"` // Only for folders
	IsDefault  bool   `json:"isDefault"`            // Whether it's a default item
}

type Breadcrumb struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func getFolderContents(folderID string, foldersOnly bool) (*FolderContentResponse, error) {
	// Read combined folder structure
	structure, err := readCombinedFolderStructure()
	log.Errorf("Debug: structure contents: %+v\n", structure)

	if err != nil {
		return nil, fmt.Errorf("failed to read folder structure: %v", err)
	}

	// Check if folder exists
	folder, exists := structure.Items[folderID]
	log.Errorf("Debug: folder %v exists", folderID)
	if !exists {
		return nil, fmt.Errorf("folder not found: %s", folderID)
	}

	// Prepare response
	response := &FolderContentResponse{
		Folder: FolderInfo{
			ID:   folderID,
			Name: folder.Name,
			Type: folder.Type,
		},
		Items: make([]FolderItemInfo, 0),
	}

	// Get folder contents
	log.Errorf("structure.Order %v", structure.Order)
	childIDs := structure.Order[folderID]
	log.Errorf("ChildIds %v ", childIDs)
	for _, childID := range childIDs {
		child, exists := structure.Items[childID]
		if !exists {
			continue // Skip if item doesn't exist
		}
		log.Info("Debug: Checking childID %s, Item: %v ", childID, child)
		// Skip if we only want folders and this is not a folder
		if foldersOnly && child.Type != "folder" {
			continue
		}

		item := FolderItemInfo{
			ID:        childID,
			Name:      child.Name,
			Type:      child.Type,
			IsDefault: child.IsDefault, // Include isDefault flag
		}

		// If it's a folder, count its children
		if child.Type == "folder" {
			if children, exists := structure.Order[childID]; exists {
				item.ChildCount = len(children)
			}
		}

		response.Items = append(response.Items, item)
	}

	// Generate breadcrumbs
	response.Breadcrumbs = generateBreadcrumbs(folderID, structure)

	log.Errorf("Debug: structure contents: %+v\n", response)
	return response, nil
}

func generateBreadcrumbs(folderID string, structure *FolderStructure) []Breadcrumb {
	breadcrumbs := make([]Breadcrumb, 0)
	currentID := folderID

	for currentID != "" {
		item, exists := structure.Items[currentID]
		if !exists {
			break
		}

		// Add to start of breadcrumbs (reverse order)
		breadcrumbs = append([]Breadcrumb{{
			ID:   currentID,
			Name: item.Name,
		}}, breadcrumbs...)

		currentID = item.ParentID
	}

	return breadcrumbs
}

func ProcessGetFolderContentsRequest(ctx *fasthttp.RequestCtx) {
	folderID := utils.ExtractParamAsString(ctx.UserValue("folder-id"))
	if folderID == "" {
		folderID = rootFolderID
	}

	// Check if we only want folders
	foldersOnly := string(ctx.QueryArgs().Peek("foldersOnly")) == "true"

	contents, err := getFolderContents(folderID, foldersOnly)
	if err != nil {
		log.Errorf("ProcessGetFolderContentsRequest: failed to get folder contents: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}

	utils.WriteJsonResponse(ctx, contents)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// Check if a folder is a default folder
func isDefaultFolder(id string) bool {
	defaultStructure, err := readDefaultFolderStructure()
	if err != nil {
		return false
	}

	item, exists := defaultStructure.Items[id]
	return exists && item.Type == "folder"
}

type UpdateFolderRequest struct {
	Name     string `json:"name,omitempty"`     // New name for the folder
	ParentID string `json:"parentId,omitempty"` // New parent folder ID
}

func updateFolder(folderID string, req *UpdateFolderRequest) error {
	if folderID == rootFolderID {
		return fmt.Errorf("cannot update root folder")
	}

	if isDefaultFolder(folderID) {
		return fmt.Errorf("cannot update default folder")
	}

	// Read current folder structure
	structure, err := readFolderStructure()
	if err != nil {
		return fmt.Errorf("failed to read folder structure: %v", err)
	}

	// Check if folder exists
	folder, exists := structure.Items[folderID]
	if !exists {
		return fmt.Errorf("folder not found: %s", folderID)
	}

	// If moving to new parent
	if req.ParentID != "" && req.ParentID != folder.ParentID {
		// Validate new parent exists and is a folder
		newParent, exists := structure.Items[req.ParentID]
		if !exists {
			return fmt.Errorf("new parent folder not found: %s", req.ParentID)
		}
		if newParent.Type != "folder" {
			return fmt.Errorf("new parent must be a folder")
		}

		// Check for circular reference
		if wouldCreateCircularReference(folderID, req.ParentID, structure) {
			return fmt.Errorf("cannot move folder: would create circular reference")
		}

		// Remove from old parent's order
		if oldParentOrder, exists := structure.Order[folder.ParentID]; exists {
			newOrder := make([]string, 0)
			for _, id := range oldParentOrder {
				if id != folderID {
					newOrder = append(newOrder, id)
				}
			}
			structure.Order[folder.ParentID] = newOrder
		}

		// Add to new parent's order
		structure.Order[req.ParentID] = append(structure.Order[req.ParentID], folderID)

		// Update parent ID
		folder.ParentID = req.ParentID
	}

	// Update name if provided
	if req.Name != "" && req.Name != folder.Name {
		// Check for duplicate names in the same parent
		parentID := folder.ParentID
		if req.ParentID != "" {
			parentID = req.ParentID
		}

		for _, siblingID := range structure.Order[parentID] {
			if sibling, exists := structure.Items[siblingID]; exists {
				if sibling.Name == req.Name && siblingID != folderID {
					return fmt.Errorf("folder with name %s already exists in this location", req.Name)
				}
			}
		}
		folder.Name = req.Name
	}

	// Update the folder in the structure
	structure.Items[folderID] = folder

	// Write updated structure
	if err := writeFolderStructure(structure); err != nil {
		return fmt.Errorf("failed to write folder structure: %v", err)
	}

	return nil
}

// Helper function to check for circular references
func wouldCreateCircularReference(folderID, newParentID string, structure *FolderStructure) bool {
	current := newParentID
	for current != "" {
		if current == folderID {
			return true
		}
		if item, exists := structure.Items[current]; exists {
			current = item.ParentID
		} else {
			break
		}
	}
	return false
}

func ProcessUpdateFolderRequest(ctx *fasthttp.RequestCtx) {
	folderID := utils.ExtractParamAsString(ctx.UserValue("folder-id"))
	if folderID == "" {
		utils.SetBadMsg(ctx, "Folder ID is required")
		return
	}

	var req UpdateFolderRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		log.Errorf("ProcessUpdateFolderRequest: failed to unmarshal request: %v", err)
		utils.SetBadMsg(ctx, "Invalid request body")
		return
	}

	if err := updateFolder(folderID, &req); err != nil {
		log.Errorf("ProcessUpdateFolderRequest: failed to update folder: %v", err)
		utils.SetBadMsg(ctx, err.Error())
		return
	}

	response := map[string]string{
		"message": "Folder updated successfully",
	}
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func deleteFolder(folderID string) error {
	if folderID == rootFolderID {
		return fmt.Errorf("cannot delete root folder")
	}

	if isDefaultFolder(folderID) {
		return fmt.Errorf("cannot delete default folder")
	}

	// Read current folder structure
	structure, err := readFolderStructure()
	if err != nil {
		return fmt.Errorf("failed to read folder structure: %v", err)
	}

	// Check if folder exists
	folder, exists := structure.Items[folderID]
	if !exists {
		return fmt.Errorf("folder not found: %s", folderID)
	}

	// Get all items to be deleted (recursive)
	itemsToDelete := make([]string, 0)
	collectItemsToDelete(folderID, structure, &itemsToDelete)

	// Delete all dashboard files
	for _, itemID := range itemsToDelete {
		item := structure.Items[itemID]
		if item.Type == "dashboard" {
			if err := deleteDashboardFile(itemID); err != nil {
				log.Errorf("deleteFolder: failed to delete dashboard file %s: %v", itemID, err)
				// Continue with other deletions
			}
		}
	}

	// Remove from parent's order
	if oldParentOrder, exists := structure.Order[folder.ParentID]; exists {
		newOrder := make([]string, 0)
		for _, id := range oldParentOrder {
			if id != folderID {
				newOrder = append(newOrder, id)
			}
		}
		structure.Order[folder.ParentID] = newOrder
	}

	// Delete all items from structure
	for _, itemID := range itemsToDelete {
		delete(structure.Items, itemID)
		delete(structure.Order, itemID) // Remove order list if it's a folder
	}

	// Write updated structure
	if err := writeFolderStructure(structure); err != nil {
		return fmt.Errorf("failed to write folder structure: %v", err)
	}

	// Upload changes to blob storage
	if err := blob.UploadQueryNodeDir(); err != nil {
		log.Errorf("deleteFolder: Failed to upload query nodes dir, err=%v", err)
		return err
	}

	return nil
}

// Helper function to recursively collect all items to be deleted
func collectItemsToDelete(folderID string, structure *FolderStructure, itemsToDelete *[]string) {
	// Add current folder to delete list
	*itemsToDelete = append(*itemsToDelete, folderID)

	// Recursively process children
	if childIDs, exists := structure.Order[folderID]; exists {
		for _, childID := range childIDs {
			if child, exists := structure.Items[childID]; exists {
				if child.Type == "folder" {
					collectItemsToDelete(childID, structure, itemsToDelete)
				} else {
					*itemsToDelete = append(*itemsToDelete, childID)
				}
			}
		}
	}
}

// Helper function to delete dashboard file
func deleteDashboardFile(dashboardID string) error {
	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + dashboardID + ".json"
	return os.Remove(dashboardDetailsFname)
}

func ProcessDeleteFolderRequest(ctx *fasthttp.RequestCtx) {
	folderID := utils.ExtractParamAsString(ctx.UserValue("folder-id"))
	if folderID == "" {
		utils.SetBadMsg(ctx, "Folder ID is required")
		return
	}

	if err := deleteFolder(folderID); err != nil {
		log.Errorf("ProcessDeleteFolderRequest: failed to delete folder: %v", err)
		utils.SetBadMsg(ctx, err.Error())
		return
	}

	response := map[string]string{
		"message": "Folder and its contents deleted successfully",
	}
	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

type DashboardListItem struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	FullPath   string `json:"fullPath"`   // Full path like "GrafanaCloud/Cardinality management - 1"
	ParentPath string `json:"parentPath"` // Parent folder path like "GrafanaCloud"
}

// DashboardListResponse represents the response for dashboard listing
type DashboardListResponse struct {
	Dashboards []DashboardListItem `json:"dashboards"`
}

func getAllDashboardsWithPaths() (*DashboardListResponse, error) {
	// Read folder structure
	structure, err := readCombinedFolderStructure()
	if err != nil {
		return nil, fmt.Errorf("failed to read folder structure: %v", err)
	}

	response := &DashboardListResponse{
		Dashboards: make([]DashboardListItem, 0),
	}

	// Helper function to get full path of a folder
	getFolderPath := func(folderID string) string {
		if folderID == rootFolderID {
			return "" // Return empty string for root level
		}
		path := ""
		currentID := folderID
		for currentID != "" && currentID != rootFolderID {
			if item, exists := structure.Items[currentID]; exists {
				if path == "" {
					path = item.Name
				} else {
					path = item.Name + "/" + path
				}
				currentID = item.ParentID
			} else {
				break
			}
		}
		return path
	}

	// Recursively process all items
	var processFolder func(folderID string)
	processFolder = func(folderID string) {
		items := structure.Order[folderID]
		for _, itemID := range items {
			item, exists := structure.Items[itemID]
			if !exists {
				continue
			}

			if item.Type == "dashboard" {
				parentPath := getFolderPath(item.ParentID)
				var fullPath string

				if parentPath == "" {
					// Root level dashboard
					fullPath = "Dashboards/" + item.Name
				} else {
					// Dashboard in a folder
					fullPath = parentPath + "/" + item.Name
				}

				dashboardItem := DashboardListItem{
					ID:         itemID,
					Name:       item.Name,
					FullPath:   fullPath,
					ParentPath: parentPath,
				}
				response.Dashboards = append(response.Dashboards, dashboardItem)
			} else if item.Type == "folder" {
				processFolder(itemID) // Recursively process subfolders
			}
		}
	}

	// Start processing from root
	processFolder(rootFolderID)

	return response, nil
}

func ProcessListAllDashboardsRequest(ctx *fasthttp.RequestCtx) {
	dashboards, err := getAllDashboardsWithPaths()
	if err != nil {
		log.Errorf("ProcessListAllDashboardsRequest: failed to list dashboards: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}

	utils.WriteJsonResponse(ctx, dashboards)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

// TODO: After migration is complete :
// 1. Remove getAllIdsFileName function
// 3. Remove migration-related code from InitDashboards
// 4. Remove migrateToFolderStructure function

func getAllIdsFileName(orgid uint64) string {
	baseDir := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards"
	allidsBaseFname := baseDir + "/allids"

	if orgid == 0 {
		return allidsBaseFname + ".json"
	}
	return allidsBaseFname + "-" + strconv.FormatUint(orgid, 10) + ".json"
}

func migrateToFolderStructure(orgid uint64) error {
	folderStructureLock.Lock()
	defer folderStructureLock.Unlock()

	folderFile := getFolderStructureFilePath()

	// If folder_structure.json exists, we're already migrated
	if _, err := os.Stat(folderFile); err == nil {
		return nil
	}

	// Create new folder structure
	structure := FolderStructure{
		Items: map[string]FolderItem{
			rootFolderID: {
				Name:     "Root",
				Type:     "folder",
				ParentID: "",
			},
		},
		Order: map[string][]string{
			rootFolderID: {},
		},
	}

	allidsFname := getAllIdsFileName(orgid)
	data, _ := os.ReadFile(allidsFname)

	if len(data) > 0 {
		var existingDashboards map[string]string
		if err := json.Unmarshal(data, &existingDashboards); err != nil {
			return fmt.Errorf("failed to unmarshal allids.json: %v", err)
		}

		for dashID, dashName := range existingDashboards {
			// Verify dashboard details file exists
			detailsPath := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + dashID + ".json"
			if _, err := os.Stat(detailsPath); err != nil {
				continue
			}

			structure.Items[dashID] = FolderItem{
				Name:     dashName,
				Type:     "dashboard",
				ParentID: rootFolderID,
			}
			structure.Order[rootFolderID] = append(structure.Order[rootFolderID], dashID)
		}
	}

	// Write new structure
	newData, err := json.MarshalIndent(structure, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal folder structure: %v", err)
	}

	if err := os.WriteFile(folderFile, newData, 0644); err != nil {
		return fmt.Errorf("failed to write folder structure: %v", err)
	}

	// After successful migration, we can remove allids.json
	// if len(data) > 0 {
	// 	if err := os.Remove(allidsFname); err != nil {
	// 		log.Warnf("Failed to remove allids.json after migration: %v", err)
	// 	}
	// }

	// log.Info("Successfully migrated to folder structure")
	return nil
}
