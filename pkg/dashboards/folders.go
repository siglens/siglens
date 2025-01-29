package dashboards

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/siglens/siglens/pkg/blob"
	"github.com/siglens/siglens/pkg/config"
	"github.com/siglens/siglens/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/valyala/fasthttp"
)

const (
	ItemTypeFolder    = "folder"
	ItemTypeDashboard = "dashboard"
	ItemTypeAll       = "all"
)

// item in the folder structure
type StoredFolderItem struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	ParentID    string `json:"parentId"`
	IsDefault   bool   `json:"isDefault"`
	CreatedAtMs int64  `json:"createdAtMs"`
}

type FolderStructure struct {
	Items map[string]StoredFolderItem `json:"items"` // uuid -> item
	Order map[string][]string         `json:"order"` // parentId -> ordered child IDs
}

type CreateFolderRequest struct {
	Name     string `json:"name"`
	ParentID string `json:"parentId"`
}

type FolderContentResponse struct {
	Folder      FolderItemResponse   `json:"folder"`
	Items       []FolderItemResponse `json:"items"`
	Breadcrumbs []Breadcrumb         `json:"breadcrumbs"`
}

type FolderItemResponse struct {
	ID         string `json:"id"`
	Name       string `json:"name"`
	Type       string `json:"type"`
	ChildCount int    `json:"childCount,omitempty"`
	IsDefault  bool   `json:"isDefault"`
}

type Breadcrumb struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

type UpdateFolderRequest struct {
	Name     string `json:"name,omitempty"`
	ParentID string `json:"parentId,omitempty"`
}

type ListItemsRequest struct {
	Sort        string `json:"sort"`     // alpha-asc, alpha-desc, created-asc, created-desc
	Query       string `json:"query"`    // Search term (name only)
	Type        string `json:"type"`     // dashboard, folder, or all
	StarredOnly bool   `json:"starred"`  // When true, show only starred items. When false, show all items
	FolderID    string `json:"folderId"` // Filter by folder
}

type ItemMetadata struct {
	ID          string    `json:"id"`
	Name        string    `json:"name"`
	Type        string    `json:"type"`
	ParentID    string    `json:"parentId"`
	ParentName  string    `json:"parentName"`
	FullPath    string    `json:"fullPath"`
	IsStarred   bool      `json:"isStarred"`
	CreatedAtMs time.Time `json:"createdAtMs"`
	Description string    `json:"description,omitempty"`
}

type ListItemsResponse struct {
	Items      []ItemMetadata `json:"items"`
	TotalCount int            `json:"totalCount"`
}

var (
	folderStructureLock sync.RWMutex
	rootFolderID        = "root-folder"
)

func getFolderStructureFilePath(myid uint64) string {
	if myid == 0 {
		return config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/folder_structure.json"
	}
	return config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/folder_structure-" + strconv.FormatUint(myid, 10) + ".json"
}

func getDefaultFolderStructureFilePath() string {
	return "defaultDBs/folder_structure.json"
}

func isDefaultFolder(id string) bool {
	defaultStructure, err := readDefaultFolderStructure()
	if err != nil {
		return false
	}

	item, exists := defaultStructure.Items[id]
	return exists && item.Type == ItemTypeFolder
}

func InitFolderStructure(myid uint64) error {
	folderStructureLock.Lock()
	defer folderStructureLock.Unlock()

	filePath := getFolderStructureFilePath(myid)

	if _, err := os.Stat(filePath); err == nil {
		return nil
	}

	structure := FolderStructure{
		Items: map[string]StoredFolderItem{
			rootFolderID: {
				Name:     "Root",
				Type:     ItemTypeFolder,
				ParentID: "",
			},
		},
		Order: map[string][]string{
			rootFolderID: {},
		},
	}

	data, err := json.MarshalIndent(structure, "", "  ")
	if err != nil {
		return fmt.Errorf("InitFolderStructure: failed to marshal initial folder structure: %v", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("InitFolderStructure: failed to write initial folder structure: %v", err)
	}

	return nil
}

func readCombinedFolderStructure(myid uint64) (*FolderStructure, error) {

	userStructure, err := readFolderStructure(myid)
	if err != nil {
		return nil, fmt.Errorf("readCombinedFolderStructure: failed to read user folder structure: %v", err)
	}

	// Read default folder structure
	defaultData, err := os.ReadFile(getDefaultFolderStructureFilePath())
	if err != nil {
		if os.IsNotExist(err) {
			return userStructure, nil
		}
		return nil, fmt.Errorf("readCombinedFolderStructure: failed to read default folder structure: %v", err)
	}

	var defaultStructure FolderStructure
	if err := json.Unmarshal(defaultData, &defaultStructure); err != nil {
		return nil, fmt.Errorf("readCombinedFolderStructure: failed to unmarshal default folder structure: %v", err)
	}

	// Merge default items into user structure
	for id, item := range defaultStructure.Items {
		if _, exists := userStructure.Items[id]; !exists {
			item.IsDefault = true // Mark as default item
			userStructure.Items[id] = item
		}
	}

	for parentID, defaultChildren := range defaultStructure.Order {
		if _, exists := userStructure.Order[parentID]; !exists {
			userStructure.Order[parentID] = defaultChildren
		} else {
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
		return nil, fmt.Errorf("readDefaultFolderStructure: failed to read default folder structure: %v", err)
	}

	var structure FolderStructure
	if err := json.Unmarshal(data, &structure); err != nil {
		return nil, fmt.Errorf("readDefaultFolderStructure: failed to unmarshal default folder structure: %v", err)
	}

	return &structure, nil
}

func readFolderStructure(myid uint64) (*FolderStructure, error) {
	folderStructureLock.RLock()
	defer folderStructureLock.RUnlock()

	data, err := os.ReadFile(getFolderStructureFilePath(myid))
	if err != nil {
		return nil, fmt.Errorf("readFolderStructure: failed to read folder structure: %v", err)
	}

	var structure FolderStructure
	if err := json.Unmarshal(data, &structure); err != nil {
		return nil, fmt.Errorf("readFolderStructure: failed to unmarshal folder structure: %v", err)
	}

	return &structure, nil
}

func writeFolderStructure(structure *FolderStructure, myid uint64) error {
	folderStructureLock.Lock()
	defer folderStructureLock.Unlock()

	data, err := json.MarshalIndent(structure, "", "  ")
	if err != nil {
		return fmt.Errorf("writeFolderStructure: failed to marshal folder structure: %v", err)
	}

	if err := os.WriteFile(getFolderStructureFilePath(myid), data, 0644); err != nil {
		return fmt.Errorf("writeFolderStructure: failed to write folder structure: %v", err)
	}

	return nil
}

func createFolder(req *CreateFolderRequest, myid uint64) (string, error) {
	if req.Name == "" {
		return "", errors.New("folder name cannot be empty")
	}

	if req.ParentID == "" {
		req.ParentID = rootFolderID
	}

	structure, err := readFolderStructure(myid)
	if err != nil {
		return "", err
	}

	// Validate parent exists and is a folder
	parent, exists := structure.Items[req.ParentID]
	if !exists {
		return "", errors.New("Parent folder not found")
	}
	if parent.Type != ItemTypeFolder {
		return "", errors.New("Parent must be a folder")
	}

	for _, childID := range structure.Order[req.ParentID] {
		if child, exists := structure.Items[childID]; exists {
			if child.Type == ItemTypeFolder && child.Name == req.Name {
				return "", errors.New("Folder with same name already exists in this location!")
			}
		}
	}

	folderID := uuid.New().String()

	structure.Items[folderID] = StoredFolderItem{
		Name:        req.Name,
		Type:        ItemTypeFolder,
		ParentID:    req.ParentID,
		CreatedAtMs: time.Now().UnixMilli(),
		IsDefault:   false,
	}

	structure.Order[folderID] = []string{}

	structure.Order[req.ParentID] = append(structure.Order[req.ParentID], folderID)

	if err := writeFolderStructure(structure, myid); err != nil {
		return "", err
	}

	return folderID, nil
}

func getFolderContents(folderID string, foldersOnly bool, myid uint64) (*FolderContentResponse, error) {
	structure, err := readCombinedFolderStructure(myid)

	if err != nil {
		if err := InitDashboards(myid); err != nil {
			return nil, fmt.Errorf("getFolderContents: failed to initialize dashboards: %v", err)
		}
		structure, err = readCombinedFolderStructure(myid)
		if err != nil {
			return nil, fmt.Errorf("getFolderContents: failed to read folder structure: %v", err)
		}
	}

	folder, exists := structure.Items[folderID]
	if !exists {
		return nil, fmt.Errorf("getFolderContents: folder not found: %s", folderID)
	}

	response := &FolderContentResponse{
		Folder: FolderItemResponse{
			ID:        folderID,
			Name:      folder.Name,
			Type:      folder.Type,
			IsDefault: folder.IsDefault,
		},
		Items: make([]FolderItemResponse, 0),
	}

	childIDs := structure.Order[folderID]
	for _, childID := range childIDs {
		child, exists := structure.Items[childID]
		if !exists {
			continue
		}
		// Skip if we only want folders and this is not a folder
		if foldersOnly && child.Type != ItemTypeFolder {
			continue
		}

		item := FolderItemResponse{
			ID:        childID,
			Name:      child.Name,
			Type:      child.Type,
			IsDefault: child.IsDefault,
		}

		// If it's a folder, count its children
		if child.Type == ItemTypeFolder {
			if children, exists := structure.Order[childID]; exists {
				item.ChildCount = len(children)
			}
		}

		response.Items = append(response.Items, item)
	}

	response.Breadcrumbs = generateBreadcrumbs(folderID, structure)

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

		breadcrumbs = append([]Breadcrumb{{
			ID:   currentID,
			Name: item.Name,
		}}, breadcrumbs...)

		currentID = item.ParentID
	}

	return breadcrumbs
}

func updateFolder(folderID string, req *UpdateFolderRequest, myid uint64) error {
	if folderID == rootFolderID {
		return fmt.Errorf("updateFolder: cannot update root folder")
	}

	if isDefaultFolder(folderID) {
		return fmt.Errorf("updateFolder: cannot update default folder")
	}

	structure, err := readFolderStructure(myid)
	if err != nil {
		return fmt.Errorf("updateFolder: failed to read folder structure: %v", err)
	}

	folder, exists := structure.Items[folderID]
	if !exists {
		return fmt.Errorf("updateFolder: folder not found: %s", folderID)
	}

	// If moving to new parent
	if req.ParentID != "" && req.ParentID != folder.ParentID {
		// Validate new parent exists and is a folder
		newParent, exists := structure.Items[req.ParentID]
		if !exists {
			return fmt.Errorf("updateFolder: new parent folder not found: %s", req.ParentID)
		}
		if newParent.Type != ItemTypeFolder {
			return fmt.Errorf("updateFolder: new parent must be a folder")
		}

		// Check for circular reference
		if wouldCreateCircularReference(folderID, req.ParentID, structure) {
			return fmt.Errorf("updateFolder: cannot move folder: would create circular reference")
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
					return fmt.Errorf("updateFolder: folder with name %s already exists in this location", req.Name)
				}
			}
		}
		folder.Name = req.Name
	}

	structure.Items[folderID] = folder

	if err := writeFolderStructure(structure, myid); err != nil {
		return fmt.Errorf("updateFolder: failed to write folder structure: %v", err)
	}

	return nil
}

// Uses Floyd's cycle-finding algorithm (tortoise and hare) to detect circular references
func wouldCreateCircularReference(folderID, newParentID string, structure *FolderStructure) bool {
	slow, fast := newParentID, newParentID
	for {
		if slow == "" {
			return false
		}
		if slow == folderID {
			return true
		}
		// Move slow pointer one step
		if item, exists := structure.Items[slow]; exists {
			slow = item.ParentID
		} else {
			return false
		}
		// Move fast pointer two steps
		for i := 0; i < 2; i++ {
			if fast == "" {
				return false
			}
			if fast == folderID {
				return true
			}
			if item, exists := structure.Items[fast]; exists {
				fast = item.ParentID
			} else {
				return false
			}
		}

		if slow == fast {
			return true
		}
	}
}

func deleteFolder(folderID string, myid uint64) error {
	if folderID == rootFolderID {
		return fmt.Errorf("deleteFolder: cannot delete root folder")
	}

	if isDefaultFolder(folderID) {
		return fmt.Errorf("deleteFolder: cannot delete default folder")
	}

	structure, err := readFolderStructure(myid)
	if err != nil {
		return fmt.Errorf("deleteFolder: failed to read folder structure: %v", err)
	}

	folder, exists := structure.Items[folderID]
	if !exists {
		return fmt.Errorf("deleteFolder: folder not found: %s", folderID)
	}

	// Get all items to be deleted (recursive)
	itemsToDelete := make([]string, 0)
	collectItemsToDelete(folderID, structure, &itemsToDelete)

	// Delete all dashboard files
	for _, itemID := range itemsToDelete {
		item := structure.Items[itemID]
		if item.Type == ItemTypeDashboard {
			if err := deleteDashboardFile(itemID); err != nil {
				log.Errorf("deleteFolder: failed to delete dashboard file %s: %v", itemID, err)
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
		delete(structure.Order, itemID)
	}

	if err := writeFolderStructure(structure, myid); err != nil {
		return fmt.Errorf("deleteFolder: failed to write folder structure: %v", err)
	}

	if err := blob.UploadQueryNodeDir(); err != nil {
		log.Errorf("deleteFolder: Failed to upload query nodes dir, err=%v", err)
		return err
	}

	return nil
}

func collectItemsToDelete(folderID string, structure *FolderStructure, itemsToDelete *[]string) {
	// Add current folder to delete list
	*itemsToDelete = append(*itemsToDelete, folderID)

	// Recursively process children
	if childIDs, exists := structure.Order[folderID]; exists {
		for _, childID := range childIDs {
			if child, exists := structure.Items[childID]; exists {
				if child.Type == ItemTypeFolder {
					collectItemsToDelete(childID, structure, itemsToDelete)
				} else {
					*itemsToDelete = append(*itemsToDelete, childID)
				}
			}
		}
	}
}

func deleteDashboardFile(dashboardID string) error {
	dashboardDetailsFname := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + dashboardID + ".json"
	return os.Remove(dashboardDetailsFname)
}

func listItems(req *ListItemsRequest, myid uint64) (*ListItemsResponse, error) {
	// Read folder structure
	structure, err := readCombinedFolderStructure(myid)
	if err != nil {
		return nil, fmt.Errorf("listItems: failed to read folder structure: %v", err)
	}

	items := make([]ItemMetadata, 0)

	getFullPath := func(itemID string) string {
		path := []string{}
		currentID := itemID
		for currentID != "" && currentID != rootFolderID {
			if item, exists := structure.Items[currentID]; exists {
				path = append([]string{item.Name}, path...)
				currentID = item.ParentID
			} else {
				break
			}
		}
		return strings.Join(path, "/")
	}

	for id, item := range structure.Items {
		// Skip root folder
		if id == rootFolderID {
			continue
		}

		// Apply folder filter
		if req.FolderID != "" {
			if req.FolderID == rootFolderID {
				// For root folder, include all items at any level
				// No filtering needed as we want to show everything
			} else {
				// For specific folder, show only immediate children
				if item.ParentID != req.FolderID {
					continue
				}
			}
		}

		// Apply type filter
		if req.Type != "" && req.Type != ItemTypeAll && item.Type != req.Type {
			continue
		}

		// Get item details
		details, _ := getDashboard(id, myid)
		isStarred := false
		var createdAtMs time.Time
		description := ""
		if item.Type == ItemTypeDashboard {
			if details != nil {
				if starred, ok := details["isFavorite"].(bool); ok {
					isStarred = starred
				}
				if created, ok := details["createdAtMs"].(float64); ok && created > 0 {
					createdAtMs = time.UnixMilli(int64(created))
				} else if created, ok := details["createdAtMs"].(int64); ok && created > 0 {
					createdAtMs = time.UnixMilli(created)
				}
				if desc, ok := details["description"].(string); ok {
					description = desc
				}
			}
		} else if item.Type == ItemTypeFolder {
			if item.CreatedAtMs > 0 {
				createdAtMs = time.UnixMilli(item.CreatedAtMs)
			}
		}

		// Apply starred filter
		if req.StarredOnly && !isStarred {
			continue
		}

		fullPath := getFullPath(id)
		// Get parent folder name
		parentName := ""
		if item.ParentID != rootFolderID {
			if parentFolder, exists := structure.Items[item.ParentID]; exists {
				parentName = parentFolder.Name
			}
		}

		metadata := ItemMetadata{
			ID:          id,
			Name:        item.Name,
			Type:        item.Type,
			ParentID:    item.ParentID,
			ParentName:  parentName,
			FullPath:    fullPath,
			IsStarred:   isStarred,
			CreatedAtMs: createdAtMs,
			Description: description,
		}

		// Apply name-only search filter
		if req.Query != "" {
			query := strings.ToLower(req.Query)
			if !strings.Contains(strings.ToLower(metadata.Name), query) {
				continue
			}
		}

		items = append(items, metadata)
	}

	switch req.Sort {
	case "alpha-asc":
		sort.Slice(items, func(i, j int) bool {
			return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
		})
	case "alpha-desc":
		sort.Slice(items, func(i, j int) bool {
			return strings.ToLower(items[i].Name) > strings.ToLower(items[j].Name)
		})
	case "created-asc":
		sort.Slice(items, func(i, j int) bool {
			return items[i].CreatedAtMs.Before(items[j].CreatedAtMs)
		})
	case "created-desc":
		sort.Slice(items, func(i, j int) bool {
			return items[i].CreatedAtMs.After(items[j].CreatedAtMs)
		})
	}

	totalCount := len(items)

	return &ListItemsResponse{
		Items:      items,
		TotalCount: totalCount,
	}, nil
}

// TODO: After migration is complete:
// 1. Remove getAllIdsFileName function
// 2. Remove migration-related code from InitDashboards
// 3. Remove migrateToFolderStructure function

func getAllIdsFileName(myid uint64) string {
	baseDir := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards"
	allidsBaseFname := baseDir + "/allids"

	if myid == 0 {
		return allidsBaseFname + ".json"
	}
	return allidsBaseFname + "-" + strconv.FormatUint(myid, 10) + ".json"
}

func migrateToFolderStructure(myid uint64) error {
	folderStructureLock.Lock()
	defer folderStructureLock.Unlock()

	folderFile := getFolderStructureFilePath(myid)

	if _, err := os.Stat(folderFile); err == nil {
		return nil
	}

	structure := FolderStructure{
		Items: map[string]StoredFolderItem{
			rootFolderID: {
				Name:     "Root",
				Type:     ItemTypeFolder,
				ParentID: "",
			},
		},
		Order: map[string][]string{
			rootFolderID: {},
		},
	}

	allidsFname := getAllIdsFileName(myid)
	data, _ := os.ReadFile(allidsFname)

	if len(data) > 0 {
		var existingDashboards map[string]string
		if err := json.Unmarshal(data, &existingDashboards); err != nil {
			return fmt.Errorf("migrateToFolderStructure: failed to unmarshal allids.json: %v", err)
		}

		for dashID, dashName := range existingDashboards {
			detailsPath := config.GetDataPath() + "querynodes/" + config.GetHostID() + "/dashboards/details/" + dashID + ".json"
			if _, err := os.Stat(detailsPath); err != nil {
				continue
			}

			structure.Items[dashID] = StoredFolderItem{
				Name:     dashName,
				Type:     ItemTypeDashboard,
				ParentID: rootFolderID,
			}
			structure.Order[rootFolderID] = append(structure.Order[rootFolderID], dashID)
		}
	}

	newData, err := json.MarshalIndent(structure, "", "  ")
	if err != nil {
		return fmt.Errorf("migrateToFolderStructure: failed to marshal folder structure: %v", err)
	}

	if err := os.WriteFile(folderFile, newData, 0644); err != nil {
		return fmt.Errorf("migrateToFolderStructure: failed to write folder structure: %v", err)
	}

	// After successful migration, we can remove allids.json
	if len(data) > 0 {
		if err := os.Remove(allidsFname); err != nil {
			log.Warnf("migrateToFolderStructure: Failed to remove allids.json after migration: %v", err)
		}
	}

	return nil
}

func ProcessCreateFolderRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	var req CreateFolderRequest
	if err := json.Unmarshal(ctx.PostBody(), &req); err != nil {
		log.Errorf("ProcessCreateFolderRequest: failed to unmarshal request: %v", err)
		utils.SetBadMsg(ctx, "Invalid request body")
		return
	}

	folderID, err := createFolder(&req, myid)
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

func ProcessGetFolderContentsRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	folderID := utils.ExtractParamAsString(ctx.UserValue("folder-id"))
	if folderID == "" {
		folderID = rootFolderID
	}

	// Check if we only want folders
	foldersOnly := string(ctx.QueryArgs().Peek("foldersOnly")) == "true"

	contents, err := getFolderContents(folderID, foldersOnly, myid)
	if err != nil {
		log.Errorf("ProcessGetFolderContentsRequest: failed to get folder contents: %v", err)
		utils.SetBadMsg(ctx, "")
		return
	}

	utils.WriteJsonResponse(ctx, contents)
	ctx.SetStatusCode(fasthttp.StatusOK)
}

func ProcessUpdateFolderRequest(ctx *fasthttp.RequestCtx, myid uint64) {
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

	if err := updateFolder(folderID, &req, myid); err != nil {
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

func ProcessDeleteFolderRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	folderID := utils.ExtractParamAsString(ctx.UserValue("folder-id"))
	if folderID == "" {
		utils.SetBadMsg(ctx, "Folder ID is required")
		return
	}

	if err := deleteFolder(folderID, myid); err != nil {
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

func ProcessListAllItemsRequest(ctx *fasthttp.RequestCtx, myid uint64) {
	req := &ListItemsRequest{
		Sort:     string(ctx.QueryArgs().Peek("sort")),
		Query:    string(ctx.QueryArgs().Peek("query")),
		Type:     string(ctx.QueryArgs().Peek("type")),
		FolderID: string(ctx.QueryArgs().Peek("folderId")),
	}

	if req.FolderID == "" {
		req.FolderID = rootFolderID
	}

	if string(ctx.QueryArgs().Peek("starred")) == "true" {
		req.StarredOnly = true
	}

	response, err := listItems(req, myid)
	if err != nil {
		log.Errorf("ProcessListAllItemsRequest: failed to list items: %v", err)
		utils.SetBadMsg(ctx, "Failed to list items")
		return
	}

	utils.WriteJsonResponse(ctx, response)
	ctx.SetStatusCode(fasthttp.StatusOK)
}
