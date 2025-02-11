package dashboards

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/siglens/siglens/pkg/config"
	"github.com/stretchr/testify/assert"
)

func initializeTestEnv(t *testing.T) {
	config.InitializeTestingConfig(t.TempDir())
	baseDir := filepath.Join(config.GetDataPath(), "querynodes", config.GetHostID(), "dashboards")
	detailsDir := filepath.Join(baseDir, "details")
	err := os.MkdirAll(detailsDir, 0764)
	assert.Nil(t, err)

	err = InitFolderStructure(0)
	assert.Nil(t, err)
}

func Test_InitFolderStructure(t *testing.T) {
	initializeTestEnv(t)

	// Verify the folder structure file exists and contains correct data
	data, err := os.ReadFile(getFolderStructureFilePath(0))
	assert.Nil(t, err)

	var structure FolderStructure
	err = json.Unmarshal(data, &structure)
	assert.Nil(t, err)

	// Check root folder exists
	rootFolder, exists := structure.Items[rootFolderID]
	assert.True(t, exists)
	assert.Equal(t, "Root", rootFolder.Name)
	assert.Equal(t, ItemTypeFolder, rootFolder.Type)
}

func Test_CreateFolder(t *testing.T) {
	initializeTestEnv(t)

	// Test case 1: Create valid folder
	req := CreateFolderRequest{
		Name:     "Test Folder",
		ParentID: rootFolderID,
	}
	folderID, err := createFolder(&req, 0)
	assert.Nil(t, err)
	assert.NotEmpty(t, folderID)

	structure, err := readFolderStructure(0)
	assert.Nil(t, err)

	folder, exists := structure.Items[folderID]
	assert.True(t, exists)
	assert.Equal(t, req.Name, folder.Name)
	assert.Equal(t, ItemTypeFolder, folder.Type)
	assert.Equal(t, rootFolderID, folder.ParentID)

	// Test case 2: Create folder with empty name
	invalidReq := CreateFolderRequest{
		Name:     "",
		ParentID: rootFolderID,
	}
	_, err = createFolder(&invalidReq, 0)
	assert.NotNil(t, err)

	// Test case 3: Create folder with invalid parent
	invalidParentReq := CreateFolderRequest{
		Name:     "Test Folder 2",
		ParentID: "non-existent-id",
	}
	_, err = createFolder(&invalidParentReq, 0)
	assert.NotNil(t, err)
}

func Test_CreateDashboard(t *testing.T) {
	initializeTestEnv(t)

	// Test case 1: Create valid dashboard
	req := CreateDashboardRequest{
		Name:        "Test Dashboard",
		Description: "Test Description",
		ParentID:    rootFolderID,
	}
	result, err := createDashboard(&req, 0)
	assert.Nil(t, err)
	assert.NotEmpty(t, result)

	for id := range result {
		dashboard, err := getDashboard(id, 0)
		assert.Nil(t, err)
		assert.Equal(t, req.Name, dashboard["name"])
		assert.Equal(t, req.Description, dashboard["description"])
	}

	// Test case 2: Create duplicate dashboard in same folder
	duplicateReq := CreateDashboardRequest{
		Name:        "Test Dashboard",
		Description: "Duplicate Dashboard",
		ParentID:    rootFolderID,
	}
	_, err = createDashboard(&duplicateReq, 0)
	assert.NotNil(t, err)
	assert.Equal(t, ErrDashboardNameExists, err.Error())

	// Test case 3: Create dashboard with empty name
	invalidReq := CreateDashboardRequest{
		Name:        "",
		Description: "Invalid Dashboard",
		ParentID:    rootFolderID,
	}
	_, err = createDashboard(&invalidReq, 0)
	assert.NotNil(t, err)
}

func Test_DeleteFolder(t *testing.T) {
	initializeTestEnv(t)

	folderReq := CreateFolderRequest{
		Name:     "Test Folder",
		ParentID: rootFolderID,
	}
	folderID, err := createFolder(&folderReq, 0)
	assert.Nil(t, err)

	dashReq := CreateDashboardRequest{
		Name:        "Test Dashboard",
		Description: "Test Description",
		ParentID:    folderID,
	}
	_, err = createDashboard(&dashReq, 0)
	assert.Nil(t, err)

	// Test case 1: Delete folder with contents
	err = deleteFolder(folderID, 0)
	assert.Nil(t, err)

	structure, err := readFolderStructure(0)
	assert.Nil(t, err)

	_, exists := structure.Items[folderID]
	assert.False(t, exists)

	// Test case 2: Try to delete root folder
	err = deleteFolder(rootFolderID, 0)
	assert.NotNil(t, err)

	// Test case 3: Try to delete non-existent folder
	err = deleteFolder("non-existent-id", 0)
	assert.NotNil(t, err)
}

func Test_ListItems(t *testing.T) {
	initializeTestEnv(t)

	folderReq := CreateFolderRequest{
		Name:     "Test Folder",
		ParentID: rootFolderID,
	}
	folderID, err := createFolder(&folderReq, 0)
	assert.Nil(t, err)

	dashReq := CreateDashboardRequest{
		Name:        "Test Dashboard",
		Description: "Test Description",
		ParentID:    folderID,
	}
	_, err = createDashboard(&dashReq, 0)
	assert.Nil(t, err)

	// Test case 1: List all items
	req := ListItemsRequest{
		Type:     ItemTypeAll,
		FolderID: rootFolderID,
	}
	result, err := listItems(&req, 0)
	assert.Nil(t, err)
	assert.Equal(t, 2, result.TotalCount) // Folder and Dashboard

	// Test case 2: List only folders
	req.Type = ItemTypeFolder
	result, err = listItems(&req, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, result.TotalCount) // Only folder

	// Test case 3: Search by name
	req.Type = ItemTypeAll
	req.Query = "Dashboard"
	result, err = listItems(&req, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, result.TotalCount) // Only dashboard matches
}

func Test_UpdateDashboard(t *testing.T) {
	initializeTestEnv(t)

	dashReq := CreateDashboardRequest{
		Name:        "Test Dashboard",
		Description: "Original Description",
		ParentID:    rootFolderID,
	}
	result, err := createDashboard(&dashReq, 0)
	assert.Nil(t, err)

	var dashID string
	for id := range result {
		dashID = id
	}

	// Test case 1: Update dashboard details
	updatedDetails := map[string]interface{}{
		"name":        "Updated Dashboard",
		"description": "Updated Description",
	}
	err = updateDashboard(dashID, "Updated Dashboard", updatedDetails, 0)
	assert.Nil(t, err)

	dashboard, err := getDashboard(dashID, 0)
	assert.Nil(t, err)
	assert.Equal(t, "Updated Dashboard", dashboard["name"])
	assert.Equal(t, "Updated Description", dashboard["description"])
}

func Test_ToggleFavorite(t *testing.T) {
	initializeTestEnv(t)

	dashReq := CreateDashboardRequest{
		Name:        "Test Dashboard",
		Description: "Test Description",
		ParentID:    rootFolderID,
	}
	result, err := createDashboard(&dashReq, 0)
	assert.Nil(t, err)

	var dashID string
	for id := range result {
		dashID = id
	}

	// Test case 1: Toggle favorite on
	isFavorite, err := toggleFavorite(dashID, 0)
	assert.Nil(t, err)
	assert.True(t, isFavorite)

	dashboard, err := getDashboard(dashID, 0)
	assert.Nil(t, err)
	assert.True(t, dashboard["isFavorite"].(bool))

	// Test case 2: Toggle favorite off
	isFavorite, err = toggleFavorite(dashID, 0)
	assert.Nil(t, err)
	assert.False(t, isFavorite)
}

func Test_UpdateFolder(t *testing.T) {
	initializeTestEnv(t)

	folder1Req := CreateFolderRequest{
		Name:     "Folder 1",
		ParentID: rootFolderID,
	}
	folder1ID, err := createFolder(&folder1Req, 0)
	assert.Nil(t, err)

	folder2Req := CreateFolderRequest{
		Name:     "Folder 2",
		ParentID: rootFolderID,
	}
	folder2ID, err := createFolder(&folder2Req, 0)
	assert.Nil(t, err)

	// Test case 1: Update folder name
	updateReq := UpdateFolderRequest{
		Name: "Updated Folder 1",
	}
	err = updateFolder(folder1ID, &updateReq, 0)
	assert.Nil(t, err)

	structure, err := readFolderStructure(0)
	assert.Nil(t, err)
	assert.Equal(t, "Updated Folder 1", structure.Items[folder1ID].Name)

	// Test case 2: Move folder to new parent
	moveReq := UpdateFolderRequest{
		ParentID: folder2ID,
	}
	err = updateFolder(folder1ID, &moveReq, 0)
	assert.Nil(t, err)

	structure, err = readFolderStructure(0)
	assert.Nil(t, err)
	assert.Equal(t, folder2ID, structure.Items[folder1ID].ParentID)

	// Test case 3: Prevent circular reference
	circularReq := UpdateFolderRequest{
		ParentID: folder1ID,
	}
	err = updateFolder(folder2ID, &circularReq, 0)
	assert.NotNil(t, err)
}

func Test_GenerateBreadcrumbs(t *testing.T) {
	initializeTestEnv(t)

	// Create nested folder structure
	folder1Req := CreateFolderRequest{
		Name:     "Level 1",
		ParentID: rootFolderID,
	}
	folder1ID, err := createFolder(&folder1Req, 0)
	assert.Nil(t, err)

	folder2Req := CreateFolderRequest{
		Name:     "Level 2",
		ParentID: folder1ID,
	}
	folder2ID, err := createFolder(&folder2Req, 0)
	assert.Nil(t, err)

	folder3Req := CreateFolderRequest{
		Name:     "Level 3",
		ParentID: folder2ID,
	}
	folder3ID, err := createFolder(&folder3Req, 0)
	assert.Nil(t, err)

	// Test case 1: Deep nested breadcrumbs
	structure, err := readFolderStructure(0)
	assert.Nil(t, err)

	breadcrumbs := generateBreadcrumbs(folder3ID, structure)
	assert.Equal(t, 4, len(breadcrumbs)) // Root -> Level 1 -> Level 2 -> Level 3
	assert.Equal(t, "Level 1", breadcrumbs[1].Name)
	assert.Equal(t, "Level 2", breadcrumbs[2].Name)
	assert.Equal(t, "Level 3", breadcrumbs[3].Name)

	// Test case 2: Root folder breadcrumbs
	breadcrumbs = generateBreadcrumbs(rootFolderID, structure)
	assert.Equal(t, 1, len(breadcrumbs))
	assert.Equal(t, "Root", breadcrumbs[0].Name)

	// Test case 3: Invalid folder ID
	breadcrumbs = generateBreadcrumbs("invalid-id", structure)
	assert.Empty(t, breadcrumbs)
}

func Test_GetFolderContents(t *testing.T) {
	initializeTestEnv(t)

	// Test case 1: Empty folder contents
	contents, err := getFolderContents(rootFolderID, false, 0)
	assert.Nil(t, err)
	assert.Empty(t, contents.Items)

	folder1Req := CreateFolderRequest{
		Name:     "Folder 1",
		ParentID: rootFolderID,
	}
	folder1ID, err := createFolder(&folder1Req, 0)
	assert.Nil(t, err)

	dashReq := CreateDashboardRequest{
		Name:        "Dashboard 1",
		Description: "Test Dashboard",
		ParentID:    folder1ID,
	}
	_, err = createDashboard(&dashReq, 0)
	assert.Nil(t, err)

	// Test case 2: Get all contents
	contents, err = getFolderContents(folder1ID, false, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(contents.Items))
	assert.Equal(t, "Dashboard 1", contents.Items[0].Name)

	// Test case 3: Get folders only
	contents, err = getFolderContents(rootFolderID, true, 0)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(contents.Items))
	assert.Equal(t, ItemTypeFolder, contents.Items[0].Type)

	// Test case 4: Invalid folder ID
	_, err = getFolderContents("invalid-id", false, 0)
	assert.NotNil(t, err)
}
