// Get folder ID from URL or use root-folder as default
function getCurrentFolderId() {
    const urlParams = new URLSearchParams(window.location.search);
    return urlParams.get('id') || 'root-folder';
}

async function getFolderContents(folderId = 'root-folder') {
    try {
        const response = await $.ajax({
            method: 'get',
            url: `api/dashboards/folders/${folderId}`,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
        });
        return response;
    } catch (error) {
        console.error('Error fetching folder contents:', error);
        return null;
    }
}

// Fetch folder contents with optional parameters
async function getDashboardFolderList(folderId, params = {}) {
    try {
        const queryParams = new URLSearchParams();

        // Add folderId
        queryParams.set('folderId', folderId);

        // Add any additional parameters
        Object.entries(params).forEach(([key, value]) => {
            if (value !== undefined && value !== null) {
                queryParams.set(key, value);
            }
        });

        const response = await $.ajax({
            method: 'get',
            url: `api/dashboards/list?${queryParams.toString()}`,
            headers: {
                'Content-Type': 'application/json; charset=utf-8',
                Accept: '*/*',
            },
            crossDomain: true,
        });
        return response;
    } catch (error) {
        console.error('Error fetching folder contents:', error);
        return null;
    }
}

// Setup search functionality
function setupSearch(searchInput, grid, folderId) {
    console.log("Setup Search");
    let searchTimeout;

    searchInput.addEventListener('input', async (e) => {
        clearTimeout(searchTimeout);
        const query = e.target.value.trim();
        console.log("query", query);
        // Update URL
        updateUrl({ search: query });

        searchTimeout = setTimeout(async () => {
            if (query) {
                const results = await getDashboardFolderList(folderId, {
                    query: query,
                    sort: 'alpha-asc',
                });
                if (results) {
                    grid.setData(results.items, true); // Switch to search view
                }
            } else {
                const folderContents = await getFolderContents(folderId);
                grid.setData(folderContents.items, false); // Return to tree view
            }
        }, 300);
    });
}

// Update URL without page reload
function updateUrl(params = {}) {
    const url = new URL(window.location.href);

    Object.entries(params).forEach(([key, value]) => {
        if (value) {
            url.searchParams.set(key, value);
        } else {
            url.searchParams.delete(key);
        }
    });

    window.history.replaceState({}, '', url);
}

// Initialize page with URL parameters
async function initializePage(grid, options = {}) {
    const urlParams = new URLSearchParams(window.location.search);
    const folderId = getCurrentFolderId();
    const searchQuery = urlParams.get('search');

    if (searchQuery) {
        // Set search input value if it exists
        const searchInput = document.getElementById('search-input');
        if (searchInput) {
            searchInput.value = searchQuery;
        }

        // Perform search
        const results = await getFolderContents(folderId, {
            query: searchQuery,
            sort: 'alpha-asc',
        });
        if (results) {
            grid.setData(results.items, true);
        }
    } else {
        // Load normal folder view
        const folderContents = await getFolderContents(folderId);
        grid.setData(folderContents.items, false);
    }
}
