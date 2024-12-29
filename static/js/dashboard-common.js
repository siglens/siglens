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

function initializeDashboardPage(options = {}) {
    const addNew = new AddNewComponent('add-new-container');
    const grid = new DashboardGrid('dashboard-grid');
    const folderId = getCurrentFolderId();

    const urlParams = new URLSearchParams(window.location.search);
    const searchQuery = urlParams.get('query');
    const sortValue = urlParams.get('sort');
    const isStarred = urlParams.get('starred') === 'true';

    // Initialize sort dropdown
    const sortDropdown = new SortDropdown('sort-container', {
        onSort: async (sortValue) => handleFilters({ sort: sortValue }),
        initialSort: sortValue,
    });

    // Setup starred checkbox
    const starredCheckbox = document.getElementById('starred-filter');
    if (starredCheckbox) {
        starredCheckbox.checked = isStarred;
        starredCheckbox.addEventListener('change', (e) => {
            handleFilters({ starred: e.target.checked });
        });
    }

    // Setup search
    const searchInput = document.getElementById('search-input');
    if (searchInput) {
        setupSearch(searchInput, grid, folderId);
        if (searchQuery) {
            searchInput.value = searchQuery;
        }
    }

    function setupSearch(searchInput, grid, folderId) {
        let searchTimeout;

        searchInput.addEventListener('input', async (e) => {
            clearTimeout(searchTimeout);
            const query = e.target.value.trim();

            searchTimeout = setTimeout(() => {
                handleFilters({ query });
            }, 300);
        });
    }

    async function handleFilters(newFilters = {}) {
        const urlParams = new URLSearchParams(window.location.search);
        const filters = {
            query: searchInput?.value?.trim() || '',
            sort: urlParams.get('sort'),
            starred: urlParams.get('starred') === 'true',
            ...newFilters,
        };

        // Update URL
        Object.entries(filters).forEach(([key, value]) => {
            if (value) {
                urlParams.set(key, value);
            } else {
                urlParams.delete(key);
            }
        });
        window.history.replaceState({}, '', `?${urlParams.toString()}`);

        const hasActiveFilters = filters.query || filters.sort || filters.starred;

        if (hasActiveFilters) {
            const results = await getDashboardFolderList(folderId, filters);
            if (results) {
                grid.setData(results.items, true); // List view for filters
            }
        } else {
            const folderContents = await getFolderContents(folderId);
            grid.setData(folderContents.items, false); // Tree view when no filters
        }
    }

    // Initial load
    const loadData = async () => {
        if (searchQuery || sortValue || isStarred) {
            await handleFilters({
                query: searchQuery,
                sort: sortValue,
                starred: isStarred,
            });
        } else {
            const folderContents = await getFolderContents(folderId);
            grid.setData(folderContents.items, false);
        }
    };

    return { grid, folderId, loadData };
}
