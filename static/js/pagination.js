// Pagination state
let currentPage = 1;
let pageSize = 20; // Default page size
let totalLoadedRecords = 0;
let hasMoreRecords = false;
let accumulatedRecords = [];
function initializePagination() {
    const paginationHtml = `
        <div class="pagination-controls">
            <div class="pagination-left">
                <span>Rows per page:</span>
                <select id="page-size-select" class="page-size-select">
                    <option value="20">20</option>
                    <option value="25">25</option>
                    <option value="50">50</option>
                    <option value="100">100</option>
                </select>
            </div>
            <div class="pagination-right"></div>
        </div>
        <div id="load-more-container" class="load-more-container"></div>
    `;

    // Add pagination HTML after the grid
    const container = document.querySelector('#LogResultsGrid');
    container.insertAdjacentHTML('afterend', paginationHtml);

    // Add event listener
    document.getElementById('page-size-select').addEventListener('change', handlePageSizeChange);
}

function handlePageSizeChange(event) {
    pageSize = parseInt(event.target.value);
    currentPage = 1;
    updateGridView();
    updatePaginationDisplay();
    updateLoadMoreMessage(); // Add here
}

function goToPage(page) {
    const totalPages = Math.ceil(totalLoadedRecords / pageSize);
    if (page < 1 || page > totalPages) return;

    currentPage = page;
    const startIndex = (currentPage - 1) * pageSize;

    // If we need more data and this is the last page
    if (startIndex + pageSize > totalLoadedRecords && hasMoreRecords) {
        loadMoreResults();
    } else {
        updateGridView();
        updatePaginationDisplay();
        updateLoadMoreMessage(); // Add here
    }
}

function handleSearchResults(results) {
    // Handle QUERY_UPDATE state
    console.log(data)
    const fromValue = data.from || 0;  // 'data' being the last request sent

    if (results.state === 'QUERY_UPDATE' && results.hits) {
        // Only reset records if this is a new search (not a load more)
        if (totalLoadedRecords === 0) {
            accumulatedRecords = [];
            currentPage = 1;
        }

        // Add new records if available
        if (results.hits.records && Array.isArray(results.hits.records)) {
            // Append new records
            accumulatedRecords = [...accumulatedRecords, ...results.hits.records];
            logsRowData = accumulatedRecords;
            totalLoadedRecords = accumulatedRecords.length;

            // If this was a load more request, update pagination
            if (results.from > 0) {
                // Calculate new total pages
                const totalPages = Math.ceil(totalLoadedRecords / pageSize);

                // If we're on the last page, stay there
                if (currentPage === Math.ceil(results.from / pageSize)) {
                    currentPage = totalPages;
                }
            }
        }

        // Update hasMoreRecords from query update
        if (results.hits.totalMatched) {
            hasMoreRecords = results.hits.totalMatched.relation === 'gte';
        }

        // Update grid and pagination
        updateGridView();
        updatePaginationDisplay();
    }
    // Handle COMPLETE state
    else if (results.state === 'COMPLETE') {
        // Update final counts and flags
        if (results.totalMatched) {
            totalLoadedRecords = results.totalMatched.value;
            hasMoreRecords = results.totalMatched.relation === 'gte';
        }

        // Update scroll-related data
        canScrollMore = results.can_scroll_more;
        if (results.total_rrc_count > 0) {
            scrollFrom = results.total_rrc_count;
            totalRrcCount = results.total_rrc_count;
        }

        // Final updates
        updateGridView();
        updatePaginationDisplay();
        updateLoadMoreMessage();
    }
}

function updatePaginationDisplay() {
    const totalPages = Math.ceil(totalLoadedRecords / pageSize);
    const pagesContainer = document.querySelector('.pagination-right');

    const startRecord = (currentPage - 1) * pageSize + 1;
    const endRecord = Math.min(currentPage * pageSize, totalLoadedRecords);

    let paginationHTML = `
        <button class="page-btn" ${currentPage === 1 ? 'disabled' : ''} onclick="goToPage(1)">
            <i class="fa fa-angle-double-left"></i>
        </button>
        <button class="page-btn" ${currentPage === 1 ? 'disabled' : ''} onclick="goToPage(${currentPage - 1})">
            <i class="fa fa-angle-left"></i>
        </button>
        <div class="page-numbers">`;

    // Show pages with ellipsis if needed
    if (totalPages <= 7) {
        // Show all pages if total pages is 7 or less
        for (let i = 1; i <= totalPages; i++) {
            paginationHTML += createPageButton(i);
        }
    } else {
        // Show pages with ellipsis
        paginationHTML += createPageButton(1);
        if (currentPage > 3) paginationHTML += '<span class="page-ellipsis">...</span>';

        for (let i = Math.max(2, currentPage - 1); i <= Math.min(currentPage + 1, totalPages - 1); i++) {
            paginationHTML += createPageButton(i);
        }

        if (currentPage < totalPages - 2) paginationHTML += '<span class="page-ellipsis">...</span>';
        if (totalPages > 1) paginationHTML += createPageButton(totalPages);
    }

    paginationHTML += `</div>
        <button class="page-btn" ${currentPage === totalPages ? 'disabled' : ''} onclick="goToPage(${currentPage + 1})">
            <i class="fa fa-angle-right"></i>
        </button>
        <button class="page-btn" ${currentPage === totalPages ? 'disabled' : ''} onclick="goToPage(${totalPages})">
            <i class="fa fa-angle-double-right"></i>
        </button>
        <span class="pagination-info">
            Showing ${startRecord}-${endRecord} of ${totalLoadedRecords} records
        </span>`;

    pagesContainer.innerHTML = paginationHTML;
}

function createPageButton(pageNum) {
    return `<button class="page-number ${pageNum === currentPage ? 'active' : ''}" 
            onclick="goToPage(${pageNum})">${pageNum}</button>`;
}

function updateLoadMoreMessage() {
    const messageContainer = document.getElementById('load-more-container');
    if (!messageContainer) return;
    console.log('updateLoadMoreMessage: hasMoreRecords', hasMoreRecords);
    console.log('updateLoadMoreMessage: isLastPage()', isLastPage());
    if (hasMoreRecords && isLastPage()) {
        messageContainer.innerHTML = `
            Search results are limited to ${totalLoadedRecords} documents. 
            <a href="#" onclick="loadMoreResults()" class="load-more-link">Load more</a>`;
        messageContainer.style.display = 'block';
    } else {
        messageContainer.style.display = 'none';
    }
}

function loadMoreResults() {
    console.log("Loading results...");
    const data = getSearchFilter(true, true);
    data.from = totalLoadedRecords;
    // data.size = pageSize;

    if (initialSearchData && (
        data.searchText !== initialSearchData.searchText ||
        data.indexName !== initialSearchData.indexName ||
        data.startEpoch !== initialSearchData.startEpoch ||
        data.endEpoch !== initialSearchData.endEpoch ||
        data.queryLanguage !== initialSearchData.queryLanguage
    )) {
        // Show error if search params changed
        scrollingErrorPopup();
        return;
    }

    console.log('loadMoreResults', data);
    console.trace('loadMoreResults called');
    isLoadingMore = true;
    doSearch(data)
        .then(() => {
            isLoadingMore = false;
        })
        .catch((error) => {
            isLoadingMore = false;
            console.error('Error loading more results:', error);
        });
}

function isLastPage() {
    return currentPage === Math.ceil(totalLoadedRecords / pageSize);
}
