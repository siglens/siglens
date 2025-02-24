// Pagination state
let currentPage = 1;
let pageSize = 20; // Default page size
let totalLoadedRecords = 0;
let hasMoreRecords = false;

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
    let records;
    let hasMore = false;

    if (results.hits?.records) {
        records = results.hits.records;
        hasMore = results.hits.totalMatched.relation === 'gte';
        totalLoadedRecords = results.hits.totalMatched.value;
    }

    if (results.state === 'COMPLETE') {
        hasMore = results.totalMatched.relation === 'gte';
        hasMoreRecords = hasMore;
        updateLoadMoreMessage();
    }

    if (records) {
        if (results.from === 0) {
            logsRowData = records;
            currentPage = 1;
        } else {
            logsRowData = [...logsRowData, ...records];
        }

        hasMoreRecords = hasMore;
        updateGridView();
        updatePaginationDisplay();
        updateLoadMoreMessage();
    }
}

function updateGridView() {
    const startIndex = (currentPage - 1) * pageSize;
    const endIndex = Math.min(startIndex + pageSize, totalLoadedRecords);
    const currentPageData = logsRowData.slice(startIndex, endIndex);
    gridOptions.api.setRowData(currentPageData);
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
    const data = getSearchFilter(false, false);
    data.from = totalLoadedRecords;
    data.size = pageSize;
    doSearch(data);
}

function isLastPage() {
    return currentPage === Math.ceil(totalLoadedRecords / pageSize);
}
