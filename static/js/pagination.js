let currentPage = 1;
let pageSize = 50; // Default page size
let totalLoadedRecords = 0;
let hasMoreRecords = false;
let accumulatedRecords = [];
let isLoadingMore = false;

//eslint-disable-next-line no-unused-vars
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

    const paginationContainer = document.querySelector('#pagination-container');
    paginationContainer.innerHTML = paginationHtml;
    $('#pagination-container').hide();

    document.getElementById('page-size-select').addEventListener('change', handlePageSizeChange);
}

function handlePageSizeChange(event) {
    pageSize = parseInt(event.target.value);
    currentPage = 1; // Reset to first page when changing page size

    if (lastQType === 'aggs-query' || lastQType === 'segstats-query') {
        paginateAggsData(segStatsRowData);
    } else {
        updateGridView();
        updatePaginationDisplay();
        updateLoadMoreMessage();
    }
}

//eslint-disable-next-line no-unused-vars
function goToPage(page) {
    const totalPages = Math.ceil(totalLoadedRecords / pageSize);
    if (page < 1 || page > totalPages) return;

    currentPage = page;

    if (lastQType === 'aggs-query' || lastQType === 'segstats-query') {
        paginateAggsData(segStatsRowData);
    } else {
        const startIndex = (currentPage - 1) * pageSize;

        if (startIndex + pageSize > totalLoadedRecords && hasMoreRecords) {
            loadMoreResults();
        } else {
            updateGridView();
            updatePaginationDisplay();
            updateLoadMoreMessage();
        }
    }
}

//eslint-disable-next-line no-unused-vars
function updatePaginationState(results) {
    $('#pagination-container').show();

    if (results.qtype === 'logs-query') {
        if (results.state === 'QUERY_UPDATE' && results.hits) {
            // Only reset records if this is a new search (not a load more)
            if (totalLoadedRecords === 0) {
                currentPage = 1;
            }

            // Update total loaded records
            if (results.hits.records) {
                totalLoadedRecords = accumulatedRecords.length;

                if (results.from > 0) {
                    const totalPages = Math.ceil(totalLoadedRecords / pageSize);

                    // If we're on the last page, stay there
                    if (currentPage === Math.ceil(results.from / pageSize)) {
                        currentPage = totalPages;
                    }
                }
            }

            if (results.hits.totalMatched) {
                hasMoreRecords = results.hits.totalMatched.relation === 'gte';
            }
        } else if (results.state === 'COMPLETE') {
            if (results.totalMatched) {
                totalLoadedRecords = accumulatedRecords.length;
                hasMoreRecords = results.totalMatched.relation === 'gte';
            }
        }
    } else if (results.qtype === 'aggs-query' || results.qtype === 'segstats-query') {
        if (results.state === 'COMPLETE') {
            currentPage = 1;
            totalLoadedRecords = results.bucketCount || 0;
            hasMoreRecords = false;
        }
    }
    updatePaginationDisplay();

    if (results.state === 'COMPLETE') {
        updateLoadMoreMessage();
    }
}

function updatePaginationDisplay() {
    if (!totalLoadedRecords) return;

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
        Showing ${startRecord.toLocaleString()}-${endRecord.toLocaleString()} of ${hasMoreRecords ? `${totalLoadedRecords?.toLocaleString()}+` : totalLoadedRecords?.toLocaleString()} records
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

    if (lastQType === 'aggs-query' || lastQType === 'segstats-query') {
        messageContainer.style.display = 'none';
        return;
    }

    if (hasMoreRecords && isLastPage()) {
        if (isLoadingMore) {
            messageContainer.innerHTML = `
                <div class="loading-more">
                    <div class="loading-spinner"></div>
                    <span>Loading more results...</span>
                </div>`;
        } else {
            messageContainer.innerHTML = `
                Search results are limited to ${totalLoadedRecords} documents. 
                <a href="#" onclick="loadMoreResults()" class="load-more-link">Load more</a>`;
        }
        messageContainer.style.display = 'block';
    } else {
        messageContainer.style.display = 'none';
    }
}

function loadMoreResults() {
    if (isLoadingMore) return;

    isLoadingMore = true;
    updateLoadMoreMessage();

    const data = getSearchFilter(true, true, false);
    data.from = totalLoadedRecords;

    if (initialSearchData && (data.searchText !== initialSearchData.searchText || data.indexName !== initialSearchData.indexName || data.startEpoch !== initialSearchData.startEpoch || data.endEpoch !== initialSearchData.endEpoch || data.queryLanguage !== initialSearchData.queryLanguage)) {
        // Show error if search params changed
        scrollingErrorPopup();
        isLoadingMore = false;
        updateLoadMoreMessage();
        return;
    }

    doSearch(data).finally(() => {
        isLoadingMore = false;
        updateLoadMoreMessage();
    });
}

function isLastPage() {
    return currentPage === Math.ceil(totalLoadedRecords / pageSize);
}

function scrollingErrorPopup() {
    $('.popupOverlay').addClass('active');
    $('#error-popup.popupContent').addClass('active');

    $('#okay-button').on('click', function () {
        $('.popupOverlay').removeClass('active');
        $('#error-popup.popupContent').removeClass('active');
    });
}
