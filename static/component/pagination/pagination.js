/*
 * Copyright (c) 2021-2025 SigScalr, Inc.
 *
 * This file is part of SigLens Observability Solution
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
class Pagination {
    constructor(containerId, options = {}) {
        this.containerId = containerId;
        this.container = document.getElementById(containerId);

        this.config = {
            pageSize: 20,
            pageSizeOptions: [10, 20, 50, 100],
            showPageSizeSelector: true,
            maxVisiblePages: 7,
            ...options,
        };

        this.currentPage = 1;
        this.totalRecords = 0;

        this.onPageChange = options.onPageChange || (() => {});
        this.onPageSizeChange = options.onPageSizeChange || (() => {});

        this.init();
    }

    init() {
        this.render();
        this.attachEventListeners();
        this.hide();
    }

    render() {
        const html = `
            <div class="pagination-controls">
                <div class="pagination-left">
                    ${this.config.showPageSizeSelector ? this.renderPageSizeSelector() : ''}
                </div>
                <div class="pagination-right"></div>
            </div>
        `;
        this.container.innerHTML = html;
    }

    renderPageSizeSelector() {
        const options = this.config.pageSizeOptions.map((size) => `<option value="${size}" ${size === this.config.pageSize ? 'selected' : ''}>${size}</option>`).join('');

        return `
            <span>Rows per page:</span>
            <select class="page-size-select">${options}</select>
        `;
    }

    attachEventListeners() {
        const select = this.container.querySelector('.page-size-select');
        if (select) {
            select.addEventListener('change', (e) => {
                this.config.pageSize = parseInt(e.target.value);
                this.currentPage = 1;
                this.onPageSizeChange(this.config.pageSize);
                this.updateDisplay();
            });
        }
    }

    goToPage(page) {
        const totalPages = Math.ceil(this.totalRecords / this.config.pageSize);
        if (page < 1 || page > totalPages) return;

        this.currentPage = page;
        this.onPageChange(page, this.config.pageSize);
        this.updateDisplay();
    }

    updateState(totalRecords, currentPage = 1) {
        this.totalRecords = totalRecords;
        this.currentPage = currentPage;
        this.updateDisplay();
    }

    updateDisplay() {
        if (!this.totalRecords) return;

        const totalPages = Math.ceil(this.totalRecords / this.config.pageSize);
        const pagesContainer = this.container.querySelector('.pagination-right');

        const startRecord = (this.currentPage - 1) * this.config.pageSize + 1;
        const endRecord = Math.min(this.currentPage * this.config.pageSize, this.totalRecords);

        let html = `
            <button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} 
                    onclick="window.paginationInstances['${this.containerId}'].goToPage(1)">
                <i class="fa fa-angle-double-left"></i>
            </button>
            <button class="page-btn" ${this.currentPage === 1 ? 'disabled' : ''} 
                    onclick="window.paginationInstances['${this.containerId}'].goToPage(${this.currentPage - 1})">
                <i class="fa fa-angle-left"></i>
            </button>
            <div class="page-numbers">`;

        if (totalPages <= this.config.maxVisiblePages) {
            for (let i = 1; i <= totalPages; i++) {
                html += this.createPageButton(i);
            }
        } else {
            html += this.createPageButton(1);
            if (this.currentPage > 3) html += '<span class="page-ellipsis">...</span>';

            for (let i = Math.max(2, this.currentPage - 1); i <= Math.min(this.currentPage + 1, totalPages - 1); i++) {
                html += this.createPageButton(i);
            }

            if (this.currentPage < totalPages - 2) html += '<span class="page-ellipsis">...</span>';
            if (totalPages > 1) html += this.createPageButton(totalPages);
        }

        html += `</div>
        <button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} 
                onclick="window.paginationInstances['${this.containerId}'].goToPage(${this.currentPage + 1})">
            <i class="fa fa-angle-right"></i>
        </button>
        <button class="page-btn" ${this.currentPage === totalPages ? 'disabled' : ''} 
                onclick="window.paginationInstances['${this.containerId}'].goToPage(${totalPages})">
            <i class="fa fa-angle-double-right"></i>
        </button>
        <span class="pagination-info">
            Showing ${startRecord.toLocaleString()}-${endRecord.toLocaleString()} of ${this.totalRecords.toLocaleString()} records
        </span>`;

        pagesContainer.innerHTML = html;
    }

    createPageButton(pageNum) {
        return `<button class="page-number ${pageNum === this.currentPage ? 'active' : ''}" 
                onclick="window.paginationInstances['${this.containerId}'].goToPage(${pageNum})">${pageNum}</button>`;
    }

    show() {
        this.container.style.display = 'block';
    }
    hide() {
        this.container.style.display = 'none';
    }
    reset() {
        this.currentPage = 1;
        this.totalRecords = 0;
        this.updateDisplay();
    }

    getCurrentPage() {
        return this.currentPage;
    }
    getPageSize() {
        return this.config.pageSize;
    }
    getTotalRecords() {
        return this.totalRecords;
    }
    getTotalPages() {
        return Math.ceil(this.totalRecords / this.config.pageSize);
    }
}

window.paginationInstances = window.paginationInstances || {};

function createPagination(containerId, options = {}) {
    const instance = new Pagination(containerId, options);
    window.paginationInstances[containerId] = instance;
    return instance;
}
