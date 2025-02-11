/*
 * Copyright (c) 2021-2024 SigScalr, Inc.
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

class FolderDropdown {
    constructor(containerId, options = {}) {
        this.container = document.getElementById(containerId);
        this.options = {
            onSelect: options.onSelect || (() => {}),
            excludeFolderId: options.excludeFolderId || null,
            showRoot: options.showRoot || false,
            placeholder: options.placeholder || 'Select Folder',
            initialFolder: options.initialFolder || null,
        };

        // Store child folder IDs that need to be excluded
        this.excludedFolderIds = new Set();
        if (this.options.excludeFolderId) {
            this.excludedFolderIds.add(this.options.excludeFolderId);
            this.loadExcludedFolders();
        }

        this.selectedFolder = this.options.initialFolder;
        this.render();
        this.loadFolders();
    }

    render() {
        const template = `
            <div class="folder-select-dropdown">
                <button class="folder-select-btn grey-dropdown-btn btn">
                    <div>
                        <i class="fa fa-folder" style="color: #FFB84D; margin-right: 8px;"></i>
                        <span class="selected-folder">
                            ${this.selectedFolder ? this.selectedFolder.name : this.options.placeholder}
                        </span>                   
                    </div>
                    <img class="orange" src="assets/arrow-btn.svg" alt="expand">                
                </button>
                <div class="folder-dropdown-content">
                    <div class="folder-search-container">
                        <input type="text" 
                               class="folder-search form-control" 
                               placeholder="Search folders...">
                    </div>
                    <ul class="folder-tree"></ul>
                </div>
            </div>`;

        this.container.innerHTML = template;
        this.attachEventListeners();
    }

    async loadFolders(searchQuery = '') {
        try {
            if (searchQuery) {
                const response = await getDashboardFolderList('root-folder', {
                    type: 'folder',
                    query: searchQuery,
                });
                if (response && response.items) {
                    // Filter out the current folder and its children
                    const filteredItems = response.items.filter((item) => !this.excludedFolderIds.has(item.id) && !item.isDefault);
                    this.renderSearchResults(filteredItems);
                }
            } else {
                const response = await getFolderContents('root-folder', {
                    foldersOnly: true,
                });
                if (response && response.items) {
                    // Filter out the current folder and its children
                    const filteredItems = response.items.filter((item) => !this.excludedFolderIds.has(item.id) && !item.isDefault);
                    this.renderFolderTree(filteredItems);
                }
            }
        } catch (error) {
            console.error('Error loading folders:', error);
        }
    }

    getSelectedFolder() {
        return this.selectedFolder;
    }

    selectFolder(folderItem) {
        const folderName = folderItem.querySelector('.folder-name').textContent;
        this.container.querySelector('.selected-folder').textContent = folderName;
        this.container.querySelector('.folder-dropdown-content').classList.remove('show');

        this.selectedFolder = {
            id: folderItem.dataset.folderId,
            name: folderName,
        };

        this.options.onSelect(this.selectedFolder);
    }

    renderSearchResults(folders) {
        const listElement = this.container.querySelector('.folder-tree');
        listElement.innerHTML = '';

        folders.forEach((folder) => {
            const li = document.createElement('li');
            li.className = 'folder-item';
            li.dataset.folderId = folder.id;

            li.innerHTML = `
                <div class="folder-item-row">
                    <i class="fa fa-folder" style="color: #FFB84D"></i>
                    <span class="folder-name">${folder.name}</span>
                </div>
            `;

            listElement.appendChild(li);
        });
    }

    renderFolderTree(folders, parentElement = null) {
        const listElement = parentElement || this.container.querySelector('.folder-tree');

        if (!parentElement) {
            listElement.innerHTML = '';
            if (this.options.showRoot) {
                const rootLi = document.createElement('li');
                rootLi.className = 'folder-item';
                rootLi.dataset.folderId = 'root-folder';

                rootLi.innerHTML = `
                    <div class="folder-item-row">
                        <i class="fa fa-folder" style="color: #FFB84D"></i>
                        <span class="folder-name">Dashboards</span>
                    </div>
                `;
                listElement.appendChild(rootLi);
            }
        }

        folders.forEach((folder) => {
            const li = document.createElement('li');
            li.className = 'folder-item';
            li.dataset.folderId = folder.id;

            let arrow = folder.childCount > 0 ? '<i class="fa fa-chevron-right folder-arrow"></i>' : '<span style="padding-left: 24px;"></span>';

            li.innerHTML = `
                <div class="folder-item-row">
                    ${arrow}
                    <i class="fa fa-folder" style="color: #FFB84D"></i>
                    <span class="folder-name">${folder.name}</span>
                </div>
            `;

            listElement.appendChild(li);
        });
    }

    async loadExcludedFolders() {
        try {
            const response = await getDashboardFolderList(this.options.excludeFolderId, {
                type: 'folder',
            });

            if (response && response.items) {
                response.items.forEach((item) => {
                    if (item.parentId === this.options.excludeFolderId) {
                        this.excludedFolderIds.add(item.id);
                    }
                });
            }
        } catch (error) {
            console.error('Error loading excluded folders:', error);
        }
    }

    attachEventListeners() {
        const btn = this.container.querySelector('.folder-select-btn');
        btn.addEventListener('click', (e) => {
            this.container.querySelector('.folder-dropdown-content').classList.toggle('show');
        });

        const searchInput = this.container.querySelector('.folder-search');
        let searchTimeout;
        searchInput.addEventListener('input', (e) => {
            clearTimeout(searchTimeout);
            const query = e.target.value.trim();

            searchTimeout = setTimeout(() => {
                this.loadFolders(query);
            }, 300);
        });

        const folderTree = this.container.querySelector('.folder-tree');
        folderTree.addEventListener('click', async (e) => {
            const folderItem = e.target.closest('.folder-item');
            if (!folderItem) return;

            const arrow = folderItem.querySelector('.folder-arrow');
            if (arrow && e.target.closest('.folder-arrow')) {
                // Toggle folder expansion
                arrow.classList.toggle('expanded');
                if (arrow.classList.contains('expanded')) {
                    arrow.classList.replace('fa-chevron-right', 'fa-chevron-down');
                    const response = await getFolderContents(folderItem.dataset.folderId, {
                        foldersOnly: true,
                    });
                    if (response?.items?.length > 0) {
                        const subList = document.createElement('ul');
                        folderItem.appendChild(subList);
                        this.renderFolderTree(response.items, subList);
                    }
                } else {
                    arrow.classList.replace('fa-chevron-down', 'fa-chevron-right');
                    const subList = folderItem.querySelector('ul');
                    if (subList) subList.remove();
                }
            } else {
                this.selectFolder(folderItem);
            }
        });

        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) {
                this.container.querySelector('.folder-dropdown-content').classList.remove('show');
            }
        });
    }
}
