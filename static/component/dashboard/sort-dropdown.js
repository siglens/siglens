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

class SortDropdown {
    constructor(containerId, options = {}) {
        this.container = document.getElementById(containerId);
        this.options = {
            onSort: options.onSort || (() => {}),
            initialSort: options.initialSort || null,
        };
        this.sortMappings = {
            'Alphabetically (A-Z)': 'alpha-asc',
            'Alphabetically (Z-A)': 'alpha-desc',
            'Newest First': 'created-desc',
            'Oldest First': 'created-asc',
        };

        this.reverseMappings = Object.fromEntries(Object.entries(this.sortMappings).map(([k, v]) => [v, k]));

        this.render();
        this.attachEventListeners();

        // Set initial state if provided
        if (this.options.initialSort) {
            this.setActiveSort(this.options.initialSort);
        }
    }

    render() {
        const template = `
            <div class="dropdown">
                <button class="btn dropdown-toggle grey-dropdown-btn" data-toggle="dropdown"
                    aria-haspopup="true" aria-expanded="true" data-bs-toggle="dropdown">
                    <span class="sort-text" style="margin-right: 6px;">Sort</span>
                    <img class="dropdown-arrow orange" src="assets/arrow-btn.svg" alt="expand">
                    <span class="clear-sort" style="display: none; margin-left: 8px; cursor: pointer;">✕</span>
                </button>
                <div class="dropdown-menu box-shadow dropdown-menu-style dd-width-150">
                    <li class="dropdown-option" data-sort="alpha-asc">Alphabetically (A-Z)</li>
                    <li class="dropdown-option" data-sort="alpha-desc">Alphabetically (Z-A)</li>
                    <li class="dropdown-option" data-sort="created-desc">Newest First</li>
                    <li class="dropdown-option" data-sort="created-asc">Oldest First</li>
                </div>
            </div>
        `;

        this.container.innerHTML = template;

        // Set initial active state if provided
        if (this.options.initialSort) {
            const initialText = this.reverseMappings[this.options.initialSort];
            if (initialText) {
                this.container.querySelector('.sort-text').textContent = initialText;
                const activeOption = this.container.querySelector(`[data-sort="${this.options.initialSort}"]`);
                if (activeOption) {
                    activeOption.classList.add('active');
                }
            }
        }
    }

    attachEventListeners() {
        const dropdownOptions = this.container.querySelectorAll('.dropdown-option');

        dropdownOptions.forEach((option) => {
            option.addEventListener('click', async (e) => {
                dropdownOptions.forEach((opt) => opt.classList.remove('active'));

                e.target.classList.add('active');

                const sortText = e.target.textContent;
                this.container.querySelector('.sort-text').textContent = sortText;

                this.container.querySelector('.clear-sort').style.display = 'inline';

                const sortValue = e.target.dataset.sort;
                await this.handleSort(sortValue);
            });
        });

        const clearBtn = this.container.querySelector('.clear-sort');
        clearBtn.addEventListener('click', (e) => {
            e.stopPropagation();
            this.clearSort();
        });
    }

    async handleSort(sortValue) {
        const url = new URL(window.location.href);
        url.searchParams.set('sort', sortValue);
        window.history.replaceState({}, '', url);

        await this.options.onSort(sortValue);
    }

    setSort(sortValue) {
        const option = this.container.querySelector(`[data-sort="${sortValue}"]`);
        if (option) {
            option.click();
        }
    }

    setActiveSort(sortValue) {
        if (!sortValue) {
            this.container.querySelector('.sort-text').textContent = 'Sort';
            this.container.querySelector('.clear-sort').style.display = 'none';
            this.container.querySelectorAll('.dropdown-option').forEach((opt) => opt.classList.remove('active'));
            return;
        }
        const sortText = this.reverseMappings[sortValue];
        this.container.querySelector('.sort-text').textContent = sortText;
        this.container.querySelector('.clear-sort').style.display = 'inline';

        const options = this.container.querySelectorAll('.dropdown-option');
        options.forEach((option) => {
            option.classList.toggle('active', option.dataset.sort === sortValue);
        });
    }

    clearSort() {
        this.setActiveSort(null);
        this.options.onSort(null);
    }
}
