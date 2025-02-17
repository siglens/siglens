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
class SearchableDropdown {
    constructor(container, options = {}) {
        this.type = options.type;
        this.options = {
            items: options.items || [],
            selectedValues: options.selectedValues || ['All'],
            onChange: options.onChange || (() => {}),
        };
        this.options.selectedValues = this.options.selectedValues.filter((value) => {
            return value === 'All' || this.options.items.includes(value);
        });
        this.container = container;
        this.focusedOptionIndex = -1;
        this.init();
    }

    init() {
        this.createStructure();
        this.setupEventListeners();
        this.updateTags();
        this.updateSelectedCount();
    }

    createStructure() {
        const hasSelectedValues = this.options.selectedValues.length > 0;

        const structure = `
            <div class="dropdown-type">
                ${this.type}
            </div>
            <div class="dropdown-container">
                <div class="dropdown-button">
                    ${
                        hasSelectedValues
                            ? this.options.selectedValues
                                  .map((value) => {
                                      if (value === 'All') {
                                          return `
                                <div class="selected-tag">
                                    <span class="tag-text">All</span>
                                    <button class="tag-remove" data-value="All">×</button>
                                </div>
                            `;
                                      }
                                      return `
                            <div class="selected-tag">
                                <span class="tag-text">${value}</span>
                                <button class="tag-remove" data-value="${value}">×</button>
                            </div>
                        `;
                                  })
                                  .join('')
                            : ''
                    }
                    <div class="search-wrapper-ctn">
                        <input type="text"/>
                    </div>
                    <button class="close-button">×</button>
                </div>
                <div class="dropdown-content">
                    <div class="selected-count">Selected (${this.options.selectedValues.length})</div>
                    <label class="option">
                        <input type="checkbox" class="checkbox" value="All" 
                            ${this.options.selectedValues.includes('All') ? 'checked' : ''}>
                        All
                    </label>
                    ${this.options.items
                        .map(
                            (item) => `
                        <label class="option">
                            <input type="checkbox" class="checkbox" value="${item}" 
                                ${this.options.selectedValues.includes(item) ? 'checked' : ''}>
                            ${item}
                        </label>
                    `
                        )
                        .join('')}
                </div>
            </div>
        `;

        this.container.innerHTML = structure;

        // Add initial styles if not present
        if (!document.getElementById('searchable-dropdown-styles')) {
            const styles = `
                .dropdown-type {
                    padding: 2px 8px;
                    border: 1px solid var(--border-btn-color);
                    height: 28px;
                    border-radius: 3px 0 0 3px;
                    display: flex;
                    align-items: center;
                    font-weight: bold;
                    font-size: 11px;
                    background-color: var(--drop-down-btn-bg-regular);
                }
                .dropdown-container {
                    position: relative;
                    display: inline-block;
                    min-width:100px;
                }
                .dropdown-button {
                    width: 100%;
                    height: 28px;
                    padding: 0px 4px;
                    border: 1px solid var(--border-btn-color);                
                    cursor: pointer;
                    display: flex;
                    flex-wrap: wrap;
                    align-items: center;
                    gap: 5px;
                    border-radius: 0 3px 3px 0;
                    font-size:12px;
                }
        
                .dropdown-button input {
                    background: var(--bg-color);
                    border: none;
                    color: white;
                    width: 100%;
                    padding: 0px;
                    cursor: text;
                    height:26px;
                }
                .dropdown-button input:focus { outline: none; background: var(--bg-color); }
                .selected-tag {
                    background: var(--datatable-header-bg-color);
                    border-radius: 3px;
                    display: flex;
                    align-items: center;
                    gap: 5px;
                    max-width: calc(100% - 10px);
                    height: 20px;
                    padding:0px 8px;
                }
                .tag-text {
                    overflow: hidden;
                    text-overflow: ellipsis;
                    white-space: nowrap;
                }
                .tag-remove {
                    background: none;
                    border: none;
                    color: #888;
                    cursor: pointer;
                    padding: 0 2px;
                    font-size: 16px;
                }
                .tag-remove:hover { color: #999; }
                .close-button {
                    background: none;
                    border: none;
                    color: #666;
                    cursor: pointer;
                    padding: 0 5px;
                    margin-left: auto;
                    font-size: 18px;
                    line-height: normal;
                }
                .close-button:hover { color: #999; }
                .dropdown-container .dropdown-content {
                    display: none;
                    position: absolute;
                    top: 100%;
                    left: 0;
                    min-width: 100%;
                    width: fit-content;
                    border: 1px solid #333;
                    border-radius: 4px;
                    margin-top: 4px;
                    max-height: 300px;
                    overflow-y: auto;
                    z-index: 1000;
                    background: var(--timepicker-bg-color);
                    border: 1px solid var(--timepicker-border-color);                    
                    box-shadow: 0 6px 12px rgba(0, 0, 0, 0.175);

                }
                .dropdown-container .dropdown-content.show { display: block; }
                .option {
                    padding: 8px 10px;
                    cursor: pointer;
                    display: flex;
                    align-items: center;
                    gap: 8px;
                    white-space: nowrap;
                }
                .option:hover, .option.focused { background: var(--index-drop-down-item-inactive-bg-color); }
                .selected-count {
                    padding: 8px 10px;
                    color: #888;
                    border-bottom: 1px solid var(--timepicker-border-color);                    
                }
                .checkbox {
                    width: 16px;
                    height: 16px;
                    margin: 0;
                }
            `;

            const styleElement = document.createElement('style');
            styleElement.id = 'searchable-dropdown-styles';
            styleElement.textContent = styles;
            document.head.appendChild(styleElement);
        }

        // Set initial selected values
        this.options.selectedValues.forEach((value) => {
            const checkbox = this.container.querySelector(`.checkbox[value="${value}"]`);
            if (checkbox) checkbox.checked = true;
        });
    }

    setupEventListeners() {
        const dropdownButton = this.container.querySelector('.dropdown-button');
        const searchInput = dropdownButton.querySelector('input');
        const dropdownContent = this.container.querySelector('.dropdown-content');
        const closeButton = this.container.querySelector('.close-button');

        searchInput.addEventListener('keydown', this.handleKeydown.bind(this));

        dropdownButton.addEventListener('click', (e) => {
            if (!e.target.classList.contains('close-button') && !e.target.classList.contains('tag-remove')) {
                dropdownContent.classList.toggle('show');
                searchInput.focus();
            }
        });

        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target)) {
                dropdownContent.classList.remove('show');
            }
        });

        this.container.querySelectorAll('.checkbox').forEach((checkbox) => {
            checkbox.addEventListener('change', this.handleCheckboxChange.bind(this));
        });

        dropdownButton.addEventListener('click', this.handleTagRemoval.bind(this));
        closeButton.addEventListener('click', this.handleClose.bind(this));

        searchInput.style.width = '5px';

        searchInput.addEventListener('input', (e) => {
            searchInput.style.width = Math.max(5, e.target.value.length * 8) + 'px';
            this.handleSearch(e);
        });

        searchInput.addEventListener('blur', () => {
            setTimeout(() => {
                searchInput.value = '';
                searchInput.style.width = '5px';
            }, 200);
        });

        document.addEventListener('click', (e) => {
            if (!this.container.contains(e.target) && !e.target.classList.contains('checkbox')) {
                dropdownContent.classList.remove('show');
            }
        });
    }

    handleKeydown(e) {
        const dropdownContent = this.container.querySelector('.dropdown-content');
        const visibleOptions = this.getVisibleOptions();

        switch (e.key) {
            case 'ArrowDown':
                e.preventDefault();
                if (!dropdownContent.classList.contains('show')) {
                    dropdownContent.classList.add('show');
                }
                this.updateFocusedOption(this.focusedOptionIndex < visibleOptions.length - 1 ? this.focusedOptionIndex + 1 : 0);
                break;

            case 'ArrowUp':
                e.preventDefault();
                if (!dropdownContent.classList.contains('show')) {
                    dropdownContent.classList.add('show');
                }
                this.updateFocusedOption(this.focusedOptionIndex > 0 ? this.focusedOptionIndex - 1 : visibleOptions.length - 1);
                break;

            case 'Enter':
                e.preventDefault();
                if (this.focusedOptionIndex >= 0) {
                    const focusedOption = visibleOptions[this.focusedOptionIndex];
                    const checkbox = focusedOption.querySelector('.checkbox');
                    checkbox.checked = !checkbox.checked;
                    checkbox.dispatchEvent(new Event('change'));
                }
                break;

            case 'Escape':
                dropdownContent.classList.remove('show');
                this.focusedOptionIndex = -1;
                this.updateFocusedOption(-1);
                break;
        }
    }

    handleSearch(e) {
        const searchTerm = e.target.value.toLowerCase();
        const options = this.container.querySelectorAll('.option');

        options.forEach((option) => {
            const text = option.textContent.toLowerCase();
            option.style.display = text.includes(searchTerm) ? '' : 'none';
        });

        this.focusedOptionIndex = -1;
        this.updateFocusedOption(-1);
    }

    handleCheckboxChange(e) {
        const checkbox = e.target;
        const allCheckbox = this.container.querySelector('.checkbox[value="All"]');

        if (checkbox.value === 'All' && checkbox.checked) {
            // If "All" is selected, uncheck other options
            this.container.querySelectorAll('.checkbox:not([value="All"])').forEach((cb) => {
                cb.checked = false;
            });
        } else if (checkbox.value !== 'All' && checkbox.checked) {
            // If any other option is selected, uncheck "All"
            allCheckbox.checked = false;
        }

        this.updateSelectedCount();
        this.updateTags();
        this.notifyChange();
    }

    handleTagRemoval(e) {
        if (e.target.classList.contains('tag-remove')) {
            const value = e.target.dataset.value;
            const checkbox = this.container.querySelector(`.checkbox[value="${value}"]`);
            if (checkbox) {
                checkbox.checked = false;
                this.updateSelectedCount();
                this.updateTags();
                this.notifyChange();
            }
            e.stopPropagation();
        }
    }

    handleClose(e) {
        e.stopPropagation();
        this.container.querySelectorAll('.checkbox').forEach((cb) => {
            cb.checked = false;
        });
        this.updateSelectedCount();
        this.updateTags();
        this.notifyChange();

        const searchInput = this.container.querySelector('input');
        searchInput.value = '';
        this.container.querySelectorAll('.option').forEach((option) => {
            option.style.display = '';
        });
    }

    getVisibleOptions() {
        return Array.from(this.container.querySelectorAll('.option')).filter((option) => option.style.display !== 'none');
    }

    updateFocusedOption(newIndex) {
        const visibleOptions = this.getVisibleOptions();
        this.container.querySelectorAll('.option').forEach((option) => option.classList.remove('focused'));

        if (newIndex >= 0 && newIndex < visibleOptions.length) {
            this.focusedOptionIndex = newIndex;
            visibleOptions[newIndex].classList.add('focused');
            visibleOptions[newIndex].scrollIntoView({
                block: 'nearest',
                behavior: 'smooth',
            });
        }
    }

    createTag(value) {
        const tag = document.createElement('div');
        tag.className = 'selected-tag';
        tag.innerHTML = `
            <span class="tag-text">${value}</span>
            <button class="tag-remove" data-value="${value}">×</button>
        `;
        return tag;
    }

    updateTags() {
        const dropdownButton = this.container.querySelector('.dropdown-button');
        const searchWrapper = dropdownButton.querySelector('.search-wrapper-ctn');

        dropdownButton.querySelectorAll('.selected-tag').forEach((tag) => tag.remove());

        const selectedCheckboxes = this.container.querySelectorAll('.checkbox:checked');
        selectedCheckboxes.forEach((checkbox) => {
            const tag = this.createTag(checkbox.value);
            dropdownButton.insertBefore(tag, searchWrapper);
        });
    }

    updateSelectedCount() {
        const selectedCount = this.container.querySelectorAll('.checkbox:checked').length;
        this.container.querySelector('.selected-count').textContent = `Selected (${selectedCount})`;
    }

    notifyChange() {
        const selectedValues = Array.from(this.container.querySelectorAll('.checkbox:checked')).map((checkbox) => checkbox.value);
        this.options.onChange(selectedValues);
    }

    getSelectedValues() {
        return Array.from(this.container.querySelectorAll('.checkbox:checked')).map((checkbox) => checkbox.value);
    }

    setSelectedValues(values) {
        this.container.querySelectorAll('.checkbox').forEach((checkbox) => {
            checkbox.checked = false;
        });

        values.forEach((value) => {
            const checkbox = this.container.querySelector(`.checkbox[value="${value}"]`);
            if (checkbox) checkbox.checked = true;
        });

        this.updateSelectedCount();
        this.updateTags();
    }

    clearSelection() {
        this.setSelectedValues([]);
    }
}
