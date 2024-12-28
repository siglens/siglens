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
                // Remove active class from all options
                dropdownOptions.forEach((opt) => opt.classList.remove('active'));

                // Add active class to clicked option
                e.target.classList.add('active');

                // Update button text
                const sortText = e.target.textContent;
                this.container.querySelector('.sort-text').textContent = sortText;

                this.container.querySelector('.clear-sort').style.display = 'inline';

                // Get sort value and trigger callback
                const sortValue = e.target.dataset.sort;
                await this.handleSort(sortValue);
            });
        });

        // Setup clear button handler
        const clearBtn = this.container.querySelector('.clear-sort');
        clearBtn.addEventListener('click', (e) => {
            e.stopPropagation(); // Prevent dropdown from opening
            this.clearSort();
        });
    }

    async handleSort(sortValue) {
        // Update URL with sort parameter
        const url = new URL(window.location.href);
        url.searchParams.set('sort', sortValue);
        window.history.replaceState({}, '', url);

        // Call the onSort callback
        await this.options.onSort(sortValue);
    }

    // Method to programmatically set sort
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
