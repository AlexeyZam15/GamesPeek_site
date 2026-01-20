// games/static/games/js/modules/filters-search.js

const FilterSearch = {
    // Настройка поиска по фильтрам
    setupSearchFilters() {
        console.log('Setting up search filters...');

        // Поиск в Search Filters (не влияют на find_similar)
        this.setupSearchInput('platform-search', '.platform-item', 'data-platform-name');
        this.setupSearchInput('game-type-search', '.game-type-item', 'data-game-type-name');

        // Поиск в Similarity Filters (влияют на find_similar)
        this.setupSearchInput('genre-search', '.genre-item', 'data-genre-name');
        this.setupSearchInput('keyword-search', '.keyword-item', 'data-keyword-name');
        this.setupSearchInput('theme-search', '.theme-item', 'data-theme-name');
        this.setupSearchInput('perspective-search', '.perspective-item', 'data-perspective-name');
        this.setupSearchInput('game-mode-search', '.game-mode-item', 'data-game-mode-name');
    },

    // Настройка одного поля поиска
    setupSearchInput(inputId, itemSelector, dataAttribute) {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) return;

        // Удаляем старые обработчики
        const newInput = searchInput.cloneNode(true);
        searchInput.parentNode.replaceChild(newInput, searchInput);

        newInput.addEventListener('input', (e) => {
            const searchTerm = e.target.value.toLowerCase().trim();
            const items = document.querySelectorAll(itemSelector);

            let hasVisibleItems = false;

            items.forEach(item => {
                const itemName = item.getAttribute(dataAttribute);
                const isMatch = itemName && itemName.includes(searchTerm);

                if (isMatch) {
                    item.style.display = 'block';
                    hasVisibleItems = true;
                } else {
                    item.style.display = 'none';
                }
            });

            // Сортируем после фильтрации с задержкой
            setTimeout(() => {
                this.triggerSortAfterSearch();
            }, 150);
        });

        // Обработчик очистки поиска
        newInput.addEventListener('search', () => {
            if (newInput.value === '') {
                setTimeout(() => {
                    const items = document.querySelectorAll(itemSelector);
                    items.forEach(item => {
                        item.style.display = 'block';
                    });

                    // Сортируем после очистки
                    setTimeout(() => {
                        this.triggerSortAfterSearch();
                    }, 100);
                }, 10);
            }
        });
    },

    // Триггер сортировки после поиска
    triggerSortAfterSearch() {
        console.log('Triggering sort after search...');

        if (window.FilterManager && window.FilterManager.sort) {
            // Используем forceSortAllLists для гарантированной сортировки
            window.FilterManager.sort.forceSortAllLists();
        } else {
            console.error('FilterManager or sort not found');
        }
    },

    // Быстрый поиск по всем фильтрам
    quickSearchAll(term) {
        const searchConfigs = [
            { inputId: 'genre-search', selector: '.genre-item', attr: 'data-genre-name' },
            { inputId: 'keyword-search', selector: '.keyword-item', attr: 'data-keyword-name' },
            { inputId: 'platform-search', selector: '.platform-item', attr: 'data-platform-name' },
            { inputId: 'theme-search', selector: '.theme-item', attr: 'data-theme-name' },
            { inputId: 'perspective-search', selector: '.perspective-item', attr: 'data-perspective-name' },
            { inputId: 'game-mode-search', selector: '.game-mode-item', attr: 'data-game-mode-name' },
            { inputId: 'game-type-search', selector: '.game-type-item', attr: 'data-game-type-name' }
        ];

        searchConfigs.forEach(config => {
            const input = document.getElementById(config.inputId);
            if (input) {
                input.value = term;
                input.dispatchEvent(new Event('input'));
            }
        });
    }
};

export default FilterSearch;