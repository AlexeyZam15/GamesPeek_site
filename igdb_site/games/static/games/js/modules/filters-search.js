// games/static/games/js/modules/filters-search.js

const FilterSearch = {
    // Настройка поиска по фильтрам
    setupSearchFilters() {
        console.log('Setting up search filters...');

        // Поиск по жанрам
        this.setupSearchInput('genre-search', '.genre-item', 'data-genre-name');

        // Поиск по ключевым словам
        this.setupSearchInput('keyword-search', '.keyword-item', 'data-keyword-name');

        // Поиск по платформам
        this.setupSearchInput('platform-search', '.platform-item', 'data-platform-name');

        // Поиск по темам
        this.setupSearchInput('theme-search', '.theme-item', 'data-theme-name');

        // Поиск по перспективам
        this.setupSearchInput('perspective-search', '.perspective-item', 'data-perspective-name');

        // Поиск по режимам игры
        this.setupSearchInput('game-mode-search', '.game-mode-item', 'data-game-mode-name');
    },

    // Настройка одного поля поиска
    setupSearchInput(inputId, itemSelector, dataAttribute) {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) return;

        searchInput.addEventListener('input', (e) => {
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

            // Сортируем после фильтрации
            this.triggerSortAfterSearch();
        });

        // Обработчик очистки поиска
        searchInput.addEventListener('search', () => {
            if (searchInput.value === '') {
                setTimeout(() => {
                    const items = document.querySelectorAll(itemSelector);
                    items.forEach(item => {
                        item.style.display = 'block';
                    });
                    this.triggerSortAfterSearch();
                }, 10);
            }
        });
    },

    // Триггер сортировки после поиска
    triggerSortAfterSearch() {
        // Используем глобальную функцию сортировки
        if (window.FilterManager && window.FilterManager.sort) {
            window.FilterManager.sort.sortFilterLists();
        } else {
            // Fallback на прямую сортировку
            setTimeout(() => {
                const sortModule = document.querySelector('script[src*="filters-sort"]');
                if (sortModule && sortModule.dataset.module) {
                    eval(sortModule.dataset.module).sortFilterLists();
                }
            }, 50);
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
            { inputId: 'game-mode-search', selector: '.game-mode-item', attr: 'data-game-mode-name' }
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