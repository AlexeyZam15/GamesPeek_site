// games/static/games/js/game_list/filters_js/filters-sort.js

console.log('========================================');
console.log('!!! FILTERS-SORT.JS VERSION 4.0 - MINIMAL SORTING ONLY !!!');
console.log('========================================');

const FilterSortDebugTimer = {
    marks: {},
    start(label) {
        this.marks[label] = performance.now();
    },
    end(label) {
        const endTime = performance.now();
        const startTime = this.marks[label];
        if (startTime) {
            const duration = (endTime - startTime).toFixed(2);
            console.warn(`[TIMER] ${label} took ${duration} ms`);
            delete this.marks[label];
        }
    }
};

const FilterSort = {
    isSorting: false,

    // ТОЛЬКО ЭТИ ФИЛЬТРЫ СОРТИРУЕМ (без ключевых слов)
    filterTypes: [
        { name: 'platforms', container: '.platform-grid', itemClass: 'platform-item', checkboxClass: 'platform-checkbox' },
        { name: 'game-types', container: '.game-type-grid', itemClass: 'game-type-item', checkboxClass: 'game-type-checkbox' },
        { name: 'search-genres', container: '.search-genre-grid', itemClass: 'search-genre-item', checkboxClass: 'search-genre-checkbox' },
        { name: 'search-themes', container: '.search-theme-grid', itemClass: 'search-theme-item', checkboxClass: 'search-theme-checkbox' },
        { name: 'search-perspectives', container: '.search-perspective-grid', itemClass: 'search-perspective-item', checkboxClass: 'search-perspective-checkbox' },
        { name: 'search-game-modes', container: '.search-game-mode-grid', itemClass: 'search-game-mode-item', checkboxClass: 'search-game-mode-checkbox' },
        { name: 'search-engines', container: '.search-engine-grid', itemClass: 'search-engine-item', checkboxClass: 'search-engine-checkbox' },
        { name: 'genres', container: '.genre-grid', itemClass: 'genre-item', checkboxClass: 'genre-checkbox' },
        { name: 'themes', container: '.theme-grid', itemClass: 'theme-item', checkboxClass: 'theme-checkbox' },
        { name: 'perspectives', container: '.perspective-grid', itemClass: 'perspective-item', checkboxClass: 'perspective-checkbox' },
        { name: 'game-modes', container: '.game-mode-grid', itemClass: 'game-mode-item', checkboxClass: 'game-mode-checkbox' },
        { name: 'engines', container: '.engine-grid', itemClass: 'engine-item', checkboxClass: 'engine-checkbox' }
    ],

    sortFilterLists() {
        if (this.isSorting) return;
        this.isSorting = true;

        FilterSortDebugTimer.start('sortFilterLists');
        console.log('FilterSort: Starting (keywords SKIPPED)...');

        // Логируем размеры фильтров
        this.logFilterSizes();

        // Обрабатываем каждый фильтр
        this.filterTypes.forEach(filter => {
            this.sortSingleFilterList(filter);
        });

        this.isSorting = false;
        FilterSortDebugTimer.end('sortFilterLists');
    },

    logFilterSizes() {
        console.log('FilterSort: Filter sizes:');
        this.filterTypes.forEach(filter => {
            const container = document.querySelector(filter.container);
            if (container) {
                const items = container.querySelectorAll(`.${filter.itemClass}`);
                if (items.length > 0) {
                    console.log(`  ${filter.name}: ${items.length} items`);
                }
            }
        });

        // Отдельно логируем ключевые слова (которые НЕ обрабатываем)
        const keywordContainers = ['.keyword-grid', '.search-keyword-grid'];
        keywordContainers.forEach(selector => {
            const container = document.querySelector(selector);
            if (container) {
                const items = container.querySelectorAll('.keyword-item, .search-keyword-item');
                if (items.length > 0) {
                    console.log(`  SKIPPED keywords (${selector}): ${items.length} items`);
                }
            }
        });
    },

    sortSingleFilterList(filter) {
        const container = document.querySelector(filter.container);
        if (!container) return;

        const allItems = Array.from(container.querySelectorAll(`.${filter.itemClass}`));
        if (allItems.length === 0) return;

        // Если элементов больше 50, не сортируем (слишком много)
        if (allItems.length > 50) {
            return;
        }

        // Собираем выбранные элементы
        const selectedItems = [];
        const unselectedItems = [];

        for (let i = 0; i < allItems.length; i++) {
            const item = allItems[i];
            const checkbox = item.querySelector(`.${filter.checkboxClass}`);
            if (checkbox && checkbox.checked) {
                selectedItems.push(item);
            } else {
                unselectedItems.push(item);
            }
        }

        // Если нет выбранных или они уже в начале - пропускаем
        if (selectedItems.length === 0) return;

        // Проверяем, не находятся ли выбранные уже в начале
        let alreadyAtStart = true;
        for (let i = 0; i < selectedItems.length; i++) {
            const checkbox = allItems[i]?.querySelector(`.${filter.checkboxClass}`);
            if (!(checkbox && checkbox.checked)) {
                alreadyAtStart = false;
                break;
            }
        }

        if (alreadyAtStart) return;

        // Обновляем порядок
        this.updateContainerOrder(container, [...selectedItems, ...unselectedItems]);
    },

    updateContainerOrder(container, sortedItems) {
        try {
            const fragment = document.createDocumentFragment();
            for (let i = 0; i < sortedItems.length; i++) {
                if (sortedItems[i] && sortedItems[i].nodeType === Node.ELEMENT_NODE) {
                    fragment.appendChild(sortedItems[i]);
                }
            }
            container.innerHTML = '';
            container.appendChild(fragment);
        } catch (error) {
            console.error('Error updating container order:', error);
        }
    },

    forceSortAllLists() {
        console.log('FilterSort: Force sorting...');
        this.isSorting = false;
        this.sortFilterLists();
    },

    clearCache() {
        console.log('FilterSort: Cache cleared');
    }
};

export default FilterSort;