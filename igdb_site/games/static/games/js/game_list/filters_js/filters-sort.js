// games/static/games/js/game_list/filters_js/filters-sort.js

console.log('========================================');
console.log('!!! FILTERS-SORT.JS VERSION 7.0 - REMOVED SIZE LIMIT FOR PLATFORMS !!!');
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
        } else {
            console.warn(`[TIMER] No start mark found for: ${label}`);
        }
    }
};

const FilterSort = {
    isSorting: false,

    // Фильтры для сортировки
    filterTypes: [
        // SEARCH FILTERS - платформы
        {
            name: 'search-platforms',
            container: '.search-platform-grid',
            itemClass: 'search-platform-item',
            checkboxClass: 'search-platform-checkbox',
            dataAttr: 'data-platform-name',
            sortBySelected: true,
            maxItems: 500  // Увеличиваем лимит для платформ
        },
        // SEARCH FILTERS - game types
        {
            name: 'search-game-types',
            container: '.search-game-type-grid',
            itemClass: 'search-game-type-item',
            checkboxClass: 'search-game-type-checkbox',
            dataAttr: 'data-game-type-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SEARCH FILTERS - genres
        {
            name: 'search-genres',
            container: '.search-genre-grid',
            itemClass: 'search-genre-item',
            checkboxClass: 'search-genre-checkbox',
            dataAttr: 'data-genre-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SEARCH FILTERS - themes
        {
            name: 'search-themes',
            container: '.search-theme-grid',
            itemClass: 'search-theme-item',
            checkboxClass: 'search-theme-checkbox',
            dataAttr: 'data-theme-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SEARCH FILTERS - perspectives
        {
            name: 'search-perspectives',
            container: '.search-perspective-grid',
            itemClass: 'search-perspective-item',
            checkboxClass: 'search-perspective-checkbox',
            dataAttr: 'data-perspective-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SEARCH FILTERS - game modes
        {
            name: 'search-game-modes',
            container: '.search-game-mode-grid',
            itemClass: 'search-game-mode-item',
            checkboxClass: 'search-game-mode-checkbox',
            dataAttr: 'data-game-mode-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SEARCH FILTERS - engines
        {
            name: 'search-engines',
            container: '.search-engine-grid',
            itemClass: 'search-engine-item',
            checkboxClass: 'search-engine-checkbox',
            dataAttr: 'data-engine-name',
            sortBySelected: true,
            maxItems: 500
        },

        // SIMILARITY FILTERS - platforms
        {
            name: 'platforms',
            container: '.platform-grid',
            itemClass: 'platform-item',
            checkboxClass: 'platform-checkbox',
            dataAttr: 'data-platform-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SIMILARITY FILTERS - game types
        {
            name: 'game-types',
            container: '.game-type-grid',
            itemClass: 'game-type-item',
            checkboxClass: 'game-type-checkbox',
            dataAttr: 'data-game-type-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SIMILARITY FILTERS - genres
        {
            name: 'genres',
            container: '.genre-grid',
            itemClass: 'genre-item',
            checkboxClass: 'genre-checkbox',
            dataAttr: 'data-genre-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SIMILARITY FILTERS - themes
        {
            name: 'themes',
            container: '.theme-grid',
            itemClass: 'theme-item',
            checkboxClass: 'theme-checkbox',
            dataAttr: 'data-theme-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SIMILARITY FILTERS - perspectives
        {
            name: 'perspectives',
            container: '.perspective-grid',
            itemClass: 'perspective-item',
            checkboxClass: 'perspective-checkbox',
            dataAttr: 'data-perspective-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SIMILARITY FILTERS - game modes
        {
            name: 'game-modes',
            container: '.game-mode-grid',
            itemClass: 'game-mode-item',
            checkboxClass: 'game-mode-checkbox',
            dataAttr: 'data-game-mode-name',
            sortBySelected: true,
            maxItems: 500
        },
        // SIMILARITY FILTERS - engines
        {
            name: 'engines',
            container: '.engine-grid',
            itemClass: 'engine-item',
            checkboxClass: 'engine-checkbox',
            dataAttr: 'data-engine-name',
            sortBySelected: true,
            maxItems: 500
        }
    ],


    sortFilterLists() {
        if (this.isSorting) return;
        this.isSorting = true;

        FilterSortDebugTimer.start('sortFilterLists');
        console.log('FilterSort: Starting with platform sorting...');

        // Логируем размеры фильтров
        this.logFilterSizes();

        // Специальная отладка для платформ
        const searchPlatformContainer = document.querySelector('.search-platform-grid');
        const similarityPlatformContainer = document.querySelector('.platform-grid');

        console.log('Search platforms container found:', !!searchPlatformContainer);
        if (searchPlatformContainer) {
            const platformItems = searchPlatformContainer.querySelectorAll('.search-platform-item');
            const selectedPlatforms = searchPlatformContainer.querySelectorAll('.search-platform-checkbox:checked');
            console.log(`Search platforms: ${platformItems.length} items, ${selectedPlatforms.length} selected`);
            selectedPlatforms.forEach(cb => {
                console.log(`  Selected platform ID: ${cb.value}`);
            });
        }

        console.log('Similarity platforms container found:', !!similarityPlatformContainer);
        if (similarityPlatformContainer) {
            const platformItems = similarityPlatformContainer.querySelectorAll('.platform-item');
            const selectedPlatforms = similarityPlatformContainer.querySelectorAll('.platform-checkbox:checked');
            console.log(`Similarity platforms: ${platformItems.length} items, ${selectedPlatforms.length} selected`);
            selectedPlatforms.forEach(cb => {
                console.log(`  Selected platform ID: ${cb.value}`);
            });
        }

        // Обрабатываем каждый фильтр
        this.filterTypes.forEach(filter => {
            const container = document.querySelector(filter.container);
            if (container) {
                const items = container.querySelectorAll(`.${filter.itemClass}`);
                const maxItems = filter.maxItems || 100;

                if (items.length > 0 && items.length <= maxItems) {
                    console.log(`Processing ${filter.name}: ${items.length} items`);
                    this.sortSingleFilterList(container, items, filter);
                } else if (items.length > maxItems) {
                    console.log(`FilterSort: ${filter.name} has ${items.length} items, limit is ${maxItems}, skipping sort`);
                }
            }
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
                    const selectedCount = this.countSelectedItems(container, filter);
                    console.log(`  ${filter.name}: ${items.length} items (${selectedCount} selected)`);
                }
            }
        });
    },

    countSelectedItems(container, filter) {
        const selectedCheckboxes = container.querySelectorAll(`.${filter.checkboxClass}:checked`);
        return selectedCheckboxes.length;
    },

    sortSingleFilterList(container, items, filter) {
        if (filter.sortBySelected) {
            // Сортируем: выбранные вверху, затем остальные по алфавиту
            this.sortWithSelectedFirst(container, items, filter);
        } else {
            // Простая сортировка по алфавиту
            this.sortAlphabetically(container, items, filter);
        }
    },

    sortWithSelectedFirst(container, items, filter) {
        FilterSortDebugTimer.start(`sortWithSelectedFirst_${filter.name}`);

        // Оптимизация: собираем выбранные и невыбранные элементы за один проход
        const selectedItems = [];
        const unselectedItems = [];
        const itemsLength = items.length;

        for (let i = 0; i < itemsLength; i++) {
            const item = items[i];
            const checkbox = item.querySelector(`.${filter.checkboxClass}`);
            if (checkbox && checkbox.checked) {
                selectedItems.push(item);
            } else {
                unselectedItems.push(item);
            }
        }

        console.log(`FilterSort: ${filter.name} - found ${selectedItems.length} selected items out of ${items.length}`);

        // Если нет выбранных - пропускаем
        if (selectedItems.length === 0) {
            FilterSortDebugTimer.end(`sortWithSelectedFirst_${filter.name}`);
            return;
        }

        // Проверяем, не находятся ли выбранные уже в начале
        let alreadyAtStart = true;
        const selectedLength = selectedItems.length;
        for (let i = 0; i < selectedLength; i++) {
            if (items[i] !== selectedItems[i]) {
                alreadyAtStart = false;
                break;
            }
        }

        if (alreadyAtStart) {
            console.log(`FilterSort: ${filter.name} - selected items already at top, skipping`);
            FilterSortDebugTimer.end(`sortWithSelectedFirst_${filter.name}`);
            return;
        }

        // Оптимизированная сортировка: кэшируем имена для сравнения
        const getName = (item) => {
            if (filter.dataAttr) {
                const nameFromAttr = item.getAttribute(filter.dataAttr);
                if (nameFromAttr) return nameFromAttr;
            }
            const label = item.querySelector('label');
            if (label) {
                let labelText = label.textContent.trim();
                labelText = labelText.replace(/\s*\(\d+\)\s*$/, '').trim();
                if (labelText) return labelText;
            }
            let text = item.textContent.trim();
            text = text.replace(/\s*\(\d+\)\s*$/, '').trim();
            return text || 'unknown';
        };

        // Сортируем выбранные элементы по имени (алфавитно)
        selectedItems.sort((a, b) => {
            const nameA = getName(a);
            const nameB = getName(b);
            return nameA.localeCompare(nameB);
        });

        // Сортируем невыбранные элементы по имени (алфавитно)
        unselectedItems.sort((a, b) => {
            const nameA = getName(a);
            const nameB = getName(b);
            return nameA.localeCompare(nameB);
        });

        // Объединяем: сначала выбранные, потом остальные
        const sortedItems = [...selectedItems, ...unselectedItems];

        // Обновляем порядок в DOM
        this.updateContainerOrder(container, sortedItems);

        console.log(`FilterSort: ${filter.name} - moved ${selectedItems.length} selected items to top`);
        FilterSortDebugTimer.end(`sortWithSelectedFirst_${filter.name}`);
    },

    sortAlphabetically(container, items, filter) {
        // Сортируем по имени
        items.sort((a, b) => {
            const nameA = this.getItemName(a, filter);
            const nameB = this.getItemName(b, filter);
            return nameA.localeCompare(nameB);
        });

        this.updateContainerOrder(container, items);
        console.log(`FilterSort: ${filter.name} - sorted alphabetically`);
    },

    getItemName(item, filter) {
        // 1. Пытаемся получить из data-атрибута
        if (filter.dataAttr) {
            const nameFromAttr = item.getAttribute(filter.dataAttr);
            if (nameFromAttr) return nameFromAttr;
        }

        // 2. Пытаемся получить из текста лейбла
        const label = item.querySelector('label');
        if (label) {
            let labelText = label.textContent.trim();
            // Убираем количество игр в скобках если есть
            labelText = labelText.replace(/\s*\(\d+\)\s*$/, '').trim();
            if (labelText) return labelText;
        }

        // 3. Пытаемся получить из текста элемента
        let text = item.textContent.trim();
        text = text.replace(/\s*\(\d+\)\s*$/, '').trim();

        return text || 'unknown';
    },

    updateContainerOrder(container, sortedItems) {
        try {
            // Сохраняем текущий скролл
            const scrollTop = container.scrollTop;

            const fragment = document.createDocumentFragment();
            for (let i = 0; i < sortedItems.length; i++) {
                if (sortedItems[i] && sortedItems[i].nodeType === Node.ELEMENT_NODE) {
                    fragment.appendChild(sortedItems[i]);
                }
            }

            // Очищаем и заполняем заново
            container.innerHTML = '';
            container.appendChild(fragment);

            // Восстанавливаем скролл
            container.scrollTop = scrollTop;

            console.log(`FilterSort: Updated container order, restored scroll position to ${scrollTop}`);
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