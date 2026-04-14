// games/static/games/js/game_list/filters_js/filters-search.js

// Добавляем служебный объект для таймеров
const FilterSearchDebugTimer = {
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

const FilterSearch = {
    // Настройка поиска по фильтрам
    setupSearchFilters() {
        FilterSearchDebugTimer.start('setupSearchFilters');
        console.log('Setting up search filters...');

        // Конфигурация всех поисковых полей
        const searchConfigs = [
            { inputId: 'platform-search', itemSelector: '.search-platform-item', dataAttr: 'data-platform-name' },
            { inputId: 'game-type-search', itemSelector: '.search-game-type-item', dataAttr: 'data-game-type-name' },
            { inputId: 'search-genre-search', itemSelector: '.search-genre-item', dataAttr: 'data-genre-name' },
            { inputId: 'search-keyword-search', itemSelector: '.search-keyword-item', dataAttr: 'data-keyword-name' },
            { inputId: 'search-theme-search', itemSelector: '.search-theme-item', dataAttr: 'data-theme-name' },
            { inputId: 'search-perspective-search', itemSelector: '.search-perspective-item', dataAttr: 'data-perspective-name' },
            { inputId: 'search-game-mode-search', itemSelector: '.search-game-mode-item', dataAttr: 'data-game-mode-name' },
            { inputId: 'search-engine-search', itemSelector: '.search-engine-item', dataAttr: 'data-engine-name' },
            { inputId: 'genre-search', itemSelector: '.genre-item', dataAttr: 'data-genre-name' },
            { inputId: 'keyword-search', itemSelector: '.keyword-item', dataAttr: 'data-keyword-name' },
            { inputId: 'theme-search', itemSelector: '.theme-item', dataAttr: 'data-theme-name' },
            { inputId: 'perspective-search', itemSelector: '.perspective-item', dataAttr: 'data-perspective-name' },
            { inputId: 'game-mode-search', itemSelector: '.game-mode-item', dataAttr: 'data-game-mode-name' },
            { inputId: 'engine-search', itemSelector: '.engine-item', dataAttr: 'data-engine-name' }
        ];

        // Создаем debounced функцию сортировки один раз
        const debouncedSort = this.debounce(() => {
            if (window.FilterManager && window.FilterManager.sort) {
                window.FilterManager.sort.forceSortAllLists();
            }
        }, 150);

        // Настраиваем каждое поле поиска
        searchConfigs.forEach(config => {
            this.setupSingleSearchInput(config.inputId, config.itemSelector, config.dataAttr, debouncedSort);
        });

        FilterSearchDebugTimer.end('setupSearchFilters');
    },

    // Новый метод для настройки одного поля поиска
    setupSingleSearchInput(inputId, itemSelector, dataAttr, debouncedSort) {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) {
            console.log(`Search input ${inputId} not found`);
            return;
        }

        console.log(`Setting up search for ${inputId} with selector ${itemSelector}`);

        const originalInput = searchInput;
        const newInput = originalInput.cloneNode(false);

        for (let attr of originalInput.attributes) {
            newInput.setAttribute(attr.name, attr.value);
        }
        newInput.value = originalInput.value;

        const handleInput = (e) => {
            const searchTerm = e.target.value.toLowerCase().trim();
            const items = document.querySelectorAll(itemSelector);

            let visibleCount = 0;
            const termLength = searchTerm.length;

            for (const item of items) {
                const itemName = item.getAttribute(dataAttr);
                if (itemName && (termLength === 0 || itemName.toLowerCase().includes(searchTerm))) {
                    item.style.display = 'block';
                    visibleCount++;
                } else {
                    item.style.display = 'none';
                }
            }

            console.log(`Search "${searchTerm}" in ${inputId}: ${visibleCount} visible items of ${items.length}`);

            if (inputId === 'keyword-search' || inputId === 'search-keyword-search') {
                this.handleKeywordSearchUpdate(searchTerm, visibleCount);
            }

            debouncedSort();
        };

        newInput.addEventListener('input', handleInput);
        newInput.addEventListener('search', () => {
            if (newInput.value === '') {
                setTimeout(() => {
                    const items = document.querySelectorAll(itemSelector);
                    for (const item of items) {
                        item.style.display = 'block';
                    }
                    if (inputId === 'keyword-search' || inputId === 'search-keyword-search') {
                        this.handleKeywordSearchClear();
                    }
                    debouncedSort();
                }, 10);
            }
        });

        originalInput.parentNode.replaceChild(newInput, originalInput);
        console.log(`Search setup complete for ${inputId}`);
    },

    // Вспомогательный метод debounce
    debounce(func, wait) {
        let timeout;
        return function executedFunction(...args) {
            const later = () => {
                clearTimeout(timeout);
                func(...args);
            };
            clearTimeout(timeout);
            timeout = setTimeout(later, wait);
        };
    },

    // Настройка одного поля поиска
    setupSearchInput(inputId, itemSelector, dataAttribute) {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) {
            console.log(`Search input ${inputId} not found`);
            return;
        }

        console.log(`Setting up search for ${inputId} with selector ${itemSelector}`);

        // Удаляем все существующие обработчики
        const originalInput = searchInput;
        const newInput = originalInput.cloneNode(false);

        // Копируем все атрибуты
        for (let attr of originalInput.attributes) {
            newInput.setAttribute(attr.name, attr.value);
        }

        // Копируем значение
        newInput.value = originalInput.value;

        // Обработчик ввода
        const handleInput = (e) => {
            const searchTerm = e.target.value.toLowerCase().trim();
            const items = document.querySelectorAll(itemSelector);

            let visibleCount = 0;

            items.forEach(item => {
                const itemName = item.getAttribute(dataAttribute);
                const isMatch = itemName && itemName.toLowerCase().includes(searchTerm);

                if (isMatch) {
                    item.style.display = 'block';
                    visibleCount++;
                } else {
                    item.style.display = 'none';
                }
            });

            console.log(`Search "${searchTerm}" in ${inputId}: ${visibleCount} visible items of ${items.length}`);

            // ОСОБЫЙ СЛУЧАЙ: для ключевых слов обновляем пагинацию
            if (inputId === 'keyword-search' || inputId === 'search-keyword-search') {
                this.handleKeywordSearchUpdate(searchTerm, visibleCount);
            }

            // Сортируем после фильтрации с задержкой
            setTimeout(() => {
                this.triggerSortAfterSearch();
            }, 150);
        };

        // Добавляем обработчики
        newInput.addEventListener('input', handleInput);
        newInput.addEventListener('search', () => {
            if (newInput.value === '') {
                setTimeout(() => {
                    const items = document.querySelectorAll(itemSelector);
                    items.forEach(item => {
                        item.style.display = 'block';
                    });

                    // ОСОБЫЙ СЛУЧАЙ: для ключевых слов восстанавливаем пагинацию
                    if (inputId === 'keyword-search' || inputId === 'search-keyword-search') {
                        this.handleKeywordSearchClear();
                    }

                    // Сортируем после очистки
                    setTimeout(() => {
                        this.triggerSortAfterSearch();
                    }, 100);
                }, 10);
            }
        });

        // Заменяем элемент
        originalInput.parentNode.replaceChild(newInput, originalInput);
        console.log(`Search setup complete for ${inputId}`);
    },

    // Обработка обновления поиска по ключевым словам
    handleKeywordSearchUpdate(searchTerm, visibleCount) {
        console.log(`Keyword search: "${searchTerm}", ${visibleCount} visible items`);

        if (window.KeywordsPagination) {
            if (typeof window.KeywordsPagination.updateAfterSearch === 'function') {
                window.KeywordsPagination.updateAfterSearch();
            }
        } else {
            console.log('KeywordsPagination module not available');
        }
    },

    // Обработка очистки поиска по ключевым словам
    handleKeywordSearchClear() {
        console.log('Keyword search cleared');

        if (window.KeywordsPagination) {
            if (typeof window.KeywordsPagination.forceUpdate === 'function') {
                window.KeywordsPagination.forceUpdate();
            }
        } else {
            console.log('KeywordsPagination module not available');
        }
    },

    // Триггер сортировки после поиска
    triggerSortAfterSearch() {
        console.log('Triggering sort after search...');

        if (window.FilterManager && window.FilterManager.sort) {
            window.FilterManager.sort.forceSortAllLists();
        } else {
            console.error('FilterManager or sort not found');
        }
    },

    // Быстрый поиск по всем фильтрам
    quickSearchAll(term) {
        const searchConfigs = [
            // Search Filters
            { inputId: 'platform-search', selector: '.search-platform-item', attr: 'data-platform-name' },
            { inputId: 'game-type-search', selector: '.search-game-type-item', attr: 'data-game-type-name' },
            { inputId: 'search-genre-search', selector: '.search-genre-item', attr: 'data-genre-name' },
            { inputId: 'search-keyword-search', selector: '.search-keyword-item', attr: 'data-keyword-name' },
            { inputId: 'search-theme-search', selector: '.search-theme-item', attr: 'data-theme-name' },
            { inputId: 'search-perspective-search', selector: '.search-perspective-item', attr: 'data-perspective-name' },
            { inputId: 'search-game-mode-search', selector: '.search-game-mode-item', attr: 'data-game-mode-name' },
            { inputId: 'search-engine-search', selector: '.search-engine-item', attr: 'data-engine-name' },

            // Similarity Filters
            { inputId: 'genre-search', selector: '.genre-item', attr: 'data-genre-name' },
            { inputId: 'keyword-search', selector: '.keyword-item', attr: 'data-keyword-name' },
            { inputId: 'theme-search', selector: '.theme-item', attr: 'data-theme-name' },
            { inputId: 'perspective-search', selector: '.perspective-item', attr: 'data-perspective-name' },
            { inputId: 'game-mode-search', selector: '.game-mode-item', attr: 'data-game-mode-name' },
            { inputId: 'engine-search', selector: '.engine-item', attr: 'data-engine-name' }
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