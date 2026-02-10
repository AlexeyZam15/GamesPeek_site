// games/static/games/js/game_list/filters-search.js

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

            let hasVisibleItems = false;
            let visibleCount = 0;

            items.forEach(item => {
                const itemName = item.getAttribute(dataAttribute);
                const isMatch = itemName && itemName.includes(searchTerm);

                if (isMatch) {
                    item.style.display = 'block';
                    hasVisibleItems = true;
                    visibleCount++;
                } else {
                    item.style.display = 'none';
                }
            });

            console.log(`Search "${searchTerm}": ${visibleCount} visible items`);

            // ОСОБЫЙ СЛУЧАЙ: для ключевых слов обновляем пагинацию
            if (inputId === 'keyword-search') {
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
                    if (inputId === 'keyword-search') {
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
    },

    // Обработка обновления поиска по ключевым словам
    handleKeywordSearchUpdate(searchTerm, visibleCount) {
        console.log(`Keyword search: "${searchTerm}", ${visibleCount} visible items`);

        if (window.KeywordsPagination) {
            if (typeof window.KeywordsPagination.updateAfterSearch === 'function') {
                window.KeywordsPagination.updateAfterSearch();
            }

            // Если есть видимые элементы, обновляем пагинацию
            if (visibleCount > 0) {
                console.log(`Updating keywords pagination for ${visibleCount} visible items`);
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