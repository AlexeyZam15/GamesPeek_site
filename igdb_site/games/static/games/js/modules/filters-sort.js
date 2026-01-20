// games/static/games/js/modules/filters-sort.js

const FilterSort = {
    // Список всех типов фильтров для обработки
    filterTypes: [
        // Search Filters (не влияют на find_similar)
        { container: '.platform-grid', itemClass: 'platform-item', checkboxClass: 'platform-checkbox' },
        { container: '.game-type-grid', itemClass: 'game-type-item', checkboxClass: 'game-type-checkbox' },

        // Similarity Filters (влияют на find_similar)
        { container: '.genre-grid', itemClass: 'genre-item', checkboxClass: 'genre-checkbox' },
        { container: '.keyword-grid', itemClass: 'keyword-item', checkboxClass: 'keyword-checkbox' },
        { container: '.theme-grid', itemClass: 'theme-item', checkboxClass: 'theme-checkbox' },
        { container: '.perspective-grid', itemClass: 'perspective-item', checkboxClass: 'perspective-checkbox' },
        { container: '.game-mode-grid', itemClass: 'game-mode-item', checkboxClass: 'game-mode-checkbox' }
    ],

    // Флаг для предотвращения множественных сортировок
    isSorting: false,

    // Основная функция сортировки
    sortFilterLists() {
        if (this.isSorting) return;

        this.isSorting = true;
        console.log('Sorting filter lists...');

        try {
            this.filterTypes.forEach(filterType => {
                this.sortSingleFilterList(filterType);
            });
        } catch (error) {
            console.error('Error sorting filter lists:', error);
        } finally {
            this.isSorting = false;
        }
    },

    // Сортировка одного списка фильтров
    sortSingleFilterList(filterType) {
        const container = document.querySelector(filterType.container);
        if (!container) {
            console.log(`Container ${filterType.container} not found`);
            return;
        }

        // Получаем ВСЕ элементы, включая скрытые через поиск
        const allItems = Array.from(container.querySelectorAll(`.${filterType.itemClass}`));
        if (allItems.length === 0) return;

        // Разделяем на выбранные и невыбранные
        const selectedItems = [];
        const unselectedItems = [];

        allItems.forEach(item => {
            const checkbox = item.querySelector(`.${filterType.checkboxClass}`);
            if (checkbox && checkbox.checked) {
                selectedItems.push(item);
            } else {
                unselectedItems.push(item);
            }
        });

        console.log(`Sorting ${filterType.container}: Selected: ${selectedItems.length}, Unselected: ${unselectedItems.length}`);

        // Сохраняем текущий порядок элементов перед сортировкой
        const currentOrder = allItems.map(item => this.getItemId(item));

        // Создаем новый порядок: выбранные сначала, потом невыбранные
        const newOrder = [
            ...selectedItems.map(item => this.getItemId(item)),
            ...unselectedItems.map(item => this.getItemId(item))
        ];

        // Проверяем, нужно ли менять порядок
        if (this.needToUpdateOrder(currentOrder, newOrder)) {
            this.updateContainerOrder(container, [...selectedItems, ...unselectedItems]);
        }
    },

    // Получение ID элемента (значение чекбокса)
    getItemId(item) {
        const checkbox = item.querySelector('input[type="checkbox"]');
        return checkbox ? checkbox.value : 'unknown';
    },

    // Проверка необходимости обновления порядка (по ID элементов)
    needToUpdateOrder(currentOrder, newOrder) {
        if (currentOrder.length !== newOrder.length) {
            console.log('Different lengths, need update');
            return true;
        }

        for (let i = 0; i < currentOrder.length; i++) {
            if (currentOrder[i] !== newOrder[i]) {
                console.log(`Order changed at position ${i}: ${currentOrder[i]} -> ${newOrder[i]}`);
                return true;
            }
        }

        return false;
    },

    // Обновление порядка элементов в контейнере (оптимизированная версия)
    updateContainerOrder(container, sortedItems) {
        try {
            // Используем DocumentFragment для минимальной перерисовки
            const fragment = document.createDocumentFragment();

            sortedItems.forEach(item => {
                if (item && item.nodeType === Node.ELEMENT_NODE) {
                    fragment.appendChild(item);
                }
            });

            // Быстрая замена содержимого
            container.innerHTML = '';
            container.appendChild(fragment);

            console.log(`Updated order for ${container.className}, ${sortedItems.length} items`);
        } catch (error) {
            console.error('Error updating container order:', error);
        }
    },

    // Быстрая сортировка для одного типа (оптимизированная версия)
    quickSortFilterList(containerSelector, itemSelector, checkboxSelector) {
        const container = document.querySelector(containerSelector);
        if (!container) return;

        const items = Array.from(container.querySelectorAll(itemSelector));
        if (items.length === 0) return;

        const selected = [];
        const unselected = [];

        items.forEach(item => {
            const checkbox = item.querySelector(checkboxSelector);
            if (checkbox && checkbox.checked) {
                selected.push(item);
            } else {
                unselected.push(item);
            }
        });

        // Обновляем если есть выбранные элементы
        if (selected.length > 0) {
            this.updateContainerOrder(container, [...selected, ...unselected]);
        }
    },

    // Сортировка конкретного типа фильтра по названию
    sortSpecificFilter(filterName) {
        console.log(`Sorting specific filter: ${filterName}`);

        const filterMap = {
            'genres': { container: '.genre-grid', itemClass: 'genre-item', checkboxClass: 'genre-checkbox' },
            'keywords': { container: '.keyword-grid', itemClass: 'keyword-item', checkboxClass: 'keyword-checkbox' },
            'platforms': { container: '.platform-grid', itemClass: 'platform-item', checkboxClass: 'platform-checkbox' },
            'themes': { container: '.theme-grid', itemClass: 'theme-item', checkboxClass: 'theme-checkbox' },
            'perspectives': { container: '.perspective-grid', itemClass: 'perspective-item', checkboxClass: 'perspective-checkbox' },
            'game_modes': { container: '.game-mode-grid', itemClass: 'game-mode-item', checkboxClass: 'game-mode-checkbox' },
            'game_types': { container: '.game-type-grid', itemClass: 'game-type-item', checkboxClass: 'game-type-checkbox' }
        };

        const filterType = filterMap[filterName];
        if (filterType) {
            this.sortSingleFilterList(filterType);
        }
    },

    // Принудительная сортировка всех списков (после поиска и т.д.)
    forceSortAllLists() {
        console.log('Force sorting all lists...');
        this.isSorting = false; // Сбрасываем флаг
        this.sortFilterLists();
    }
};

export default FilterSort;