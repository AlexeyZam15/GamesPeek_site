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

    // Основная функция сортировки
    sortFilterLists() {
        console.log('Sorting filter lists...');

        this.filterTypes.forEach(filterType => {
            this.sortSingleFilterList(filterType);
        });
    },

    // Сортировка одного списка фильтров
    sortSingleFilterList(filterType) {
        const container = document.querySelector(filterType.container);
        if (!container) {
            console.log(`Container ${filterType.container} not found`);
            return;
        }

        // Получаем только видимые элементы (без display: none)
        const allItems = Array.from(container.querySelectorAll(`.${filterType.itemClass}`));

        // Отфильтровываем скрытые элементы
        const visibleItems = allItems.filter(item => {
            const style = window.getComputedStyle(item);
            return style.display !== 'none' &&
                   style.visibility !== 'hidden' &&
                   item.offsetParent !== null;
        });

        if (visibleItems.length === 0) return;

        // Разделяем на выбранные и невыбранные
        const selectedItems = [];
        const unselectedItems = [];

        visibleItems.forEach(item => {
            const checkbox = item.querySelector(`.${filterType.checkboxClass}`);
            if (checkbox && checkbox.checked) {
                selectedItems.push(item);
            } else {
                unselectedItems.push(item);
            }
        });

        console.log(`Sorting ${filterType.container}: Selected: ${selectedItems.length}, Unselected: ${unselectedItems.length}`);

        // Сортируем выбранные и невыбранные по позиции в DOM
        const sortedSelected = this.sortByDOMPosition(selectedItems);
        const sortedUnselected = this.sortByDOMPosition(unselectedItems);

        // Объединяем
        const sortedItems = [...sortedSelected, ...sortedUnselected];

        // Обновляем DOM только если порядок изменился
        if (this.needToUpdateOrder(visibleItems, sortedItems)) {
            this.updateContainerOrder(container, sortedItems);
        }
    },

    // Сортировка по позиции в DOM (более эффективно)
    sortByDOMPosition(items) {
        return items.sort((a, b) => {
            // Сравниваем позицию в DOM
            const position = a.compareDocumentPosition(b);

            if (position & Node.DOCUMENT_POSITION_FOLLOWING) {
                // a перед b
                return -1;
            } else if (position & Node.DOCUMENT_POSITION_PRECEDING) {
                // a после b
                return 1;
            }
            return 0;
        });
    },

    // Получение уникального ID элемента
    getItemId(item) {
        const checkbox = item.querySelector('input[type="checkbox"]');
        return checkbox ? checkbox.value : null;
    },

    // Проверка необходимости обновления порядка
    needToUpdateOrder(currentItems, newItems) {
        if (currentItems.length !== newItems.length) return true;

        for (let i = 0; i < currentItems.length; i++) {
            if (currentItems[i] !== newItems[i]) {
                return true;
            }
        }
        return false;
    },

    // Обновление порядка элементов в контейнере
    updateContainerOrder(container, sortedItems) {
        // Временно отключаем анимации и скрываем для избежания мерцания
        const originalDisplay = container.style.display;
        container.style.display = 'none';
        container.style.transition = 'none';

        // Используем DocumentFragment для минимальной перерисовки
        const fragment = document.createDocumentFragment();

        sortedItems.forEach(item => {
            if (item && item.nodeType === Node.ELEMENT_NODE) {
                fragment.appendChild(item);
            }
        });

        // Заменяем содержимое
        container.innerHTML = '';
        container.appendChild(fragment);

        // Восстанавливаем отображение и анимации
        setTimeout(() => {
            container.style.display = originalDisplay;
            container.style.transition = '';
        }, 10);
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

        // Сортируем по позиции в DOM
        const sortedSelected = this.sortByDOMPosition(selected);
        const sortedUnselected = this.sortByDOMPosition(unselected);

        // Объединяем и обновляем если нужно
        const sorted = [...sortedSelected, ...sortedUnselected];
        if (this.needToUpdateOrder(items, sorted)) {
            this.updateContainerOrder(container, sorted);
        }
    }
};

export default FilterSort;