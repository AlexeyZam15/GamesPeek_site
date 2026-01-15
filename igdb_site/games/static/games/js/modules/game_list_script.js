// games/static/games/js/modules/game_list_script.js

const GameListScript = {
    // Инициализация
    init() {
        console.log('GameListScript initialized');

        this.form = document.getElementById('main-search-form');
        if (!this.form) {
            console.error('Main search form not found');
            return;
        }

        // Инициализация всех компонентов
        this.initializeSearchInputs();
        this.initializeShowAllToggles();
        this.setupActiveTagRemoval();
        this.setupAutoSubmit();
        this.setupCheckboxListeners();

        console.log('GameListScript initialization completed');
    },

    // Инициализация полей поиска
    initializeSearchInputs() {
        console.log('Initializing search inputs...');

        const searchConfigs = [
            { id: 'genre-search', selector: '.genre-item', attr: 'data-genre-name' },
            { id: 'keyword-search', selector: '.keyword-item', attr: 'data-keyword-name' },
            { id: 'platform-search', selector: '.platform-item', attr: 'data-platform-name' },
            { id: 'theme-search', selector: '.theme-item', attr: 'data-theme-name' },
            { id: 'perspective-search', selector: '.perspective-item', attr: 'data-perspective-name' },
            { id: 'developer-search', selector: '.developer-item', attr: 'data-developer-name' },
            { id: 'game-mode-search', selector: '.game-mode-item', attr: 'data-game-mode-name' },
            { id: 'game-type-search', selector: '.game-type-item', attr: 'data-game-type-name' }
        ];

        searchConfigs.forEach(({ id, selector, attr }) => {
            this.setupSearchInput(id, selector, attr);
        });
    },

    // Настройка одного поля поиска
    setupSearchInput(inputId, itemSelector, nameAttribute) {
        const searchInput = document.getElementById(inputId);
        if (!searchInput) return;

        searchInput.addEventListener('input', () => {
            const searchTerm = searchInput.value.toLowerCase().trim();
            const items = document.querySelectorAll(itemSelector);

            items.forEach(item => {
                const itemName = item.getAttribute(nameAttribute).toLowerCase();
                const isMatch = itemName.includes(searchTerm);
                item.style.display = isMatch ? 'block' : 'none';
            });
        });

        // Обработчик для очистки поля
        searchInput.addEventListener('search', () => {
            if (searchInput.value === '') {
                searchInput.dispatchEvent(new Event('input'));
            }
        });
    },

    // Инициализация переключателей "Show All"
    initializeShowAllToggles() {
        console.log('Initializing show all toggles...');

        const toggleConfigs = [
            {
                btnSelector: '.show-all-genres-btn',
                listSelector: '.genre-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-keywords-btn',
                listSelector: '.keyword-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-platforms-btn',
                listSelector: '.platform-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-themes-btn',
                listSelector: '.theme-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-perspectives-btn',
                listSelector: '.perspective-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-developers-btn',
                listSelector: '.developer-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-game-modes-btn',
                listSelector: '.game-mode-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            },
            {
                btnSelector: '.show-all-game-types-btn',
                listSelector: '.game-type-list',
                defaultHeight: '200px',
                expandedHeight: '400px'
            }
        ];

        toggleConfigs.forEach(({ btnSelector, listSelector, defaultHeight, expandedHeight }) => {
            this.setupShowAllToggle(btnSelector, listSelector, defaultHeight, expandedHeight);
        });
    },

    // Настройка одного переключателя "Show All"
    setupShowAllToggle(btnSelector, listSelector, defaultHeight = '200px', expandedHeight = '400px') {
        const showAllBtn = document.querySelector(btnSelector);
        const listElement = document.querySelector(listSelector);

        if (!showAllBtn || !listElement) return;

        let isExpanded = false;

        showAllBtn.addEventListener('click', () => {
            isExpanded = !isExpanded;

            if (isExpanded) {
                listElement.style.maxHeight = expandedHeight;
                const showText = showAllBtn.querySelector('.show-text');
                const hideText = showAllBtn.querySelector('.hide-text');
                if (showText) showText.style.display = 'none';
                if (hideText) hideText.style.display = 'inline';
            } else {
                listElement.style.maxHeight = defaultHeight;
                const showText = showAllBtn.querySelector('.show-text');
                const hideText = showAllBtn.querySelector('.hide-text');
                if (showText) showText.style.display = 'inline';
                if (hideText) hideText.style.display = 'none';
            }
        });
    },

    // Удаление активных тегов
    setupActiveTagRemoval() {
        console.log('Setting up active tag removal...');

        const tagConfigs = [
            { class: 'active-genre-tag', attr: 'data-genre-id', checkboxClass: '.genre-checkbox' },
            { class: 'active-keyword-tag', attr: 'data-keyword-id', checkboxClass: '.keyword-checkbox' },
            { class: 'active-platform-tag', attr: 'data-platform-id', checkboxClass: '.platform-checkbox' },
            { class: 'active-theme-tag', attr: 'data-theme-id', checkboxClass: '.theme-checkbox' },
            { class: 'active-perspective-tag', attr: 'data-perspective-id', checkboxClass: '.perspective-checkbox' },
            { class: 'active-developer-tag', attr: 'data-developer-id', checkboxClass: '.developer-checkbox' },
            { class: 'active-game-mode-tag', attr: 'data-game-mode-id', checkboxClass: '.game-mode-checkbox' },
            { class: 'active-game-type-tag', attr: 'data-game-type-id', checkboxClass: '.game-type-checkbox' }
        ];

        tagConfigs.forEach(({ class: tagClass, attr, checkboxClass }) => {
            this.setupTagRemovalForType(tagClass, attr, checkboxClass);
        });
    },

    // Настройка удаления тегов для одного типа
    setupTagRemovalForType(tagClass, attr, checkboxClass) {
        document.querySelectorAll(`.${tagClass}`).forEach(tag => {
            tag.addEventListener('click', (e) => {
                e.preventDefault();
                e.stopPropagation();

                const id = tag.getAttribute(attr);
                const checkbox = document.querySelector(`${checkboxClass}[value="${id}"]`);

                if (checkbox) {
                    checkbox.checked = false;
                    // Обновляем скрытые поля и отправляем форму
                    this.updateHiddenFields();

                    // Сохраняем позицию прокрутки перед отправкой
                    if (window.FilterManager && window.FilterManager.handlers) {
                        window.FilterManager.handlers.saveScrollPosition();
                    }

                    // Небольшая задержка для гарантии сохранения позиции
                    setTimeout(() => {
                        this.form.submit();
                    }, 50);
                }
            });
        });
    },

    // Обновление скрытых полей формы
    updateHiddenFields() {
        console.log('Updating hidden fields...');

        // Маппинг полей
        const fieldMapping = [
            { selector: '.genre-checkbox:checked', field: 'genres-field' },
            { selector: '.keyword-checkbox:checked', field: 'keywords-field' },
            { selector: '.platform-checkbox:checked', field: 'platforms-field' },
            { selector: '.theme-checkbox:checked', field: 'themes-field' },
            { selector: '.perspective-checkbox:checked', field: 'perspectives-field' },
            { selector: '.developer-checkbox:checked', field: 'developers-field' },
            { selector: '.game-mode-checkbox:checked', field: 'game-modes-field' },
            { selector: '.game-type-checkbox:checked', field: 'game-types-field' }
        ];

        // Обновляем каждое поле
        fieldMapping.forEach(({ selector, field }) => {
            const checkboxes = document.querySelectorAll(selector);
            const ids = Array.from(checkboxes).map(cb => cb.value);
            const fieldElement = document.getElementById(field);
            if (fieldElement) {
                fieldElement.value = ids.join(',');
            }
        });

        // Обновляем поле поиска похожих игр (только для критериев похожести)
        this.updateFindSimilarField();
    },

    // Обновление поля find_similar (только для критериев похожести)
    updateFindSimilarField() {
        const findSimilarField = document.getElementById('find_similar_field');
        if (!findSimilarField) return;

        // Критерии похожести (только Genres, Keywords, Themes, Perspectives, Game Modes)
        // ИСКЛЮЧАЕМ: Platforms, Developers, Game Types
        const similarityCriteria = [
            document.querySelectorAll('.genre-checkbox:checked').length,
            document.querySelectorAll('.keyword-checkbox:checked').length,
            document.querySelectorAll('.theme-checkbox:checked').length,
            document.querySelectorAll('.perspective-checkbox:checked').length,
            document.querySelectorAll('.game-mode-checkbox:checked').length
        ];

        const hasSimilarityCriteria = similarityCriteria.some(count => count > 0);
        findSimilarField.value = hasSimilarityCriteria ? '1' : '0';

        console.log(`Find similar field updated: ${hasSimilarityCriteria ? 'enabled' : 'disabled'}`);
    },

    // Настройка автоматической отправки
    setupAutoSubmit() {
        console.log('Setting up auto-submit...');

        // Для критериев похожести - включаем режим похожих игр при изменении
        const similarityCheckboxes = [
            '.genre-checkbox',
            '.keyword-checkbox',
            '.theme-checkbox',
            '.perspective-checkbox',
            '.game-mode-checkbox'
        ];

        similarityCheckboxes.forEach(selector => {
            document.querySelectorAll(selector).forEach(checkbox => {
                checkbox.addEventListener('change', () => {
                    // Автоматически включаем find_similar
                    document.getElementById('find_similar_field').value = '1';
                    this.updateHiddenFields(); // Только обновляем поля, не отправляем
                });
            });
        });

        // Для поисковых фильтров (Platforms, Developers, Game Types) - не включаем find_similar
        const searchFilterCheckboxes = [
            '.platform-checkbox',
            '.developer-checkbox',
            '.game-type-checkbox'
        ];

        searchFilterCheckboxes.forEach(selector => {
            document.querySelectorAll(selector).forEach(checkbox => {
                checkbox.addEventListener('change', () => {
                    this.updateHiddenFields(); // Только обновляем поля
                    // find_similar остается без изменений
                });
            });
        });

        // Сортировка - отправляем форму
        const sortSelect = document.querySelector('select[name="sort"]');
        if (sortSelect) {
            sortSelect.addEventListener('change', () => {
                // Сохраняем позицию прокрутки
                if (window.FilterManager && window.FilterManager.handlers) {
                    window.FilterManager.handlers.saveScrollPosition();
                }

                this.form.submit();
            });
        }
    },

    // Настройка слушателей чекбоксов (для мгновенного обновления UI)
    setupCheckboxListeners() {
        console.log('Setting up checkbox listeners...');

        const allCheckboxes = document.querySelectorAll(
            '.genre-checkbox, .keyword-checkbox, .platform-checkbox, ' +
            '.theme-checkbox, .perspective-checkbox, .developer-checkbox, ' +
            '.game-mode-checkbox, .game-type-checkbox'
        );

        allCheckboxes.forEach(checkbox => {
            checkbox.addEventListener('change', () => {
                // Обновляем скрытые поля при любом изменении чекбокса
                this.updateHiddenFields();

                // Обновляем сортировку в UI, если доступен FilterManager
                if (window.FilterManager && window.FilterManager.sort) {
                    setTimeout(() => {
                        window.FilterManager.sort.sortFilterLists();
                    }, 50);
                }
            });
        });
    },

    // Восстановление выбранных чекбоксов из URL
    restoreSelectedCheckboxes() {
        console.log('Restoring selected checkboxes from URL...');

        const urlParams = new URLSearchParams(window.location.search);

        // Маппинг параметров URL на селекторы чекбоксов
        const paramMapping = [
            { param: 'g', selector: '.genre-checkbox' },
            { param: 'k', selector: '.keyword-checkbox' },
            { param: 'p', selector: '.platform-checkbox' },
            { param: 't', selector: '.theme-checkbox' },
            { param: 'pp', selector: '.perspective-checkbox' },
            { param: 'd', selector: '.developer-checkbox' },
            { param: 'gm', selector: '.game-mode-checkbox' },
            { param: 'gt', selector: '.game-type-checkbox' }
        ];

        paramMapping.forEach(({ param, selector }) => {
            const paramValue = urlParams.get(param);
            if (paramValue) {
                const ids = paramValue.split(',').map(id => id.trim());
                ids.forEach(id => {
                    const checkbox = document.querySelector(`${selector}[value="${id}"]`);
                    if (checkbox) {
                        checkbox.checked = true;
                    }
                });
            }
        });

        // Восстанавливаем find_similar
        const findSimilar = urlParams.get('find_similar');
        const findSimilarField = document.getElementById('find_similar_field');
        if (findSimilarField) {
            findSimilarField.value = findSimilar === '1' ? '1' : '0';
        }

        // Обновляем скрытые поля после восстановления
        this.updateHiddenFields();
    }
};

// Автоматическая инициализация при загрузке DOM
document.addEventListener('DOMContentLoaded', () => {
    // Инициализируем с задержкой для гарантии полной загрузки DOM
    setTimeout(() => {
        GameListScript.init();
        // Восстанавливаем выбранные чекбоксы из URL
        GameListScript.restoreSelectedCheckboxes();

        // Связываем с FilterManager если он существует
        if (window.FilterManager) {
            window.FilterManager.gameList = GameListScript;
        }
    }, 100);
});

export default GameListScript;